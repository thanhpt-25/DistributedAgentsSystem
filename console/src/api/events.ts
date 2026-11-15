// One Request's Event_Stream (GET /v1/requests/{id}/events). SSE over fetch, because EventSource cannot send X-API-Key.
import { getKey } from './client'

export interface SseFrame {
  event: string
  id: string
  data: unknown
}

export type StreamState = 'connecting' | 'live' | 'reconnecting' | 'ended' | `error ${number}`

const TERMINAL_EVENT = /^request\.(completed|failed|cancelled)$/
const TERMINAL_STATUS = new Set(['Completed', 'Failed', 'Cancelled'])
const RETRY_BASE_MS = 2000
const RETRY_MAX_MS = 30_000

/** Wait before reconnect number `failures` (0 for the first): doubles from 2 s to a 30 s ceiling, so an outage or a
 *  429 on the stream limit is not hammered every 2 seconds. */
export const retryDelay = (failures: number): number => Math.min(RETRY_MAX_MS, RETRY_BASE_MS * 2 ** failures)

/**
 * Whether this frame says the Request is finished: its terminal event, or the opening `snapshot` of a Request that
 * already ended (the Gateway then sends what is stored and closes the stream, with no terminal event of its own).
 */
export function isTerminalFrame(frame: SseFrame): boolean {
  if (TERMINAL_EVENT.test(frame.event)) return true
  if (frame.event !== 'snapshot' || typeof frame.data !== 'object' || frame.data === null) return false
  return TERMINAL_STATUS.has(String((frame.data as { status?: unknown }).status))
}

/** Whole frames in `buffer`, and the incomplete tail to keep for the next chunk. */
export function splitFrames(buffer: string): { frames: string[]; rest: string } {
  const parts = buffer.split(/\r?\n\r?\n/)
  const rest = parts.pop() ?? ''
  return { frames: parts, rest }
}

export function parseFrame(raw: string): SseFrame | null {
  let event = 'message'
  let id = ''
  let data = ''
  for (const line of raw.split(/\r?\n/)) {
    if (line.startsWith(':')) continue // keepalive comment
    const i = line.indexOf(':')
    const field = i === -1 ? line : line.slice(0, i)
    const value = i === -1 ? '' : line.slice(i + 1).replace(/^ /, '')
    if (field === 'event') event = value
    else if (field === 'id') id = value
    else if (field === 'data') data += (data ? '\n' : '') + value
  }
  if (!data) return null
  let parsed: unknown = data
  try {
    parsed = JSON.parse(data)
  } catch {
    /* show the raw text */
  }
  return { event, id, data: parsed }
}

async function readFrames(stream: ReadableStream<Uint8Array>, onFrame: (f: SseFrame) => boolean): Promise<boolean> {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  for (;;) {
    const { value, done } = await reader.read()
    if (done) return false
    const { frames, rest } = splitFrames(buffer + decoder.decode(value, { stream: true }))
    buffer = rest
    for (const raw of frames) {
      const frame = parseFrame(raw)
      if (frame && onFrame(frame)) {
        void reader.cancel().catch(() => undefined)
        return true
      }
    }
  }
}

const sleep = (ms: number, signal: AbortSignal) =>
  new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, ms)
    signal.addEventListener('abort', () => { clearTimeout(timer); resolve() }, { once: true })
  })

interface Follow {
  onEvent: (frame: SseFrame) => void
  onState?: (state: StreamState) => void
  signal: AbortSignal
}

/** Follow until a terminal event, a 4xx, or abort; reconnect with Last-Event-ID otherwise. */
export async function followEvents(requestId: string, { onEvent, onState, signal }: Follow): Promise<void> {
  let lastId = ''
  let failures = 0 // consecutive connections that delivered nothing
  const path = `/v1/requests/${encodeURIComponent(requestId)}/events`
  while (!signal.aborted) {
    try {
      onState?.('connecting')
      const res = await fetch(path, {
        signal,
        headers: { 'X-API-Key': getKey(), Accept: 'text/event-stream', ...(lastId ? { 'Last-Event-ID': lastId } : {}) },
      })
      if (!res.ok || !res.body) {
        onState?.(`error ${res.status}`)
        if (res.status < 500 && res.status !== 429) return
        throw new Error('retry')
      }
      onState?.('live')
      let sawTerminal = false
      await readFrames(res.body, (frame) => {
        if (frame.id) lastId = frame.id
        failures = 0 // the connection works: the next drop starts from the short delay again
        onEvent(frame)
        sawTerminal ||= isTerminalFrame(frame)
        return false // keep reading: the Gateway closes the stream itself, after a finished Request's stored events
      })
      if (sawTerminal) {
        onState?.('ended')
        return
      }
    } catch {
      if (signal.aborted) return
    }
    onState?.('reconnecting')
    await sleep(retryDelay(failures), signal)
    failures += 1
  }
}
