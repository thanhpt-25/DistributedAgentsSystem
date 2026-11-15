// Gateway client. Same origin only: nginx (deploy/config/console.nginx.conf) and the Vite dev server proxy /v1 and
// /healthz, so the browser needs no CORS. The key lives in sessionStorage: it dies with the tab.
import type { Health } from './types'

const KEY = 'das.apiKey'
export const UNAUTHORIZED_EVENT = 'das:unauthorized'

export function getKey(): string {
  try {
    return sessionStorage.getItem(KEY) ?? ''
  } catch {
    return ''
  }
}

export function setKey(key: string): void {
  try {
    if (key) sessionStorage.setItem(KEY, key)
    else sessionStorage.removeItem(KEY)
  } catch {
    /* storage blocked: the key then lives only until the page reloads */
  }
}

export interface ErrorBody {
  error?: string
  parameter?: string
  scope?: string
  status?: string
  current_state?: string
}

export class ApiError extends Error {
  readonly status: number
  readonly body: ErrorBody | null
  readonly retryAfter: string | null

  constructor(status: number, body: ErrorBody | null, retryAfter: string | null) {
    super(body?.error ?? `HTTP ${status}`)
    this.name = 'ApiError'
    this.status = status
    this.body = body
    this.retryAfter = retryAfter
  }

  /** One line for a toast or an alert. */
  describe(): string {
    const b = this.body
    const detail = b?.parameter
      ? ` (${b.parameter})`
      : b?.scope
        ? ` (needs ${b.scope})`
        : b?.current_state
          ? ` (${b.current_state})`
          : b?.status
            ? ` (${b.status})`
            : ''
    const wait = this.status === 429 && this.retryAfter ? `; retry in ${this.retryAfter}s` : ''
    return `${this.message}${detail}${wait}`
  }
}

/** The Gateway said this key is not usable (401 unknown key, 403 no read scope). A network error or a 5xx says nothing about it. */
export const isRejectedKey = (e: unknown): boolean => e instanceof ApiError && (e.status === 401 || e.status === 403)

const UNREACHABLE = 'The Gateway is unreachable. Your key is kept: reload to try again.'

/** What the sign-in screen says when the probe fails. A 5xx or a network error is an outage, not a verdict on the key. */
export function signInFailureMessage(e: unknown): string {
  if (!(e instanceof ApiError)) return UNREACHABLE
  if (e.status === 401) return 'That key was not accepted.'
  return e.status >= 500 ? UNREACHABLE : e.describe()
}

export const errorMessage = (e: unknown): string =>
  e instanceof ApiError ? e.describe() : e instanceof Error ? e.message : String(e)

export type Query = Record<string, string | number | undefined | null>

export function buildQuery(query?: Query): string {
  const params = new URLSearchParams()
  for (const [k, v] of Object.entries(query ?? {})) {
    if (v !== undefined && v !== null && v !== '') params.set(k, String(v))
  }
  const s = params.toString()
  return s ? `?${s}` : ''
}

interface Options {
  method?: 'GET' | 'POST'
  query?: Query
  body?: unknown
  headers?: Record<string, string>
  signal?: AbortSignal
}

export async function api<T>(path: string, { method = 'GET', query, body, headers, signal }: Options = {}): Promise<T> {
  const res = await fetch(path + buildQuery(query), {
    method,
    signal,
    headers: {
      'X-API-Key': getKey(),
      ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}),
      ...headers,
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })
  const text = await res.text()
  let data: unknown = null
  if (text) {
    try {
      data = JSON.parse(text)
    } catch {
      data = { error: 'bad_response' }
    }
  }
  if (res.status === 401) window.dispatchEvent(new Event(UNAUTHORIZED_EVENT)) // the auth provider signs out
  if (!res.ok) throw new ApiError(res.status, data as ErrorBody | null, res.headers.get('Retry-After'))
  return data as T
}

/** /healthz is public and answers 503 with a body when unhealthy: both are a report, not an error. */
export async function fetchHealth(signal?: AbortSignal): Promise<Health> {
  try {
    const res = await fetch('/healthz', { signal })
    return { ...(await res.json()), http: res.status } as Health
  } catch (e) {
    if (e instanceof DOMException && e.name === 'AbortError') throw e
    return { overall: 'unreachable', http: 0 }
  }
}
