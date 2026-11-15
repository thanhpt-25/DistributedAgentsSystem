import { describe, expect, it } from 'vitest'
import { isTerminalFrame, parseFrame, retryDelay, splitFrames } from './events'

describe('retryDelay', () => {
  it('doubles from two seconds', () => {
    expect([0, 1, 2, 3].map(retryDelay)).toEqual([2000, 4000, 8000, 16000])
  })
  it('stops growing at thirty seconds, however long the outage', () => {
    expect(retryDelay(4)).toBe(30_000)
    expect(retryDelay(50)).toBe(30_000)
  })
})

describe('isTerminalFrame', () => {
  const frame = (event: string, data: unknown) => ({ event, id: '1-0', data })
  it('recognises the terminal events', () => {
    expect(isTerminalFrame(frame('request.completed', { status: 'Completed' }))).toBe(true)
    expect(isTerminalFrame(frame('request.failed', {}))).toBe(true)
    expect(isTerminalFrame(frame('request.cancelled', {}))).toBe(true)
  })
  it('recognises the snapshot of a Request that already finished, which has no terminal event of its own', () => {
    expect(isTerminalFrame(frame('snapshot', { status: 'Completed' }))).toBe(true)
    expect(isTerminalFrame(frame('snapshot', { status: 'Failed' }))).toBe(true)
  })
  it('keeps following a Request that is still running', () => {
    expect(isTerminalFrame(frame('snapshot', { status: 'Running' }))).toBe(false)
    expect(isTerminalFrame(frame('job.dispatched', { job_id: 'j2' }))).toBe(false)
    expect(isTerminalFrame(frame('snapshot', 'not an object'))).toBe(false)
    expect(isTerminalFrame(frame('snapshot', null))).toBe(false)
  })
})

describe('splitFrames', () => {
  it('returns whole frames and keeps the unfinished tail', () => {
    const { frames, rest } = splitFrames('event: a\ndata: 1\n\nevent: b\ndata: 2\n\nevent: c\nda')
    expect(frames).toEqual(['event: a\ndata: 1', 'event: b\ndata: 2'])
    expect(rest).toBe('event: c\nda')
  })
  it('accepts CRLF line endings and a chunk split inside the blank line', () => {
    expect(splitFrames('event: a\r\ndata: 1\r\n\r\n').frames).toEqual(['event: a\r\ndata: 1'])
    expect(splitFrames('event: a\ndata: 1\n').frames).toEqual([])
  })
})

describe('parseFrame', () => {
  it('reads event, id and JSON data', () => {
    expect(parseFrame('event: job.dispatched\nid: 1731657662123-0\ndata: {"job_id":"j2"}')).toEqual({
      event: 'job.dispatched', id: '1731657662123-0', data: { job_id: 'j2' },
    })
  })
  it('defaults the event name and keeps non-JSON data as text', () => {
    expect(parseFrame('data: hello')).toEqual({ event: 'message', id: '', data: 'hello' })
  })
  it('joins multi-line data', () => {
    expect(parseFrame('data: a\ndata: b')?.data).toBe('a\nb')
  })
  it('ignores keepalive comments and frames without data', () => {
    expect(parseFrame(': keepalive')).toBeNull()
    expect(parseFrame('event: x')).toBeNull()
  })
})
