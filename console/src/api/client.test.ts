import { describe, expect, it } from 'vitest'
import { ApiError, buildQuery, errorMessage, isRejectedKey, signInFailureMessage } from './client'

describe('isRejectedKey', () => {
  it('is true only when the Gateway says the key is unusable', () => {
    expect(isRejectedKey(new ApiError(401, { error: 'unauthorized' }, null))).toBe(true)
    expect(isRejectedKey(new ApiError(403, { error: 'forbidden', scope: 'read' }, null))).toBe(true)
  })
  it('is false for outages, so a reload during a Gateway restart keeps the key', () => {
    expect(isRejectedKey(new ApiError(503, { error: 'service_unavailable' }, null))).toBe(false)
    expect(isRejectedKey(new ApiError(429, { error: 'rate_limited' }, '3'))).toBe(false)
    expect(isRejectedKey(new TypeError('Failed to fetch'))).toBe(false)
  })
})

describe('signInFailureMessage', () => {
  it('calls a rejected key a rejected key', () => {
    expect(signInFailureMessage(new ApiError(401, { error: 'unauthorized' }, null))).toBe('That key was not accepted.')
  })
  it('calls an outage an outage, whether the proxy answered 5xx or the network failed', () => {
    const outage = 'The Gateway is unreachable. Your key is kept: reload to try again.'
    expect(signInFailureMessage(new ApiError(502, { error: 'bad_response' }, null))).toBe(outage)
    expect(signInFailureMessage(new TypeError('Failed to fetch'))).toBe(outage)
  })
  it('passes other answers through, such as a rate limit', () => {
    expect(signInFailureMessage(new ApiError(429, { error: 'rate_limited' }, '3'))).toBe('rate_limited; retry in 3s')
  })
})

describe('ApiError.describe', () => {
  it('names the offending parameter, scope or state', () => {
    expect(new ApiError(400, { error: 'invalid_query', parameter: 'status' }, null).describe()).toBe('invalid_query (status)')
    expect(new ApiError(403, { error: 'forbidden', scope: 'admin' }, null).describe()).toBe('forbidden (needs admin)')
    expect(new ApiError(409, { error: 'record_not_pending', current_state: 'Replayed' }, null).describe()).toBe('record_not_pending (Replayed)')
  })
  it('adds the retry delay to a 429', () => {
    expect(new ApiError(429, { error: 'rate_limited' }, '3').describe()).toBe('rate_limited; retry in 3s')
  })
  it('falls back to the status code without a body', () => {
    expect(new ApiError(502, null, null).describe()).toBe('HTTP 502')
    expect(errorMessage(new Error('boom'))).toBe('boom')
  })
})

describe('buildQuery', () => {
  it('drops empty values and encodes the rest', () => {
    expect(buildQuery({ a: 'x y', b: undefined, c: '', d: null, e: 0 })).toBe('?a=x+y&e=0')
    expect(buildQuery()).toBe('')
  })
})
