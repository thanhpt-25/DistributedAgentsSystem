import { describe, expect, it } from 'vitest'
import { buildSubmitBody } from './submit'

const empty = { prompt: 'hello', priority: '', tokens: '', cost: '' }

describe('buildSubmitBody', () => {
  it('sends only the prompt when nothing else is filled in', () => {
    expect(buildSubmitBody(empty)).toEqual({ prompt: 'hello' })
  })
  it('adds a priority when one is chosen', () => {
    expect(buildSubmitBody({ ...empty, priority: 'high' })).toEqual({ prompt: 'hello', priority: 'high' })
  })
  it('sends the token limit as a number, because the Gateway rejects a string', () => {
    expect(buildSubmitBody({ ...empty, tokens: '5000' }).budget).toEqual({ max_total_tokens: 5000 })
  })
  it('keeps the cost limit as text so no precision is lost', () => {
    expect(buildSubmitBody({ ...empty, cost: '0.10' }).budget).toEqual({ max_cost: '0.10' })
  })
  it('combines both limits and never returns an empty budget object', () => {
    expect(buildSubmitBody({ ...empty, tokens: '1', cost: '2' }).budget).toEqual({ max_total_tokens: 1, max_cost: '2' })
    expect('budget' in buildSubmitBody(empty)).toBe(false)
  })
})
