import { describe, expect, it } from 'vitest'
import { ago, countOf, duration, num, shortId } from './format'

describe('ago', () => {
  const now = Date.parse('2026-11-15T12:00:00Z')
  it('counts seconds, minutes, hours and days', () => {
    expect(ago('2026-11-15T11:59:30Z', now)).toBe('30s ago')
    expect(ago('2026-11-15T11:55:00Z', now)).toBe('5m ago')
    expect(ago('2026-11-15T09:00:00Z', now)).toBe('3h ago')
    expect(ago('2026-11-12T12:00:00Z', now)).toBe('3d ago')
  })
  it('says just now for a clock-skewed future time and a dash for garbage', () => {
    expect(ago('2026-11-15T12:00:05Z', now)).toBe('just now')
    expect(ago('not a date', now)).toBe('–')
  })
})

describe('duration', () => {
  it('picks a readable unit', () => {
    expect(duration('2026-11-15T12:00:00.000Z', '2026-11-15T12:00:00.250Z')).toBe('250ms')
    expect(duration('2026-11-15T12:00:00Z', '2026-11-15T12:00:07.5Z')).toBe('7.5s')
    expect(duration('2026-11-15T12:00:00Z', '2026-11-15T12:02:05Z')).toBe('2m 5s')
  })
  it('never goes negative and survives bad input', () => {
    expect(duration('2026-11-15T12:00:05Z', '2026-11-15T12:00:00Z')).toBe('0ms')
    expect(duration('x', 'y')).toBe('–')
  })
})

describe('shortId / num / countOf', () => {
  it('shortens only long ids', () => {
    expect(shortId('worker-1')).toBe('worker-1')
    expect(shortId('00000001-aaaa-4bbb-8ccc-000000000001')).toBe('00000001…0001')
    expect(shortId(null)).toBe('–')
  })
  it('formats numbers with a dash for missing values', () => {
    expect(num(null)).toBe('–')
    expect(num(0)).toBe('0')
  })
  it('marks a cut-off page with a plus', () => {
    expect(countOf({ items: [1, 2, 3] })).toBe('3')
    expect(countOf({ items: [1, 2, 3], next_cursor: 'c' })).toBe('3+')
    expect(countOf(undefined)).toBe('–')
  })
})
