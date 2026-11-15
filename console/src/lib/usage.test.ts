import { describe, expect, it } from 'vitest'
import type { UsageReport } from '../api/types'
import { lastDays, mergeReports, modelRows, usageCsv } from './usage'

const model = (total: number, cost: unknown = null) => ({
  provider: 'ollama', model: 'gemma3', input_tokens: total * 0.75, output_tokens: total * 0.25, total_tokens: total,
  estimated_cost: cost as never,
})

const report = (tenant: string, submitted: number, failed: number, cost: string | null): UsageReport => ({
  tenant, from: '2026-11-14', to: '2026-11-15',
  days: [
    { date: '2026-11-14', requests: { submitted, finished: { Completed: 2, Failed: failed } }, models: [model(100, cost ? { amount: cost } : null)] },
    { date: '2026-11-15', requests: { submitted: 1, finished: { Completed: 1 } }, models: [model(50)] },
  ],
  totals: {
    requests: { submitted: submitted + 1, finished: { Completed: 3, Failed: failed } },
    input_tokens: 112, output_tokens: 38, total_tokens: 150,
    estimated_cost: cost ? { currency: 'USD', amount: cost, complete: false } : null,
  },
})

describe('mergeReports', () => {
  it('adds tenants together day by day and in the totals', () => {
    const m = mergeReports([report('a', 4, 1, '0.50'), report('b', 6, 0, '0.25')])
    expect(m.submitted).toBe(12)
    expect(m.completed).toBe(6)
    expect(m.failed).toBe(1)
    expect(m.total).toBe(300)
    expect(m.cost).toBeCloseTo(0.75)
    expect(m.days.map((d) => [d.date, d.submitted, d.tokens])).toEqual([['2026-11-14', 10, 200], ['2026-11-15', 2, 100]])
  })
  it('reports cost as unknown without a price table, and incomplete when any tenant is', () => {
    expect(mergeReports([report('a', 1, 0, null)]).costKnown).toBe(false)
    const m = mergeReports([report('a', 1, 0, '1.00')])
    expect(m.costKnown).toBe(true)
    expect(m.costComplete).toBe(false)
    expect(m.currency).toBe('USD')
  })
  it('handles no reports', () => {
    expect(mergeReports([])).toMatchObject({ submitted: 0, days: [] })
  })
})

describe('modelRows', () => {
  it('groups by provider and model, biggest first, summing known costs only', () => {
    const rows = modelRows(mergeReports([report('a', 4, 0, '0.50')]).days)
    expect(rows).toEqual([{ provider: 'ollama', model: 'gemma3', input: 112.5, output: 37.5, total: 150, cost: 0.5 }])
  })
  it('leaves cost null when no model was priced', () => {
    expect(modelRows(mergeReports([report('a', 4, 0, null)]).days)[0]?.cost).toBeNull()
  })
})

describe('usageCsv', () => {
  it('writes a header and one quoted row per tenant and day', () => {
    const lines = usageCsv([report('a', 4, 1, null)]).split('\n')
    expect(lines[0]).toBe('"tenant","date","submitted","completed","failed","cancelled","input_tokens","output_tokens","total_tokens"')
    expect(lines).toHaveLength(3)
    expect(lines[1]).toBe('"a","2026-11-14","4","2","1","0","75","25","100"')
  })
})

describe('lastDays', () => {
  it('is inclusive of today', () => {
    expect(lastDays(7, new Date('2026-11-15T10:00:00Z'))).toEqual({ from: '2026-11-09', to: '2026-11-15' })
    expect(lastDays(1, new Date('2026-11-15T10:00:00Z'))).toEqual({ from: '2026-11-15', to: '2026-11-15' })
  })
})
