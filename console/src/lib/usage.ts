// Pure helpers over Usage_Reports: fold tenants together, group by model, export CSV.
import type { UsageModel, UsageReport } from '../api/types'

export interface MergedDay {
  date: string
  submitted: number
  tokens: number
  models: UsageModel[]
}

export interface Merged {
  submitted: number
  completed: number
  failed: number
  cancelled: number
  input: number
  output: number
  total: number
  cost: number
  currency: string | null
  costKnown: boolean
  costComplete: boolean
  days: MergedDay[]
}

export interface ModelRow {
  provider: string
  model: string
  input: number
  output: number
  total: number
  cost: number | null
}

export const isoDay = (d: Date): string => d.toISOString().slice(0, 10)

export function lastDays(n: number, today: Date = new Date()): { from: string; to: string } {
  return { from: isoDay(new Date(today.getTime() - (n - 1) * 86_400_000)), to: isoDay(today) }
}

function costOf(m: UsageModel): number | null {
  const c = m.estimated_cost
  if (c === null || c === undefined) return null
  const value = typeof c === 'object' ? Number(c.amount) : Number(c)
  return Number.isFinite(value) ? value : null
}

export function mergeReports(reports: UsageReport[]): Merged {
  const days = new Map<string, MergedDay>()
  const t = { submitted: 0, completed: 0, failed: 0, cancelled: 0, input: 0, output: 0, total: 0, cost: 0 }
  let currency: string | null = null
  let costKnown = false
  let costComplete = true
  for (const r of reports) {
    for (const d of r.days) {
      const e = days.get(d.date) ?? { date: d.date, submitted: 0, tokens: 0, models: [] }
      e.submitted += d.requests.submitted
      e.tokens += d.models.reduce((a, m) => a + m.total_tokens, 0)
      e.models.push(...d.models)
      days.set(d.date, e)
    }
    const { requests, input_tokens, output_tokens, total_tokens, estimated_cost } = r.totals
    t.submitted += requests.submitted
    t.completed += requests.finished.Completed ?? 0
    t.failed += requests.finished.Failed ?? 0
    t.cancelled += requests.finished.Cancelled ?? 0
    t.input += input_tokens
    t.output += output_tokens
    t.total += total_tokens
    if (estimated_cost) {
      costKnown = true
      currency = estimated_cost.currency
      t.cost += Number(estimated_cost.amount)
      costComplete &&= estimated_cost.complete
    }
  }
  return { ...t, currency, costKnown, costComplete, days: [...days.values()].sort((a, b) => a.date.localeCompare(b.date)) }
}

export function modelRows(days: MergedDay[]): ModelRow[] {
  const by = new Map<string, ModelRow>()
  for (const d of days) {
    for (const m of d.models) {
      const key = `${m.provider}/${m.model}`
      const row = by.get(key) ?? { provider: m.provider, model: m.model, input: 0, output: 0, total: 0, cost: null }
      row.input += m.input_tokens
      row.output += m.output_tokens
      row.total += m.total_tokens
      const c = costOf(m)
      if (c !== null) row.cost = (row.cost ?? 0) + c
      by.set(key, row)
    }
  }
  return [...by.values()].sort((a, b) => b.total - a.total)
}

export function usageCsv(reports: UsageReport[]): string {
  const rows: (string | number)[][] = [
    ['tenant', 'date', 'submitted', 'completed', 'failed', 'cancelled', 'input_tokens', 'output_tokens', 'total_tokens'],
  ]
  for (const r of reports) {
    for (const d of r.days) {
      const sum = (k: 'input_tokens' | 'output_tokens' | 'total_tokens') => d.models.reduce((a, m) => a + m[k], 0)
      const f = d.requests.finished
      rows.push([r.tenant, d.date, d.requests.submitted, f.Completed ?? 0, f.Failed ?? 0, f.Cancelled ?? 0, sum('input_tokens'), sum('output_tokens'), sum('total_tokens')])
    }
  }
  return rows.map((row) => row.map((c) => `"${String(c).replaceAll('"', '""')}"`).join(',')).join('\n')
}
