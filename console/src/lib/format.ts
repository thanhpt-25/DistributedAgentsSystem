export const num = (n: number | null | undefined): string => (n === null || n === undefined ? '–' : n.toLocaleString())

export function ago(iso: string, now: number = Date.now()): string {
  const t = Date.parse(iso)
  if (Number.isNaN(t)) return '–'
  const s = Math.round((now - t) / 1000)
  if (s < 0) return 'just now'
  if (s < 60) return `${s}s ago`
  if (s < 3600) return `${Math.floor(s / 60)}m ago`
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`
  return `${Math.floor(s / 86400)}d ago`
}

export function duration(fromIso: string, toIso: string): string {
  const a = Date.parse(fromIso)
  const b = Date.parse(toIso)
  if (Number.isNaN(a) || Number.isNaN(b)) return '–'
  const s = Math.max(0, (b - a) / 1000)
  if (s < 1) return `${Math.round(s * 1000)}ms`
  if (s < 60) return `${s.toFixed(1)}s`
  return `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`
}

export function shortId(id: string | null | undefined): string {
  if (!id) return '–'
  return id.length > 13 ? `${id.slice(0, 8)}…${id.slice(-4)}` : id
}

/** `count` plus a "+" when the page was cut off by the Gateway's limit. */
export const countOf = (page: { items: unknown[]; next_cursor?: string } | undefined): string =>
  page ? `${page.items.length}${page.next_cursor ? '+' : ''}` : '–'
