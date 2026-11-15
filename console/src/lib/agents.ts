// Pure helpers over the agent registry.
import type { Agent } from '../api/types'

export interface Coverage {
  cap: string
  up: number
  total: number
}

/** Per worker capability: how many workers declare it, and how many of those are Available. */
export function workerCoverage(agents: readonly Agent[]): Coverage[] {
  const by = new Map<string, Coverage>()
  for (const a of agents) {
    if (a.role !== 'worker') continue
    for (const cap of a.capabilities) {
      const prev = by.get(cap) ?? { cap, up: 0, total: 0 }
      by.set(cap, { cap, up: prev.up + (a.status === 'Available' ? 1 : 0), total: prev.total + 1 })
    }
  }
  return [...by.values()].sort((a, b) => a.cap.localeCompare(b.cap))
}

export type CoverageLevel = 'none' | 'single' | 'redundant'

export const coverageLevel = (c: Coverage): CoverageLevel => (c.up === 0 ? 'none' : c.up === 1 ? 'single' : 'redundant')

export interface AgentFilter {
  role: string
  status: string
  q: string
}

/** Role and status match exactly; the search text matches the id, host or any capability, ignoring case. */
export function filterAgents(agents: readonly Agent[], { role, status, q }: AgentFilter): Agent[] {
  const needle = q.trim().toLowerCase()
  return agents.filter(
    (a) =>
      (!role || a.role === role) &&
      (!status || a.status === status) &&
      (!needle || [a.agent_id, a.host, ...a.capabilities].some((s) => s?.toLowerCase().includes(needle))),
  )
}
