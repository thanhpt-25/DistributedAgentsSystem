import { describe, expect, it } from 'vitest'
import type { Agent } from '../api/types'
import { coverageLevel, filterAgents, workerCoverage } from './agents'

const agent = (over: Partial<Agent>): Agent => ({
  agent_id: 'a', role: 'worker', host: 'h', capabilities: [], status: 'Available',
  last_heartbeat: '2026-11-15T10:00:00Z', in_flight: 0, drained: false, ...over,
})

const fleet: Agent[] = [
  agent({ agent_id: 'w1', host: 'node-1', capabilities: ['research'] }),
  agent({ agent_id: 'w2', host: 'node-2', capabilities: ['research', 'review'], status: 'Draining' }),
  agent({ agent_id: 'w3', host: 'node-3', capabilities: ['review'], status: 'Unavailable' }),
  agent({ agent_id: 'm1', role: 'manager', host: 'mgr-1' }),
]

describe('workerCoverage', () => {
  it('counts declared and available workers per capability, sorted', () => {
    expect(workerCoverage(fleet)).toEqual([
      { cap: 'research', up: 1, total: 2 },
      { cap: 'review', up: 0, total: 2 },
    ])
  })
  it('ignores non-workers and an empty registry', () => {
    expect(workerCoverage([agent({ role: 'manager', capabilities: ['x'] })])).toEqual([])
    expect(workerCoverage([])).toEqual([])
  })
})

describe('coverageLevel', () => {
  it('flags no worker, a single point of failure, and redundancy', () => {
    expect(coverageLevel({ cap: 'c', up: 0, total: 2 })).toBe('none')
    expect(coverageLevel({ cap: 'c', up: 1, total: 3 })).toBe('single')
    expect(coverageLevel({ cap: 'c', up: 2, total: 2 })).toBe('redundant')
  })
})

describe('filterAgents', () => {
  const none = { role: '', status: '', q: '' }
  it('returns everything without filters', () => {
    expect(filterAgents(fleet, none)).toHaveLength(4)
  })
  it('matches role and status exactly', () => {
    expect(filterAgents(fleet, { ...none, role: 'manager' }).map((a) => a.agent_id)).toEqual(['m1'])
    expect(filterAgents(fleet, { ...none, status: 'Draining' }).map((a) => a.agent_id)).toEqual(['w2'])
  })
  it('searches id, host and capabilities case-insensitively', () => {
    expect(filterAgents(fleet, { ...none, q: ' NODE-3 ' }).map((a) => a.agent_id)).toEqual(['w3'])
    expect(filterAgents(fleet, { ...none, q: 'Review' }).map((a) => a.agent_id)).toEqual(['w2', 'w3'])
  })
  it('survives agents whose identity is hidden from a tenant', () => {
    const hidden = agent({ agent_id: undefined, host: undefined, capabilities: ['research'] })
    expect(filterAgents([hidden], { ...none, q: 'research' })).toHaveLength(1)
    expect(filterAgents([hidden], { ...none, q: 'node' })).toHaveLength(0)
  })
})
