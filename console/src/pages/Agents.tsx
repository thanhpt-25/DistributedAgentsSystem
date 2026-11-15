// Agents: the registry as the Gateway sees it, worker capability coverage, and Operator drain/undrain.
import { CButton, CCol, CFormCheck, CFormInput, CFormSelect, CModal, CModalBody, CModalFooter, CModalHeader, CModalTitle, CRow, CSpinner } from '@coreui/react'
import { useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useAgentControl, useAgents } from '../api/hooks'
import type { Agent, AgentStatus } from '../api/types'
import { useAuth } from '../auth/AuthContext'
import { num } from '../lib/format'
import { coverageLevel, filterAgents, workerCoverage, type AgentFilter } from '../lib/agents'
import DataTable from '../ui/DataTable'
import { CopyId, ErrorAlert, Field, FilterCard, PageHeader, RelTime, Section, StatCard, StatusBadge } from '../ui/parts'

const STATUSES: AgentStatus[] = ['Available', 'Draining', 'Unavailable']

type Command = 'drain' | 'undrain'
type NamedAgent = Agent & { agent_id: string }

interface Control {
  agent: NamedAgent
  command: Command
}

function ControlModal({ control, onClose }: { control: Control; onClose: () => void }) {
  const mutation = useAgentControl()
  const [release, setRelease] = useState(false)
  const drain = control.command === 'drain'
  const { agent } = control
  return (
    <CModal visible alignment="center" onClose={onClose}>
      <CModalHeader><CModalTitle>{drain ? 'Drain agent?' : 'Undrain agent?'}</CModalTitle></CModalHeader>
      <CModalBody className="d-grid gap-3">
        <p className="mb-0">
          {drain ? `${agent.role} ${agent.agent_id} stops taking new work and finishes what it holds.` : `${agent.role} ${agent.agent_id} takes new work again.`}
        </p>
        {drain && <CFormCheck id="release" label="Also release the leases it holds" checked={release} onChange={(e) => setRelease(e.target.checked)} />}
      </CModalBody>
      <CModalFooter>
        <CButton color="secondary" variant="outline" onClick={onClose}>Cancel</CButton>
        <CButton
          color={drain ? 'danger' : 'primary'}
          disabled={mutation.isPending}
          onClick={() => mutation.mutate({ agentId: agent.agent_id, command: control.command, releaseLeases: release }, { onSettled: onClose })}
        >
          {mutation.isPending ? <CSpinner size="sm" /> : drain ? 'Drain' : 'Undrain'}
        </CButton>
      </CModalFooter>
    </CModal>
  )
}

function AgentStats({ all, roles }: { all: Agent[]; roles: string[] }) {
  const count = (s: string) => all.filter((a) => a.status === s).length
  const inRole = (r: string) => all.filter((a) => a.role === r)
  return (
    <CRow className="g-3 mb-3">
      {roles.map((r) => (
        <CCol key={r} sm={6} xl>
          <StatCard label={`${r}s`} value={num(inRole(r).length)} sub={`${inRole(r).filter((a) => a.status === 'Available').length} available`} tone="info" />
        </CCol>
      ))}
      <CCol sm={6} xl><StatCard label="Draining" value={num(count('Draining'))} sub="finishing in-flight work" tone={count('Draining') ? 'warning' : undefined} /></CCol>
      <CCol sm={6} xl><StatCard label="Unavailable" value={num(count('Unavailable'))} sub="missed heartbeats" tone={count('Unavailable') ? 'danger' : 'success'} /></CCol>
    </CRow>
  )
}

interface FiltersProps {
  roles: string[]
  filter: AgentFilter
  onChange: (key: keyof AgentFilter, value: string) => void
}

function AgentFilters({ roles, filter, onChange }: FiltersProps) {
  return (
    <FilterCard>
      <Field label="Role" htmlFor="role" cols={{ sm: 4, lg: 2 }}>
        <CFormSelect id="role" size="sm" value={filter.role} onChange={(e) => onChange('role', e.target.value)}>
          <option value="">all roles</option>
          {roles.map((r) => <option key={r} value={r}>{r}</option>)}
        </CFormSelect>
      </Field>
      <Field label="Status" htmlFor="status" cols={{ sm: 4, lg: 2 }}>
        <CFormSelect id="status" size="sm" value={filter.status} onChange={(e) => onChange('status', e.target.value)}>
          <option value="">any status</option>
          {STATUSES.map((s) => <option key={s} value={s}>{s}</option>)}
        </CFormSelect>
      </Field>
      <Field label="Search" htmlFor="q" cols={{ sm: 4, lg: 4 }}>
        <CFormInput id="q" size="sm" type="search" placeholder="agent, host or capability" value={filter.q} onChange={(e) => onChange('q', e.target.value)} />
      </Field>
    </FilterCard>
  )
}

/** Drain or undrain, for an Operator looking at an agent that can take the command. */
function AgentAction({ agent, onControl }: { agent: Agent; onControl: (c: Control) => void }) {
  if (!agent.agent_id || agent.role === 'gateway' || agent.status === 'Unavailable') return null
  const named = agent as NamedAgent
  return agent.status === 'Draining' ? (
    <CButton size="sm" color="secondary" variant="outline" onClick={() => onControl({ agent: named, command: 'undrain' })}>Undrain</CButton>
  ) : (
    <CButton size="sm" color="danger" variant="outline" onClick={() => onControl({ agent: named, command: 'drain' })}>Drain</CButton>
  )
}

interface RegistryProps {
  rows: Agent[]
  operator: boolean
  onControl: (c: Control) => void
}

function RegistryTable({ rows, operator, onControl }: RegistryProps) {
  return (
    <DataTable
      rows={rows}
      rowKey={(a) => a.agent_id ?? `${a.role}-${a.host}-${a.capabilities.join()}`}
      empty="No agents match."
      columns={[
        { header: 'Agent', cell: (a) => (a.agent_id ? <CopyId id={a.agent_id} label={a.agent_id} /> : <span className="text-body-secondary">hidden</span>) },
        { header: 'Role', cell: (a) => a.role },
        { header: 'Host', cell: (a) => <span className="mono">{a.host ?? '–'}</span> },
        { header: 'Status', cell: (a) => <><StatusBadge status={a.status} />{a.drained && <> <StatusBadge status="drained" tone="secondary" /></>}</> },
        { header: 'Capabilities', cell: (a) => (a.capabilities.length ? a.capabilities.join(', ') : '–') },
        { header: 'In flight', numeric: true, cell: (a) => num(a.in_flight) },
        { header: 'Heartbeat', cell: (a) => <RelTime iso={a.last_heartbeat} /> },
        ...(operator ? [{ header: '', cell: (a: Agent) => <AgentAction agent={a} onControl={onControl} /> }] : []),
      ]}
    />
  )
}

const COVERAGE_BADGE = {
  none: <StatusBadge status="no worker" tone="danger" />,
  single: <StatusBadge status="single point" tone="warning" />,
  redundant: <StatusBadge status="redundant" tone="success" />,
}

function CoverageTable({ agents }: { agents: Agent[] }) {
  return (
    <DataTable
      rows={workerCoverage(agents)}
      rowKey={(c) => c.cap}
      empty="No worker capabilities registered."
      columns={[
        { header: 'Capability', cell: (c) => <span className="mono">{c.cap}</span> },
        { header: 'Available workers', numeric: true, cell: (c) => `${c.up} / ${c.total}` },
        { header: 'Coverage', cell: (c) => COVERAGE_BADGE[coverageLevel(c)] },
      ]}
    />
  )
}

export default function Agents() {
  const { caps } = useAuth()
  const agents = useAgents()
  const [params, setParams] = useSearchParams()
  const [control, setControl] = useState<Control | null>(null)
  const filter: AgentFilter = { role: params.get('role') ?? '', status: params.get('status') ?? '', q: params.get('q') ?? '' }
  const all = useMemo(() => agents.data ?? [], [agents.data])
  const roles = useMemo(() => [...new Set(all.map((a) => a.role))].sort(), [all])

  const setFilter = (key: keyof AgentFilter, value: string) => {
    const next = new URLSearchParams(params)
    if (value) next.set(key, value)
    else next.delete(key)
    setParams(next, { replace: true })
  }

  return (
    <>
      <PageHeader title="Agents" />
      {agents.isError && <ErrorAlert error={agents.error} />}
      <AgentStats all={all} roles={roles} />
      <AgentFilters roles={roles} filter={filter} onChange={setFilter} />
      <Section title="Registry" flush>
        <RegistryTable rows={filterAgents(all, filter)} operator={caps.operator} onControl={setControl} />
      </Section>
      <Section title="Worker capability coverage" flush>
        <CoverageTable agents={all} />
      </Section>
      {control && <ControlModal control={control} onClose={() => setControl(null)} />}
    </>
  )
}
