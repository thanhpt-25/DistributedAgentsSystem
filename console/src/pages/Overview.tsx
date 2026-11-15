// Overview: the screen an operator keeps open. Health, fleet, queue pressure, failures and spend.
import { CCol, CRow } from '@coreui/react'
import { useMemo, type ReactNode } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { useAgents, useDlqPending, useHealth, useRequestPage, useUsage } from '../api/hooks'
import type { Agent, DeadLetter, Health, Page, RequestListItem } from '../api/types'
import { useAuth } from '../auth/AuthContext'
import { countOf, num } from '../lib/format'
import { lastDays, mergeReports, type Merged } from '../lib/usage'
import BarChartCard from '../ui/BarChartCard'
import DataTable from '../ui/DataTable'
import { CopyId, ErrorAlert, PageHeader, RelTime, Section, StatCard, StatusBadge, type Tone } from '../ui/parts'

const RECENT = 10
const COUNT_LIMIT = 100
const BUSY_QUEUE = 50
const FAIL_WARN_PCT = 10

const Tile = ({ children }: { children: ReactNode }) => (
  <CCol sm={6} xl={3}>
    {children}
  </CCol>
)

const linkButton = (to: string, text: string) => (
  <Link className="btn btn-sm btn-outline-secondary" to={to}>
    {text}
  </Link>
)

interface FleetProps {
  health?: Health
  fleet: Agent[]
  pending?: Page<RequestListItem>
  running?: Page<RequestListItem>
  dlq?: Page<DeadLetter>
  showDlq: boolean
}

/** Gateway health, fleet state, queue pressure and (for admins) pending dead letters. */
function FleetTiles({ health, fleet, pending, running, dlq, showDlq }: FleetProps) {
  const count = (status: string) => fleet.filter((a) => a.status === status).length
  const inFlight = fleet.reduce((n, a) => n + a.in_flight, 0)
  const fleetTone: Tone = count('Unavailable') ? 'danger' : count('Draining') ? 'warning' : 'success'
  const queueTone: Tone = (pending?.items.length ?? 0) > BUSY_QUEUE ? 'warning' : 'info'
  return (
    <>
      <Tile><StatCard label="Gateway" value={health?.overall ?? '…'} sub={health?.redis ? `Redis ${health.redis}` : undefined} tone={health?.overall === 'healthy' ? 'success' : health ? 'danger' : undefined} /></Tile>
      <Tile><StatCard label="Agents available" value={`${count('Available')}/${fleet.length}`} sub={`${count('Draining')} draining · ${count('Unavailable')} down`} tone={fleetTone} /></Tile>
      <Tile><StatCard label="Jobs in flight" value={num(inFlight)} sub="per latest heartbeats" tone="info" /></Tile>
      <Tile><StatCard label="Active requests" value={`${countOf(pending)} + ${countOf(running)}`} sub="pending + running" tone={queueTone} /></Tile>
      {showDlq && dlq && (
        <Tile><StatCard label="Dead letters pending" value={countOf(dlq)} sub="need a replay or discard" tone={dlq.items.length ? 'danger' : 'success'} /></Tile>
      )}
    </>
  )
}

/** Seven-day failure rate, tokens and cost. */
function UsageTiles({ merged }: { merged: Merged }) {
  const finished = merged.completed + merged.failed + merged.cancelled
  const failPct = finished ? (merged.failed / finished) * 100 : null
  const failTone: Tone = failPct === null || failPct === 0 ? 'success' : failPct > FAIL_WARN_PCT ? 'danger' : 'warning'
  return (
    <>
      <Tile><StatCard label="Failure rate, 7d" value={failPct === null ? '–' : `${failPct.toFixed(1)}%`} sub={`${num(merged.failed)} of ${num(finished)} finished`} tone={failTone} /></Tile>
      <Tile><StatCard label="Tokens, 7d" value={num(merged.total)} sub={`${num(merged.input)} in · ${num(merged.output)} out`} /></Tile>
      <Tile><StatCard label="Est. cost, 7d" value={merged.costKnown ? merged.cost.toFixed(2) : '–'} sub={merged.costKnown ? merged.currency : 'no price table'} /></Tile>
    </>
  )
}

function RecentRequests({ rows, onOpen }: { rows: RequestListItem[]; onOpen: (id: string) => void }) {
  return (
    <Section title="Recent requests" actions={linkButton('/requests', 'All requests')} flush>
      <DataTable
        rows={rows}
        rowKey={(r) => r.request_id}
        onRowClick={(r) => onOpen(r.request_id)}
        empty="No requests yet."
        columns={[
          { header: 'Request', cell: (r) => <CopyId id={r.request_id} /> },
          { header: 'Status', cell: (r) => <StatusBadge status={r.status} /> },
          { header: 'Jobs', numeric: true, cell: (r) => num(r.job_count) },
          { header: 'Tokens', numeric: true, cell: (r) => num(r.token_usage.total) },
          { header: 'Created', cell: (r) => <RelTime iso={r.created_at} /> },
        ]}
      />
    </Section>
  )
}

function AgentsCard({ fleet }: { fleet: Agent[] }) {
  return (
    <Section title="Agents" actions={linkButton('/agents', 'Manage')} flush>
      <DataTable
        rows={fleet}
        rowKey={(a) => a.agent_id ?? `${a.role}-${a.host}-${a.capabilities.join()}`}
        empty="No agents registered."
        columns={[
          { header: 'Agent', cell: (a) => (a.agent_id ? <CopyId id={a.agent_id} label={a.agent_id} /> : a.role) },
          { header: 'Status', cell: (a) => <StatusBadge status={a.drained ? 'Drained' : a.status} tone={a.drained ? 'secondary' : undefined} /> },
          { header: 'In flight', numeric: true, cell: (a) => num(a.in_flight) },
          { header: 'Heartbeat', cell: (a) => <RelTime iso={a.last_heartbeat} /> },
        ]}
      />
    </Section>
  )
}

export default function Overview() {
  const { caps } = useAuth()
  const navigate = useNavigate()
  const health = useHealth()
  const agents = useAgents()
  const pending = useRequestPage('Pending', COUNT_LIMIT)
  const running = useRequestPage('Running', COUNT_LIMIT)
  const latest = useRequestPage(undefined, RECENT)
  const dlq = useDlqPending()
  const range = useMemo(() => lastDays(7), [])
  const usage = useUsage(range)
  const merged = useMemo(() => (usage.data ? mergeReports(usage.data) : null), [usage.data])
  const failure = [agents.error, pending.error, running.error, latest.error].find(Boolean)

  return (
    <>
      <PageHeader title="Overview" />
      {failure ? <ErrorAlert error={failure} /> : null}
      <CRow className="g-3 mb-3">
        <FleetTiles health={health.data} fleet={agents.data ?? []} pending={pending.data} running={running.data} dlq={dlq.data} showDlq={caps.admin} />
        {merged && <UsageTiles merged={merged} />}
      </CRow>
      <CRow className="g-3">
        <CCol lg={7}><RecentRequests rows={latest.data?.items ?? []} onOpen={(id) => navigate(`/requests/${encodeURIComponent(id)}`)} /></CCol>
        <CCol lg={5}><AgentsCard fleet={agents.data ?? []} /></CCol>
      </CRow>
      {merged && (
        <BarChartCard title="Requests submitted, last 7 days" datasetLabel="Submitted" labels={merged.days.map((d) => d.date.slice(5))} values={merged.days.map((d) => d.submitted)} />
      )}
    </>
  )
}
