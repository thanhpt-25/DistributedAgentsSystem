// Requests: filter (kept in the URL), page through, auto-refresh, submit a new one.
import { CButton, CFormInput, CFormSelect, CFormSwitch } from '@coreui/react'
import { useMemo, useState } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { useRequestList, type RequestFilters } from '../api/hooks'
import { REQUEST_STATUSES, type RequestListItem, type RequestStatus } from '../api/types'
import { duration, num } from '../lib/format'
import DataTable from '../ui/DataTable'
import {
  CenteredSpinner, CopyId, ErrorAlert, Field, FilterCard, ListFooter, PageHeader, RelTime, Section, StatusBadge, ToggleGroup,
} from '../ui/parts'
import SubmitModal from './SubmitModal'

const PAGE_SIZES = [20, 50, 100]
const DEFAULT_LIMIT = PAGE_SIZES[0]

function parseFilters(p: URLSearchParams): RequestFilters {
  const limit = Number(p.get('limit'))
  return {
    status: (p.get('status') ?? '').split(',').filter((s): s is RequestStatus => (REQUEST_STATUSES as readonly string[]).includes(s)),
    after: p.get('after') ?? '',
    before: p.get('before') ?? '',
    limit: PAGE_SIZES.includes(limit) ? limit : DEFAULT_LIMIT,
  }
}

function toParams(f: RequestFilters): URLSearchParams {
  const p = new URLSearchParams()
  if (f.status.length) p.set('status', f.status.join(','))
  if (f.after) p.set('after', f.after)
  if (f.before) p.set('before', f.before)
  if (f.limit !== DEFAULT_LIMIT) p.set('limit', String(f.limit))
  return p
}

interface FiltersProps {
  filters: RequestFilters
  update: (patch: Partial<RequestFilters>) => void
}

function FiltersCard({ filters, update }: FiltersProps) {
  const toggle = (s: RequestStatus) =>
    update({ status: filters.status.includes(s) ? filters.status.filter((x) => x !== s) : [...filters.status, s] })
  return (
    <FilterCard>
      <Field label="Status" cols={{ lg: 'auto' }}>
        <ToggleGroup label="Status filter" options={REQUEST_STATUSES} isOn={(s) => filters.status.includes(s)} onToggle={toggle} />
      </Field>
      <Field label="Created after" htmlFor="after" cols={{ sm: 6, lg: 3 }}>
        <CFormInput id="after" type="datetime-local" size="sm" value={filters.after} onChange={(e) => update({ after: e.target.value })} />
      </Field>
      <Field label="Created before" htmlFor="before" cols={{ sm: 6, lg: 3 }}>
        <CFormInput id="before" type="datetime-local" size="sm" value={filters.before} onChange={(e) => update({ before: e.target.value })} />
      </Field>
      <Field label="Page size" htmlFor="limit" cols={{ sm: 4, lg: 1 }}>
        <CFormSelect id="limit" size="sm" value={filters.limit} onChange={(e) => update({ limit: Number(e.target.value) })}>
          {PAGE_SIZES.map((n) => <option key={n} value={n}>{n}</option>)}
        </CFormSelect>
      </Field>
    </FilterCard>
  )
}

function RequestTable({ rows, onOpen }: { rows: RequestListItem[]; onOpen: (id: string) => void }) {
  return (
    <DataTable
      rows={rows}
      rowKey={(r) => r.request_id}
      onRowClick={(r) => onOpen(r.request_id)}
      empty="No requests match."
      columns={[
        { header: 'Request', cell: (r) => <CopyId id={r.request_id} /> },
        { header: 'Status', cell: (r) => <StatusBadge status={r.status} /> },
        { header: 'Created', cell: (r) => <RelTime iso={r.created_at} /> },
        { header: 'Duration', cell: (r) => (r.finished_at ? duration(r.created_at, r.finished_at) : <span className="text-body-secondary">in progress</span>) },
        { header: 'Jobs', numeric: true, cell: (r) => num(r.job_count) },
        { header: 'In', numeric: true, cell: (r) => num(r.token_usage.input) },
        { header: 'Out', numeric: true, cell: (r) => num(r.token_usage.output) },
      ]}
    />
  )
}

export default function Requests() {
  const navigate = useNavigate()
  const [params, setParams] = useSearchParams()
  const filters = useMemo(() => parseFilters(params), [params])
  const [auto, setAuto] = useState(true)
  const [submitOpen, setSubmitOpen] = useState(false)
  const query = useRequestList(filters, auto)
  const rows = query.data?.pages.flatMap((p) => p.items) ?? []
  const open = (id: string) => navigate(`/requests/${encodeURIComponent(id)}`)
  const update = (patch: Partial<RequestFilters>) => setParams(toParams({ ...filters, ...patch }), { replace: true })

  return (
    <>
      <PageHeader
        title="Requests"
        actions={
          <>
            <CFormSwitch label="Auto-refresh" checked={auto} onChange={(e) => setAuto(e.target.checked)} className="me-2" />
            <CButton color="primary" onClick={() => setSubmitOpen(true)}>New request</CButton>
          </>
        }
      />
      <FiltersCard filters={filters} update={update} />
      {query.isError && <ErrorAlert error={query.error} />}
      <Section title="Requests" flush>
        {query.isPending ? <CenteredSpinner /> : <RequestTable rows={rows} onOpen={open} />}
      </Section>
      <ListFooter shown={rows.length} hasMore={query.hasNextPage} loading={query.isFetchingNextPage} onMore={() => void query.fetchNextPage()} />
      {submitOpen && <SubmitModal onClose={() => setSubmitOpen(false)} onDone={open} />}
    </>
  )
}
