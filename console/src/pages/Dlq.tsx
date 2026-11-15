// Dead letters: Jobs that exhausted retries or were malformed. Inspect, replay or discard (admin scope).
import { CButton, CFormInput, CFormSelect } from '@coreui/react'
import { useMemo } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'
import { useDlqAction, useDlqList, type DlqFilters } from '../api/hooks'
import { DLQ_REASONS, DLQ_STATUSES, type DeadLetter, type DlqStatus } from '../api/types'
import { num, shortId } from '../lib/format'
import DataTable from '../ui/DataTable'
import { CenteredSpinner, CopyId, ErrorAlert, Field, FilterCard, ListFooter, PageHeader, RelTime, Section, StatusBadge, ToggleGroup } from '../ui/parts'
import { useConfirm } from '../ui/Providers'

/** Replay and discard for one record, each behind a confirm dialog. Shows nothing once the record is no longer Pending. */
export function DlqActions({ record }: { record: DeadLetter }) {
  const confirm = useConfirm()
  const replay = useDlqAction('replay')
  const discard = useDlqAction('discard')
  if (record.status !== 'Pending') return null
  const canReplay = record.reason !== 'malformed' && record.request_id !== null

  const onReplay = async () => {
    const ok = await confirm({
      title: 'Replay this job?',
      body: `Job ${record.job_id ?? '?'} of request ${shortId(record.request_id)} is re-dispatched with a new attempt. A Failed request becomes Pending again.`,
      confirmLabel: 'Replay',
    })
    if (ok) replay.mutate(record.dead_letter_id)
  }
  const onDiscard = async () => {
    const ok = await confirm({ title: 'Discard this record?', body: 'The record is marked Discarded and the request stays as it is.', confirmLabel: 'Discard', danger: true })
    if (ok) discard.mutate(record.dead_letter_id)
  }
  return (
    <span className="d-inline-flex gap-2" onClick={(e) => e.stopPropagation()}>
      {canReplay && <CButton size="sm" color="secondary" variant="outline" disabled={replay.isPending} onClick={onReplay}>Replay</CButton>}
      <CButton size="sm" color="danger" variant="outline" disabled={discard.isPending} onClick={onDiscard}>Discard</CButton>
    </span>
  )
}

function parseFilters(p: URLSearchParams): DlqFilters {
  const status = p.get('status') as DlqStatus | null
  return {
    status: status && DLQ_STATUSES.includes(status) ? status : 'Pending',
    reason: p.get('reason') ?? '',
    request_id: p.get('request_id') ?? '',
    capability: p.get('capability') ?? '',
  }
}

interface FiltersProps {
  filters: DlqFilters
  update: (patch: Partial<DlqFilters>) => void
}

function FiltersCard({ filters, update }: FiltersProps) {
  return (
    <FilterCard>
      <Field label="Status" cols={{ lg: 'auto' }}>
        <ToggleGroup label="Status filter" options={DLQ_STATUSES} isOn={(s) => filters.status === s} onToggle={(s) => update({ status: s })} />
      </Field>
      <Field label="Reason" htmlFor="reason" cols={{ sm: 4, lg: 2 }}>
        <CFormSelect id="reason" size="sm" value={filters.reason} onChange={(e) => update({ reason: e.target.value })}>
          <option value="">any reason</option>
          {DLQ_REASONS.map((r) => <option key={r} value={r}>{r}</option>)}
        </CFormSelect>
      </Field>
      <Field label="Request ID" htmlFor="rid" cols={{ sm: 4, lg: 3 }}>
        <CFormInput id="rid" size="sm" value={filters.request_id} onChange={(e) => update({ request_id: e.target.value.trim() })} />
      </Field>
      <Field label="Capability" htmlFor="cap" cols={{ sm: 4, lg: 2 }}>
        <CFormInput id="cap" size="sm" value={filters.capability} onChange={(e) => update({ capability: e.target.value.trim() })} />
      </Field>
    </FilterCard>
  )
}

function DlqTable({ rows, onOpen }: { rows: DeadLetter[]; onOpen: (id: string) => void }) {
  return (
    <DataTable
      rows={rows}
      rowKey={(r) => r.dead_letter_id}
      onRowClick={(r) => onOpen(r.dead_letter_id)}
      empty="No dead letters match. That is good news."
      columns={[
        { header: 'Dead letter', cell: (r) => <CopyId id={r.dead_letter_id} /> },
        { header: 'Reason', cell: (r) => <StatusBadge status={r.reason} tone={r.reason === 'malformed' ? 'danger' : 'warning'} /> },
        { header: 'Request', cell: (r) => (r.request_id ? <Link className="mono" to={`/requests/${encodeURIComponent(r.request_id)}`} onClick={(e) => e.stopPropagation()}>{shortId(r.request_id)}</Link> : '–') },
        { header: 'Job', cell: (r) => <span className="mono">{r.job_id ?? '–'}</span> },
        { header: 'Attempt', numeric: true, cell: (r) => num(r.attempt) },
        { header: 'Last error', cell: (r) => <span title={r.last_error ?? ''}>{(r.last_error ?? '–').slice(0, 70)}</span> },
        { header: 'When', cell: (r) => <RelTime iso={r.dead_lettered_at} /> },
        { header: 'Status', cell: (r) => <StatusBadge status={r.status} /> },
        { header: '', cell: (r) => <DlqActions record={r} /> },
      ]}
    />
  )
}

export default function Dlq() {
  const navigate = useNavigate()
  const [params, setParams] = useSearchParams()
  const filters = useMemo(() => parseFilters(params), [params])
  const query = useDlqList(filters)
  const rows = query.data?.pages.flatMap((p) => p.items) ?? []
  const update = (patch: Partial<DlqFilters>) =>
    setParams(new URLSearchParams(Object.entries({ ...filters, ...patch }).filter(([, v]) => v)), { replace: true })
  const notReady = (query.error as { body?: { error?: string } } | null)?.body?.error === 'index_not_ready'

  return (
    <>
      <PageHeader title="Dead letters" />
      <FiltersCard filters={filters} update={update} />
      {query.isError && <ErrorAlert error={notReady ? new Error('The tenant index is still being built. Try again shortly.') : query.error} />}
      <Section title="Records" flush>
        {query.isPending ? <CenteredSpinner /> : <DlqTable rows={rows} onOpen={(id) => navigate(`/dlq/${id}`)} />}
      </Section>
      <ListFooter shown={rows.length} hasMore={query.hasNextPage} loading={query.isFetchingNextPage} onMore={() => void query.fetchNextPage()} />
    </>
  )
}
