// One Request: state, Jobs, transitions, usage, budget, webhook, final result, and a live Event_Stream tail.
import { CButton, CCol, CListGroup, CListGroupItem, CProgress, CRow } from '@coreui/react'
import { useEffect, useRef } from 'react'
import { Link, useParams } from 'react-router-dom'
import { useCancelRequest, useEventFeed, useRequest } from '../api/hooks'
import type { Budget, RequestView, Transition } from '../api/types'
import { duration, num, shortId } from '../lib/format'
import DataTable from '../ui/DataTable'
import JobResultsCard from './JobResults'
import { CenteredSpinner, CopyId, ErrorAlert, KeyValue, PageHeader, RelTime, Section, StatusBadge } from '../ui/parts'
import { useConfirm } from '../ui/Providers'

const TERMINAL = new Set(['Completed', 'Failed', 'Cancelled'])
const HOT_PCT = 90
const LONG_EVENT = 400

const DeadLetterLink = ({ id }: { id?: string | null }) =>
  id ? <Link className="mono ms-1" to={`/dlq/${id}`}>{shortId(id)}</Link> : null

/** Highest attempt number any transition recorded for this Job. */
const attemptsOf = (transitions: Transition[], jobId: string): number =>
  Math.max(0, ...transitions.map((t) => (t.scope !== 'request' && t.job_id === jobId ? t.attempt : 0)))

function TransitionLine({ t }: { t: Transition }) {
  if (t.scope === 'request') {
    return (
      <span>
        request <StatusBadge status={t.from_status} /> → <StatusBadge status={t.to_status} /> <DeadLetterLink id={t.dead_letter_id} />
      </span>
    )
  }
  return (
    <span>
      <span className="mono">{t.job_id}</span> {t.from_state ? <StatusBadge status={t.from_state} /> : '∅'} → <StatusBadge status={t.to_state} />
      {t.attempt ? <span className="text-body-secondary"> · attempt {t.attempt}</span> : null}
      {t.worker_id ? <span className="text-body-secondary"> · {t.worker_id}</span> : null}
      <DeadLetterLink id={t.dead_letter_id} />
    </span>
  )
}

function BudgetMeter({ label, used, max }: { label: string; used: number; max: number }) {
  const pct = Math.min(100, (used / max) * 100)
  return (
    <div className="mb-3">
      <div className="small text-body-secondary mb-1">{label}</div>
      <CProgress value={pct} color={pct >= HOT_PCT ? 'danger' : 'success'} />
    </div>
  )
}

function BudgetCard({ budget }: { budget: Budget }) {
  const { limits, used } = budget
  return (
    <Section title="Budget">
      <div className="mb-3"><StatusBadge status={budget.exceeded ? 'exceeded' : 'within limits'} tone={budget.exceeded ? 'danger' : 'success'} /></div>
      {limits.max_total_tokens ? <BudgetMeter label={`Tokens ${num(used.total_tokens)} / ${num(limits.max_total_tokens)}`} used={used.total_tokens} max={limits.max_total_tokens} /> : null}
      {limits.max_cost ? <BudgetMeter label={`Cost ${used.cost ?? '–'} / ${limits.max_cost}`} used={Number(used.cost ?? 0)} max={Number(limits.max_cost)} /> : null}
    </Section>
  )
}

function SummaryCard({ view, done }: { view: RequestView; done: boolean }) {
  const first = view.transitions[0]?.at
  const last = view.transitions.at(-1)?.at
  return (
    <Section title="Summary">
      <KeyValue
        rows={[
          ['Request ID', <CopyId key="id" id={view.request_id} label={view.request_id} />],
          ['Status', <StatusBadge key="s" status={view.status} />],
          ['Priority', view.priority],
          ['Replans', num(view.replans)],
          ['Jobs', num(view.jobs.length)],
          ['Elapsed', first ? duration(first, done && last ? last : new Date().toISOString()) : '–'],
        ]}
      />
    </Section>
  )
}

function JobsCard({ view }: { view: RequestView }) {
  return (
    <Section title="Jobs" flush>
      <DataTable
        rows={view.jobs}
        rowKey={(j) => j.job_id}
        empty="Not planned yet."
        columns={[
          { header: 'Job', cell: (j) => <span className="mono">{j.job_id}</span> },
          { header: 'State', cell: (j) => <StatusBadge status={j.state} /> },
          { header: 'Attempts', numeric: true, cell: (j) => num(attemptsOf(view.transitions, j.job_id)) },
        ]}
      />
    </Section>
  )
}

function TransitionsCard({ transitions }: { transitions: Transition[] }) {
  return (
    <Section title="Transitions" flush>
      {transitions.length ? (
        <CListGroup flush>
          {transitions.map((t, i) => (
            <CListGroupItem key={`${t.at}-${i}`} className="d-flex gap-3 small">
              <span className="text-body-secondary text-nowrap" style={{ minWidth: '5rem' }}><RelTime iso={t.at} /></span>
              <TransitionLine t={t} />
            </CListGroupItem>
          ))}
        </CListGroup>
      ) : (
        <div className="p-3 text-body-secondary">No transitions yet.</div>
      )}
    </Section>
  )
}

function UsageCard({ view }: { view: RequestView }) {
  const cost = view.estimated_cost
  const costText = cost ? `${cost.amount} ${cost.currency}${cost.complete ? '' : ' (incomplete)'}` : 'No price table: cost unknown.'
  return (
    <Section title="Token usage & cost">
      <KeyValue
        rows={[
          ...Object.entries(view.token_usage).map(([k, v]): [string, string] => [k, typeof v === 'object' ? JSON.stringify(v) : String(v)]),
          ['Estimated cost', costText],
        ]}
      />
    </Section>
  )
}

function LiveEventsCard({ feed }: { feed: ReturnType<typeof useEventFeed> }) {
  const end = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const el = end.current
    if (el) el.scrollTop = el.scrollHeight
  }, [feed.events.length])
  return (
    <Section title="Live events" actions={<StatusBadge status={feed.state} />} flush>
      <div ref={end} className="feed p-3" role="log" aria-label="Live events">
        {feed.events.length === 0 && <span className="text-body-secondary">Waiting for events…</span>}
        {feed.events.map((e) => (
          <div key={e.key}><b>{e.event}</b> {e.text.length > LONG_EVENT ? `${e.text.slice(0, LONG_EVENT)}…` : e.text}</div>
        ))}
      </div>
    </Section>
  )
}

function Outcome({ view }: { view: RequestView }) {
  const fr = view.final_result
  return (
    <>
      {view.error && <ErrorAlert error={new Error(`Error: ${view.error}`)} />}
      {fr && (
        <Section title="Final result" flush>
          <div className="p-3 d-grid gap-3">
            {fr.summary ? <pre className="block">{fr.summary}</pre> : <span className="text-body-secondary">No summary.</span>}
            {fr.jobs.length > 0 && (
              <DataTable
                rows={fr.jobs}
                rowKey={(j) => j.job_id}
                columns={[
                  { header: 'Job', cell: (j) => <span className="mono">{j.job_id}</span> },
                  { header: 'State', cell: (j) => <StatusBadge status={j.state} /> },
                  { header: 'Worker', cell: (j) => j.worker_id ?? '–' },
                  { header: 'In', numeric: true, cell: (j) => num(j.input_tokens) },
                  { header: 'Out', numeric: true, cell: (j) => num(j.output_tokens) },
                ]}
              />
            )}
            {fr.failures.length > 0 && (
              <details>
                <summary>{fr.failures.length} failure(s)</summary>
                <pre className="block mt-2">{JSON.stringify(fr.failures, null, 2)}</pre>
              </details>
            )}
          </div>
        </Section>
      )}
      {view.replay_history.length > 0 && (
        <Section title="Replay history" flush>
          <DataTable
            rows={view.replay_history}
            rowKey={(e) => `${e.dead_letter_id}-${e.at}`}
            columns={[
              { header: 'When', cell: (e) => <RelTime iso={e.at} /> },
              { header: 'Dead letter', cell: (e) => <DeadLetterLink id={e.dead_letter_id} /> },
              { header: 'Error', cell: (e) => e.error ?? '–' },
            ]}
          />
        </Section>
      )}
    </>
  )
}

function RequestError({ id, error }: { id: string; error: unknown }) {
  const notFound = (error as { status?: number }).status === 404
  const shown = notFound ? new Error(`Request ${id} was not found. It may belong to another tenant or have been purged.`) : error
  return (
    <>
      <PageHeader title={`Request ${shortId(id)}`} />
      <ErrorAlert error={shown} />
    </>
  )
}

export default function RequestDetail() {
  const { id = '' } = useParams()
  const confirm = useConfirm()
  const request = useRequest(id)
  const cancel = useCancelRequest(id)
  const view = request.data
  const feed = useEventFeed(id, view?.status)
  const done = view ? TERMINAL.has(view.status) : false

  const onCancel = async () => {
    const ok = await confirm({ title: 'Cancel this request?', body: 'Pending and running Jobs are stopped. This cannot be undone.', confirmLabel: 'Cancel request', danger: true })
    if (ok) cancel.mutate()
  }

  if (request.isError) return <RequestError id={id} error={request.error} />
  if (!view) return <CenteredSpinner />
  return (
    <>
      <PageHeader
        title={`Request ${shortId(id)}`}
        actions={
          <>
            <CButton color="secondary" variant="outline" onClick={() => void request.refetch()}>Refresh</CButton>
            <CButton color="danger" variant="outline" disabled={done || cancel.isPending} onClick={onCancel}>Cancel request</CButton>
          </>
        }
      />
      <SummaryCard view={view} done={done} />
      <Outcome view={view} />
      <JobResultsCard jobs={view.jobs} />
      <CRow className="g-3">
        <CCol lg={6}>
          <JobsCard view={view} />
          <TransitionsCard transitions={view.transitions} />
        </CCol>
        <CCol lg={6}>
          <UsageCard view={view} />
          {view.budget && <BudgetCard budget={view.budget} />}
          {view.webhook && (
            <Section title="Webhook">
              <KeyValue rows={[['State', view.webhook.state], ['Attempts', num(view.webhook.attempts)], ['Last status', view.webhook.last_status ?? '–']]} />
            </Section>
          )}
          <LiveEventsCard feed={feed} />
        </CCol>
      </CRow>
    </>
  )
}
