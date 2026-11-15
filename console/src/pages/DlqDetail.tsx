import { CButton, CCol, CRow } from '@coreui/react'
import { Link, useParams } from 'react-router-dom'
import { useDlqItem } from '../api/hooks'
import type { DeadLetterDetail } from '../api/types'
import { shortId } from '../lib/format'
import { CenteredSpinner, ErrorAlert, KeyValue, PageHeader, RelTime, Section } from '../ui/parts'
import { DlqActions } from './Dlq'

function decodeBase64(b64: string): string | null {
  try {
    return atob(b64)
  } catch {
    return null
  }
}

function RecordCard({ r }: { r: DeadLetterDetail }) {
  return (
    <Section title="Record">
      <KeyValue
        rows={[
          ['Dead letter ID', <span key="i" className="mono">{r.dead_letter_id}</span>],
          ['Reason', r.reason],
          ['Status', r.status],
          ['Request', r.request_id ? <Link key="r" className="mono" to={`/requests/${encodeURIComponent(r.request_id)}`}>{r.request_id}</Link> : '–'],
          ['Job', r.job_id ?? '–'],
          ['Attempt', r.attempt ?? '–'],
          ['Capabilities', r.capabilities?.join(', ') ?? '–'],
          ['Last error', r.last_error ?? '–'],
          ['Dead-lettered', <RelTime key="d" iso={r.dead_lettered_at} />],
          ['Status changed', <RelTime key="c" iso={r.status_changed_at} />],
        ]}
      />
    </Section>
  )
}

function StateCard({ r }: { r: DeadLetterDetail }) {
  return (
    <Section title="Current state">
      <KeyValue
        rows={[
          ['Request exists', r.request_exists === null ? '–' : String(r.request_exists)],
          ['Request status', r.request_status ?? '–'],
          ['Job state', r.job_state ?? '–'],
          ['Job attempt', r.job_attempt ?? '–'],
        ]}
      />
    </Section>
  )
}

function Payload({ r }: { r: DeadLetterDetail }) {
  const decoded = r.raw_b64 ? decodeBase64(r.raw_b64) : null
  return (
    <>
      {r.message !== undefined && (
        <Section title="Original message"><pre className="block">{JSON.stringify(r.message, null, 2)}</pre></Section>
      )}
      {r.raw_b64 && (
        <Section title={`Raw bytes${r.truncated ? ' (truncated)' : ''}`}>
          <pre className="block">{r.raw_b64}</pre>
          {decoded !== null && (
            <details className="mt-3">
              <summary>Decoded as text</summary>
              <pre className="block mt-2">{decoded}</pre>
            </details>
          )}
        </Section>
      )}
    </>
  )
}

export default function DlqDetail() {
  const { id = '' } = useParams()
  const query = useDlqItem(id)
  const r = query.data
  const title = `Dead letter ${shortId(id)}`

  if (query.isError) {
    const notFound = (query.error as { status?: number }).status === 404
    return (
      <>
        <PageHeader title={title} />
        <ErrorAlert error={notFound ? new Error('Dead letter not found, or not visible to your key.') : query.error} />
      </>
    )
  }
  if (!r) return <CenteredSpinner />
  return (
    <>
      <PageHeader
        title={title}
        actions={
          <>
            <CButton color="secondary" variant="outline" onClick={() => void query.refetch()}>Refresh</CButton>
            <DlqActions record={r} />
          </>
        }
      />
      <CRow className="g-3">
        <CCol lg={6}><RecordCard r={r} /></CCol>
        <CCol lg={6}><StateCard r={r} /></CCol>
      </CRow>
      <Payload r={r} />
    </>
  )
}
