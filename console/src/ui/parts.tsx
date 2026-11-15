// Small shared pieces: status badges, stat tiles, copyable ids, page header, key/value lists.
import { CAlert, CBadge, CButton, CCard, CCardBody, CCardHeader, CCol, CFormLabel, CRow, CSpinner } from '@coreui/react'
import { Fragment, useEffect, type ReactNode } from 'react'
import { errorMessage } from '../api/client'
import { ago, shortId } from '../lib/format'
import { useToast } from './Providers'

export type Tone = 'success' | 'info' | 'warning' | 'danger' | 'secondary'

// Colour is semantic: the word on the badge is the real signal, the colour only speeds up scanning.
const TONE: Record<string, Tone> = {
  Completed: 'success', Succeeded: 'success', Available: 'success', Replayed: 'success', healthy: 'success', connected: 'success', live: 'success',
  Running: 'info', Dispatched: 'info', Ready: 'info',
  Draining: 'warning', Pending: 'warning', Waiting: 'warning', TimedOut: 'warning', connecting: 'warning', reconnecting: 'warning',
  Failed: 'danger', Unavailable: 'danger', unhealthy: 'danger', unreachable: 'danger', disconnected: 'danger', malformed: 'danger',
  Cancelled: 'secondary', Skipped: 'secondary', Discarded: 'secondary', ended: 'secondary',
}

export function StatusBadge({ status, tone }: { status: string; tone?: Tone }) {
  const color = tone ?? TONE[status] ?? 'secondary'
  return (
    <CBadge color={color} shape="rounded-pill" className={`px-2 py-1 fw-medium ${color === 'warning' ? 'text-dark' : ''}`}>
      {status}
    </CBadge>
  )
}

export function StatCard({ label, value, sub, tone }: { label: string; value: ReactNode; sub?: ReactNode; tone?: Tone }) {
  return (
    <CCard className={`stat-card h-100 ${tone ? `tone-${tone}` : ''}`}>
      <CCardBody>
        <div className="eyebrow mb-1">{label}</div>
        <div className="stat-value">{value}</div>
        {sub && <div className="small text-body-secondary mt-1">{sub}</div>}
      </CCardBody>
    </CCard>
  )
}

export function Section({ title, actions, flush, children }: { title: string; actions?: ReactNode; flush?: boolean; children: ReactNode }) {
  return (
    <CCard className="mb-3">
      <CCardHeader className="d-flex align-items-center justify-content-between gap-2">
        <h2>{title}</h2>
        {actions}
      </CCardHeader>
      {flush ? children : <CCardBody>{children}</CCardBody>}
    </CCard>
  )
}

export function PageHeader({ title, actions }: { title: string; actions?: ReactNode }) {
  useEffect(() => {
    document.title = `${title} · DAS Console`
  }, [title])
  return (
    <div className="d-flex flex-wrap align-items-center gap-2 mb-4">
      <h1 className="h3 mb-0 me-auto">{title}</h1>
      {actions}
    </div>
  )
}

export function CopyId({ id, label }: { id: string; label?: string }) {
  const toast = useToast()
  return (
    <button
      type="button"
      className="id-button"
      title={`Copy ${id}`}
      onClick={async (e) => {
        e.stopPropagation()
        try {
          await navigator.clipboard.writeText(id)
          toast.info('Copied')
        } catch {
          toast.error('Copy blocked by the browser')
        }
      }}
    >
      {label ?? shortId(id)}
    </button>
  )
}

export function RelTime({ iso }: { iso: string | null | undefined }) {
  if (!iso) return <>–</>
  return <time dateTime={iso} title={new Date(iso).toLocaleString()}>{ago(iso)}</time>
}

export const EmptyState = ({ text }: { text: string }) => <div className="text-center text-body-secondary py-5">{text}</div>

export const ErrorAlert = ({ error }: { error: unknown }) => (
  <CAlert color="danger" role="alert">
    {errorMessage(error)}
  </CAlert>
)

export const CenteredSpinner = () => (
  <div className="text-center py-5">
    <CSpinner color="primary" />
  </div>
)

/** The card that holds a page's filters: a responsive row of labelled fields. */
export const FilterCard = ({ children }: { children: ReactNode }) => (
  <CCard className="mb-3">
    <CCardBody>
      <CRow className="g-3 align-items-end">{children}</CRow>
    </CCardBody>
  </CCard>
)

export interface Cols {
  sm?: number
  lg?: number | 'auto'
}

export const Field = ({ label, htmlFor, cols, children }: { label: string; htmlFor?: string; cols?: Cols; children: ReactNode }) => (
  <CCol {...cols}>
    <CFormLabel htmlFor={htmlFor} className="eyebrow">
      {label}
    </CFormLabel>
    {children}
  </CCol>
)

/** A row of toggle buttons that wraps on a narrow screen. `isOn` says which are pressed. */
export function ToggleGroup<T extends string>({ label, options, isOn, onToggle }: { label: string; options: readonly T[]; isOn: (o: T) => boolean; onToggle: (o: T) => void }) {
  return (
    <div className="d-flex flex-wrap gap-1" role="group" aria-label={label}>
      {options.map((o) => (
        <CButton key={o} size="sm" color="secondary" variant={isOn(o) ? undefined : 'outline'} active={isOn(o)} onClick={() => onToggle(o)}>
          {o}
        </CButton>
      ))}
    </div>
  )
}

/** "N shown" and a Load more button, under a paged list. */
export function ListFooter({ shown, hasMore, loading, onMore }: { shown: number; hasMore: boolean; loading: boolean; onMore: () => void }) {
  return (
    <div className="d-flex align-items-center gap-3 text-body-secondary small">
      <span>{shown} shown</span>
      {hasMore ? (
        <CButton size="sm" color="secondary" variant="outline" disabled={loading} onClick={onMore}>
          {loading ? <CSpinner size="sm" /> : 'Load more'}
        </CButton>
      ) : (
        <span>end of results</span>
      )}
    </div>
  )
}

export function KeyValue({ rows }: { rows: [string, ReactNode][] }) {
  return (
    <dl className="row mb-0 small">
      {rows.map(([k, v]) => (
        <Fragment key={k}>
          <dt className="col-sm-4 fw-normal text-body-secondary">{k}</dt>
          <dd className="col-sm-8 text-break">{v ?? '–'}</dd>
        </Fragment>
      ))}
    </dl>
  )
}
