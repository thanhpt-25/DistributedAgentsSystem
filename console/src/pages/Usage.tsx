// Usage and cost: per-tenant Usage_Reports, charts, per-model table, CSV export.
import { CButton, CCol, CFormInput, CRow } from '@coreui/react'
import { useMemo } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useUsage } from '../api/hooks'
import type { UsageReport } from '../api/types'
import { useAuth } from '../auth/AuthContext'
import { num } from '../lib/format'
import { isoDay, lastDays, mergeReports, modelRows, usageCsv, type Merged } from '../lib/usage'
import BarChartCard from '../ui/BarChartCard'
import DataTable from '../ui/DataTable'
import { CenteredSpinner, ErrorAlert, Field, FilterCard, PageHeader, Section, StatCard } from '../ui/parts'

const PRESETS = [7, 30, 90]

interface Range {
  from: string
  to: string
}

function download(filename: string, text: string) {
  const url = URL.createObjectURL(new Blob([text], { type: 'text/csv' }))
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  a.click()
  URL.revokeObjectURL(url)
}

function RangeFilters({ range, onChange }: { range: Range; onChange: (r: Range) => void }) {
  return (
    <FilterCard>
      <Field label="From" htmlFor="from" cols={{ sm: 4, lg: 2 }}>
        <CFormInput id="from" size="sm" type="date" value={range.from} onChange={(e) => onChange({ ...range, from: e.target.value })} />
      </Field>
      <Field label="To" htmlFor="to" cols={{ sm: 4, lg: 2 }}>
        <CFormInput id="to" size="sm" type="date" value={range.to} onChange={(e) => onChange({ ...range, to: e.target.value })} />
      </Field>
      <CCol lg="auto" className="d-flex gap-2">
        {PRESETS.map((n) => (
          <CButton key={n} size="sm" color="secondary" variant="outline" onClick={() => onChange(lastDays(n))}>{n} days</CButton>
        ))}
      </CCol>
    </FilterCard>
  )
}

function UsageTiles({ merged, range }: { merged: Merged; range: Range }) {
  const finished = merged.completed + merged.failed + merged.cancelled
  const cost = merged.costKnown ? merged.cost.toFixed(2) : '–'
  const costSub = merged.costKnown ? `${merged.currency}${merged.costComplete ? '' : ' · some models unpriced'}` : 'no price table'
  return (
    <CRow className="g-3 mb-3">
      <CCol sm={6} xl><StatCard label="Submitted" value={num(merged.submitted)} sub={`${range.from} → ${range.to}`} tone="info" /></CCol>
      <CCol sm={6} xl><StatCard label="Completed" value={num(merged.completed)} sub="finished successfully" tone="success" /></CCol>
      <CCol sm={6} xl><StatCard label="Failed" value={num(merged.failed)} sub={finished ? `${((merged.failed / finished) * 100).toFixed(1)}% of finished` : undefined} tone={merged.failed ? 'danger' : undefined} /></CCol>
      <CCol sm={6} xl><StatCard label="Tokens" value={num(merged.total)} sub={`${num(merged.input)} in · ${num(merged.output)} out`} /></CCol>
      <CCol sm={6} xl><StatCard label="Est. cost" value={cost} sub={costSub} /></CCol>
    </CRow>
  )
}

function TenantTable({ reports, onPick }: { reports: UsageReport[]; onPick: (tenant: string) => void }) {
  return (
    <Section title="By tenant" flush>
      <DataTable
        rows={reports}
        rowKey={(r) => r.tenant}
        onRowClick={(r) => onPick(r.tenant)}
        columns={[
          { header: 'Tenant', cell: (r) => r.tenant },
          { header: 'Submitted', numeric: true, cell: (r) => num(r.totals.requests.submitted) },
          { header: 'Completed', numeric: true, cell: (r) => num(r.totals.requests.finished.Completed ?? 0) },
          { header: 'Failed', numeric: true, cell: (r) => num(r.totals.requests.finished.Failed ?? 0) },
          { header: 'Tokens', numeric: true, cell: (r) => num(r.totals.total_tokens) },
          { header: 'Est. cost', numeric: true, cell: (r) => (r.totals.estimated_cost ? `${Number(r.totals.estimated_cost.amount).toFixed(4)} ${r.totals.estimated_cost.currency}` : '–') },
        ]}
      />
    </Section>
  )
}

function ModelTable({ merged }: { merged: Merged }) {
  return (
    <Section title="By model" flush>
      <DataTable
        rows={modelRows(merged.days)}
        rowKey={(r) => `${r.provider}/${r.model}`}
        empty="No model usage in this range."
        columns={[
          { header: 'Provider', cell: (r) => r.provider },
          { header: 'Model', cell: (r) => <span className="mono">{r.model}</span> },
          { header: 'Input', numeric: true, cell: (r) => num(r.input) },
          { header: 'Output', numeric: true, cell: (r) => num(r.output) },
          { header: 'Total', numeric: true, cell: (r) => num(r.total) },
          { header: 'Est. cost', numeric: true, cell: (r) => (r.cost === null ? '–' : r.cost.toFixed(4)) },
        ]}
      />
    </Section>
  )
}

function UsageBody({ reports, range, onPickTenant }: { reports: UsageReport[]; range: Range; onPickTenant: (tenant: string) => void }) {
  const merged = useMemo(() => mergeReports(reports), [reports])
  const labels = merged.days.map((d) => d.date.slice(5))
  return (
    <>
      <UsageTiles merged={merged} range={range} />
      <CRow className="g-3">
        <CCol lg={6}><BarChartCard title="Requests submitted per day" datasetLabel="Submitted" labels={labels} values={merged.days.map((d) => d.submitted)} /></CCol>
        <CCol lg={6}><BarChartCard title="Tokens per day" datasetLabel="Tokens" labels={labels} values={merged.days.map((d) => d.tokens)} /></CCol>
      </CRow>
      {reports.length > 1 && <TenantTable reports={reports} onPick={onPickTenant} />}
      <ModelTable merged={merged} />
    </>
  )
}

export default function Usage() {
  const { setTenant } = useAuth()
  const [params, setParams] = useSearchParams()
  const defaults = useMemo(() => lastDays(7), [])
  const range: Range = { from: params.get('from') ?? defaults.from, to: params.get('to') ?? isoDay(new Date()) }
  const usage = useUsage(range)
  const reports = usage.data

  return (
    <>
      <PageHeader
        title="Usage & cost"
        actions={
          <CButton color="secondary" variant="outline" disabled={!reports} onClick={() => reports && download(`das-usage-${range.from}_${range.to}.csv`, usageCsv(reports))}>
            Export CSV
          </CButton>
        }
      />
      <RangeFilters range={range} onChange={(next) => setParams(new URLSearchParams({ ...next }), { replace: true })} />
      {usage.isError && <ErrorAlert error={usage.error} />}
      {usage.isPending && <CenteredSpinner />}
      {reports && <UsageBody reports={reports} range={range} onPickTenant={setTenant} />}
    </>
  )
}
