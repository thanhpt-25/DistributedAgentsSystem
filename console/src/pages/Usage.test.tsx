import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { lastDays } from '../lib/usage'
import { LocationProbe, OPERATOR, TENANT_ADMIN, renderPage, stubGateway } from '../test/gateway'
import Usage from './Usage'

const report = (tenant: string, submitted: number, cost = '0.5000') => ({
  tenant, from: '2026-11-09', to: '2026-11-15',
  days: [{
    date: '2026-11-14',
    requests: { submitted, finished: { Completed: submitted - 1, Failed: 1 } },
    models: [{ provider: 'ollama', model: 'gemma3', input_tokens: 900, output_tokens: 300, total_tokens: 1200, estimated_cost: null }],
  }],
  totals: {
    requests: { submitted, finished: { Completed: submitted - 1, Failed: 1 } },
    input_tokens: 900, output_tokens: 300, total_tokens: 1200,
    estimated_cost: { currency: 'USD', amount: cost, complete: true },
  },
})

const operatorWith = (...reports: ReturnType<typeof report>[]) => ({ ...OPERATOR, 'GET /v1/admin/usage': { tenants: reports } })
const renderUsage = () => renderPage(<><Usage /><LocationProbe /></>, { path: '/usage' })

describe('Usage', () => {
  it('adds the tenants together in the totals and lists each model once', async () => {
    stubGateway(operatorWith(report('default', 10), report('acme', 20)))
    renderUsage()
    expect(await screen.findByText('30')).toBeInTheDocument() // 10 + 20 submitted
    expect(screen.getByText('1.00')).toBeInTheDocument() // 0.50 + 0.50
    const models = within(screen.getByRole('heading', { name: 'By model' }).closest('.card') as HTMLElement)
    expect(models.getByText('gemma3')).toBeInTheDocument()
    expect(models.getByText('2,400')).toBeInTheDocument() // 1200 tokens from each tenant
  })

  it('breaks the numbers down by tenant for an Operator, and a click scopes the console to that tenant', async () => {
    const calls = stubGateway(operatorWith(report('default', 10), report('acme', 20)))
    renderUsage()
    const byTenant = within((await screen.findByRole('heading', { name: 'By tenant' })).closest('.card') as HTMLElement)
    await userEvent.click(byTenant.getByText('acme'))
    await waitFor(() => expect(calls.some((c) => c.path === '/v1/admin/usage' && c.query.get('tenant') === 'acme')).toBe(true))
  })

  it('uses the tenant’s own report, with no tenant table, for a key that is not an Operator', async () => {
    const calls = stubGateway({ ...TENANT_ADMIN, 'GET /v1/usage': report('acme', 7) })
    renderUsage()
    expect(await screen.findByText('gemma3')).toBeInTheDocument()
    expect(screen.queryByRole('heading', { name: 'By tenant' })).not.toBeInTheDocument()
    expect(calls.some((c) => c.path === '/v1/usage')).toBe(true)
  })

  it('asks for the range a preset names, and keeps it in the URL', async () => {
    const calls = stubGateway(operatorWith(report('default', 10)))
    renderUsage()
    await userEvent.click(await screen.findByRole('button', { name: '30 days' }))
    const { from, to } = lastDays(30)
    await waitFor(() => expect(screen.getByTestId('location')).toHaveTextContent(`/usage?from=${from}&to=${to}`))
    await waitFor(() => expect(calls.some((c) => c.query.get('from') === from)).toBe(true))
  })

  it('exports the days as CSV', async () => {
    stubGateway(operatorWith(report('default', 10)))
    let exported: Blob | undefined
    URL.createObjectURL = vi.fn((blob: Blob | MediaSource) => { exported = blob as Blob; return 'blob:test' })
    URL.revokeObjectURL = vi.fn()
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => undefined) // jsdom cannot follow a download
    renderUsage()
    await screen.findByText('gemma3')
    await userEvent.click(screen.getByRole('button', { name: 'Export CSV' }))
    const csv = await exported?.text()
    expect(csv).toContain('"tenant","date","submitted"')
    expect(csv).toContain('"default","2026-11-14","10","9","1","0","900","300","1200"')
  })

  it('shows the Gateway’s complaint about a bad range', async () => {
    // Only the page's call carries a date range; the sign-in probe has none and must still succeed.
    stubGateway({
      ...OPERATOR,
      'GET /v1/admin/usage': (call: { query: URLSearchParams }) =>
        call.query.get('from') ? new Response(JSON.stringify({ error: 'invalid_query', parameter: 'to' }), { status: 400 }) : { tenants: [] },
    })
    renderUsage()
    expect(await screen.findByRole('alert')).toHaveTextContent('invalid_query (to)')
  })
})
