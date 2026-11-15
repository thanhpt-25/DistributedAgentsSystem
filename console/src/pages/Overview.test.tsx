import { act, screen, waitFor } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { OPERATOR, READ_ONLY, agent, json, renderPage, stubGateway } from '../test/gateway'
import Overview from './Overview'

const request = (id: string, status: string) => ({
  request_id: id, status, created_at: '2026-11-15T10:00:00Z', finished_at: null, job_count: 3, token_usage: { input: 10, output: 5, total: 15 },
})

const usage = (failed: number, completed: number) => ({
  tenant: 'default', from: '2026-11-09', to: '2026-11-15',
  days: [{
    date: '2026-11-14', requests: { submitted: failed + completed, finished: { Completed: completed, Failed: failed } },
    models: [{ provider: 'ollama', model: 'gemma3', input_tokens: 900, output_tokens: 300, total_tokens: 1200, estimated_cost: null }],
  }],
  totals: {
    requests: { submitted: failed + completed, finished: { Completed: completed, Failed: failed } },
    input_tokens: 900, output_tokens: 300, total_tokens: 1200, estimated_cost: null,
  },
})

const fleet = [agent({ agent_id: 'w1' }), agent({ agent_id: 'w2', in_flight: 2 }), agent({ agent_id: 'w3', status: 'Unavailable' })]

const busy = {
  ...OPERATOR,
  'GET /v1/agents': fleet,
  'GET /v1/requests': (call: { query: URLSearchParams }) => {
    const status = call.query.get('status')
    if (status === 'Pending') return { items: [request('r-1', 'Pending'), request('r-2', 'Pending')] }
    if (status === 'Running') return { items: [request('r-3', 'Running')] }
    return { items: [request('r-3', 'Running')] }
  },
  'GET /v1/admin/dlq': { items: [{ dead_letter_id: 'd'.repeat(32) }], next_cursor: 'more' },
  'GET /v1/admin/usage': { tenants: [usage(1, 3)] },
}

describe('Overview', () => {
  it('summarises the fleet, the queue and the dead letters', async () => {
    stubGateway(busy)
    renderPage(<Overview />)
    expect(await screen.findByText('2/3')).toBeInTheDocument() // agents available
    expect(screen.getByText('0 draining · 1 down')).toBeInTheDocument()
    expect(await screen.findByText('2 + 1')).toBeInTheDocument() // pending + running
    expect(await screen.findByText('1+')).toBeInTheDocument() // dead letters: one shown and more behind it
  })

  it('shows the seven-day failure rate and token totals from the usage report', async () => {
    stubGateway(busy)
    renderPage(<Overview />)
    expect(await screen.findByText('25.0%')).toBeInTheDocument() // 1 failed of 4 finished
    expect(screen.getByText('1 of 4 finished')).toBeInTheDocument()
    expect(screen.getByText('1,200')).toBeInTheDocument()
  })

  it('lists the latest requests and the agents, each linking to its page', async () => {
    stubGateway(busy)
    renderPage(<Overview />)
    expect(await screen.findByRole('link', { name: 'All requests' })).toHaveAttribute('href', '/requests')
    expect(screen.getByRole('link', { name: 'Manage' })).toHaveAttribute('href', '/agents')
    expect(await screen.findByText('w3')).toBeInTheDocument()
  })

  it('has no dead-letter tile for a key that cannot read the queue', async () => {
    const calls = stubGateway({ ...READ_ONLY, 'GET /v1/agents': fleet, 'GET /v1/usage': usage(0, 2) })
    renderPage(<Overview />)
    await screen.findByText('2/3')
    await waitFor(() => expect(calls.some((c) => c.path === '/v1/admin/dlq')).toBe(true)) // the capability probe finished
    await act(async () => undefined)
    expect(screen.queryByText('Dead letters pending')).not.toBeInTheDocument()
  })

  it('shows the Gateway’s error when a list cannot be loaded', async () => {
    stubGateway({ ...OPERATOR, 'GET /v1/agents': () => json({ error: 'service_unavailable' }, 503) })
    renderPage(<Overview />)
    expect(await screen.findByRole('alert')).toHaveTextContent('service_unavailable')
  })
})
