import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { Route, Routes } from 'react-router-dom'
import { describe, expect, it } from 'vitest'
import { OPERATOR, json, renderPage, sse, stubGateway } from '../test/gateway'
import RequestDetail from './RequestDetail'

const view = (over: Record<string, unknown> = {}) => ({
  request_id: 'r-1', status: 'Running', priority: 'normal', replans: 0,
  jobs: [
    {
      job_id: 'j1', state: 'Succeeded', instruction: 'Find the sources', capabilities: ['research'], depends_on: [],
      attempt: 1, worker_id: 'w1', reason: null,
      result: { attempt: 1, status: 'Succeeded', content: 'Three sources found', error: null, usage: { input_tokens: 10, output_tokens: 5, total_tokens: 15 }, duration_ms: 1500 },
    },
    { job_id: 'j2', state: 'Dispatched', instruction: 'Write it up', capabilities: ['writing'], depends_on: ['j1'], attempt: 1, worker_id: 'w2', reason: null, result: null },
  ],
  transitions: [
    { job_id: 'j1', from_state: null, to_state: 'Ready', attempt: 0, at: '2026-11-15T10:00:00Z' },
    { job_id: 'j1', from_state: 'Ready', to_state: 'Succeeded', attempt: 1, worker_id: 'w1', at: '2026-11-15T10:00:05Z' },
  ],
  token_usage: { input_tokens: 1000, output_tokens: 207, total_tokens: 1207 },
  estimated_cost: null, replay_history: [], budget: null, webhook: null, ...over,
})

const running = [{ event: 'snapshot', id: '1-0', data: { status: 'Running' } }, { event: 'job.dispatched', id: '2-0', data: { job_id: 'j2' } }]

const renderDetail = (routes: Record<string, unknown>) => {
  const calls = stubGateway({ ...OPERATOR, 'GET /v1/requests/r-1/events': () => sse(running), ...routes })
  renderPage(<Routes><Route path="/requests/:id" element={<RequestDetail />} /></Routes>, { path: '/requests/r-1' })
  return calls
}

describe('Request detail', () => {
  it('shows the jobs, their transitions and the token usage', async () => {
    renderDetail({ 'GET /v1/requests/r-1': view() })
    expect(await screen.findByText('input_tokens')).toBeInTheDocument()
    expect(screen.getByText('1000')).toBeInTheDocument()
    expect(screen.getByText('No price table: cost unknown.')).toBeInTheDocument()
    expect(screen.getAllByText('Succeeded').length).toBeGreaterThanOrEqual(2) // the job and its transition
    expect(screen.getAllByText(/attempt 1/).length).toBeGreaterThanOrEqual(1) // the transition line and the job panel
  })

  it('shows what each worker was asked and answered', async () => {
    const calls = renderDetail({ 'GET /v1/requests/r-1': view() })
    expect(await screen.findByText('Three sources found')).toBeInTheDocument()
    expect(screen.getByText('Find the sources')).toBeInTheDocument()
    expect(screen.getByText('Write it up')).toBeInTheDocument()
    expect(screen.getByText('No result yet.')).toBeInTheDocument()
    expect(calls.find((c) => c.path === '/v1/requests/r-1')?.query.get('detail')).toBe('true')
  })

  it('streams events into the live feed', async () => {
    renderDetail({ 'GET /v1/requests/r-1': view() })
    expect(await screen.findByText('job.dispatched')).toBeInTheDocument()
  })

  it('stops following a request that is already finished', async () => {
    renderDetail({
      'GET /v1/requests/r-1': view({ status: 'Completed' }),
      'GET /v1/requests/r-1/events': () => sse([{ event: 'snapshot', id: '1-0', data: { status: 'Completed' } }]),
    })
    expect(await screen.findByText('ended')).toBeInTheDocument()
  })

  it('cancels only after confirming', async () => {
    const calls = renderDetail({ 'GET /v1/requests/r-1': view(), 'POST /v1/requests/r-1/cancel': { status: 'Cancelled' } })
    const user = userEvent.setup()
    await user.click(await screen.findByRole('button', { name: 'Cancel request' }))
    expect(calls.some((c) => c.method === 'POST')).toBe(false)
    await user.click(within(await screen.findByRole('dialog')).getByRole('button', { name: 'Cancel request' }))
    expect(await screen.findByText('Request cancelled')).toBeInTheDocument()
    expect(calls.find((c) => c.method === 'POST')?.path).toBe('/v1/requests/r-1/cancel')
  })

  it('cannot cancel a finished request', async () => {
    renderDetail({ 'GET /v1/requests/r-1': view({ status: 'Completed' }) })
    await waitFor(() => expect(screen.getByRole('button', { name: 'Cancel request' })).toBeDisabled())
  })

  it('shows the final result and the replay history of a replayed request', async () => {
    renderDetail({
      'GET /v1/requests/r-1': view({
        status: 'Completed',
        final_result: { summary: 'Queues win on latency.', jobs: [], failures: [] },
        replay_history: [{ dead_letter_id: 'a'.repeat(32), at: '2026-11-15T09:00:00Z', error: 'boom' }],
      }),
    })
    expect(await screen.findByText('Queues win on latency.')).toBeInTheDocument()
    expect(screen.getByText('boom')).toBeInTheDocument()
  })

  it('says so when the request does not exist or belongs to another tenant', async () => {
    renderDetail({ 'GET /v1/requests/r-1': () => json({ error: 'not_found' }, 404) })
    expect(await screen.findByRole('alert')).toHaveTextContent('was not found')
  })
})
