import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'
import { LocationProbe, OPERATOR, renderPage, stubGateway } from '../test/gateway'
import Requests from './Requests'

const item = (id: string, status: string) => ({
  request_id: id, status, created_at: '2026-11-15T10:00:00Z', finished_at: status === 'Running' ? null : '2026-11-15T10:00:02Z',
  job_count: 3, token_usage: { input: 10, output: 5, total: 15 },
})

const renderRequests = () => renderPage(<><Requests /><LocationProbe /></>, { path: '/requests' })
const table = async () => within(await screen.findByRole('table'))

describe('Requests', () => {
  it('lists requests and opens one when its row is clicked', async () => {
    stubGateway({ ...OPERATOR, 'GET /v1/requests': { items: [item('r-1', 'Completed')] } })
    renderRequests()
    await userEvent.click((await table()).getByText('Completed'))
    expect(screen.getByTestId('location')).toHaveTextContent('/requests/r-1')
  })

  it('filters by status, keeps the filter in the URL, and asks the Gateway for it', async () => {
    const calls = stubGateway({ ...OPERATOR, 'GET /v1/requests': { items: [item('r-1', 'Failed')] } })
    renderRequests()
    await userEvent.click(await screen.findByRole('button', { name: 'Failed' }))
    await waitFor(() => expect(screen.getByTestId('location')).toHaveTextContent('/requests?status=Failed'))
    await waitFor(() => expect(calls.some((c) => c.path === '/v1/requests' && c.query.get('status') === 'Failed')).toBe(true))
  })

  it('restores the filters from the URL, so a shared link shows the same view', async () => {
    const calls = stubGateway({ ...OPERATOR, 'GET /v1/requests': { items: [] } })
    renderPage(<Requests />, { path: '/requests?status=Running,Failed&limit=50' })
    await waitFor(() => expect(calls.some((c) => c.query.get('status') === 'Running,Failed' && c.query.get('limit') === '50')).toBe(true))
    expect(await screen.findByRole('button', { name: 'Running' })).toHaveClass('active')
  })

  it('pages with Load more by sending the cursor it was given', async () => {
    const calls = stubGateway({
      ...OPERATOR,
      'GET /v1/requests': (call: { query: URLSearchParams }) =>
        call.query.get('cursor') ? { items: [item('r-2', 'Failed')] } : { items: [item('r-1', 'Completed')], next_cursor: 'c2' },
    })
    renderRequests()
    await userEvent.click(await screen.findByRole('button', { name: 'Load more' }))
    expect(await (await table()).findByText('Failed')).toBeInTheDocument()
    expect((await table()).getByText('Completed')).toBeInTheDocument()
    expect(calls.some((c) => c.query.get('cursor') === 'c2')).toBe(true)
    expect(await screen.findByText('end of results')).toBeInTheDocument()
  })

  it('submits a request with a budget and an idempotency key, then opens it', async () => {
    const calls = stubGateway({ ...OPERATOR, 'POST /v1/requests': { request_id: 'r-new' } })
    renderRequests()
    const user = userEvent.setup()
    await user.click(await screen.findByRole('button', { name: 'New request' }))
    const dialog = await screen.findByRole('dialog')
    await user.type(within(dialog).getByLabelText('Prompt'), 'Compare queues and HTTP')
    await user.type(within(dialog).getByLabelText('Max tokens'), '5000')
    await user.click(within(dialog).getByRole('button', { name: 'Submit' }))

    await waitFor(() => expect(screen.getByTestId('location')).toHaveTextContent('/requests/r-new'))
    const post = calls.find((c) => c.method === 'POST')
    expect(post?.body).toEqual({ prompt: 'Compare queues and HTTP', budget: { max_total_tokens: 5000 } })
    expect(post?.headers.get('Idempotency-Key')).toMatch(/^[0-9a-f-]{36}$/)
  })

  it('shows the Gateway’s refusal inside the dialog and keeps what was typed', async () => {
    stubGateway({ ...OPERATOR, 'POST /v1/requests': () => new Response(JSON.stringify({ error: 'quota_exceeded', limit: 100 }), { status: 429 }) })
    renderRequests()
    const user = userEvent.setup()
    await user.click(await screen.findByRole('button', { name: 'New request' }))
    const dialog = await screen.findByRole('dialog')
    await user.type(within(dialog).getByLabelText('Prompt'), 'hello')
    await user.click(within(dialog).getByRole('button', { name: 'Submit' }))
    expect(await within(dialog).findByText(/quota_exceeded/)).toBeInTheDocument()
    expect(within(dialog).getByLabelText('Prompt')).toHaveValue('hello')
  })
})
