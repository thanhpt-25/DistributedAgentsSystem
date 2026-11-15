import { screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'
import { OPERATOR, json, renderPage, stubGateway } from '../test/gateway'
import Dlq from './Dlq'

const record = (over: Record<string, unknown> = {}) => ({
  dead_letter_id: 'a'.repeat(32), reason: 'retries_exhausted', status: 'Pending', request_id: 'r-1', job_id: 'j1', attempt: 3,
  capabilities: ['research'], last_error: 'LLM timeout', dead_lettered_at: '2026-11-15T10:00:00Z', status_changed_at: null, discarded_at: null,
  ...over,
})

const queue = { items: [record(), record({ dead_letter_id: 'b'.repeat(32), reason: 'malformed', request_id: null, job_id: null })] }
// Look inside the table: the Reason filter's <option>s carry the same words.
const rowOf = async (reason: string) => {
  const cell = within(await screen.findByRole('table')).getByText(reason)
  return within(cell.closest('tr') as HTMLElement)
}

describe('Dead letters', () => {
  it('replays a record only after confirming, and reports the new attempt', async () => {
    const calls = stubGateway({ ...OPERATOR, 'GET /v1/admin/dlq': queue, 'POST /v1/admin/dlq/*/replay': { job_id: 'j1', attempt: 4 } })
    renderPage(<Dlq />, { path: '/dlq' })
    const user = userEvent.setup()
    await user.click((await rowOf('retries_exhausted')).getByRole('button', { name: 'Replay' }))
    expect(calls.some((c) => c.method === 'POST')).toBe(false) // nothing is sent before the dialog is confirmed
    const dialog = await screen.findByRole('dialog')
    expect(dialog).toHaveTextContent('Job j1 of request r-1')
    await user.click(within(dialog).getByRole('button', { name: 'Replay' }))
    expect(await screen.findByText('Replayed: job j1, attempt 4')).toBeInTheDocument()
    expect(calls.find((c) => c.method === 'POST')?.path).toBe(`/v1/admin/dlq/${'a'.repeat(32)}/replay`)
  })

  it('offers no replay for a malformed record, only discard', async () => {
    stubGateway({ ...OPERATOR, 'GET /v1/admin/dlq': queue })
    renderPage(<Dlq />, { path: '/dlq' })
    const malformed = await rowOf('malformed')
    expect(malformed.queryByRole('button', { name: 'Replay' })).not.toBeInTheDocument()
    expect(malformed.getByRole('button', { name: 'Discard' })).toBeInTheDocument()
  })

  it('does not discard when the dialog is cancelled', async () => {
    const calls = stubGateway({ ...OPERATOR, 'GET /v1/admin/dlq': queue })
    renderPage(<Dlq />, { path: '/dlq' })
    const user = userEvent.setup()
    await user.click((await rowOf('malformed')).getByRole('button', { name: 'Discard' }))
    await user.click(within(await screen.findByRole('dialog')).getByRole('button', { name: 'Cancel' }))
    expect(calls.some((c) => c.method === 'POST')).toBe(false)
  })

  it('explains a conflict from the Gateway, such as a record that was already replayed', async () => {
    stubGateway({
      ...OPERATOR,
      'GET /v1/admin/dlq': queue,
      'POST /v1/admin/dlq/*/discard': () => json({ error: 'record_not_pending', current_state: 'Replayed' }, 409),
    })
    renderPage(<Dlq />, { path: '/dlq' })
    const user = userEvent.setup()
    await user.click((await rowOf('malformed')).getByRole('button', { name: 'Discard' }))
    await user.click(within(await screen.findByRole('dialog')).getByRole('button', { name: 'Discard' }))
    expect(await screen.findByText('record_not_pending (Replayed)')).toBeInTheDocument()
  })

  it('says so when the queue is empty', async () => {
    stubGateway({ ...OPERATOR, 'GET /v1/admin/dlq': { items: [] } })
    renderPage(<Dlq />, { path: '/dlq' })
    expect(await screen.findByText('No dead letters match. That is good news.')).toBeInTheDocument()
  })
})
