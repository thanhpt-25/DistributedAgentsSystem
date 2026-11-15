import { screen } from '@testing-library/react'
import { Route, Routes } from 'react-router-dom'
import { describe, expect, it } from 'vitest'
import { OPERATOR, json, renderPage, stubGateway } from '../test/gateway'
import DlqDetail from './DlqDetail'

const id = 'a'.repeat(32)

const detail = (over: Record<string, unknown> = {}) => ({
  dead_letter_id: id, reason: 'retries_exhausted', status: 'Pending', request_id: 'r-1', job_id: 'j1', attempt: 3,
  capabilities: ['research'], last_error: 'LLM timeout after 30s', dead_lettered_at: '2026-11-15T10:00:00Z',
  status_changed_at: null, discarded_at: null,
  message: { payload: { job_id: 'j1' } }, request_status: 'Failed', job_state: 'Failed', job_attempt: 3, request_exists: true,
  ...over,
})

const renderDetail = (route: unknown) => {
  stubGateway({ ...OPERATOR, [`GET /v1/admin/dlq/${id}`]: route })
  renderPage(<Routes><Route path="/dlq/:id" element={<DlqDetail />} /></Routes>, { path: `/dlq/${id}` })
}

describe('Dead letter detail', () => {
  it('shows the record, what became of its request and job, and the original message', async () => {
    renderDetail(detail())
    expect(await screen.findByText('LLM timeout after 30s')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'r-1' })).toHaveAttribute('href', '/requests/r-1')
    expect(screen.getByText('Request status')).toBeInTheDocument()
    expect(screen.getByText(/"job_id": "j1"/)).toBeInTheDocument()
  })

  it('offers replay and discard while the record is pending, and nothing once it is not', async () => {
    renderDetail(detail())
    expect(await screen.findByRole('button', { name: 'Discard' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Replay' })).toBeInTheDocument()
  })

  it('shows no actions for a record that was already replayed', async () => {
    renderDetail(detail({ status: 'Replayed' }))
    await screen.findByText('LLM timeout after 30s')
    expect(screen.queryByRole('button', { name: 'Discard' })).not.toBeInTheDocument()
  })

  it('shows the raw bytes of a malformed message, decoded when they are text', async () => {
    renderDetail(detail({ reason: 'malformed', request_id: null, job_id: null, message: undefined, raw_b64: btoa('{broken json'), truncated: true }))
    expect(await screen.findByText('Raw bytes (truncated)')).toBeInTheDocument()
    expect(screen.getByText('{broken json')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Replay' })).not.toBeInTheDocument()
  })

  it('says so when the record is missing or belongs to another tenant', async () => {
    renderDetail(() => json({ error: 'not_found' }, 404))
    expect(await screen.findByRole('alert')).toHaveTextContent('not found, or not visible to your key')
  })
})
