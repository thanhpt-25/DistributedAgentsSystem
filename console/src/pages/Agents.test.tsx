import { act, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'
import { OPERATOR, READ_ONLY, agent, renderPage, stubGateway } from '../test/gateway'
import Agents from './Agents'

const fleet = [
  agent({ agent_id: 'm1', role: 'manager', host: 'mgr-1', capabilities: [] }),
  agent({ agent_id: 'w1' }),
  agent({ agent_id: 'w2', host: 'node-2', capabilities: ['review'], status: 'Draining', drained: true }),
]

const renderAgents = () => renderPage(<Agents />, { path: '/agents' })
const rowOf = async (id: string) => within((await screen.findByText(id)).closest('tr') as HTMLElement)

describe('Agents', () => {
  it('narrows the registry with the search box', async () => {
    stubGateway({ ...OPERATOR, 'GET /v1/agents': fleet })
    renderAgents()
    await screen.findByText('w1')
    await userEvent.type(screen.getByLabelText('Search'), 'review')
    await waitFor(() => expect(screen.queryByText('w1')).not.toBeInTheDocument())
    expect(screen.getByText('w2')).toBeInTheDocument()
  })

  it('flags a capability that no available worker serves, and one served by a single worker', async () => {
    stubGateway({ ...OPERATOR, 'GET /v1/agents': fleet })
    renderAgents()
    expect(await screen.findByText('no worker')).toBeInTheDocument() // review: its only worker is draining
    expect(screen.getByText('single point')).toBeInTheDocument() // research: only w1
  })

  it('lets an Operator drain a worker, optionally releasing its leases', async () => {
    const calls = stubGateway({ ...OPERATOR, 'GET /v1/agents': fleet, 'POST /v1/admin/agents/*/drain': { agent_id: 'w1', status: 'Draining' } })
    renderAgents()
    const user = userEvent.setup()
    const row = await rowOf('w1')
    await waitFor(() => expect(row.getByRole('button', { name: 'Drain' })).toBeInTheDocument()) // after the key's role is known
    await user.click(row.getByRole('button', { name: 'Drain' }))
    const dialog = await screen.findByRole('dialog')
    await user.click(within(dialog).getByLabelText('Also release the leases it holds'))
    await user.click(within(dialog).getByRole('button', { name: 'Drain' }))
    expect(await screen.findByText('drain accepted: now Draining')).toBeInTheDocument()
    const post = calls.find((c) => c.method === 'POST')
    expect(post?.path).toBe('/v1/admin/agents/w1/drain')
    expect(post?.body).toEqual({ release_leases: true })
  })

  it('offers Undrain for a draining agent', async () => {
    stubGateway({ ...OPERATOR, 'GET /v1/agents': fleet })
    renderAgents()
    const row = await rowOf('w2')
    await waitFor(() => expect(row.getByRole('button', { name: 'Undrain' })).toBeInTheDocument())
  })

  it('never offers drain to a key that is not an Operator', async () => {
    const calls = stubGateway({ ...READ_ONLY, 'GET /v1/agents': fleet })
    renderAgents()
    await screen.findByText('w1')
    await waitFor(() => expect(calls.some((c) => c.path === '/v1/admin/dlq')).toBe(true)) // the capability probe finished
    await act(async () => undefined)
    expect(screen.queryByRole('button', { name: /Drain/ })).not.toBeInTheDocument()
  })
})
