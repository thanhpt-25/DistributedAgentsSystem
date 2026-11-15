// Sign-in and capability flows, through the whole app (router, providers, layout) against a stubbed Gateway.
import { QueryClient } from '@tanstack/react-query'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'
import App from './App'
import { getKey, setKey } from './api/client'
import { OPERATOR, READ_ONLY, TENANT_ADMIN, json, stubGateway } from './test/gateway'

const startApp = () => render(<App client={new QueryClient({ defaultOptions: { queries: { retry: false } } })} />)

async function signIn(key: string) {
  const user = userEvent.setup()
  await user.type(await screen.findByLabelText('API key'), key)
  await user.click(screen.getByRole('button', { name: 'Sign in' }))
}

describe('signing in', () => {
  it('shows the sign-in screen and asks the Gateway nothing when no key is stored', async () => {
    const calls = stubGateway(OPERATOR)
    startApp()
    expect(await screen.findByLabelText('API key')).toBeInTheDocument()
    expect(calls).toHaveLength(0)
  })

  it('rejects a wrong key and remembers nothing', async () => {
    stubGateway({ 'GET /v1/agents': () => json({ error: 'unauthorized' }, 401) })
    startApp()
    await signIn('wrong-key')
    expect(await screen.findByRole('alert')).toHaveTextContent('That key was not accepted.')
    expect(getKey()).toBe('')
  })

  it('signs an Operator in: every page, the tenant picker, and the key kept for the tab', async () => {
    stubGateway(OPERATOR)
    startApp()
    await signIn('operator-key')
    expect(await screen.findByRole('heading', { name: 'Overview' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /Dead letters/ })).toBeInTheDocument()
    expect(screen.getByText('Operator')).toBeInTheDocument()
    expect(screen.getByLabelText('Tenant scope')).toHaveTextContent('acme')
    expect(getKey()).toBe('operator-key')
  })

  it('shows a tenant admin the dead letters but no tenant picker', async () => {
    stubGateway(TENANT_ADMIN)
    startApp()
    await signIn('tenant-key')
    expect(await screen.findByRole('link', { name: /Dead letters/ })).toBeInTheDocument()
    expect(screen.getByText('Tenant admin')).toBeInTheDocument()
    expect(screen.queryByLabelText('Tenant scope')).not.toBeInTheDocument()
  })

  it('hides the admin pages from a read-only key', async () => {
    stubGateway(READ_ONLY)
    startApp()
    await signIn('reader-key')
    expect(await screen.findByRole('heading', { name: 'Overview' })).toBeInTheDocument()
    expect(screen.getByText('Read-only')).toBeInTheDocument()
    expect(screen.queryByRole('link', { name: /Dead letters/ })).not.toBeInTheDocument()
  })
})

describe('reloading with a stored key', () => {
  it('signs in again by itself', async () => {
    setKey('stored-key')
    stubGateway(OPERATOR)
    startApp()
    expect(await screen.findByRole('heading', { name: 'Overview' })).toBeInTheDocument()
  })

  it('keeps the key when the Gateway is unreachable, and says so', async () => {
    setKey('stored-key')
    stubGateway({ 'GET /v1/agents': () => { throw new TypeError('Failed to fetch') } })
    startApp()
    expect(await screen.findByRole('alert')).toHaveTextContent('Your key is kept')
    expect(getKey()).toBe('stored-key')
  })

  it('keeps the key through a 502 from the proxy as well', async () => {
    setKey('stored-key')
    stubGateway({ 'GET /v1/agents': () => json({ error: 'bad_response' }, 502) })
    startApp()
    expect(await screen.findByRole('alert')).toHaveTextContent('The Gateway is unreachable')
    expect(getKey()).toBe('stored-key')
  })

  it('forgets a key the Gateway rejects', async () => {
    setKey('revoked-key')
    stubGateway({ 'GET /v1/agents': () => json({ error: 'unauthorized' }, 401) })
    startApp()
    expect(await screen.findByLabelText('API key')).toBeInTheDocument()
    expect(getKey()).toBe('')
  })
})
