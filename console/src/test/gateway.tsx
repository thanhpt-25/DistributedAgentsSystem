// Test helpers: a Gateway stand-in for `fetch`, and a renderer that wraps a page in the app's real providers.
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render } from '@testing-library/react'
import type { ReactElement } from 'react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import { vi } from 'vitest'
import { setKey } from '../api/client'
import type { Agent } from '../api/types'
import { AuthProvider } from '../auth/AuthContext'
import { ConfirmProvider, ThemeProvider, ToastProvider } from '../ui/Providers'

export interface Call {
  method: string
  path: string
  query: URLSearchParams
  headers: Headers
  body: unknown
}

/** A route answers with a value (sent as JSON), a Response, or a function of the call returning either. */
export type Route = unknown

export const json = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })

/** A Server-Sent Events response that sends `frames` and then closes, as the Gateway does for a finished Request. */
export function sse(frames: { event: string; id?: string; data: unknown }[]): Response {
  const text = frames
    .map((f) => `event: ${f.event}\n${f.id ? `id: ${f.id}\n` : ''}data: ${JSON.stringify(f.data)}\n\n`)
    .join('')
  return new Response(text, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })
}

// "METHOD /path/*/more": `*` stands for one path segment.
const matcher = (key: string) => new RegExp(`^${key.replace(/[.+?^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '[^/]+')}$`)

/** Replace `fetch` with a router over `routes` (keys are "METHOD /path"). Unlisted calls answer 404. Returns the log of calls. */
export function stubGateway(routes: Record<string, Route>): Call[] {
  const calls: Call[] = []
  const table = Object.entries(routes).map(([key, route]) => ({ route, pattern: matcher(key) }))
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input), 'http://localhost')
      const method = (init?.method ?? 'GET').toUpperCase()
      const body = init?.body ? JSON.parse(String(init.body)) : undefined
      calls.push({ method, path: url.pathname, query: url.searchParams, headers: new Headers(init?.headers), body })
      const hit = table.find((t) => t.pattern.test(`${method} ${url.pathname}`))
      if (!hit) return json({ error: 'not_found' }, 404)
      const reply = typeof hit.route === 'function' ? await hit.route(calls.at(-1)) : hit.route
      return reply instanceof Response ? reply : json(reply)
    }),
  )
  return calls
}

export const agent = (over: Partial<Agent> = {}): Agent => ({
  agent_id: 'w1', role: 'worker', host: 'node-1', capabilities: ['research'], status: 'Available',
  last_heartbeat: '2026-11-15T10:00:00Z', in_flight: 0, drained: false, ...over,
})

const emptyUsage = (tenant: string) => ({
  tenant, from: '2026-11-09', to: '2026-11-15', days: [],
  totals: { requests: { submitted: 0, finished: {} }, input_tokens: 0, output_tokens: 0, total_tokens: 0, estimated_cost: null },
})

const forbidden = (scope: string) => () => json({ error: 'forbidden', scope }, 403)

/** What an Operator key sees: every probe answers 200. */
export const OPERATOR: Record<string, Route> = {
  'GET /v1/agents': [],
  'GET /v1/requests': { items: [] },
  'GET /v1/admin/usage': { tenants: [emptyUsage('default'), emptyUsage('acme')] },
  'GET /v1/admin/dlq': { items: [] },
  'GET /healthz': { overall: 'healthy' },
}

/** A tenant admin: the Operator-only route answers 403, the dead-letter queue works. */
export const TENANT_ADMIN: Record<string, Route> = {
  ...OPERATOR,
  'GET /v1/admin/usage': forbidden('operator'),
  'GET /v1/usage': emptyUsage('acme'),
}

/** A read-only key: every admin route answers 403. */
export const READ_ONLY: Record<string, Route> = { ...TENANT_ADMIN, 'GET /v1/admin/dlq': forbidden('admin') }

/** Shows where the MemoryRouter is, so a test can assert on navigation and on filters kept in the URL. */
export function LocationProbe() {
  const { pathname, search } = useLocation()
  return <output data-testid="location">{pathname + search}</output>
}

/** Render `ui` inside the app's providers with a stored key, a fresh query cache (no retries) and a router. */
export function renderPage(ui: ReactElement, { path = '/', key = 'test-key' }: { path?: string; key?: string } = {}) {
  setKey(key)
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={client}>
      <ThemeProvider>
        <ToastProvider>
          <ConfirmProvider>
            <AuthProvider>
              <MemoryRouter initialEntries={[path]}>{ui}</MemoryRouter>
            </AuthProvider>
          </ConfirmProvider>
        </ToastProvider>
      </ThemeProvider>
    </QueryClientProvider>,
  )
}
