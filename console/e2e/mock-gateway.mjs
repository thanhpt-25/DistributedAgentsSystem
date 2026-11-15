// A stand-in Gateway for the browser tests: it serves the built console (dist/) and answers /v1 with fixtures, keeping
// just enough state that drain, replay, cancel and submit show their effect. POST /__reset restores the fixtures.
// Keys: e2e-operator-key sees everything; e2e-reader-key may read, and every /v1/admin route answers 403 for it.
import { createServer } from 'node:http'
import { readFile } from 'node:fs/promises'
import { extname, join, normalize } from 'node:path'
import { fileURLToPath } from 'node:url'

const DIST = fileURLToPath(new URL('../dist/', import.meta.url))
const PORT = Number(process.env.PORT ?? 4173)
const ROLES = { 'e2e-operator-key': 'operator', 'e2e-reader-key': 'reader' }
const TYPES = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml' }
const ago = (minutes) => new Date(Date.now() - minutes * 60_000).toISOString()
const DLQ_ID = 'd'.repeat(32)

function freshState() {
  return {
    agents: [
      { agent_id: 'manager-1', role: 'manager', host: 'm1', capabilities: [], status: 'Available', in_flight: 1 },
      { agent_id: 'worker-research', role: 'worker', host: 'w1', capabilities: ['research'], status: 'Available', in_flight: 2 },
      { agent_id: 'worker-review', role: 'worker', host: 'w2', capabilities: ['review'], status: 'Available', in_flight: 0 },
      { agent_id: 'worker-old', role: 'worker', host: 'w3', capabilities: ['review'], status: 'Unavailable', in_flight: 0 },
    ],
    requests: [
      { request_id: 'r-done', status: 'Completed', created_at: ago(30), finished_at: ago(29) },
      { request_id: 'r-run', status: 'Running', created_at: ago(2), finished_at: null },
    ],
    dlq: [{
      dead_letter_id: DLQ_ID, reason: 'retries_exhausted', status: 'Pending', request_id: 'r-done', job_id: 'j1', attempt: 3,
      capabilities: ['research'], last_error: 'LLM timeout after 30s', dead_lettered_at: ago(5), status_changed_at: null, discarded_at: null,
    }],
  }
}
let state = freshState()

const agentView = (a) => ({ ...a, last_heartbeat: ago(0), drained: a.status === 'Draining' && a.in_flight === 0 })
const listItem = (r) => ({ ...r, job_count: 2, token_usage: { input: 100, output: 50, total: 150 } })

function requestView(r) {
  const done = r.status === 'Completed'
  return {
    request_id: r.request_id, status: r.status, priority: 'normal', replans: 0,
    jobs: [{ job_id: 'j1', state: done ? 'Succeeded' : 'Dispatched' }, { job_id: 'j2', state: done ? 'Succeeded' : 'Waiting' }],
    transitions: [{ job_id: 'j1', from_state: null, to_state: 'Ready', attempt: 0, at: r.created_at }],
    token_usage: { input_tokens: 100, output_tokens: 50, total_tokens: 150 }, estimated_cost: null,
    replay_history: [], budget: null, webhook: null,
    ...(done ? { final_result: { summary: 'Answer from the stub.', jobs: [], failures: [] } } : {}),
  }
}

function usageReport(tenant, query) {
  const today = new Date().toISOString().slice(0, 10)
  const finished = { Completed: 3, Failed: 1 }
  const model = { provider: 'ollama', model: 'fake-llm', input_tokens: 300, output_tokens: 100, total_tokens: 400, estimated_cost: null }
  return {
    tenant, from: query.get('from') ?? today, to: query.get('to') ?? today,
    days: [{ date: today, requests: { submitted: 4, finished }, models: [model] }],
    totals: { requests: { submitted: 4, finished }, input_tokens: 300, output_tokens: 100, total_tokens: 400, estimated_cost: null },
  }
}

const send = (res, status, body) => {
  res.writeHead(status, { 'Content-Type': 'application/json' })
  res.end(JSON.stringify(body))
}

const readBody = (req) =>
  new Promise((resolve) => {
    let text = ''
    req.on('data', (chunk) => { text += chunk })
    req.on('end', () => resolve(text ? JSON.parse(text) : undefined))
  })

function streamEvents(req, res, request) {
  res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' })
  const frame = (event, id, data) => res.write(`event: ${event}\nid: ${id}\ndata: ${JSON.stringify(data)}\n\n`)
  frame('snapshot', '1-0', requestView(request))
  if (request.status === 'Completed') {
    frame('request.completed', '2-0', { status: 'Completed' })
    return res.end()
  }
  frame('job.dispatched', '2-0', { job_id: 'j1' })
  const keepalive = setInterval(() => res.write(': keepalive\n\n'), 5000)
  req.on('close', () => clearInterval(keepalive))
}

async function api(req, res, url) {
  const role = ROLES[req.headers['x-api-key']]
  if (!role) return send(res, 401, { error: 'unauthorized' })
  const path = url.pathname
  const q = url.searchParams
  if (path.startsWith('/v1/admin/') && role !== 'operator') return send(res, 403, { error: 'forbidden', scope: 'admin' })
  const match = (re) => path.match(re)
  let m

  if (req.method === 'GET') {
    if (path === '/v1/agents') return send(res, 200, state.agents.map(agentView))
    if (path === '/v1/requests') {
      const wanted = q.get('status')?.split(',')
      const items = state.requests.filter((r) => !wanted || wanted.includes(r.status)).map(listItem)
      return send(res, 200, { items })
    }
    if ((m = match(/^\/v1\/requests\/([^/]+)\/events$/))) {
      const request = state.requests.find((r) => r.request_id === m[1])
      return request ? streamEvents(req, res, request) : send(res, 404, { error: 'not_found' })
    }
    if ((m = match(/^\/v1\/requests\/([^/]+)$/))) {
      const request = state.requests.find((r) => r.request_id === m[1])
      return request ? send(res, 200, requestView(request)) : send(res, 404, { error: 'not_found' })
    }
    if (path === '/v1/usage') return send(res, 200, usageReport('default', q))
    if (path === '/v1/admin/usage') return send(res, 200, { tenants: [usageReport('default', q)] })
    if (path === '/v1/admin/dlq') {
      const status = q.get('status')
      return send(res, 200, { items: state.dlq.filter((d) => !status || d.status === status) })
    }
    if ((m = match(/^\/v1\/admin\/dlq\/([^/]+)$/))) {
      const record = state.dlq.find((d) => d.dead_letter_id === m[1])
      if (!record) return send(res, 404, { error: 'not_found' })
      return send(res, 200, { ...record, message: { payload: { job_id: record.job_id } }, request_status: 'Failed', job_state: 'Failed', job_attempt: 3, request_exists: true })
    }
  }

  if (req.method === 'POST') {
    const body = await readBody(req)
    if (path === '/v1/requests') {
      state.requests.unshift({ request_id: 'r-new', status: 'Pending', created_at: ago(0), finished_at: null, prompt: body?.prompt })
      return send(res, 202, { request_id: 'r-new' })
    }
    if ((m = match(/^\/v1\/requests\/([^/]+)\/cancel$/))) {
      const request = state.requests.find((r) => r.request_id === m[1])
      if (!request) return send(res, 404, { error: 'not_found' })
      Object.assign(request, { status: 'Cancelled', finished_at: ago(0) })
      return send(res, 202, { status: 'Cancelled' })
    }
    if ((m = match(/^\/v1\/admin\/agents\/([^/]+)\/(drain|undrain)$/))) {
      const found = state.agents.find((a) => a.agent_id === m[1])
      if (!found) return send(res, 404, { error: 'not_found' })
      found.status = m[2] === 'drain' ? 'Draining' : 'Available'
      return send(res, 202, { agent_id: found.agent_id, status: found.status })
    }
    if ((m = match(/^\/v1\/admin\/dlq\/([^/]+)\/(replay|discard)$/))) {
      const record = state.dlq.find((d) => d.dead_letter_id === m[1])
      if (!record) return send(res, 404, { error: 'not_found' })
      record.status = m[2] === 'replay' ? 'Replayed' : 'Discarded'
      return send(res, m[2] === 'replay' ? 202 : 200, { dead_letter_id: record.dead_letter_id, request_id: record.request_id, job_id: record.job_id, attempt: 4, status: record.status })
    }
  }
  return send(res, 404, { error: 'not_found' })
}

async function serveStatic(res, pathname) {
  const file = normalize(join(DIST, pathname === '/' ? 'index.html' : pathname))
  const known = file.startsWith(DIST) ? await readFile(file).catch(() => null) : null
  const body = known ?? (await readFile(join(DIST, 'index.html'))) // single-page app: unknown paths get the shell
  res.writeHead(200, { 'Content-Type': TYPES[extname(known ? file : 'index.html')] ?? 'application/octet-stream' })
  res.end(body)
}

createServer(async (req, res) => {
  const url = new URL(req.url ?? '/', 'http://localhost')
  if (url.pathname === '/healthz') return send(res, 200, { overall: 'healthy' })
  if (url.pathname === '/__reset' && req.method === 'POST') {
    state = freshState()
    return send(res, 200, { reset: true })
  }
  if (url.pathname.startsWith('/v1/')) return api(req, res, url)
  return serveStatic(res, url.pathname)
}).listen(PORT, '127.0.0.1', () => console.log(`mock gateway and console on http://127.0.0.1:${PORT}`))
