# DAS Console

A browser UI for tracking and operating a DAS deployment, built with **React 19, TypeScript, Vite and
[CoreUI for React](https://coreui.io/react/)**. It adds no Gateway endpoint and no server-side state: everything it shows
comes from the Gateway's existing API.

```bash
# run it next to the stack (builds the image, serves on http://localhost:8088)
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.console.yml up -d --build

# or develop against a Gateway on localhost:8080 (DAS_GATEWAY_URL overrides)
cd console && npm install && npm run dev
```

Sign in with an API key. The console probes what the key may do and shows only those pages:

| Key | Sees |
|---|---|
| Admin key / Operator | everything, across tenants (a tenant picker appears), plus drain and undrain |
| Tenant key with `admin` scope | its own tenant's requests, agents, dead letters, usage |
| Tenant key with `read` scope | requests, agents, usage |

| Page | What it does | Endpoints |
|---|---|---|
| Overview | health, fleet, queue pressure, dead letters, 7-day failure rate, tokens and cost | `/healthz`, `/v1/agents`, `/v1/requests`, `/v1/admin/dlq`, `/v1/(admin/)usage` |
| Requests | filter by status and time, page through, submit a request (priority, budget, idempotency key) | `/v1/requests` |
| Request detail | Jobs, transitions, tokens, cost, budget, webhook, final result, replay history, cancel, live event tail | `/v1/requests/{id}`, `/cancel`, `/events` (SSE) |
| Agents | registry, worker capability coverage, drain and undrain | `/v1/agents`, `/v1/admin/agents/{id}/(un)drain` |
| Dead letters | inspect, replay, discard | `/v1/admin/dlq` |
| Usage & cost | per-day charts, per-tenant and per-model tables, CSV export | `/v1/usage`, `/v1/admin/usage` |

## How it is built

- **CoreUI** supplies the sidebar, header, cards, tables, modals, toasts, form controls and the light/dark/auto colour
  mode. `src/theme.scss` overrides its Sass variables (warm paper, ink, one vermilion accent) so it does not look like the
  stock template; charts use `@coreui/react-chartjs` and follow the theme.
- **TanStack Query** owns server state: caching, polling (every 5 s on live pages) and invalidation after a mutation. No page
  runs its own timer.
- **Filters live in the URL** (`#/requests?status=Failed`), so a view can be shared.
- **Same origin.** `deploy/config/console.nginx.conf` serves the build and proxies `/v1` and `/healthz` to the Gateway, so no
  CORS is needed. SSE needs `proxy_buffering off`. nginx also sends the Content-Security-Policy; the page loads no inline script.
- **The key lives in `sessionStorage`**: gone when the tab closes, never in a cookie. A 401 signs you out.
- **Gateway strings are never HTML.** React escapes them, and a test forbids `dangerouslySetInnerHTML`.

```
src/api/      client (fetch, ApiError), events (SSE over fetch), hooks (React Query), types
src/auth/     sign-in, capability probe, tenant scope
src/layout/   CoreUI shell: sidebar, header, colour-mode switch
src/pages/    one file per page
src/ui/       providers (theme, toasts, confirm), badges, stat cards, tables, chart card
src/lib/      pure helpers (formatting, usage maths), unit-tested
```

## Checks

```bash
npm run typecheck     # tsc, strict
npm test              # vitest: formatting, usage maths, SSE parsing
npm run build         # typecheck + production bundle in dist/
uv run pytest tests/unit/test_console_static.py   # API paths exist in ROUTE_SCOPES, pins, Dockerfile, nginx
```

Not covered, because the Gateway has no endpoint for it: audit entries (they are log lines), Redis or RabbitMQ internals, and
editing configuration. Use the observability stack (Grafana, Prometheus, Jaeger; linked in the sidebar) for metric history.
