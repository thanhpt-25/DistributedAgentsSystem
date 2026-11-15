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
- **The image** (`deploy/docker/console.Dockerfile`) pins both base images by digest, runs as the non-root `nginx` user and
  listens on 8080. The nginx config is a template: `DAS_GATEWAY_URL` (default `http://gateway:8080`) and `DAS_RESOLVER`
  (default `127.0.0.11`, Docker's DNS) point it at the Gateway, so the same image runs outside Compose (on Kubernetes, set
  the Service URL and the cluster DNS address). There is no Helm chart for the console yet.
- **The key lives in `sessionStorage`**: gone when the tab closes, never in a cookie. The Gateway rejecting it (401 or 403)
  signs you out; an outage does not, and the live feed backs off from 2 s to 30 s while the Gateway is away.
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
npm run typecheck       # tsc, strict
npm test                # Vitest: the pure logic, and every page in jsdom against a stubbed Gateway
npm run test:coverage   # the same, with a coverage floor (statements and lines 85%, functions 80%, branches 70%)
npm run build           # typecheck + production bundle in dist/
npm run e2e:install     # once: the Chromium that Playwright drives
npm run e2e             # ten browser flows against dist/ and e2e/mock-gateway.mjs (run `npm run build` first)
uv run pytest tests/unit/test_console_static.py   # API paths exist in ROUTE_SCOPES, pins, Dockerfile, nginx
```

The component tests (`src/**/*.test.tsx`) render a page inside the app's real providers and answer `fetch` from a table of
routes (`src/test/gateway.tsx`); an unlisted call answers 404. The browser tests (`e2e/`) cover the critical flows end to end:
sign-in, the live feed, submit, cancel, drain, replay, a read-only key, a Gateway outage during a reload, and the colour mode.

Not covered, because the Gateway has no endpoint for it: audit entries (they are log lines), Redis or RabbitMQ internals, and
editing configuration. Use the observability stack (Grafana, Prometheus, Jaeger; linked in the sidebar) for metric history.
