# DAS Helm chart

Runs the three DAS roles on Kubernetes: the Gateway API, the Manager agents and the Worker agents. Docker Compose
(`deploy/docker-compose.yml`) stays a supported way to run DAS; this chart is the Kubernetes one.

The chart does not deploy Redis or RabbitMQ. They are prerequisites, and every secret comes from a Secret that already
exists. Nothing in `values.yaml` is a secret.

## What it installs

| Object | Count |
|---|---|
| Gateway `Deployment` and `Service` | 1 (2 replicas by default) |
| Manager `Deployment` | 1 (2 replicas by default) |
| Worker `Deployment` | one per entry of `workers`, each with its own Capabilities |
| `PodDisruptionBudget` | one per Deployment (`maxUnavailable: 1`) |
| `ConfigMap` | one per role, holding that role's agent configuration |
| `ServiceAccount` | 1, unless `serviceAccount.create` is false |
| KEDA `ScaledObject` | one per Worker Deployment, only with `keda.enabled` |

Every pod sets `DAS__GRPC__ADVERTISE_HOST` from its own IP and `DAS__AGENT_ID` from its own name, and probes `/livez`
(startup, liveness) and `/readyz` (readiness) on its health port. The advertise host has to be the pod IP: the Gateway
reaches an agent at that address for drain and cancel, and a pod name is not reachable.

## Prerequisites

- **Helm 3** and a current Kubernetes cluster. The chart is tested on Kubernetes 1.37 (kind).
- **Redis**, as one instance, or as the master that Redis Sentinel names (see Redis Sentinel below). Redis Cluster is not
  supported: all DAS state is in Redis, and the state scripts update keys in different slots at once, which a cluster
  cannot run.
- **RabbitMQ 4.1 with the management plugin.** The Manager applies the delivery-limit policy through the management
  API, and the transport declares every queue itself, so no definitions import is needed.
- **The three images**, `das-gateway`, `das-manager` and `das-worker`, built from `deploy/docker/` and pushed where the
  cluster can pull them (`image.repository`, `image.tag`, `imagePullSecrets`).
- **KEDA**, only if you enable `keda.enabled`.

## Secrets

Create the Secret first and name it in `existingSecret`. The chart reads these keys:

| Key | Used for | Needed |
|---|---|---|
| `API_KEYS` | `DAS__API_KEYS`, the Gateway's API keys (comma separated) | yes, Gateway |
| `ADMIN_API_KEYS` | `DAS__ADMIN_API_KEYS`, the Admin API keys (comma separated) | no |
| `REDIS_URL` | `DAS__REDIS__URL`, when it carries credentials | yes, unless `redis.url` is set |
| `RABBITMQ_URL` | `DAS__RABBITMQ__URL`, when it carries credentials | yes, unless `rabbitmq.url` is set |
| `LLM_API_KEY` | `DAS__LLM__API_KEY`, for a hosted model provider | no |

```sh
kubectl create secret generic das-secrets \
  --from-literal=API_KEYS='<api key>[,<api key>...]' \
  --from-literal=ADMIN_API_KEYS='<admin key>' \
  --from-literal=REDIS_URL='redis://:<password>@<host>:6379/0' \
  --from-literal=RABBITMQ_URL='amqp://<user>:<password>@<host>:5672/'
```

`secretEnv` maps an environment variable to a key of that Secret, per role, so you can add or rename entries. A plain
`redis.url` or `rabbitmq.url` replaces the matching `secretEnv` entry: use them only for URLs without credentials.

The chart refuses to render, and says which path, when a value would put a secret in a manifest: a URL with credentials
in `redis.url`, `rabbitmq.url`, `rabbitmq.managementUrl`, `lifecycle.preStopDrain.gatewayUrl` or anywhere under
`config`, and any secret field of the agent configuration under `config` (`api_keys`, `admin_api_keys`,
`webhook_secrets`, `gateway.cursor_secret`, `llm.api_key` and the other `llm.aws_*` keys, `tracing.otlp_headers`,
`redis.password`, `redis.sentinel_password`, `http.token`, `http_broker.token`, `kafka.sasl_password` and
`kafka.ssl_key_password`). The
message never repeats the value.

## Install

```sh
helm install das deploy/helm/das -n das --create-namespace -f my-values.yaml
```

The smallest useful `my-values.yaml`:

```yaml
existingSecret: das-secrets
image: {repository: registry.example.com/das, tag: "1.0.0"}
redis: {url: redis://redis.infra:6379/0}
rabbitmq: {managementUrl: http://rabbitmq.infra:15672}
config:
  manager: {llm: {provider: openai, model: gpt-4o}}
  worker: {llm: {provider: openai, model: gpt-4o}}
```

`ci/values-min.yaml` and `ci/values-all.yaml` are two complete examples; the tests render both, and
`tests/k8s/values-kind.yaml` is the one the kind cluster tests install.

## Values

| Value | What it sets |
|---|---|
| `nameOverride`, `fullnameOverride` | Object names; the default is `<release>-das`, or just `<release>` when it already contains `das` |
| `image` | `repository` (one image per role, `<repository>-gateway` and so on), `tag` (default: the chart's appVersion), `pullPolicy` |
| `imagePullSecrets` | Pull secrets for the images |
| `existingSecret` | **Required.** The Secret every secret value is read from |
| `secretEnv` | Environment variable to Secret key, per role |
| `redis` | `url`, credential-free, for one instance; or `mode: sentinel` with `sentinels`, `masterName` and `db` |
| `rabbitmq` | `url` (credential-free) and `managementUrl`, which only the Manager receives |
| `config` | The agent configuration, `common` for every role and `gateway`, `manager`, `worker` for one; see below |
| `gateway` | `replicas`, `resources`, `service.type`, `service.port` |
| `manager` | `replicas`, `resources` |
| `workers` | One Deployment per entry: `name`, `capabilities`, `replicas`, `resources` |
| `terminationGracePeriodSeconds` | Time a pod gets to stop, see the grace period rule |
| `lifecycle` | `preStopDrain`, see below |
| `keda` | Autoscaling of the Worker Deployments, see below |
| `serviceAccount` | `create`, `name`, `annotations` |

`config` is rendered into one ConfigMap per role. Each file is built from defaults, then `config.common`, then
`config.<role>`, then the role itself, each layer overriding the one before; a change to it rolls the pods.

## Redis Sentinel

The chart runs no Redis, Sentinel included; it points the agents at yours. For a Sentinel-managed Redis set the mode and
name the sentinels, and leave `redis.url` empty:

```yaml
redis:
  mode: sentinel
  sentinels: [sentinel-0.redis.infra:26379, sentinel-1.redis.infra:26379, sentinel-2.redis.infra:26379]
  masterName: mymaster
secretEnv:
  manager: {DAS__REDIS__PASSWORD: REDIS_PASSWORD}   # and the same under gateway and worker
```

`sentinels` takes 1 to 10 `host:port`, `masterName` is the name the Sentinels monitor, and `db` (0 to 15) picks the
database. In this mode the chart leaves the `REDIS_URL` entry of `secretEnv` out, so that key need not be in your Secret.
The Redis and Sentinel passwords are secrets: `DAS__REDIS__PASSWORD` and `DAS__REDIS__SENTINEL_PASSWORD`, taken from the
Secret like every other. The chart refuses `mode: cluster`, a `url` beside `sentinels`, `sentinels` or `masterName`
without `mode: sentinel`, and a sentinel address that carries credentials. The key layout did not change, so there is
nothing to migrate when you switch a running installation to Sentinel.

## Tenants

Tenants are configuration, not chart values of their own: put them under `config.gateway.tenants`, exactly as in the
commented example of `deploy/config/gateway.yaml`.

```yaml
config:
  gateway:
    tenants:
      - id: team-a
        keys:
          - fingerprint: "484857e0"        # first 8 hex characters of sha256(api key), never the key
            scopes: [submit, read]
        max_active_requests: 200
```

A key is named by its fingerprint (`printf %s "$KEY" | shasum -a 256 | cut -c1-8`). The key itself stays in the Secret
named by `existingSecret`, listed in `API_KEYS`. A fingerprint that matches no key, an admin key, or two tenants stops the
Gateway at startup.

One ordering rule: configure tenants only after a Manager has logged `tenant_index_backfilled`. That happens once, on the
first start after an upgrade from a release without tenants; until then a listing for a tenant is refused with
`503 index_not_ready`.

## The grace period rule

A stopping pod gets SIGTERM, then SIGKILL after `terminationGracePeriodSeconds` (default 45). The agents need
`limits.shutdown_timeout` (default 30) to finish what they run, plus 10 seconds for the rest of the shutdown. The chart
refuses to render when `terminationGracePeriodSeconds` is less than `limits.shutdown_timeout` plus 10 seconds, so raise
both together.

## Autoscaling with KEDA

With `keda.enabled`, each Worker Deployment gets a `ScaledObject` with one RabbitMQ trigger per Capability, on the queue
`das.jobs.<capability>`. KEDA reads the broker URL from the Worker's own `DAS__RABBITMQ__URL`, so no second credential
exists. While KEDA is on, the Deployments leave `replicas` to it, so `helm upgrade` does not reset what it scaled.

| Value | Meaning |
|---|---|
| `keda.enabled` | Create the `ScaledObject`s |
| `keda.queueLength` | Messages per replica the scaler aims for |
| `keda.pollingInterval` | Seconds between queue checks |
| `keda.minReplicaCount` | Never fewer replicas than this. Default 1; see below |
| `keda.maxReplicaCount` | Never more replicas than this |

Keep `keda.minReplicaCount` at 1 or more. A Manager dispatches a Job only when a registered Worker has its Capability,
and fails it as `no capable worker` after `limits.capability_wait`. A pool scaled to zero therefore never lets its queue
grow, and nothing would scale it back up.

## Draining a pod before it stops

`lifecycle.preStopDrain.enabled` adds a `preStop` hook to the Manager and Worker pods (not the Gateway, which is not an
agent) that runs `das drain` for the pod's own agent id, so no new Job is sent to it before SIGTERM. The hook always
exits 0: a failed drain never keeps a pod from stopping.

| Value | Meaning |
|---|---|
| `lifecycle.preStopDrain.enabled` | Add the hook |
| `lifecycle.preStopDrain.gatewayUrl` | Where to send the drain; default the chart's own Gateway Service |
| `lifecycle.preStopDrain.adminKeySecretKey` | The key of `existingSecret` that holds the Admin API key |

The hook puts one Admin API key into the environment of every Manager and Worker pod. Use a dedicated admin key in its
own Secret key, listed in `ADMIN_API_KEYS` as well, rather than pointing this at the whole list. If the Secret key holds
a comma separated list, the hook uses its first key.

## A Worker that is killed repeatedly

A Job is returned to the queue when its Worker dies without acknowledging it (a node loss, an out-of-memory kill, a
rolling restart). RabbitMQ counts those returns, and after the delivery limit it dead-letters the Job instead of
delivering it again. The limit is `max_attempts - 1`, so with the default `max_attempts: 3` a Job whose Worker is killed
three times in a row ends as a dead letter, and the Manager records it for inspection and replay through the admin API.
A Worker that keeps being killed on the same Job therefore does not loop for ever; look for the dead letter, and for
why the pod keeps dying, before replaying it.

## Testing the chart

```sh
helm lint deploy/helm/das -f deploy/helm/das/ci/values-all.yaml
uv run pytest tests/unit/test_chart_templates.py tests/unit/test_chart_skeleton.py tests/unit/test_chart_secrets.py
DAS_K8S_E2E=1 uv run pytest tests/integration/k8s        # a throwaway kind cluster; see CLAUDE.md
```

The render tests use the real `helm` and are skipped without it, except when `CI=true`.
