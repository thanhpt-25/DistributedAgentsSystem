# Manager_Agent image (Req 14.3). Build from the repo root:
#   docker build -f deploy/docker/manager.Dockerfile -t das-manager .
# Role, ports and credentials come only from Agent_Config (mounted at
# /etc/das/config.yaml) and DAS_* environment variables.

# ---- build stage: resolve dependencies from uv.lock -------------------------
FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.8.4 /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv

WORKDIR /src

# Third-party dependencies first so this layer is cached across code changes.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project --no-editable

# Then the project itself (non-editable, so /src is not needed at runtime).
COPY das ./das
RUN uv sync --frozen --no-dev --no-editable

# ---- runtime stage ----------------------------------------------------------
FROM python:3.12-slim

RUN groupadd --system --gid 10001 das \
 && useradd --system --uid 10001 --gid das --no-create-home --shell /usr/sbin/nologin das

COPY --from=builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER das:das

# Default health_port; override via Agent_Config. The gRPC port is set by
# grpc.bind in Agent_Config and published by Compose.
EXPOSE 8081

ENTRYPOINT ["das"]
CMD ["run", "--config", "/etc/das/config.yaml"]
