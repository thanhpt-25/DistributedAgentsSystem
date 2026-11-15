# Admin console image: builds the React app, serves it with nginx and proxies /v1 and /healthz to the Gateway.
# Build from the repo root:
#   docker build -f deploy/docker/console.Dockerfile -t das-console:local .
#
# Base images are pinned by digest, so a rebuild cannot pick up a different image; the tag is only a label for humans.
# To upgrade one, change the digest (`docker buildx imagetools inspect <image> --format '{{.Manifest.Digest}}'`).
#   build stage: node:22-alpine           = Node 22.23.5
#   runtime:     nginx-unprivileged:1.27  = nginx 1.27.5, runs as uid 101 (not root) and listens on 8080
FROM node:22-alpine@sha256:1314fb30ec37592d5845e5a160d522c0e680972aa534549940b8d73bde88481a AS build
WORKDIR /app
COPY console/package.json console/package-lock.json ./
RUN npm ci --no-audit --no-fund
# Named inputs, not `COPY console/`: the host's node_modules and dist must never leak into the build.
COPY console/index.html console/tsconfig.json console/vite.config.ts ./
COPY console/src ./src
RUN npm run build

FROM nginxinc/nginx-unprivileged:1.27-alpine@sha256:65e3e85dbaed8ba248841d9d58a899b6197106c23cb0ff1a132b7bfe0547e4c0
# Where the Gateway is and which DNS server resolves it: the defaults suit Docker Compose. Set both for another
# runtime (Kubernetes: the Service URL and the cluster DNS address). The image's entrypoint fills them into the template.
ENV DAS_GATEWAY_URL=http://gateway:8080 \
    DAS_RESOLVER=127.0.0.11
COPY deploy/config/console.nginx.conf /etc/nginx/templates/default.conf.template
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=5 CMD wget -q -O /dev/null http://127.0.0.1:8080/ || exit 1
