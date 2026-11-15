# Admin console image: builds the React app, serves it with nginx and proxies /v1 and /healthz to the Gateway.
# Build from the repo root:
#   docker build -f deploy/docker/console.Dockerfile -t das-console:local .
FROM node:22-alpine AS build
WORKDIR /app
COPY console/package.json console/package-lock.json ./
RUN npm ci --no-audit --no-fund
# Named inputs, not `COPY console/`: the host's node_modules and dist must never leak into the build.
COPY console/index.html console/tsconfig.json console/vite.config.ts ./
COPY console/src ./src
RUN npm run build

FROM nginx:1.27-alpine
COPY deploy/config/console.nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
