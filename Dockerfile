# syntax=docker/dockerfile:1

# ---- build stage: compile a static binary ----
FROM golang:1.26-alpine AS build
WORKDIR /src

# The module has no third-party dependencies today, but keep the download
# step so adding any is cached across rebuilds.
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kv-store ./cmd/server

# ---- runtime stage: minimal, non-root ----
FROM alpine:3.21
RUN apk add --no-cache ca-certificates \
    && addgroup -S kv \
    && adduser -S -G kv kv \
    && mkdir -p /data \
    && chown kv:kv /data
COPY --from=build /out/kv-store /usr/local/bin/kv-store

ENV DATA_DIR=/data
USER kv
EXPOSE 8081

# Liveness: the process is up and serving. Readiness (leader known in a
# cluster) is checked by docker-compose via /readyz.
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=6 \
  CMD wget -qO- http://127.0.0.1:8081/healthz || exit 1

ENTRYPOINT ["kv-store"]