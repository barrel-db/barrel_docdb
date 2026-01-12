# barrel_docdb Docker image
# Multi-stage build for minimal runtime image

# Build stage - use official Debian-based Erlang image
FROM erlang:27 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    make \
    gcc \
    g++ \
    libsnappy-dev \
    liblz4-dev \
    libzstd-dev \
    cmake \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy rebar config first for better layer caching
COPY rebar.config rebar.lock ./

# Copy source, include, and config
COPY src ./src
COPY include ./include
COPY config ./config

# Initialize a dummy git repo (needed by rocksdb compile hooks)
RUN git init && \
    git config user.email "build@docker" && \
    git config user.name "Docker Build" && \
    git add -A && \
    git commit -m "build" --allow-empty

# Build production release
RUN rebar3 as prod release

# Runtime stage - use Debian slim for better compatibility
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openssl \
    libncurses6 \
    libstdc++6 \
    libsnappy1v5 \
    liblz4-1 \
    libzstd1 \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy release from builder
COPY --from=builder /app/_build/prod/rel/barrel_docdb ./

# Environment variables with defaults
ENV BARREL_DATA_DIR=/data
ENV BARREL_HTTP_PORT=8080
ENV BARREL_HTTP_ENABLED=true
ENV BARREL_HTTP_ACCEPTORS=100
ENV BARREL_HTTP_MAX_CONNECTIONS=infinity
# TLS/HTTP2 configuration (disabled by default)
ENV BARREL_HTTP_TLS_ENABLED=false
# ENV BARREL_HTTP_CERTFILE=/certs/server.pem
# ENV BARREL_HTTP_KEYFILE=/certs/server-key.pem
# ENV BARREL_HTTP_CACERTFILE=/certs/ca.pem
ENV BARREL_HTTP_VERIFY=verify_none
# Node configuration
ENV BARREL_NODE_NAME=barrel@localhost
ENV BARREL_COOKIE=barrel_secret
ENV BARREL_LOG_LEVEL=info
ENV BARREL_ZONE=

# Expose HTTP and HTTPS ports
EXPOSE 8080 8443

# Volumes for data persistence and TLS certificates
VOLUME ["/data", "/certs"]

# Health check
HEALTHCHECK --interval=5s --timeout=5s --start-period=60s --retries=5 \
    CMD wget -q -O /dev/null http://localhost:${BARREL_HTTP_PORT}/health || exit 1

# Create data directory
RUN mkdir -p /data && chmod 755 /data

# Start barrel_docdb in foreground
CMD ["bin/barrel_docdb", "foreground"]
