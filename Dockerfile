# syntax=docker/dockerfile:1.7
#
# Build stage — statically linked against musl libc so the runtime stage
# can be `scratch`. Workspace context = arbitro-io/ (parent of arbitro/).
#
# Build context excludes are tracked at `arbitro/.dockerignore`. Docker
# only reads `.dockerignore` at the context ROOT, so before `docker build`:
#
#   cp arbitro/.dockerignore .dockerignore
#
# CI runs that step automatically (.github/workflows/ci.yml). Without it,
# the local context can grow to >4 GB by sweeping in `target/` artifacts.
FROM rust:1.88-slim AS builder

# musl toolchain for static linking. ~30 MB extra in the builder layer
# but doesn't ship to the runtime image.
RUN apt-get update && \
    apt-get install -y --no-install-recommends musl-tools && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY arbitro/ ./arbitro/
COPY arbitro-kit/ ./arbitro-kit/

WORKDIR /build/arbitro
# rust-toolchain.toml may re-sync the toolchain on first cargo invocation,
# so install the musl target AFTER the COPY to guarantee it matches the
# active toolchain.
RUN rustup target add x86_64-unknown-linux-musl
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/arbitro/target \
    cargo build --release --target x86_64-unknown-linux-musl -p arbitro-server && \
    cp target/x86_64-unknown-linux-musl/release/arbitro-server /tmp/arbitro-server

# Runtime stage — `scratch` is empty. Statically linked binary needs
# nothing else. Final image ≈ binary size (~2 MB) + a few KB metadata.
FROM scratch
COPY --from=builder /tmp/arbitro-server /arbitro-server
EXPOSE 9898
# M26: run as non-root. `scratch` has no /etc/passwd so we use a
# numeric UID/GID — 65532 is the conventional "nonroot" value used by
# distroless. Containers running on k8s with `runAsNonRoot: true` need
# this, and there's no reason to ship root by default.
USER 65532:65532
# Explicit signal so `docker stop` / k8s preStop triggers the broker's
# graceful shutdown (signal handler in main.rs).
STOPSIGNAL SIGTERM
ENTRYPOINT ["/arbitro-server"]
