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
FROM rust:1.85-slim AS builder

# musl toolchain for static linking. ~30 MB extra in the builder layer
# but doesn't ship to the runtime image.
RUN apt-get update && \
    apt-get install -y --no-install-recommends musl-tools && \
    rm -rf /var/lib/apt/lists/* && \
    rustup target add x86_64-unknown-linux-musl

WORKDIR /build
COPY arbitro/ ./arbitro/
COPY arbitro-kit/ ./arbitro-kit/

WORKDIR /build/arbitro
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/arbitro/target \
    cargo build --release --target x86_64-unknown-linux-musl -p arbitro-server && \
    cp target/x86_64-unknown-linux-musl/release/arbitro-server /tmp/arbitro-server

# Runtime stage — `scratch` is empty. Statically linked binary needs
# nothing else. Final image ≈ binary size (~2 MB) + a few KB metadata.
FROM scratch
COPY --from=builder /tmp/arbitro-server /arbitro-server
EXPOSE 9898
ENTRYPOINT ["/arbitro-server"]
