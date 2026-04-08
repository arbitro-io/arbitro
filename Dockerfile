# Build stage
FROM rust:1.82-slim AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p arbitro-server

# Runtime stage
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/arbitro-server /usr/local/bin/
EXPOSE 9898
ENTRYPOINT ["arbitro-server"]
