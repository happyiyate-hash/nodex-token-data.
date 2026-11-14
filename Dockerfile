FROM rust:1-bookworm AS builder
WORKDIR /app

COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/nodex-token-data /usr/local/bin/nodex-token-data

ENV NODEX_BIND_ADDR=0.0.0.0:8081
EXPOSE 8081
CMD ["nodex-token-data"]
