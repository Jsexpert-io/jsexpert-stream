FROM rust:1.98-slim-bookworm AS builder
WORKDIR /app
RUN apt-get update \
  && apt-get install -y --no-install-recommends build-essential cmake pkg-config libcurl4-openssl-dev libssl-dev \
  && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && printf 'fn main() {}\n' > src/main.rs && cargo build --release && rm -rf src
COPY src ./src
RUN cargo test --release && cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates wget \
  && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/jsexpert-stream /usr/local/bin/jsexpert-stream
ENV RUST_LOG=info
EXPOSE 4002
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/jsexpert-stream"]
