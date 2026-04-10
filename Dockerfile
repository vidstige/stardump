FROM rust:1.94-bookworm AS build
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --bin ingest --bin query-api

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/target/release/ingest /usr/local/bin/ingest
COPY --from=build /app/target/release/query-api /usr/local/bin/query-api

ENTRYPOINT ["/usr/local/bin/query-api"]
