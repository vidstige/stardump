FROM lukemathwalker/cargo-chef:latest-rust-1.94-bookworm AS chef
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked --bin ingest --bin build-index --bin query-api

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --uid 10001 stardump

COPY --from=build /app/target/release/ingest /usr/local/bin/ingest
COPY --from=build /app/target/release/build-index /usr/local/bin/build-index
COPY --from=build /app/target/release/query-api /usr/local/bin/query-api
COPY sh/ingest-job-entrypoint.sh /usr/local/bin/ingest-job
COPY sh/build-index-job-entrypoint.sh /usr/local/bin/build-index-job
RUN chmod 755 /usr/local/bin/ingest-job /usr/local/bin/build-index-job

USER stardump
ENTRYPOINT ["/usr/local/bin/query-api"]
