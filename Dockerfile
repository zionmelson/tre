# syntax=docker/dockerfile:1.6

ARG RUST_VERSION=1.77
ARG APP_NAME=tre-arb-bot

FROM rust:${RUST_VERSION}-slim-bullseye AS build
WORKDIR /app
ENV PKG_CONFIG_ALLOW_CROSS=1
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    ca-certificates \
    clang \
    cmake \
    pkg-config \
    protobuf-compiler \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock build.rs ./
COPY bitquery ./bitquery
COPY jito ./jito
COPY src ./src
COPY idls ./idls
COPY docs ./docs
COPY alt_registry.json config.template.json ./

RUN cargo build --locked --release --bin ${APP_NAME}

FROM debian:bookworm-slim AS runtime
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /app runner

COPY --from=build /app/target/release/${APP_NAME} /usr/local/bin/${APP_NAME}
COPY --from=build /app/alt_registry.json /app/alt_registry.json
COPY --from=build /app/config.template.json /app/config.template.json
COPY --from=build /app/idls /app/idls
COPY --from=build /app/docs /app/docs

USER runner
ENV ARBITRAGE_CONFIG=/config/config.json
ENV TRE_ALT_REGISTRY=/data/alt_registry.json
ENV PROMETHEUS_METRICS_ADDR=0.0.0.0:9898
ENV RUST_LOG=warn,tre-arb-bot=info

VOLUME ["/config", "/data"]
EXPOSE 9898

ENTRYPOINT ["tre-arb-bot"]
