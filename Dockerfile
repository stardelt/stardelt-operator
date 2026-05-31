# syntax=docker/dockerfile:1.7
#
# Multi-stage build for the stardelt-operator, mirroring stardelt-nova's image.

# ------- Stage 1: build -------
# Pin to bookworm so the build-stage glibc matches the bookworm-slim runtime
# below. The unqualified `rust:1-slim` tracks the latest Debian (trixie,
# glibc 2.39), which produces a binary that fails on bookworm (glibc 2.36) with
# `version GLIBC_2.39 not found`.
FROM rust:1-bookworm AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config && rm -rf /var/lib/apt/lists/*

# Cache dependencies: copy manifests, build a stub, then the real sources.
COPY Cargo.toml Cargo.lock* ./
COPY crates/stardelt-operator/Cargo.toml crates/stardelt-operator/
RUN mkdir -p crates/stardelt-operator/src && \
    echo "fn main() {}" > crates/stardelt-operator/src/main.rs && \
    cargo build --release -p stardelt-operator && \
    rm -rf crates/stardelt-operator/src \
       target/release/deps/stardelt_operator* \
       target/release/stardelt-operator
COPY crates/stardelt-operator/src crates/stardelt-operator/src
RUN cargo build --release -p stardelt-operator

# ------- Stage 2: minimal runtime -------
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/stardelt-operator /usr/local/bin/stardelt-operator
ENV RUST_LOG=info
ENTRYPOINT ["/usr/local/bin/stardelt-operator"]
