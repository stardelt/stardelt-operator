# stardelt-operator image: the Rust controller plus the tools it drives.
#
# The operator applies upstream Helm charts, so `helm` must be on PATH inside
# the image. kubectl is not needed — the controller talks to the API directly.
FROM rust:bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY crates ./crates
RUN cargo build --release -p stardelt-operator

FROM debian:bookworm-slim
ARG HELM_VERSION=v3.18.4
ARG TARGETARCH=amd64
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && curl -fsSL "https://get.helm.sh/helm-${HELM_VERSION}-linux-${TARGETARCH}.tar.gz" \
      | tar -xz -C /tmp \
 && mv "/tmp/linux-${TARGETARCH}/helm" /usr/local/bin/helm \
 && rm -rf /tmp/linux-${TARGETARCH} \
 && helm version --short

COPY --from=build /src/target/release/stardelt-operator /usr/local/bin/stardelt-operator

# No shell, no root.
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/stardelt-operator"]
