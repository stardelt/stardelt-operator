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

# Helm needs writable cache/config/data directories; the container runs as a
# non-root user with no home. Without these, `helm --repo` fails with
# "open /.cache/helm/repository/...: no such file or directory".
ENV HOME=/home/operator \
    HELM_CACHE_HOME=/home/operator/.cache \
    HELM_CONFIG_HOME=/home/operator/.config \
    HELM_DATA_HOME=/home/operator/.local/share
RUN mkdir -p /home/operator/.cache /home/operator/.config /home/operator/.local/share \
 && chown -R 65532:65532 /home/operator

# No shell, no root.
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/stardelt-operator"]
