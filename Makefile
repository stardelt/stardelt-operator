# stardelt-operator — build / check / package.

IMAGE ?= ghcr.io/stardelt/operator:dev
CRD_OUT ?= chart/templates/crd.yaml

.PHONY: help check fmt clippy build run crd image chart-lint

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

check: ## cargo check + clippy + fmt --check (CI gate)
	cargo fmt --check
	cargo clippy --all-targets -- -D warnings
	cargo check --all-targets

fmt: ## Format the code
	cargo fmt

clippy: ## Lint with warnings denied
	cargo clippy --all-targets -- -D warnings

build: ## Release build
	cargo build --release

run: ## Run the controller against the ambient kubeconfig
	cargo run

crd: ## Print the PlatformInstance CRD
	cargo run --quiet -- crd

regen-crd: ## Regenerate the CRD into the chart (re-add the installCRD guard by hand)
	@echo "Regenerating $(CRD_OUT) — remember to wrap it in the {{- if .Values.installCRD }} guard"
	cargo run --quiet -- crd > $(CRD_OUT)

image: ## Build the operator image
	docker build -t $(IMAGE) -f Dockerfile .

chart-lint: ## Lint the Helm chart
	helm lint chart
