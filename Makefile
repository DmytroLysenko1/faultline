SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

# The user-level go env may pin an older toolchain; let go.mod's `toolchain` line decide.
export GOTOOLCHAIN := auto

BIN := $(CURDIR)/bin

GOLANGCI_LINT_VERSION := v2.15.0
BUF_VERSION := v1.70.0

GOLANGCI_LINT := $(BIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)
BUF := $(BIN)/buf-$(BUF_VERSION)

KIND_CLUSTER := faultline
KIND_CONFIG := deploy/kind/kind-config.yaml

PROTO_FILES := $(shell find api/proto -name '*.proto' 2>/dev/null)
GO_PACKAGES := $(shell GOTOOLCHAIN=auto go list ./... 2>/dev/null)

##@ General

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n"} \
		/^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Tools

.PHONY: tools
tools: $(GOLANGCI_LINT) $(BUF) ## Install pinned external tools into ./bin

$(GOLANGCI_LINT):
	@mkdir -p $(BIN)
	GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	mv $(BIN)/golangci-lint $@

$(BUF):
	@mkdir -p $(BIN)
	GOBIN=$(BIN) go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	mv $(BIN)/buf $@

##@ Development

.PHONY: fmt
fmt: $(GOLANGCI_LINT) ## Format Go code (gofmt + goimports)
	$(GOLANGCI_LINT) fmt

.PHONY: generate
generate: $(BUF) ## Generate protobuf/connect code and bpf2go bindings
ifneq ($(PROTO_FILES),)
	PATH="$(BIN):$$PATH" $(BUF) generate
endif
ifneq ($(GO_PACKAGES),)
	go generate ./...
endif

##@ Verification

.PHONY: verify
verify: mod-check lint test-race test-probes cover-check vuln proto-lint gen-check ## Everything CI runs

.PHONY: mod-check
mod-check: ## Fail if go.mod/go.sum are not tidy
	go mod tidy -diff

.PHONY: lint
lint: $(GOLANGCI_LINT) ## Run golangci-lint
ifneq ($(GO_PACKAGES),)
	$(GOLANGCI_LINT) run ./...
else
	@echo "lint: no Go packages yet, skipping"
endif

.PHONY: test
test: ## Run unit tests
ifneq ($(GO_PACKAGES),)
	go test ./...
else
	@echo "test: no Go packages yet, skipping"
endif

.PHONY: test-race
test-race: ## Run unit tests with the race detector and coverage
ifneq ($(GO_PACKAGES),)
	go test -race -covermode=atomic -coverprofile=coverage.out ./...
else
	@echo "test-race: no Go packages yet, skipping"
endif

PROBE_PACKAGES := ./internal/interfaces/otlp/decodelimit/...
BUDGET_PACKAGES := ./internal/domain/topology/... ./internal/infrastructure/memjournal/... \
	./internal/interfaces/connectapi/... ./internal/infrastructure/selftrace/...
DRAIN_PACKAGES := ./test/pipeline/...

.PHONY: test-probes
test-probes: ## Heap and time probes without -race: decode limit, memory budget model, then pipeline drain
	go test -count=1 -run 'TestWeightBounds' $(PROBE_PACKAGES)
	go test -count=1 -run 'TestWorstCaseHeap|TestViewHeap' $(BUDGET_PACKAGES)
	go test -count=1 -run 'TestDrainFullJournal' $(DRAIN_PACKAGES)
	go test -count=1 -run 'TestLargestAllowedGraph' ./cmd/faultctl/

STRESS_PACKAGES := ./internal/application/ingest/... ./internal/application/processing/... \
	./internal/infrastructure/memjournal/... ./internal/infrastructure/memgraph/... ./test/pipeline/...
STRESS_COUNT := 200
SHUTDOWN_TESTS := TestShutdownOrder|TestDrainIncomplete|TestSignalAfterPipelineExit|TestEveryComponent|TestOTLPStuckAfterStop|\
	TestSlowComponentsAtTheEndOfShutdown|TestStatsReporter|TestStopOnATick

.PHONY: test-stress
test-stress: ## Repeat concurrency-sensitive tests under the race detector (nightly CI, before a tag)
	go test -race -count=$(STRESS_COUNT) $(STRESS_PACKAGES)
	go test -race -count=$(STRESS_COUNT) -run '$(SHUTDOWN_TESTS)' ./cmd/faultline/

.PHONY: cover-check
cover-check: test-race ## Enforce per-package coverage thresholds (NFR-MAINT-001)
ifneq ($(GO_PACKAGES),)
	./hack/cover-check.sh coverage.out
else
	@echo "cover-check: no Go packages yet, skipping"
endif

.PHONY: vuln
vuln: ## Scan reachable code for known vulnerabilities
ifneq ($(GO_PACKAGES),)
	go tool govulncheck ./...
else
	@echo "vuln: no Go packages yet, skipping"
endif

.PHONY: proto-lint
proto-lint: $(BUF) ## Lint protobuf API contract
ifneq ($(PROTO_FILES),)
	$(BUF) lint
else
	@echo "proto-lint: no .proto files yet, skipping"
endif

.PHONY: proto-breaking
proto-breaking: $(BUF) ## Check API compatibility against the last release tag (NFR-MAINT-003)
ifneq ($(PROTO_FILES),)
	@tag="$$(git describe --tags --abbrev=0 --match 'v*' 2>/dev/null || true)"; \
	if [[ -z "$$tag" ]]; then echo "proto-breaking: no release tag v* yet, skipping"; exit 0; fi; \
	echo "proto-breaking: against $$tag"; \
	$(BUF) breaking --against ".git#tag=$$tag"
else
	@echo "proto-breaking: no .proto files yet, skipping"
endif

.PHONY: gen-check
gen-check: generate ## Fail if generated code is stale
	@if [[ -n "$$(git status --porcelain -- internal/gen '*_bpfel.go' '*_bpfeb.go' '*_bpfel.o' '*_bpfeb.o')" ]]; then \
		echo "generated code is stale: run 'make generate' and commit"; exit 1; fi

##@ Local environment

.PHONY: kind-up
kind-up: ## Create local kind cluster (profile L)
	kind get clusters | grep -qx $(KIND_CLUSTER) || kind create cluster --name $(KIND_CLUSTER) --config $(KIND_CONFIG)
	kubectl --context kind-$(KIND_CLUSTER) get nodes -o wide

.PHONY: kind-down
kind-down: ## Delete local kind cluster
	kind delete cluster --name $(KIND_CLUSTER)

.PHONY: demo-capture
demo-capture: ## Step 1 slice 0: record OTel Demo traces in kind (CAPTURE_DURATION=600)
	./hack/demo-capture.sh

.PHONY: demo-down
demo-down: ## Delete the demo kind cluster
	kind delete cluster --name faultline-demo

.PHONY: btf-check
btf-check: ## Check that the Linux VM kernel supports eBPF CO-RE (BTF, cgroup v2)
	./hack/check-btf.sh

.PHONY: clean
clean: ## Remove build and test artifacts
	rm -rf $(BIN) dist coverage.out
