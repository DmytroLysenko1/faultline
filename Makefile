SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

export GOTOOLCHAIN := auto

BIN := $(CURDIR)/bin

GOLANGCI_LINT_VERSION := v2.15.0
BUF_VERSION := v1.70.0
NILAWAY_VERSION := v0.0.0-20260710181136-2378218750e4

GOLANGCI_LINT := $(BIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)
BUF := $(BIN)/buf-$(BUF_VERSION)
NILAWAY := $(BIN)/nilaway-$(NILAWAY_VERSION)

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

$(NILAWAY):
	@mkdir -p $(BIN)
	GOBIN=$(BIN) go install go.uber.org/nilaway/cmd/nilaway@$(NILAWAY_VERSION)
	mv $(BIN)/nilaway $@

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
verify: verify-static verify-tests-group verify-coverage verify-probes ## Everything CI runs

.PHONY: verify-static
verify-static: mod-check lint vuln proto-lint gen-check core-diff-check shutdown-tests-check ci-verify-check ## Static checks, one CI job

.PHONY: verify-tests
verify-tests: verify-tests-group verify-coverage ## Race tests and coverage thresholds on one machine

.PHONY: verify-tests-group
verify-tests-group: test-race ## Race tests of RACE_GROUP of RACE_GROUPS (all of them without RACE_GROUP), one CI matrix job per group

.PHONY: verify-probes
verify-probes: test-probes ## Heap and time probes, one CI job

.PHONY: ci-verify-check
ci-verify-check: ## Fail when a prerequisite of verify is not run by exactly one job of ci.yml
	./hack/ci-verify-check.sh Makefile .github/workflows/ci.yml

.PHONY: shutdown-tests-check
shutdown-tests-check: ## Fail when the SHUTDOWN_TESTS pattern has a space or selects no test for a listed name
	@case '$(SHUTDOWN_TESTS)' in *' '*) echo "SHUTDOWN_TESTS has a space: $(SHUTDOWN_TESTS)"; exit 1;; esac
	@listed="$$(go test -list '$(SHUTDOWN_TESTS)' ./cmd/faultline/ 2>&1)" || { printf '%s\n' "$$listed"; exit 1; }; \
	for name in $(SHUTDOWN_NAMES); do \
		printf '%s\n' "$$listed" | grep -q "^$$name$$" || { echo "SHUTDOWN_TESTS selects no $$name"; exit 1; }; \
	done

.PHONY: core-diff-check
core-diff-check: ## Fail when a core file changed since v0.2.0 without a line in hack/core-diff.allow
	./hack/core-diff-check.sh v0.2.0

.PHONY: nilaway
nilaway: $(NILAWAY) ## Report possible nil panics (advisory, not part of verify)
	-$(NILAWAY) -include-pkgs=github.com/DmytroLysenko1/faultline ./...

.PHONY: mod-check
mod-check: ## Fail if go.mod/go.sum are not tidy
	go mod tidy -diff

.PHONY: lint
LINESTYLE_BASE ?= $(shell git rev-parse --quiet --verify '@{upstream}' >/dev/null 2>&1 && echo '@{upstream}' || echo HEAD)

lint: $(GOLANGCI_LINT) ## Run golangci-lint and the one-line style check on changed lines
ifneq ($(GO_PACKAGES),)
	$(GOLANGCI_LINT) run ./...
	./hack/linestyle-check.sh "$(LINESTYLE_BASE)"
	@if command -v shellcheck >/dev/null; then shellcheck -x hack/*.sh; else echo "lint: shellcheck not installed, skipping hack/*.sh"; fi
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

.PHONY: test-durations
test-durations: ## Refresh hack/test-durations.txt, the package and RACE_SHARDS test times race-shards.sh balances shards and groups by
	./hack/test-durations.sh hack/test-durations.txt $(RACE_SHARD_PACKAGES)

.PHONY: test-race
RACE_SHARDS := ./cmd/faultline:4 ./internal/infrastructure/natsjs/feed:3 ./internal/infrastructure/natsjs/checkpoint:3 ./pkg/stream:2
RACE_SHARD_PACKAGES := $(foreach shard,$(RACE_SHARDS),$(firstword $(subst :, ,$(shard))))
RACE_GROUPS := 3
RACE_GROUP ?=
RACE_PROFILE = $(if $(RACE_GROUP),coverage-$(RACE_GROUP).out,coverage.out)

test-race: ## Run unit tests with the race detector and coverage; the slowest packages run in parallel shards (RACE_SHARDS)
ifneq ($(GO_PACKAGES),)
	RACE_SHARDS="$(RACE_SHARDS)" RACE_GROUP="$(RACE_GROUP)" RACE_GROUPS="$(RACE_GROUPS)" ./hack/race-shards.sh $(RACE_PROFILE)
else
	@echo "test-race: no Go packages yet, skipping"
endif

PROBE_PACKAGES := ./internal/interfaces/otlp/decodelimit/...
BUDGET_PACKAGES := ./internal/domain/topology/... ./internal/infrastructure/journaltest/... \
	./internal/interfaces/connectapi/... ./internal/infrastructure/selftrace/...
DRAIN_PACKAGES := ./test/pipeline/...

.PHONY: test-probes
test-probes: ## Heap and time probes without -race: decode limit, token parse allocations, memory budget model, then pipeline drain
	go test -count=1 -run 'TestWeightBounds' $(PROBE_PACKAGES)
	go test -count=1 -run 'TestParseDoesNotAllocate' ./internal/domain/credential/
	go test -count=1 -run 'TestWorstCaseHeap|TestViewHeap' $(BUDGET_PACKAGES)
	go test -count=1 -run 'TestDrainFullJournal' $(DRAIN_PACKAGES)
	go test -count=1 -run 'TestLargestAllowedGraph' ./cmd/faultctl/

STRESS_PACKAGES := ./internal/application/ingest/... ./internal/application/processing/... \
	./internal/infrastructure/journaltest/... ./internal/infrastructure/memgraph/... ./test/pipeline/...
STRESS_COUNT := 200
STRESS_TIMEOUT := 30m
STREAM_STRESS_COUNT := 2
STREAM_STRESS_TIMEOUT := 20m
KILL_NAMES := TestAKilledProcessorRestartsToTheSameGraphAsAnUninterruptedRun TestAKilledIngesterLosesNoAcknowledgedRequest \
	TestALeaseLostWhileServingStopsWithoutRelease
KILL_STRESS_POINTS := 10
KILL_STRESS_TIMEOUT := 45m
empty :=
space := $(empty) $(empty)
SHUTDOWN_NAMES := TestAServedStackStopsInReverseOnASignal TestRollbackGetsTheFullBudgetAfterASignal \
	TestALateUnitFailureIsCountedOnce TestAStuckLayerStopIsAbandonedAndNamed TestASecondSignalIsLeftToTheSystem \
	TestIngesterStopOrder TestAForcedOTLPStopIsAWarningOnlyWhenItsGoroutineExits \
	TestASecondSignalReachesTheSystemThroughTheIngesterWiring TestOneErrorAndOneLeaseLineInEveryOrder \
	TestTwoExitsBeforeTheSnapshotLogOneErrorAndOneLeaseLine TestProcessorStopOrderAndReadiness \
	TestAPartitionServesAndReleasesItsLeaseOnSigterm TestTheDrainWaitsForThePipelineThenCancelsItThenGivesUp \
	TestTheReleaseStepSeversBeforeItCancelsTheRenewalOnlyWhenAWriterWasAbandoned \
	TestAnAbandonedPartitionStopToleratesTheClosedConnection TestAPipelineExitWhileServingIsCodeOneAndAborting \
	TestAllRoleServesAnExportedCallAndStopsCleanly TestAllIngestStartFailureStopsTheProcessorThroughItsLayers
SHUTDOWN_TESTS := ^($(subst $(space),|,$(strip $(SHUTDOWN_NAMES))))$$
KILL_TEST := ^($(subst $(space),|,$(strip $(KILL_NAMES))))$$

BENCH_DIR := tmp/bench
BENCH_BASELINE := test/bench/baseline.txt
BENCH_COUNT := 10
PROFILE_DIR := tmp/profile
PROFILE_BENCH := BenchmarkIteration50kEdges
PROFILE_PACKAGE := ./test/pipeline/

.PHONY: bench
bench: ## Benchmarks against test/bench/baseline.txt with benchstat (BENCH_COUNT=10); new run in tmp/bench/new.txt
	@mkdir -p $(BENCH_DIR)
	go test -run='^$$' -bench=. -benchmem -count=$(BENCH_COUNT) ./... > $(BENCH_DIR)/new.txt
	go tool benchstat $(BENCH_BASELINE) $(BENCH_DIR)/new.txt

.PHONY: bench-baseline
bench-baseline: bench ## Promote tmp/bench/new.txt to the baseline (only after reviewing the benchstat diff)
	@mkdir -p $(dir $(BENCH_BASELINE))
	cp $(BENCH_DIR)/new.txt $(BENCH_BASELINE)

.PHONY: profile
profile: ## CPU and allocation profile of one pipeline iteration: tmp/profile/{cpu,mem}.pprof, top 10 of each
	@mkdir -p $(PROFILE_DIR)
	go test -run='^$$' -bench='^$(PROFILE_BENCH)$$' -benchmem \
		-cpuprofile=$(PROFILE_DIR)/cpu.pprof -memprofile=$(PROFILE_DIR)/mem.pprof \
		-o $(PROFILE_DIR)/pipeline.test $(PROFILE_PACKAGE)
	go tool pprof -top -nodecount=10 $(PROFILE_DIR)/pipeline.test $(PROFILE_DIR)/cpu.pprof
	go tool pprof -top -nodecount=10 -sample_index=alloc_space $(PROFILE_DIR)/pipeline.test $(PROFILE_DIR)/mem.pprof

.PHONY: fuzz
fuzz: ## Run every Fuzz* target for FUZZTIME each (default 60s); seed corpora already run in make test
	./hack/fuzz.sh

.PHONY: test-stress
test-stress: ## Repeat concurrency-sensitive tests under the race detector (nightly CI, before a tag)
	go test -race -count=$(STRESS_COUNT) -timeout=$(STRESS_TIMEOUT) $(STRESS_PACKAGES)
	go test -race -count=$(STRESS_COUNT) -timeout=$(STRESS_TIMEOUT) -run '$(SHUTDOWN_TESTS)' ./cmd/faultline/
	go test -race -count=$(STREAM_STRESS_COUNT) -timeout=$(STREAM_STRESS_TIMEOUT) ./pkg/stream/...
	TEST_KILL_STRESS=1 TEST_KILL_RANDOM_POINTS=$(KILL_STRESS_POINTS) TEST_KILL_REAL_TTL=1 go test -count=1 -timeout=$(KILL_STRESS_TIMEOUT) -run '$(KILL_TEST)' ./cmd/faultline/

.PHONY: cover-check
cover-check: test-race verify-coverage ## Race tests, then the per-package coverage thresholds (NFR-MAINT-001)

COVER_PARTS ?= coverage-parts

.PHONY: cover-merge
cover-merge: ## Merge the coverage-N.out profiles of all RACE_GROUPS found under COVER_PARTS into coverage.out
	./hack/cover-merge.sh $(RACE_GROUPS) $(COVER_PARTS) coverage.out

.PHONY: verify-coverage
verify-coverage: ## Enforce per-package coverage thresholds on coverage.out (NFR-MAINT-001)
ifneq ($(GO_PACKAGES),)
	./hack/cover-check.sh coverage.out
else
	@echo "verify-coverage: no Go packages yet, skipping"
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

.PHONY: image
image: ## Build the faultline server image faultline:dev
	docker build -f deploy/dev/Dockerfile -t faultline:dev .

.PHONY: kind-load
kind-load: ## Load faultline:dev into the demo kind cluster
	kind load docker-image faultline:dev --name faultline-demo

.PHONY: demo-1
demo-1: ## Step 1 demo: OTel Demo in kind -> faultline -> graph checked against the reference (DEMO_PORT=18080)
	DEMO_STEP=1 ./hack/demo-1.sh

.PHONY: demo-2
demo-2: ## Step 2 demo: demo-1 plus final RED on required edges and I6/I10/I11 from pipeline.stats (DEMO_PORT=18080)
	./hack/demo-2.sh

.PHONY: demo-check
demo-check: ## Re-check tmp/demo/graph.json against the reference and the faultline log (DEMO_CHECK_FLAGS=-red for step 2)
	go run ./hack/democheck -graph tmp/demo/graph.json $(DEMO_CHECK_FLAGS) < tmp/demo/faultline.log

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
	rm -rf $(BIN) dist coverage.out coverage-*.out coverage-parts
