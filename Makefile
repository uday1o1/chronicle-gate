SHELL := /bin/sh

GO_VERSION := 1.26.6
GO_BOOTSTRAP_IMAGE := docker.io/library/golang@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd
GOLANGCI_LINT_IMAGE := docker.io/golangci/golangci-lint@sha256:5cceeef04e53efe1470638d4b4b4f5ceefd574955ab3941b2d9a68a8c9ad5240
REFERENCE_BASELINE_IMAGE := chronicle-gate/fulfillment-projector:baseline-m2
REFERENCE_CANDIDATE_IMAGE := chronicle-gate/fulfillment-projector:candidate-r1-m2
REFERENCE_FLAKY_IMAGE := chronicle-gate/fulfillment-projector:flaky-r1-m3
REFERENCE_R4_BASELINE_IMAGE := chronicle-gate/fulfillment-projector:baseline-r4-m5
REFERENCE_R4_CANDIDATE_IMAGE := chronicle-gate/fulfillment-projector:candidate-r4-m5
REFERENCE_R4_METADATA_IMAGE := chronicle-gate/fulfillment-projector:candidate-r4-metadata-m7
REFERENCE_WORKFLOW_BASELINE_IMAGE := chronicle-gate/order-workflow:baseline-m4
REFERENCE_WORKFLOW_CANDIDATE_IMAGE := chronicle-gate/order-workflow:candidate-r2-m4
REFERENCE_EFFECT_SINK_IMAGE := chronicle-gate/effect-sink:baseline-m4
REFERENCE_STATE_BASELINE_IMAGE := chronicle-gate/state-workflow:baseline-m6
REFERENCE_STATE_R3_IMAGE := chronicle-gate/state-workflow:candidate-r3-m6
REFERENCE_STATE_R5_IMAGE := chronicle-gate/state-workflow:candidate-r5-m6
REFERENCE_STATE_R6_IMAGE := chronicle-gate/state-workflow:candidate-r6-m6
REFERENCE_ORDER_API_IMAGE := chronicle-gate/order-api:baseline-m7
REFERENCE_OUTBOX_RELAY_BASELINE_IMAGE := chronicle-gate/outbox-relay:baseline-m7
REFERENCE_OUTBOX_RELAY_CANDIDATE_IMAGE := chronicle-gate/outbox-relay:candidate-r7-m7
REFERENCE_LIFECYCLE_WORKFLOW_IMAGE := chronicle-gate/lifecycle-workflow:baseline-m7
BENCHMARK_BASELINE_IMAGE := chronicle-gate/benchmark-api:baseline-m9
BENCHMARK_CANDIDATE_IMAGE := chronicle-gate/benchmark-api:candidate-slow-m9
GO_CACHE_MOUNTS := -v chronicle-gate-go-mod-cache:/go/pkg/mod -v chronicle-gate-go-build-cache:/root/.cache/go-build

HOST_GOOS := $(shell uname -s | tr '[:upper:]' '[:lower:]')
HOST_GOARCH_RAW := $(shell uname -m)
HOST_GOARCH := $(if $(filter arm64 aarch64,$(HOST_GOARCH_RAW)),arm64,$(if $(filter x86_64 amd64,$(HOST_GOARCH_RAW)),amd64,$(HOST_GOARCH_RAW)))

VERSION ?= dev
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || printf unknown)
BUILD_DATE ?= $(shell git show -s --format=%cI HEAD 2>/dev/null || printf unknown)
LDFLAGS := -s -w -X github.com/uday1o1/chronicle-gate/internal/buildinfo.Version=$(VERSION) -X github.com/uday1o1/chronicle-gate/internal/buildinfo.Commit=$(COMMIT) -X github.com/uday1o1/chronicle-gate/internal/buildinfo.BuildDate=$(BUILD_DATE)

GO_BIN := $(shell command -v go 2>/dev/null)
ifeq ($(GO_BIN),)
GO_CMD := docker run --rm -e GOTOOLCHAIN=auto $(GO_CACHE_MOUNTS) -v "$(CURDIR):/workspace" -w /workspace $(GO_BOOTSTRAP_IMAGE) go
GO_ENV := docker run --rm -e GOTOOLCHAIN=auto $(GO_CACHE_MOUNTS) -v "$(CURDIR):/workspace" -w /workspace $(GO_BOOTSTRAP_IMAGE) env
GOFMT_CMD := docker run --rm -v "$(CURDIR):/workspace" -w /workspace $(GO_BOOTSTRAP_IMAGE) gofmt
else
GO_CMD := GOTOOLCHAIN=auto go
GO_ENV := env GOTOOLCHAIN=auto
GOFMT_CMD := gofmt
endif

.PHONY: all benchmark-images build build-cross clean fmt fmt-check fuzz-smoke govulncheck lint reference-images security-check test test-benchmark test-integration test-e2e test-race vet verify verify-common toolchain tidy

all: build

toolchain:
	@test "$$($(GO_CMD) env GOVERSION)" = "go$(GO_VERSION)" || { echo "expected go$(GO_VERSION)"; exit 1; }

build: toolchain
	@mkdir -p bin
	$(GO_ENV) CGO_ENABLED=0 GOOS=$(HOST_GOOS) GOARCH=$(HOST_GOARCH) go build -trimpath -ldflags "$(LDFLAGS)" -o bin/chronicle ./cmd/chronicle

build-cross: toolchain
	@mkdir -p dist
	$(GO_ENV) CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags "$(LDFLAGS)" -o dist/chronicle-darwin-arm64 ./cmd/chronicle
	$(GO_ENV) CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "$(LDFLAGS)" -o dist/chronicle-linux-amd64 ./cmd/chronicle
	$(GO_ENV) CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags "$(LDFLAGS)" -o dist/chronicle-linux-arm64 ./cmd/chronicle

fmt:
	@files="$$(find . -name '*.go' -not -path './.git/*')"; if test -n "$$files"; then $(GOFMT_CMD) -w $$files; fi

fmt-check:
	@files="$$(find . -name '*.go' -not -path './.git/*')"; output="$$($(GOFMT_CMD) -l $$files)"; test -z "$$output" || { echo "unformatted Go files:"; echo "$$output"; exit 1; }

lint: toolchain
	docker run --rm -e GOTOOLCHAIN=auto $(GO_CACHE_MOUNTS) -v "$(CURDIR):/workspace" -w /workspace $(GOLANGCI_LINT_IMAGE) golangci-lint run

test: toolchain
	$(GO_CMD) test -count=1 ./...

vet: toolchain
	$(GO_CMD) vet ./...

reference-images:
	docker build --pull=false --build-arg PROJECTOR_VARIANT=baseline --label dev.chronicle.reference=baseline -t $(REFERENCE_BASELINE_IMAGE) -f examples/order-lifecycle/services/fulfillment-projector/Dockerfile .
	docker build --pull=false --build-arg PROJECTOR_VARIANT=candidate-r1 --label dev.chronicle.reference=candidate-r1 -t $(REFERENCE_CANDIDATE_IMAGE) -f examples/order-lifecycle/services/fulfillment-projector/Dockerfile .
	docker build --pull=false --build-arg PROJECTOR_VARIANT=flaky-r1 --label dev.chronicle.reference=flaky-r1 -t $(REFERENCE_FLAKY_IMAGE) -f examples/order-lifecycle/services/fulfillment-projector/Dockerfile .
	docker build --pull=false --build-arg PROJECTOR_VARIANT=baseline-r4 --label dev.chronicle.reference=baseline-r4 -t $(REFERENCE_R4_BASELINE_IMAGE) -f examples/order-lifecycle/services/fulfillment-projector/Dockerfile .
	docker build --pull=false --build-arg PROJECTOR_VARIANT=candidate-r4 --label dev.chronicle.reference=candidate-r4 -t $(REFERENCE_R4_CANDIDATE_IMAGE) -f examples/order-lifecycle/services/fulfillment-projector/Dockerfile .
	docker build --pull=false --build-arg PROJECTOR_VARIANT=candidate-r4-metadata --label dev.chronicle.reference=candidate-r4-metadata -t $(REFERENCE_R4_METADATA_IMAGE) -f examples/order-lifecycle/services/fulfillment-projector/Dockerfile .
	docker build --pull=false --build-arg WORKFLOW_VARIANT=baseline --label dev.chronicle.reference=workflow-baseline -t $(REFERENCE_WORKFLOW_BASELINE_IMAGE) -f examples/order-lifecycle/services/order-workflow/Dockerfile .
	docker build --pull=false --build-arg WORKFLOW_VARIANT=candidate-r2 --label dev.chronicle.reference=workflow-candidate-r2 -t $(REFERENCE_WORKFLOW_CANDIDATE_IMAGE) -f examples/order-lifecycle/services/order-workflow/Dockerfile .
	docker build --pull=false --label dev.chronicle.reference=effect-sink -t $(REFERENCE_EFFECT_SINK_IMAGE) -f examples/order-lifecycle/services/effect-sink/Dockerfile .
	docker build --pull=false --build-arg WORKFLOW_VARIANT=baseline --label dev.chronicle.reference=state-baseline -t $(REFERENCE_STATE_BASELINE_IMAGE) -f examples/order-lifecycle/services/state-workflow/Dockerfile .
	docker build --pull=false --build-arg WORKFLOW_VARIANT=candidate-r3 --label dev.chronicle.reference=state-candidate-r3 -t $(REFERENCE_STATE_R3_IMAGE) -f examples/order-lifecycle/services/state-workflow/Dockerfile .
	docker build --pull=false --build-arg WORKFLOW_VARIANT=candidate-r5 --label dev.chronicle.reference=state-candidate-r5 -t $(REFERENCE_STATE_R5_IMAGE) -f examples/order-lifecycle/services/state-workflow/Dockerfile .
	docker build --pull=false --build-arg WORKFLOW_VARIANT=candidate-r6 --label dev.chronicle.reference=state-candidate-r6 -t $(REFERENCE_STATE_R6_IMAGE) -f examples/order-lifecycle/services/state-workflow/Dockerfile .
	docker build --pull=false --label dev.chronicle.reference=order-api -t $(REFERENCE_ORDER_API_IMAGE) -f examples/order-lifecycle/services/order-api/Dockerfile .
	docker build --pull=false --build-arg RELAY_VARIANT=baseline --label dev.chronicle.reference=outbox-relay-baseline -t $(REFERENCE_OUTBOX_RELAY_BASELINE_IMAGE) -f examples/order-lifecycle/services/outbox-relay/Dockerfile .
	docker build --pull=false --build-arg RELAY_VARIANT=candidate-r7 --label dev.chronicle.reference=outbox-relay-candidate-r7 -t $(REFERENCE_OUTBOX_RELAY_CANDIDATE_IMAGE) -f examples/order-lifecycle/services/outbox-relay/Dockerfile .
	docker build --pull=false --label dev.chronicle.reference=lifecycle-workflow -t $(REFERENCE_LIFECYCLE_WORKFLOW_IMAGE) -f examples/order-lifecycle/services/lifecycle-workflow/Dockerfile .
	$(GO_CMD) run ./tools/generate_reference_targets --baseline-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_BASELINE_IMAGE))" --candidate-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_CANDIDATE_IMAGE))" --flaky-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_FLAKY_IMAGE))" --r4-baseline-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_R4_BASELINE_IMAGE))" --r4-candidate-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_R4_CANDIDATE_IMAGE))" --r4-metadata-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_R4_METADATA_IMAGE))" --workflow-baseline-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_WORKFLOW_BASELINE_IMAGE))" --workflow-candidate-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_WORKFLOW_CANDIDATE_IMAGE))" --effect-sink-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_EFFECT_SINK_IMAGE))" --state-baseline-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_STATE_BASELINE_IMAGE))" --state-r3-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_STATE_R3_IMAGE))" --state-r5-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_STATE_R5_IMAGE))" --state-r6-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_STATE_R6_IMAGE))" --order-api-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_ORDER_API_IMAGE))" --outbox-relay-baseline-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_OUTBOX_RELAY_BASELINE_IMAGE))" --outbox-relay-candidate-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_OUTBOX_RELAY_CANDIDATE_IMAGE))" --lifecycle-workflow-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_LIFECYCLE_WORKFLOW_IMAGE))"

benchmark-images:
	docker build --pull=false --build-arg BENCHMARK_DELAY=1ms --label dev.chronicle.reference=benchmark-baseline -t $(BENCHMARK_BASELINE_IMAGE) -f examples/order-lifecycle/services/benchmark-api/Dockerfile .
	docker build --pull=false --build-arg BENCHMARK_DELAY=20ms --label dev.chronicle.reference=benchmark-candidate-slow -t $(BENCHMARK_CANDIDATE_IMAGE) -f examples/order-lifecycle/services/benchmark-api/Dockerfile .
	$(GO_CMD) run ./tools/generate_benchmark_targets --baseline-image "$$(docker image inspect --format '{{.Id}}' $(BENCHMARK_BASELINE_IMAGE))" --candidate-image "$$(docker image inspect --format '{{.Id}}' $(BENCHMARK_CANDIDATE_IMAGE))"

test-integration: reference-images build
	@mkdir -p dist run
	$(GO_ENV) CGO_ENABLED=0 GOOS=$(HOST_GOOS) GOARCH=$(HOST_GOARCH) go test -c -tags=integration -o dist/chronicle-integration.test ./tests/integration
	./dist/chronicle-integration.test -test.v

test-e2e: test-integration

test-benchmark: benchmark-images build
	@mkdir -p dist run
	$(GO_ENV) CGO_ENABLED=0 GOOS=$(HOST_GOOS) GOARCH=$(HOST_GOARCH) go test -c -tags=integration -o dist/chronicle-benchmark.test ./tests/benchmark
	./dist/chronicle-benchmark.test -test.v

fuzz-smoke: toolchain
	$(GO_CMD) test ./internal/spec -run '^$$' -fuzz '^FuzzScenarioContracts$$' -fuzztime=1s
	$(GO_CMD) test ./internal/spec -run '^$$' -fuzz '^FuzzResultAndBundleContracts$$' -fuzztime=1s
	$(GO_CMD) test ./internal/minimize -run '^$$' -fuzz '^FuzzReducerDependencyClosure$$' -fuzztime=1s
	$(GO_CMD) test ./internal/observe -run '^$$' -fuzz '^FuzzObservationCanonicalization$$' -fuzztime=1s
	$(GO_CMD) test ./internal/observe -run '^$$' -fuzz '^FuzzNormalizationIdempotence$$' -fuzztime=1s
	$(GO_CMD) test ./internal/bundle -run '^$$' -fuzz '^FuzzArchiveSafety$$' -fuzztime=1s

test-race: toolchain
	$(GO_CMD) test -race -count=1 ./...

security-check: toolchain
	$(GO_CMD) run ./tools/security_check

govulncheck: toolchain
	$(GO_CMD) run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...

tidy: toolchain
	$(GO_CMD) mod tidy

verify-common: fmt-check test vet test-race security-check govulncheck fuzz-smoke build build-cross

verify: verify-common lint test-integration

clean:
	@rm -f bin/chronicle dist/chronicle-darwin-arm64 dist/chronicle-linux-amd64 dist/chronicle-linux-arm64
