SHELL := /bin/sh

GO_VERSION := 1.26.6
GO_BOOTSTRAP_IMAGE := docker.io/library/golang@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd
GOLANGCI_LINT_IMAGE := docker.io/golangci/golangci-lint@sha256:5cceeef04e53efe1470638d4b4b4f5ceefd574955ab3941b2d9a68a8c9ad5240
REFERENCE_BASELINE_IMAGE := chronicle-gate/fulfillment-projector:baseline-m2
REFERENCE_CANDIDATE_IMAGE := chronicle-gate/fulfillment-projector:candidate-r1-m2
REFERENCE_FLAKY_IMAGE := chronicle-gate/fulfillment-projector:flaky-r1-m3
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

.PHONY: all build build-cross clean fmt fmt-check lint reference-images test test-integration test-e2e fuzz-smoke vet verify toolchain tidy

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
	$(GO_CMD) run ./tools/generate_reference_targets --baseline-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_BASELINE_IMAGE))" --candidate-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_CANDIDATE_IMAGE))" --flaky-image "$$(docker image inspect --format '{{.Id}}' $(REFERENCE_FLAKY_IMAGE))"

test-integration: reference-images build
	@mkdir -p dist
	$(GO_ENV) CGO_ENABLED=0 GOOS=$(HOST_GOOS) GOARCH=$(HOST_GOARCH) go test -c -tags=integration -o dist/chronicle-integration.test ./tests/integration
	./dist/chronicle-integration.test -test.v

test-e2e: test-integration

fuzz-smoke: toolchain
	$(GO_CMD) test -run '^$$' ./...

tidy: toolchain
	$(GO_CMD) mod tidy

verify: fmt-check lint test vet build build-cross

clean:
	@rm -f bin/chronicle dist/chronicle-darwin-arm64 dist/chronicle-linux-amd64 dist/chronicle-linux-arm64
