FROM docker.io/library/golang@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd AS build
LABEL dev.chronicle.build-cache="reference-v1"

ARG BENCHMARK_DELAY=1ms
WORKDIR /src
COPY go.mod go.sum ./
RUN GOTOOLCHAIN=auto go mod download
COPY examples/order-lifecycle/services/benchmark-api ./examples/order-lifecycle/services/benchmark-api
RUN dependencies="$(GOTOOLCHAIN=auto go list -deps ./examples/order-lifecycle/services/benchmark-api)" && \
    ! printf '%s\n' "$dependencies" | grep -E 'github.com/uday1o1/chronicle-gate/pkg/probe|go.opentelemetry|franz-go|pgx|net/http/pprof' && \
    GOTOOLCHAIN=auto CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.delayText=${BENCHMARK_DELAY}" -o /out/benchmark-api ./examples/order-lifecycle/services/benchmark-api

FROM scratch
COPY --from=build /out/benchmark-api /benchmark-api
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/benchmark-api"]
LABEL dev.chronicle.benchmark.reference="stdlib-only-v1"
