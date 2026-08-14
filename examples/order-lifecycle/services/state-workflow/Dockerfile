FROM docker.io/library/golang@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd AS build

ARG WORKFLOW_VARIANT=baseline
WORKDIR /src
COPY go.mod go.sum ./
RUN GOTOOLCHAIN=auto go mod download
COPY pkg/probe ./pkg/probe
COPY internal/controlcontract ./internal/controlcontract
COPY examples/order-lifecycle/services/state-workflow ./examples/order-lifecycle/services/state-workflow
RUN GOTOOLCHAIN=auto CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.variant=${WORKFLOW_VARIANT}" -o /out/state-workflow ./examples/order-lifecycle/services/state-workflow

FROM scratch
COPY --from=build /out/state-workflow /state-workflow
USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["/state-workflow"]
