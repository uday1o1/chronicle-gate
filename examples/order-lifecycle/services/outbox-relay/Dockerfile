FROM docker.io/library/golang@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd AS build
LABEL dev.chronicle.build-cache="reference-v1"
ARG RELAY_VARIANT=baseline
WORKDIR /src
COPY go.mod go.sum ./
RUN GOTOOLCHAIN=auto go mod download
COPY pkg/probe ./pkg/probe
COPY examples/order-lifecycle/services/outbox-relay ./examples/order-lifecycle/services/outbox-relay
RUN GOTOOLCHAIN=auto CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.variant=${RELAY_VARIANT}" -o /out/outbox-relay ./examples/order-lifecycle/services/outbox-relay

FROM scratch
COPY --from=build /out/outbox-relay /outbox-relay
USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["/outbox-relay"]
