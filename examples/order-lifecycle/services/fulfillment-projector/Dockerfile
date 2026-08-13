FROM docker.io/library/golang@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd AS build

ARG PROJECTOR_VARIANT=baseline
WORKDIR /src
COPY go.mod go.sum ./
RUN GOTOOLCHAIN=auto go mod download
COPY examples/order-lifecycle/services/fulfillment-projector ./examples/order-lifecycle/services/fulfillment-projector
RUN GOTOOLCHAIN=auto CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.variant=${PROJECTOR_VARIANT}" -o /out/projector ./examples/order-lifecycle/services/fulfillment-projector

FROM scratch
COPY --from=build /out/projector /projector
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/projector"]
