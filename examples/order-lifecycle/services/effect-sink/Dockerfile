FROM docker.io/library/golang@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN GOTOOLCHAIN=auto go mod download
COPY examples/order-lifecycle/services/effect-sink ./examples/order-lifecycle/services/effect-sink
RUN GOTOOLCHAIN=auto CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/effect-sink ./examples/order-lifecycle/services/effect-sink

FROM scratch
COPY --from=build /out/effect-sink /effect-sink
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/effect-sink"]
