FROM docker.io/library/golang@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd AS build
LABEL dev.chronicle.build-cache="reference-v1"
WORKDIR /src
COPY go.mod go.sum ./
RUN GOTOOLCHAIN=auto go mod download
COPY examples/order-lifecycle/services/order-api ./examples/order-lifecycle/services/order-api
RUN GOTOOLCHAIN=auto CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/order-api ./examples/order-lifecycle/services/order-api

FROM scratch
COPY --from=build /out/order-api /order-api
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/order-api"]
