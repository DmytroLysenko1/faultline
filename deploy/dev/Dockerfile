# faultline server image (design step 1, slice 7, D2).
# Two stages: `build` compiles a static binary with the Go toolchain, `runtime` ships only that
# binary on distroless. Base images are pinned by multi-arch index digest (arm64 laptops and
# amd64 CI build from the same bytes).

ARG GO_IMAGE=golang:1.27.3-bookworm@sha256:dc0963f32053e4bc451d24d87e3892d42d01021b61f093e98eb95c0352bb84b0
ARG RUNTIME_IMAGE=gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

FROM ${GO_IMAGE} AS build

WORKDIR /src
ENV CGO_ENABLED=0 \
    GOTOOLCHAIN=local

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/faultline ./cmd/faultline

FROM ${RUNTIME_IMAGE} AS runtime

COPY --from=build /out/faultline /faultline

USER 65532:65532
EXPOSE 4317 8080
ENTRYPOINT ["/faultline", "all"]
