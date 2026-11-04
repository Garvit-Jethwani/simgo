# syntax=docker/dockerfile:1
#
# SimGo worker image. Build from the repository root (the cloud module
# depends on the root module through a replace directive):
#
#   docker buildx build --platform linux/arm64 -f cloud/Dockerfile -t simgo-worker .
#
# The runtime image keeps the Go toolchain and git: workers check out user
# repositories and run their tests with go test.

FROM --platform=$BUILDPLATFORM golang:1.25.3-bookworm AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod ./
COPY cloud/go.mod cloud/go.sum ./cloud/
RUN cd cloud && go mod download
COPY . .
RUN cd cloud && CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/simgo-cloud ./cmd/simgo-cloud

FROM golang:1.25.3-bookworm
RUN useradd --create-home --uid 10001 simgo
USER simgo
WORKDIR /home/simgo
# Let user repositories that need a newer Go download it on demand.
ENV GOTOOLCHAIN=auto
COPY --from=build /out/simgo-cloud /usr/local/bin/simgo-cloud
ENTRYPOINT ["simgo-cloud"]
CMD ["worker"]
