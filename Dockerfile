# Build the manager binary
FROM golang:1.21 as builder
ARG TARGETOS
ARG TARGETARCH
ARG KUSTOMIZE_VERSION="v5.3.0"
ARG HELM_VERSION="v3.15.3"

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY cmd/main.go cmd/main.go
COPY pkg/ pkg/
COPY internal/ internal/

# Add a go build cache. The persistent cache helps speed up build steps,
# especially steps that involve installing packages using a package manager.
ENV GOCACHE=/root/.cache/go-build
# Build
# the GOARCH has not a default value to allow the binary be built according to the host where the command
# was called. For example, if we call make docker-build in a local env which has the Apple Silicon M1 SO
# the docker BUILDPLATFORM arg will be linux/arm64 when for Apple x86 it will be linux/amd64. Therefore,
# by leaving it empty we can ensure that the container and binary shipped on it will have the same platform.
RUN --mount=type=cache,target="/root/.cache/go-build" CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -o manager cmd/main.go

# Download kustomize binary
RUN curl --retry 3 --silent --location --remote-name \
    "https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize/${KUSTOMIZE_VERSION}/kustomize_${KUSTOMIZE_VERSION}_linux_amd64.tar.gz" && \
    tar -C /tmp -xf kustomize_${KUSTOMIZE_VERSION}_linux_amd64.tar.gz && \
    install -m 0755 /tmp/kustomize /usr/local/bin/kustomize

RUN curl --retry 3 --silent --location --remote-name https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz && \
    mkdir -p /tmp/helm && tar -C /tmp/helm -xf helm-${HELM_VERSION}-linux-amd64.tar.gz && \
    install -m 0755 /tmp/helm/linux-amd64/helm /usr/local/bin/helm

# Use alpine as minimal base image to package the manager binary
FROM alpine
WORKDIR /
COPY --from=builder /workspace/manager .
COPY --from=builder /usr/local/bin/kustomize /usr/local/bin/kustomize
COPY --from=builder /usr/local/bin/helm /usr/local/bin/helm

RUN apk add --no-cache git

ENTRYPOINT ["/manager"]
