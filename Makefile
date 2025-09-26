# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

# Image URL to use all building/pushing image targets
IMG ?= numaplane-controller
VERSION ?= latest
# BASE_VERSION will be used during release process to bump up versions
BASE_VERSION := latest
IMAGE_NAMESPACE ?= quay.io/numaproj
IMAGE_FULL_PATH ?= $(IMAGE_NAMESPACE)/$(IMG):$(VERSION)
DEPLOYMENT_NAME ?= numaplane-controller-manager
NAMESPACE ?= numaplane-system

BUILD_DATE=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
GIT_COMMIT=$(shell git rev-parse HEAD)
GIT_BRANCH=$(shell git rev-parse --symbolic-full-name --verify --quiet --abbrev-ref HEAD)
GIT_TAG=$(shell if [[ -z "`git status --porcelain`" ]]; then git describe --exact-match --tags HEAD 2>/dev/null; fi)
GIT_TREE_STATE=$(shell if [[ -z "`git status --porcelain`" ]]; then echo "clean" ; else echo "dirty"; fi)

NUMAFLOW_CRDS=$(shell kubectl get crd | grep -c 'numaflow.numaproj.io')

WRITE_DIR := $(HOME)


## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
KUBECTL ?= kubectl
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest

## Tool Versions
CONTROLLER_TOOLS_VERSION ?= v0.14.0

GCFLAGS="all=-N -l"

# ENVTEST_K8S_VERSION refers to the version of kubebuilder assets to be downloaded by envtest binary.
ENVTEST_K8S_VERSION = 1.28.0

TEST_MANIFEST_DIR_DEFAULT ?= tests/manifests/default
TEST_MANIFEST_DIR_E2E_DEFAULT ?= tests/manifests/e2e/default
TEST_NOSTRATEGY_MANIFEST_DIR ?= tests/manifests/e2e/special-cases/no-strategy
TEST_PAUSE_AND_DRAIN_MANIFEST_DIR ?= tests/manifests/e2e/special-cases/pause-and-drain

TEST_MANIFEST_DIR := $(TEST_MANIFEST_DIR_E2E_DEFAULT)

ifeq ($(STRATEGY), no-strategy)
TEST_MANIFEST_DIR := $(TEST_NOSTRATEGY_MANIFEST_DIR)
endif

ifeq ($(STRATEGY), pause-and-drain)
TEST_MANIFEST_DIR := $(TEST_PAUSE_AND_DRAIN_MANIFEST_DIR)
endif

# Set the Dockerfile path based on the condition
DOCKERFILE := Dockerfile
ifeq ($(TEST_TYPE), e2e)
# Set the Dockerfile and image version for e2e tests
DOCKERFILE := tests/e2e/coverage/Dockerfile
VERSION = e2e
endif

ARGO_ROLLOUTS_PATH ?= https://github.com/argoproj/argo-rollouts/manifests/cluster-install?ref=stable

PROMETHEUS_REQUIRED ?= true
ROLLOUTS_REQUIRED ?= true
PROMETHEUS_CHART ?= oci://ghcr.io/prometheus-community/charts/kube-prometheus-stack

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif


CURRENT_CONTEXT := $(shell [[ "`command -v kubectl`" != '' ]] && kubectl config current-context 2> /dev/null || echo "unset")

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker


.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

##@ Development

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	$(CONTROLLER_GEN) crd paths="./..." output:crd:artifacts:config=config/crd/bases
	$(KUBECTL) kustomize config/default > config/install.yaml

.PHONY: codegen
codegen:
## Generate pkg/client directory
	./hack/update-codegen.sh
	$(MAKE) manifests
	rm -rf ./vendor
	go mod tidy
## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..." 

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: codegen fmt vet envtest ## Run unit tests.
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test -covermode=atomic -coverprofile=coverage.out -coverpkg=./... -p 1 -race -short -v $$(go list ./... | grep -v /tests/e2e | grep -v /pkg/client/ | grep -v /vendor/)

test-with-coverage: test
	grep -v "github.com/numaproj/numaplane/pkg/client" coverage.out | grep -v "github.com/numaproj/numaplane/tests/" | grep -v "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1/zz_generated.deepcopy.go" | grep -v "github.com/numaproj/numaplane/internal/controller/common/test_common.go" > filtered_coverage.out
	go tool cover -func=filtered_coverage.out
	go tool cover -html=filtered_coverage.out -o coverage.html

test-functional-nc:
test-functional-monovertex:
test-functional-pipeline:
test-ppnd-e2e:
test-progressive-monovertex-e2e:
test-progressive-pipeline-e2e:
test-progressive-analysis-monovertex-e2e:
test-progressive-analysis-pipeline-e2e:
test-rider-e2e:
test-rollback-e2e:
test-force-drain-e2e:
test-no-drain-e2e:
test-%: envtest ## Run e2e tests. Note we may need to increase the timeout in the future.
	GOFLAGS="-count=1" ginkgo run -v --timeout 35m ./tests/e2e/$*

GOLANGCI_LINT = $(shell pwd)/bin/golangci-lint
GOLANGCI_LINT_VERSION ?= v1.61.0
golangci-lint:
	@[ -f $(GOLANGCI_LINT) ] || { \
	set -e ;\
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell dirname $(GOLANGCI_LINT)) $(GOLANGCI_LINT_VERSION) ;\
	}

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter & yamllint
	$(GOLANGCI_LINT) -v run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) -v run --fix

##@ Build

.PHONY: build
build: codegen fmt vet ## Build manager binary.
	go build -gcflags=${GCFLAGS} -o bin/manager cmd/main.go

.PHONY: run
run: codegen fmt vet ## Run a controller from your host.
	go run -gcflags=${GCFLAGS} ./cmd/main.go

clean:
	-rm -f bin/manager

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: image
image: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMAGE_FULL_PATH} --load -f ${DOCKERFILE} .
	$(MAKE) image-import  # Call the image-import target after building

.PHONY: image-import
image-import: ## Import docker image into the appropriate Kubernetes environment.
	@if command -v k3d >/dev/null && echo "$(CURRENT_CONTEXT)" | grep -qE '^k3d-'; then \
		echo "Saving image with k3d..."; \
		k3d image import -c `echo $(CURRENT_CONTEXT) | cut -c 5-` ${IMAGE_FULL_PATH}; \
	elif command -v kind >/dev/null && echo "$(CURRENT_CONTEXT)" | grep -qE '^kind-'; then \
		if [ "$(CONTAINER_TOOL)" = "podman" ]; then \
			echo "Saving image with kind/podman..."; \
			podman save ${IMAGE_FULL_PATH} -o ${WRITE_DIR}/numaplane-controller.tar; \
			echo "Loading image archive into kind cluster..."; \
			kind load image-archive ${WRITE_DIR}/numaplane-controller.tar --name `echo $(CURRENT_CONTEXT) | cut -c 6-`; \
			rm ${WRITE_DIR}/numaplane-controller.tar; \
		else \
			echo "Saving image with kind/docker..."; \
			kind load docker-image ${IMAGE_FULL_PATH}; \
		fi; \
	fi


.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMAGE_FULL_PATH}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMAGE_FULL_PATH=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMAGE_FULL_PATH=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
# buildx is only available for docker; TODO: add option for podman
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- docker buildx create --name project-v3-builder
	docker buildx use project-v3-builder
	- docker buildx build --push --platform=$(PLATFORMS) --tag ${IMAGE_FULL_PATH} -f Dockerfile.cross .
	- docker buildx rm project-v3-builder
	rm Dockerfile.cross

.PHONY: rollouts
rollouts:
ifeq ($(ROLLOUTS_REQUIRED), true)
	$(KUBECTL) apply -f $(TEST_MANIFEST_DIR_DEFAULT)/rollouts-ns.yaml
	$(KUBECTL) kustomize $(ARGO_ROLLOUTS_PATH) | $(KUBECTL) apply -n argo-rollouts -f -
endif

.PHONY: prometheus
prometheus:
ifeq ($(PROMETHEUS_REQUIRED), true)
	$(KUBECTL) apply -f $(TEST_MANIFEST_DIR_DEFAULT)/prometheus-ns.yaml
	helm upgrade --install prometheus $(PROMETHEUS_CHART) -n prometheus --set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false --set prometheus.logLevel=debug --hide-notes
endif

##@ Deployment

.PHONY: start
start: image prometheus rollouts
	./hack/numaflow-controller-def-generator/numaflow-controller-def-generator.sh
	$(KUBECTL) apply -f $(TEST_MANIFEST_DIR_DEFAULT)/numaplane-ns.yaml
	$(KUBECTL) kustomize $(TEST_MANIFEST_DIR) | sed 's@quay.io/numaproj/@$(IMAGE_NAMESPACE)/@' | sed 's/$(IMG):$(BASE_VERSION)/$(IMG):$(VERSION)/' | $(KUBECTL) apply -f -

##@ Build Dependencies

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary. If wrong version is installed, it will be overwritten.
$(CONTROLLER_GEN): $(LOCALBIN)
	test -s $(LOCALBIN)/controller-gen && $(LOCALBIN)/controller-gen --version | grep -q $(CONTROLLER_TOOLS_VERSION) || \
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

.PHONY: envtest
envtest: $(ENVTEST) ## Download envtest-setup locally if necessary.
$(ENVTEST): $(LOCALBIN)
	test -s $(LOCALBIN)/setup-envtest || GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest

# release - targets only available on release branch
ifneq ($(findstring release-,$(GIT_BRANCH)),)

.PHONY: prepare-release
prepare-release: check-version-warning clean update-manifests-version codegen
	git status
	@git diff --quiet || echo "\n\nPlease run 'git diff' to confirm the file changes are correct.\n"


.PHONY: release
release: check-version-warning
	@echo
	@echo "1. Make sure you have run 'VERSION=$(VERSION) make prepare-release', and confirmed all the changes are expected."
	@echo
	@echo "2. Run following commands to commit the changes to the release branch, add give a tag."
	@echo
	@echo "git commit -am \"Update manifests to $(VERSION)\""
	@echo "git push {your-remote}"
	@echo
	@echo "git tag -a $(VERSION) -m $(VERSION)"
	@echo "git push {your-remote} $(VERSION)"
	@echo

endif

.PHONY: check-version-warning
check-version-warning:
	@if [[ ! "$(VERSION)" =~ ^v[0-9]+\.[0-9]+\.[0-9]+.*$  ]]; then echo -n "It looks like you're not using a version format like 'v1.2.3', or 'v1.2.3-rc2', that version format is required for our releases. Do you wish to continue anyway? [y/N]" && read ans && [[ $${ans:-N} = y ]]; fi


.PHONY: update-manifests-version
update-manifests-version:
	cat config/manager/kustomization.yaml | sed 's/newTag: .*/newTag: $(VERSION)/' > /tmp/base_kustomization.yaml
	mv /tmp/base_kustomization.yaml config/manager/kustomization.yaml
