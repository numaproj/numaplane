#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

source $(dirname $0)/library.sh
header "running codegen"

ensure_vendor

export GO111MODULE="off"

CODEGEN_PKG=${CODEGEN_PKG:-$(ls -d -1 ./vendor/k8s.io/code-generator 2>/dev/null || echo ../code-generator)}

chmod +x ${CODEGEN_PKG}/*.sh


subheader "running codegen"
bash -x ${CODEGEN_PKG}/generate-groups.sh "deepcopy" \
  github.com/numaproj/numaplane/pkg/client github.com/numaproj/numaplane/pkg/apis \
  "numaplane:v1alpha1" \
  --go-header-file hack/boilerplate.go.txt

bash -x ${CODEGEN_PKG}/generate-groups.sh "client,informer,lister" \
  github.com/numaproj/numaplane/pkg/client github.com/numaproj/numaplane/pkg/apis \
  "numaplane:v1alpha1" \
  --go-header-file hack/boilerplate.go.txt

# gofmt the tree
subheader "running gofmt"
find . -name "*.go" -type f -print0 | xargs -0 gofmt -s -w

