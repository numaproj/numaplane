#!/bin/bash

# This script uses Kustomize and yq to generate a templated Numaflow Controller definition manifest 
# based on a version of the Numaflow numaspace-install manifest. The script can be called 
# without arguments which will generate a file in the tests/manifests/default directory based on the latest Numaflow version,
# or it can be called passing as arguments a Numaflow version and a file path which will be used as the final output yaml.
# NOTE: the version argument should not include the 'v' character, only the semantic version numbers.

BASE_DIR=../..
TEST_MANIFEST_DIR=tests/manifests/default
SCRIPT_DIR=$(dirname "$0")

# Change working directory
cd $SCRIPT_DIR

if [ ! -z "$1" ]; then
  NUMAFLOW_VERSION=$1
else
  # Get the latest Numaflow Version from the remote repository
  echo "Getting the latest Numaflow version tag..."
  NUMAFLOW_VERSION=$(git ls-remote --tags https://github.com/numaproj/numaflow | grep -v '{}' | tail -1 | sed 's/.*\///' | sed 's/^v//')
fi

OUTPUT_FILE=$TEST_MANIFEST_DIR/controller_def_$NUMAFLOW_VERSION.yaml
if [ ! -z "$2" ]; then
  OUTPUT_FILE=$2
fi

# Check if the latest version of the Numaflow Controller definitions is already available and, if so, skip the generation process
if [ -f "$BASE_DIR/$OUTPUT_FILE" ]; then
  echo "The latest version of the Numaflow Controller definitions already exists, skipping generation"
  exit 0
fi

# Download namespace-install.yaml for the Numaflow version NUMAFLOW_VERSION
echo "Downloading Numaflow v$NUMAFLOW_VERSION numaspace-install.yaml file..."
wget -nv -O namespace-install.yaml https://raw.githubusercontent.com/numaproj/numaflow/refs/tags/v$NUMAFLOW_VERSION/config/namespace-install.yaml

if [ $? -ne 0 ]; then
  echo "Unable to download the Numaflow v$NUMAFLOW_VERSION numaspace-install.yaml file"
  exit 1
fi

echo "Generating Numaflow Controller definition file for Numaflow version v$NUMAFLOW_VERSION..."

# Run kustomization to generate the new Numaflow controller definition file for the above NUMAFLOW_VERSION
kubectl kustomize . > tmp-kustomized-nfinstall.yaml

# Install yq if not present
if ! command -v yq 2>&1 >/dev/null
then
  echo "yq not found, installing it..."
  go install github.com/mikefarah/yq/v4@latest
  echo "yq installed"
fi

# Use yq to modify/add the instance field of the string literal yaml config of the numaflow-controller-config ConfigMap data field controller-config.yaml
# This is not possible via Kustomize yet. Follow https://github.com/kubernetes-sigs/kustomize/issues/4517 and https://github.com/kubernetes-sigs/kustomize/pull/5679 for future Kustomize updates.
export TMP_NFC_DEF_GEN=$(yq 'select(.kind == "ConfigMap" and .metadata.name == "numaflow-controller-config{{ .InstanceSuffix }}") | .data."controller-config.yaml" | fromyaml | .instance = "{{ .InstanceID }}"' tmp-kustomized-nfinstall.yaml)
yq 'select(.kind == "ConfigMap" and .metadata.name == "numaflow-controller-config{{ .InstanceSuffix }}") |= .data."controller-config.yaml" = strenv(TMP_NFC_DEF_GEN)' tmp-kustomized-nfinstall.yaml > tmp-full-spec.yaml

# Update the version on the base definitions configmap yaml
export TMP_NFC_DEF_GEN_NUMAFLOW_VERSION=$NUMAFLOW_VERSION
envsubst < def-configmap.yaml > tmp-versioned-def-cm.yaml

# Using yq, append tmp-full-spec.yaml to tmp-versioned-def-cm.yaml as a string literal under the controllerDefinitions fullSpec field for the specified version
export TMP_NFC_DEF_GEN_FULL_SPEC=$(cat tmp-full-spec.yaml | yq)
export TMP_NFC_DEF_GEN_FULL_SPEC_PLUS=$(yq '.data."controller_definitions.yaml" | fromyaml | .controllerDefinitions[0].fullSpec = strenv(TMP_NFC_DEF_GEN_FULL_SPEC)' tmp-versioned-def-cm.yaml)
yq '.data."controller_definitions.yaml" = strenv(TMP_NFC_DEF_GEN_FULL_SPEC_PLUS)' tmp-versioned-def-cm.yaml > $BASE_DIR/$OUTPUT_FILE

echo "Generated file $OUTPUT_FILE"

# Cleanup
rm -f namespace-install.yaml tmp-kustomized-nfinstall.yaml tmp-full-spec.yaml tmp-versioned-def-cm.yaml
