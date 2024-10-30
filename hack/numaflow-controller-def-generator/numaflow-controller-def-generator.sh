#!/bin/bash

BASE_DIR=../..
TEST_MANIFEST_DIR=tests/manifests/default
SCRIPT_DIR=$(dirname "$0")

# Change working directory
cd $SCRIPT_DIR

# Get the latest Numaflow Version from the remote repository
echo "Getting the latest Numaflow version tag..."
NUMAFLOW_VERSION=$(git ls-remote --tags https://github.com/numaproj/numaflow | grep -v '{}' | tail -1 | sed 's/.*\///' | sed 's/^v//')

OUTPUT_FILE=$TEST_MANIFEST_DIR/controller_def_$NUMAFLOW_VERSION.yaml

# Check if the latest version of the Numaflow Controller definitions is already available and, if so, skip the generation process
if [ -f "$BASE_DIR/$OUTPUT_FILE" ]; then
  echo "The latest version of the Numaflow Controller definitions already exists, skipping generation"
  exit 0
fi

# Download namespace-install.yaml for the Numaflow version NUMAFLOW_VERSION
echo "Downloading Numaflow v$NUMAFLOW_VERSION numaspace-install.yaml file..."
wget -nv https://raw.githubusercontent.com/numaproj/numaflow/refs/tags/v$NUMAFLOW_VERSION/config/namespace-install.yaml

if [ $? -ne 0 ]; then
  echo "Unable to download the Numaflow v$NUMAFLOW_VERSION numaspace-install.yaml file"
  exit 1
fi

echo "Generating Numaflow Controller definition file for Numaflow version v$NUMAFLOW_VERSION..."

# Run kustomization to generate the new Numaflow controller definition file for the above NUMAFLOW_VERSION
kubectl kustomize . > $BASE_DIR/$OUTPUT_FILE

# Install yq if not present
if ! command -v yq 2>&1 >/dev/null
then
  echo "yq not found, installing it..."
  go install github.com/mikefarah/yq/v4@latest
  echo "yq installed"
fi

# Use yq to modify/add the instance field of the string litteral yaml config of the numaflow-controller-config ConfigMap data field controller-config.yaml
# This is not possible via Kustomize yet. Follow https://github.com/kubernetes-sigs/kustomize/issues/4517 and https://github.com/kubernetes-sigs/kustomize/pull/5679 for future Kustomize updates.
export TMP_NFC_DEF_GEN=$(yq 'select(.kind == "ConfigMap" and .metadata.name == "numaflow-controller-config{{ .InstanceSuffix }}") | .data."controller-config.yaml" | fromyaml | .instance = "{{ .InstanceID }}"' $BASE_DIR/$OUTPUT_FILE)
yq 'select(.kind == "ConfigMap" and .metadata.name == "numaflow-controller-config{{ .InstanceSuffix }}") |= .data."controller-config.yaml" = strenv(TMP_NFC_DEF_GEN)' $BASE_DIR/$OUTPUT_FILE > tmp_output.yaml
cat tmp_output.yaml > $BASE_DIR/$OUTPUT_FILE

# TTODO: all the above spec must be inside the following:
# apiVersion: v1
# kind: ConfigMap
# metadata:
#   name: numaflow-controller-definitions-1.3.3 
#   namespace: numaplane-system
#   labels:
#     "numaplane.numaproj.io/config": numaflow-controller-definitions
# data:
#   controller_definitions.yaml: |
#     controllerDefinitions:
#       - version: "1.3.3"
#         fullSpec: |
#           ...<spec goes here>...

echo "Generated file $OUTPUT_FILE"

# Cleanup
rm -f namespace-install.yaml tmp_output.yaml
