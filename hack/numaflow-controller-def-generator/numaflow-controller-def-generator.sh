#!/bin/bash

BASE_DIR=../..
TEST_MANIFEST_DIR=tests/manifests/default
SCRIPT_DIR=$(dirname "$0")

# Change working directory
cd $SCRIPT_DIR

# Get the latest Numaflow Version from the remote repository
echo "Getting the latest Numaflow version tag..."
NUMAFLOW_VERSION=$(git ls-remote --tags https://github.com/numaproj/numaflow | grep -v '{}' | tail -1 | sed 's/.*\///' | sed 's/^v//')

# Check if the latest version of the Numaflow Controller definitions is already available and, if so, skip the generation process
if [ -f "$BASE_DIR/$TEST_MANIFEST_DIR/controller_def_$NUMAFLOW_VERSION.yaml" ]; then
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
kubectl kustomize . > $BASE_DIR/$TEST_MANIFEST_DIR/controller_def_$NUMAFLOW_VERSION.yaml

# Install yq if not present
if ! command -v yq 2>&1 >/dev/null
then
  echo "yq not found, installing it..."
  go install github.com/mikefarah/yq/v4@latest
  echo "yq installed"
fi



# TTODO: ...
# confwinstance=$(yq 'select(.kind == "ConfigMap" and .metadata.name == "numaflow-controller-config") | .data."controller-config.yaml" | fromyaml | .instance = "{{ .InstanceID }}"' namespace-install.yaml) yq 'select(.kind == "ConfigMap" and .metadata.name == "numaflow-controller-config") |= .data."controller-config.yaml" = strenv(confwinstance)' namespace-install.yaml > output.yaml

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










echo "Generated file $TEST_MANIFEST_DIR/controller_def_$NUMAFLOW_VERSION.yaml"

# Cleanup
rm namespace-install.yaml



