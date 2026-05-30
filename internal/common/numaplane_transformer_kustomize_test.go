/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/kyaml/filesys"
)

// This file is used to test the numaplane-transformer-config.yaml file.

// TestNumaplaneTransformerConfigKustomizeBuild ensures
// config/kustomize/numaplane-transformer-config.yaml is working with kustomize:
//   - if user adds a nameSuffix or namePrefix, the AnalysisTemplate references in PipelineRollout and MonoVertexRollout must reflect that
//   - if user adds a nameSuffix or namePrefix, ConfigMap/Secret references (including MonoVertexRollout volumes) must reflect that
//   - if a referenced ISBServiceRollout is renamed, the PipelineRollout's interStepBufferServiceName must reflect that
//   - if a targeted MonoVertexRollout is renamed, a VerticalPodAutoscaler's targetRef/name must reflect that
//   - if an image is renamed, the PipelineRollout / MonoVertexRollout image paths (including MonoVertexRollout sidecars and onSuccess sinks) must reflect it.
func TestNumaplaneTransformerConfigKustomizeBuild(t *testing.T) {
	root := moduleRoot(t)
	cfgPath := filepath.Join(root, "config", "kustomize", "numaplane-transformer-config.yaml")
	cfg, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read transformer config: %v", err)
	}

	tmp := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmp, "numaplane-transformer-config.yaml"), cfg, 0o644); err != nil {
		t.Fatal(err)
	}

	kustomization := `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

configurations:
- numaplane-transformer-config.yaml

nameSuffix: -kusttest

resources:
- monovertex-rollout.yaml
- pipeline-rollout.yaml
- analysis-template.yaml
- isbservice-rollout.yaml
- vpa.yaml

images:
- name: my-registry/pipeline-src
  newName: other-registry/pipeline-src
  newTag: v2
- name: my-registry/mvtx-src
  newName: other-registry/mvtx-src
  newTag: v2
- name: my-registry/mvtx-sidecar
  newName: other-registry/mvtx-sidecar
  newTag: v2
- name: my-registry/mvtx-onsuccess
  newName: other-registry/mvtx-onsuccess
  newTag: v2

configMapGenerator:
- name: mvtx-config
  literals:
  - foo=bar
`
	if err := os.WriteFile(filepath.Join(tmp, "kustomization.yaml"), []byte(kustomization), 0o644); err != nil {
		t.Fatal(err)
	}

	mvr := `apiVersion: numaplane.numaproj.io/v1alpha1
kind: MonoVertexRollout
metadata:
  name: test-mvtx
spec:
  strategy:
    analysis:
      templates:
      - templateName: shared-analysis-template
        clusterScope: false
  monoVertex:
    spec:
      source:
        udsource:
          container:
            image: my-registry/mvtx-src:tag
      sink:
        udsink:
          container:
            image: my-registry/mvtx-src:tag
        onSuccess:
          udsink:
            container:
              image: my-registry/mvtx-onsuccess:tag
      sidecars:
      - name: sidecar
        image: my-registry/mvtx-sidecar:tag
      volumes:
      - name: cfg
        configMap:
          name: mvtx-config
`
	if err := os.WriteFile(filepath.Join(tmp, "monovertex-rollout.yaml"), []byte(mvr), 0o644); err != nil {
		t.Fatal(err)
	}

	pl := `apiVersion: numaplane.numaproj.io/v1alpha1
kind: PipelineRollout
metadata:
  name: test-pipeline
spec:
  strategy:
    analysis:
      templates:
      - templateName: shared-analysis-template
        clusterScope: false
  pipeline:
    spec:
      interStepBufferServiceName: shared-isbsvc
      vertices:
      - name: in
        source:
          udsource:
            container:
              image: my-registry/pipeline-src:tag
`
	if err := os.WriteFile(filepath.Join(tmp, "pipeline-rollout.yaml"), []byte(pl), 0o644); err != nil {
		t.Fatal(err)
	}

	isb := `apiVersion: numaplane.numaproj.io/v1alpha1
kind: ISBServiceRollout
metadata:
  name: shared-isbsvc
spec:
  interStepBufferService:
    spec:
      jetstream:
        version: latest
`
	if err := os.WriteFile(filepath.Join(tmp, "isbservice-rollout.yaml"), []byte(isb), 0o644); err != nil {
		t.Fatal(err)
	}

	vpa := `apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: test-vpa
spec:
  targetRef:
    apiVersion: numaplane.numaproj.io/v1alpha1
    kind: MonoVertexRollout
    name: test-mvtx
`
	if err := os.WriteFile(filepath.Join(tmp, "vpa.yaml"), []byte(vpa), 0o644); err != nil {
		t.Fatal(err)
	}

	at := `apiVersion: argoproj.io/v1alpha1
kind: AnalysisTemplate
metadata:
  name: shared-analysis-template
spec:
  metrics: []
`
	if err := os.WriteFile(filepath.Join(tmp, "analysis-template.yaml"), []byte(at), 0o644); err != nil {
		t.Fatal(err)
	}

	k := krusty.MakeKustomizer(krusty.MakeDefaultOptions())
	m, err := k.Run(filesys.MakeFsOnDisk(), tmp)
	if err != nil {
		t.Fatalf("kustomize build: %v", err)
	}
	out, err := m.AsYaml()
	if err != nil {
		t.Fatalf("AsYaml: %v", err)
	}
	outputManifest := string(out)

	// nameSuffix on resources
	for _, resourceName := range []string{
		"name: test-mvtx-kusttest",
		"name: test-pipeline-kusttest",
		"name: shared-analysis-template-kusttest",
		"name: shared-isbsvc-kusttest",
		"name: test-vpa-kusttest",
	} {
		if !strings.Contains(outputManifest, resourceName) {
			t.Errorf("expected output to contain %q\n\n%s", resourceName, outputManifest)
		}
	}

	// nameReference: analysis template refs follow suffixed AnalysisTemplate name
	const analysisTemplateRef = "templateName: shared-analysis-template-kusttest"
	if strings.Count(outputManifest, analysisTemplateRef) < 2 {
		t.Errorf("expected at least two occurrences of %q (MonoVertexRollout + PipelineRollout)\n\n%s", analysisTemplateRef, outputManifest)
	}

	// nameReference: PipelineRollout's interStepBufferServiceName follows the suffixed ISBServiceRollout name
	// (spec/pipeline/spec/interStepBufferServiceName)
	if !strings.Contains(outputManifest, "interStepBufferServiceName: shared-isbsvc-kusttest") {
		t.Errorf("expected PipelineRollout interStepBufferServiceName to follow renamed ISBServiceRollout\n\n%s", outputManifest)
	}

	// nameReference: VerticalPodAutoscaler's targetRef/name follows the suffixed MonoVertexRollout name
	// (spec/targetRef/name on VerticalPodAutoscaler). Match the targetRef block specifically so this
	// isn't satisfied by the MonoVertexRollout's own (suffixed) metadata.name.
	const vpaTargetRef = "targetRef:\n    apiVersion: numaplane.numaproj.io/v1alpha1\n    kind: MonoVertexRollout\n    name: test-mvtx-kusttest"
	if !strings.Contains(outputManifest, vpaTargetRef) {
		t.Errorf("expected VerticalPodAutoscaler targetRef to follow renamed MonoVertexRollout\n\n%s", outputManifest)
	}

	// images: transformer config paths for PipelineRollout / MonoVertexRollout
	if !strings.Contains(outputManifest, "image: other-registry/pipeline-src:v2") {
		t.Errorf("expected pipeline udsource image rewrite\n\n%s", outputManifest)
	}
	if strings.Count(outputManifest, "image: other-registry/mvtx-src:v2") < 2 {
		t.Errorf("expected MonoVertexRollout udsource and udsink images rewritten (count >= 2)\n\n%s", outputManifest)
	}
	// MonoVertexRollout sidecar image rewrite (spec/monoVertex/spec/sidecars/image)
	if !strings.Contains(outputManifest, "image: other-registry/mvtx-sidecar:v2") {
		t.Errorf("expected MonoVertexRollout sidecar image rewrite\n\n%s", outputManifest)
	}
	// MonoVertexRollout onSuccess sink image rewrite (spec/monoVertex/spec/sink/onSuccess/udsink/container/image)
	if !strings.Contains(outputManifest, "image: other-registry/mvtx-onsuccess:v2") {
		t.Errorf("expected MonoVertexRollout onSuccess sink image rewrite\n\n%s", outputManifest)
	}

	// nameReference: MonoVertexRollout configMap volume ref follows the suffixed ConfigMap name
	// (spec/monoVertex/spec/volumes/configMap/name). configMapGenerator appends a content hash,
	// so match the prefix plus the nameSuffix.
	if !strings.Contains(outputManifest, "name: mvtx-config-kusttest-") {
		t.Errorf("expected MonoVertexRollout configMap volume reference to follow renamed ConfigMap\n\n%s", outputManifest)
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("go.mod not found walking up from %s", file)
		}
		dir = parent
	}
}
