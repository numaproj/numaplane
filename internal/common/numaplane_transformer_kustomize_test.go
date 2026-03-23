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
//   - if an image is renamed, the PipelineRollout / MonoVertexRollout image paths must reflect it.
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

images:
- name: my-registry/pipeline-src
  newName: other-registry/pipeline-src
  newTag: v2
- name: my-registry/mvtx-src
  newName: other-registry/mvtx-src
  newTag: v2
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

	// images: transformer config paths for PipelineRollout / MonoVertexRollout
	if !strings.Contains(outputManifest, "image: other-registry/pipeline-src:v2") {
		t.Errorf("expected pipeline udsource image rewrite\n\n%s", outputManifest)
	}
	if strings.Count(outputManifest, "image: other-registry/mvtx-src:v2") < 2 {
		t.Errorf("expected MonoVertexRollout udsource and udsink images rewritten (count >= 2)\n\n%s", outputManifest)
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
