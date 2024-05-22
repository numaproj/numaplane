package sync

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/argoproj/gitops-engine/pkg/diff"
	gittesting "github.com/argoproj/gitops-engine/pkg/utils/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

var PipelineManifest = `
{
  "apiVersion": "apps/v1",
  "kind": "Deployment",
  "metadata": {
    "name": "nginx-deployment",
    "labels": {
      "app": "nginx"
    }
  },
  "spec": {
    "replicas": 3,
    "selector": {
      "matchLabels": {
        "app": "nginx"
      }
    },
    "template": {
      "metadata": {
        "labels": {
          "app": "nginx"
        }
      },
      "spec": {
        "containers": [
          {
            "name": "nginx",
            "image": "nginx:1.15.4",
            "ports": [
              {
                "containerPort": 80
              }
            ]
          }
        ]
      }
    }
  }
}
`

func NewDeployment() *unstructured.Unstructured {
	return gittesting.Unstructured(PipelineManifest)
}

func TestIgnoreNormalizer(t *testing.T) {
	normalizer, err := NewIgnoreNormalizer(map[string]ResourceOverride{
		"*/*": {
			IgnoreDifferences: OverrideIgnoreDiff{JSONPointers: []string{"/spec/template/spec/containers"}},
		},
	})

	assert.Nil(t, err)

	deployment := NewDeployment()

	_, has, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	assert.Nil(t, err)
	assert.True(t, has)

	err = normalizer.Normalize(deployment)
	assert.Nil(t, err)
	_, has, err = unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	assert.Nil(t, err)
	assert.False(t, has)
}

// Use real example
func TestDiff(t *testing.T) {
	overrides := map[string]ResourceOverride{
		"*/*": {
			IgnoreDifferences: OverrideIgnoreDiff{JSONPointers: []string{"/status"}}},
	}

	targetBytes, err := os.ReadFile("testdata/pipeline-config.yaml")
	assert.Nil(t, err)
	targetStates := make([]*unstructured.Unstructured, 0)
	targetState := StrToUnstructured(targetBytes)
	targetStates = append(targetStates, targetState)
	assert.Nil(t, err)

	liveBytes, err := os.ReadFile("testdata/pipeline-live.yaml")
	assert.Nil(t, err)
	liveStates := make([]*unstructured.Unstructured, 0)
	liveState := StrToUnstructured(liveBytes)
	liveStates = append(liveStates, liveState)
	assert.Nil(t, err)

	dr, err := StateDiffs(targetStates, liveStates, overrides, diffOptionsForTest())
	assert.False(t, dr.Modified)
	assert.Nil(t, err)

	for _, i := range dr.Diffs {
		ascii, err := printDiff(&i)
		require.NoError(t, err)
		if ascii != "" {
			t.Log(ascii)
		}
	}
}

func diffOptionsForTest() []diff.Option {
	return []diff.Option{
		diff.IgnoreAggregatedRoles(false),
	}
}

func StrToUnstructured(yamlStr []byte) *unstructured.Unstructured {
	obj := make(map[string]interface{})
	err := yaml.Unmarshal(yamlStr, &obj)
	if err != nil {
		panic(err)
	}
	return &unstructured.Unstructured{Object: obj}
}

func printDiff(result *diff.DiffResult) (string, error) {
	var live unstructured.Unstructured
	if err := json.Unmarshal(result.NormalizedLive, &live); err != nil {
		return "", err
	}
	var target unstructured.Unstructured
	if err := json.Unmarshal(result.PredictedLive, &target); err != nil {
		return "", err
	}
	out, _ := printDiffInternal("diff", &live, &target)
	return string(out), nil
}

// printDiffInternal prints a diff between two unstructured objects using
// an external diff utility and returns the output.
func printDiffInternal(name string, live *unstructured.Unstructured, target *unstructured.Unstructured) ([]byte, error) {
	tempDir, err := os.MkdirTemp("", "numaplane-diff")
	if err != nil {
		return nil, err
	}
	targetFile := filepath.Join(tempDir, name)
	var targetData []byte
	if target != nil {
		targetData, err = yaml.Marshal(target)
		if err != nil {
			return nil, err
		}
	}
	err = os.WriteFile(targetFile, targetData, 0644)
	if err != nil {
		return nil, err
	}
	liveFile := filepath.Join(tempDir, fmt.Sprintf("%s-live.yaml", name))
	liveData := []byte("")
	if live != nil {
		liveData, err = yaml.Marshal(live)
		if err != nil {
			return nil, err
		}
	}
	err = os.WriteFile(liveFile, liveData, 0644)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command("diff", liveFile, targetFile)
	return cmd.Output()
}
