package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"

	"github.com/numaproj/numaplane/internal/common"
)

func TestGetLabel(t *testing.T) {
	yamlBytes, err := os.ReadFile("testdata/svc.yaml")
	assert.Nil(t, err)
	var obj unstructured.Unstructured
	err = yaml.Unmarshal(yamlBytes, &obj)
	assert.Nil(t, err)
	err = SetLabel(&obj, common.LabelKeyNumaplaneInstance, "my-example")
	assert.Nil(t, err)

	label, err := GetLabel(&obj, common.LabelKeyNumaplaneInstance)
	assert.Nil(t, err)
	assert.Equal(t, "my-example", label)
}

func TestGetLabelWithInvalidData(t *testing.T) {
	yamlBytes, err := os.ReadFile("testdata/svc-with-invalid-data.yaml")
	assert.Nil(t, err)
	var obj unstructured.Unstructured
	err = yaml.Unmarshal(yamlBytes, &obj)
	assert.Nil(t, err)

	_, err = GetLabel(&obj, "valid-label")
	assert.Error(t, err)
	assert.Equal(t, "failed to get labels from target object /v1, Kind=Service /my-service: .metadata.labels accessor error: contains non-string value in the map under key \"invalid-label\": <nil> is of the type <nil>, expected string", err.Error())
}

func TestCreateUpdateGetListCR(t *testing.T) {
	restConfig, _, _, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)

	pipelineSpec := numaflowv1.PipelineSpec{
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: "in",
				Source: &numaflowv1.Source{
					Generator: &numaflowv1.GeneratorSource{},
				},
			},
			{
				Name: "out",
				Sink: &numaflowv1.Sink{
					AbstractSink: numaflowv1.AbstractSink{
						Log: &numaflowv1.Log{},
					},
				},
			},
		},
		Edges: []numaflowv1.Edge{
			{
				From: "in",
				To:   "out",
			},
		},
	}
	pipelineSpecRaw, err := json.Marshal(pipelineSpec)
	assert.Nil(t, err)

	namespace := "default"

	pipelineObject := &GenericObject{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pipeline",
			APIVersion: "numaflow.numaproj.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-pipeline",
			Namespace: namespace,
		},
		Spec: runtime.RawExtension{
			Raw: pipelineSpecRaw,
		},
	}
	err = CreateCR(context.Background(), restConfig, pipelineObject, "pipelines")
	assert.Nil(t, err)
	pipelineObject, err = GetCR(context.Background(), restConfig, pipelineObject, "pipelines")
	assert.Nil(t, err)
	version1 := pipelineObject.ResourceVersion
	fmt.Printf("Created CR, resource version=%s\n", version1)

	// Updating should return the result Pipeline with the updated ResourceVersion
	pipelineObject.ObjectMeta.Labels = map[string]string{"test": "value"}
	err = UpdateCR(context.Background(), restConfig, pipelineObject, "pipelines")
	assert.Nil(t, err)
	version2 := pipelineObject.ResourceVersion

	fmt.Printf("Updated CR, resource version=%s\n", version2)
	assert.NotEqual(t, version1, version2)

	// Doing a GET should return the same thing
	pipelineObject, err = GetCR(context.Background(), restConfig, pipelineObject, "pipelines")
	assert.Nil(t, err)
	assert.Equal(t, version2, pipelineObject.ResourceVersion)

	// Do another update
	pipelineObject.ObjectMeta.Labels["test-2"] = "value-2"
	err = UpdateCR(context.Background(), restConfig, pipelineObject, "pipelines")
	assert.Nil(t, err)
	version3 := pipelineObject.ResourceVersion
	assert.NotEqual(t, version2, version3)

	// List resource
	pipelineList, err := ListCR(context.Background(), restConfig, common.NumaflowAPIGroup, common.NumaflowAPIVersion, "pipelines", namespace, "test=value", "")
	assert.Nil(t, err)
	assert.Len(t, pipelineList, 1)
}
