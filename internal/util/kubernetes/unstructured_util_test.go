package kubernetes

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/numaproj/numaplane/internal/util"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	commontest "github.com/numaproj/numaplane/tests/common"
)

func TestExtractMetadataSubmaps(t *testing.T) {
	tests := []struct {
		name                string
		metadata            map[string]interface{}
		expectedLabels      map[string]string
		expectedAnnotations map[string]string
	}{
		{
			name:                "empty metadata",
			metadata:            map[string]interface{}{},
			expectedLabels:      nil,
			expectedAnnotations: nil,
		},
		{
			name: "both labels and annotations present",
			metadata: map[string]interface{}{
				"labels": map[string]interface{}{
					"app":     "test",
					"version": "v1",
				},
				"annotations": map[string]interface{}{
					"description": "test app",
					"owner":       "team-a",
				},
			},
			expectedLabels: map[string]string{
				"app":     "test",
				"version": "v1",
			},
			expectedAnnotations: map[string]string{
				"description": "test app",
				"owner":       "team-a",
			},
		},
		{
			name: "only labels present",
			metadata: map[string]interface{}{
				"labels": map[string]interface{}{
					"app": "test",
				},
			},
			expectedLabels: map[string]string{
				"app": "test",
			},
			expectedAnnotations: nil,
		},
		{
			name: "labels with nil value",
			metadata: map[string]interface{}{
				"labels": nil,
			},
			expectedLabels:      map[string]string{},
			expectedAnnotations: map[string]string{},
		},
		{
			name: "labels with wrong type",
			metadata: map[string]interface{}{
				"labels": "not-a-map",
			},
			expectedLabels:      nil,
			expectedAnnotations: nil,
		},
		{
			name:                "nil metadata",
			metadata:            nil,
			expectedLabels:      map[string]string{},
			expectedAnnotations: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			labels, annotations := ExtractMetadataSubmaps(tt.metadata)
			assert.Equal(t, tt.expectedLabels, labels)
			assert.Equal(t, tt.expectedAnnotations, annotations)
		})
	}
}

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

func TestCreateUpdateGetListDeleteCR(t *testing.T) {
	restConfig, _, _, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	runtimeClient, err := client.New(restConfig, client.Options{Scheme: runtime.NewScheme()})
	assert.Nil(t, err)
	assert.Nil(t, SetClientSets(restConfig))

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

	namespace := "default"

	pipelineObject := &unstructured.Unstructured{Object: make(map[string]interface{})}
	pipelineObject.SetGroupVersionKind(numaflowv1.PipelineGroupVersionKind)
	pipelineObject.SetName("my-pipeline")
	pipelineObject.SetNamespace(namespace)
	var pipelineSpecMap map[string]interface{}
	err = util.StructToStruct(pipelineSpec, &pipelineSpecMap)
	assert.Nil(t, err)
	pipelineObject.Object["spec"] = pipelineSpecMap

	err = CreateResource(context.Background(), runtimeClient, pipelineObject)
	assert.Nil(t, err)
	pipelineObject, err = GetLiveResource(context.Background(), pipelineObject, "pipelines")
	assert.Nil(t, err)
	version1 := pipelineObject.GetResourceVersion()
	fmt.Printf("Created CR, resource version=%s\n", version1)

	// Get resource with cache
	pipelineObject, err = GetResource(context.Background(), runtimeClient, pipelineObject.GroupVersionKind(),
		k8stypes.NamespacedName{Namespace: pipelineObject.GetNamespace(), Name: pipelineObject.GetName()})
	assert.Nil(t, err)

	// Updating should return the result Pipeline with the updated ResourceVersion
	pipelineObject.SetLabels(map[string]string{"test": "value"})
	err = UpdateResource(context.Background(), runtimeClient, pipelineObject)
	assert.Nil(t, err)
	version2 := pipelineObject.GetResourceVersion()

	fmt.Printf("Updated CR, resource version=%s\n", version2)
	assert.NotEqual(t, version1, version2)

	// Doing a GET should return the same thing
	pipelineObject, err = GetLiveResource(context.Background(), pipelineObject, "pipelines")
	assert.Nil(t, err)
	assert.Equal(t, version2, pipelineObject.GetResourceVersion())

	// List resource
	pipelineList, err := ListLiveResource(context.Background(), common.NumaflowAPIGroup, common.NumaflowAPIVersion, "pipelines", namespace, "test=value", "")
	assert.Nil(t, err)
	assert.Len(t, pipelineList.Items, 1)

	// List resource with cache
	gvk := schema.GroupVersionKind{Group: common.NumaflowAPIGroup, Version: common.NumaflowAPIVersion, Kind: common.NumaflowPipelineKind}
	pipelineList, err = ListResources(context.Background(), runtimeClient, gvk, namespace, client.MatchingLabels{"test": "value"})
	assert.Nil(t, err)
	assert.Len(t, pipelineList.Items, 1)

	err = DeleteResource(context.Background(), runtimeClient, pipelineObject)
	assert.Nil(t, err)
	pipelineList, err = ListLiveResource(context.Background(), common.NumaflowAPIGroup, common.NumaflowAPIVersion, "pipelines", namespace, "test=value", "")
	assert.Nil(t, err)
	assert.Len(t, pipelineList.Items, 0)
}

func TestGetLoggableResource(t *testing.T) {
	tests := []struct {
		name     string
		input    *unstructured.Unstructured
		expected map[string]interface{}
	}{
		{
			name: "complete pipeline object",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "numaflow.numaproj.io/v1alpha1",
					"kind":       "Pipeline",
					"metadata": map[string]interface{}{
						"name":      "test-pipeline",
						"namespace": "test-namespace",
						"labels": map[string]interface{}{
							"app": "test",
							"numaplane.numaproj.io/parent-rollout-name": "my-rollout",
						},
						"annotations": map[string]interface{}{
							"numaflow.numaproj.io/pause-timestamp": "2025-10-08T22:47:07Z",
						},
						// These should be excluded from the clean output
						"managedFields": []interface{}{
							map[string]interface{}{
								"manager":   "numaflow",
								"operation": "Update",
								"fieldsV1":  map[string]interface{}{"f:spec": map[string]interface{}{}},
							},
						},
						"resourceVersion": "12345",
						"uid":             "abc-123-def",
						"generation":      int64(1),
					},
					"spec": map[string]interface{}{
						"vertices": []interface{}{
							map[string]interface{}{
								"name": "in",
								"source": map[string]interface{}{
									"generator": map[string]interface{}{
										"rpu":      "500",
										"duration": "1s",
									},
								},
							},
							map[string]interface{}{
								"name": "out",
								"sink": map[string]interface{}{
									"log": map[string]interface{}{},
								},
							},
						},
						"edges": []interface{}{
							map[string]interface{}{
								"from": "in",
								"to":   "out",
							},
						},
					},
					"status": map[string]interface{}{
						"phase":              "Running",
						"observedGeneration": int64(1),
						"conditions": []interface{}{
							map[string]interface{}{
								"type":   "Ready",
								"status": "True",
							},
						},
					},
				},
			},
			expected: map[string]interface{}{
				"metadata": map[string]interface{}{
					"name":      "test-pipeline",
					"namespace": "test-namespace",
					"labels": map[string]string{
						"app": "test",
						"numaplane.numaproj.io/parent-rollout-name": "my-rollout",
					},
					"annotations": map[string]string{
						"numaflow.numaproj.io/pause-timestamp": "2025-10-08T22:47:07Z",
					},
				},
				"spec": map[string]interface{}{
					"vertices": []interface{}{
						map[string]interface{}{
							"name": "in",
							"source": map[string]interface{}{
								"generator": map[string]interface{}{
									"rpu":      "500",
									"duration": "1s",
								},
							},
						},
						map[string]interface{}{
							"name": "out",
							"sink": map[string]interface{}{
								"log": map[string]interface{}{},
							},
						},
					},
					"edges": []interface{}{
						map[string]interface{}{
							"from": "in",
							"to":   "out",
						},
					},
				},
				"status": map[string]interface{}{
					"phase":              "Running",
					"observedGeneration": int64(1),
					"conditions": []interface{}{
						map[string]interface{}{
							"type":   "Ready",
							"status": "True",
						},
					},
				},
			},
		},
		{
			name: "minimal object with only name",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"name": "minimal-pipeline",
					},
					"spec": map[string]interface{}{
						"vertices": []interface{}{},
					},
				},
			},
			expected: map[string]interface{}{
				"metadata": map[string]interface{}{
					"name": "minimal-pipeline",
				},
				"spec": map[string]interface{}{
					"vertices": []interface{}{},
				},
			},
		},
		{
			name: "object without metadata",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"spec": map[string]interface{}{
						"test": "value",
					},
				},
			},
			expected: map[string]interface{}{
				"spec": map[string]interface{}{
					"test": "value",
				},
			},
		},
		{
			name: "empty object",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{},
			},
			expected: map[string]interface{}{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetLoggableResource(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
