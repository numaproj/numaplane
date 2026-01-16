package kubernetes

import (
	"context"
	"encoding/json"
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
			expectedLabels:      map[string]string{},
			expectedAnnotations: map[string]string{},
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
			expectedAnnotations: map[string]string{},
		},
		{
			name: "only annotations present",
			metadata: map[string]interface{}{
				"annotations": map[string]interface{}{
					"description": "test app",
				},
			},
			expectedLabels: map[string]string{},
			expectedAnnotations: map[string]string{
				"description": "test app",
			},
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
			expectedLabels:      map[string]string{},
			expectedAnnotations: map[string]string{},
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
	err = SetLabels(&obj, map[string]string{
		common.LabelKeyNumaplaneInstance: "my-example",
	})
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

func TestPatchAnnotations(t *testing.T) {
	restConfig, _, _, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	runtimeClient, err := client.New(restConfig, client.Options{Scheme: runtime.NewScheme()})
	assert.Nil(t, err)
	assert.Nil(t, SetClientSets(restConfig))

	ctx := context.Background()
	namespace := "default"

	tests := []struct {
		name                string
		initialAnnotations  map[string]string
		patchAnnotations    map[string]string
		expectedAnnotations map[string]string
		expectedError       bool
	}{
		{
			name:               "patch single annotation on resource with no annotations",
			initialAnnotations: nil,
			patchAnnotations: map[string]string{
				"numaplane.io/test": "value1",
			},
			expectedAnnotations: map[string]string{
				"numaplane.io/test": "value1",
			},
			expectedError: false,
		},
		{
			name: "patch single annotation on resource with existing annotations",
			initialAnnotations: map[string]string{
				"existing": "annotation",
			},
			patchAnnotations: map[string]string{
				"numaplane.io/test": "value1",
			},
			expectedAnnotations: map[string]string{
				"existing":          "annotation",
				"numaplane.io/test": "value1",
			},
			expectedError: false,
		},
		{
			name: "patch multiple annotations",
			initialAnnotations: map[string]string{
				"existing": "annotation",
			},
			patchAnnotations: map[string]string{
				"numaplane.io/test1": "value1",
				"numaplane.io/test2": "value2",
			},
			expectedAnnotations: map[string]string{
				"existing":           "annotation",
				"numaplane.io/test1": "value1",
				"numaplane.io/test2": "value2",
			},
			expectedError: false,
		},
		{
			name: "update existing annotation value",
			initialAnnotations: map[string]string{
				"numaplane.io/test": "old-value",
				"existing":          "annotation",
			},
			patchAnnotations: map[string]string{
				"numaplane.io/test": "new-value",
			},
			expectedAnnotations: map[string]string{
				"numaplane.io/test": "new-value",
				"existing":          "annotation",
			},
			expectedError: false,
		},
		{
			name: "patch with special characters in value",
			initialAnnotations: map[string]string{
				"existing": "annotation",
			},
			patchAnnotations: map[string]string{
				"numaplane.io/timestamp": "2025-12-02T10:30:00Z",
				"numaplane.io/json":      `{"key":"value"}`,
			},
			expectedAnnotations: map[string]string{
				"existing":               "annotation",
				"numaplane.io/timestamp": "2025-12-02T10:30:00Z",
				"numaplane.io/json":      `{"key":"value"}`,
			},
			expectedError: false,
		},
		{
			name:               "patch empty map (no-op)",
			initialAnnotations: map[string]string{"existing": "annotation"},
			patchAnnotations:   map[string]string{},
			expectedAnnotations: map[string]string{
				"existing": "annotation",
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a pipeline for testing
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

			pipelineObject := &unstructured.Unstructured{Object: make(map[string]interface{})}
			pipelineObject.SetGroupVersionKind(numaflowv1.PipelineGroupVersionKind)
			pipelineObject.SetName("test-pipeline")
			pipelineObject.SetNamespace(namespace)

			// Set initial annotations if any
			if tt.initialAnnotations != nil {
				pipelineObject.SetAnnotations(tt.initialAnnotations)
			}

			var pipelineSpecMap map[string]interface{}
			err = util.StructToStruct(pipelineSpec, &pipelineSpecMap)
			assert.Nil(t, err)
			pipelineObject.Object["spec"] = pipelineSpecMap

			// Create the resource
			err = CreateResource(ctx, runtimeClient, pipelineObject)
			assert.Nil(t, err)

			// Get the resource to get the initial resource version
			pipelineObject, err = GetLiveResource(ctx, pipelineObject, "pipelines")
			assert.Nil(t, err)

			// Perform the patch
			err = PatchAnnotations(ctx, runtimeClient, pipelineObject, tt.patchAnnotations)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)

				// Get the updated resource
				pipelineObject, err = GetLiveResource(ctx, pipelineObject, "pipelines")
				assert.Nil(t, err)

				// Verify annotations
				actualAnnotations := pipelineObject.GetAnnotations()
				assert.Equal(t, tt.expectedAnnotations, actualAnnotations, "annotations should match expected")
			}

			// Clean up
			err = DeleteResource(ctx, runtimeClient, pipelineObject)
			assert.Nil(t, err)
		})
	}
}

func TestPatchLabels(t *testing.T) {
	restConfig, _, _, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	runtimeClient, err := client.New(restConfig, client.Options{Scheme: runtime.NewScheme()})
	assert.Nil(t, err)
	assert.Nil(t, SetClientSets(restConfig))

	ctx := context.Background()
	namespace := "default"

	tests := []struct {
		name           string
		initialLabels  map[string]string
		patchLabels    map[string]string
		expectedLabels map[string]string
		expectedError  bool
	}{
		{
			name:          "patch single label on resource with no labels",
			initialLabels: nil,
			patchLabels: map[string]string{
				"app": "test-app",
			},
			expectedLabels: map[string]string{
				"app": "test-app",
			},
			expectedError: false,
		},
		{
			name: "patch single label on resource with existing labels",
			initialLabels: map[string]string{
				"existing": "label",
			},
			patchLabels: map[string]string{
				"app": "test-app",
			},
			expectedLabels: map[string]string{
				"existing": "label",
				"app":      "test-app",
			},
			expectedError: false,
		},
		{
			name: "patch multiple labels",
			initialLabels: map[string]string{
				"existing": "label",
			},
			patchLabels: map[string]string{
				"app":     "test-app",
				"version": "v1.0",
				"env":     "production",
			},
			expectedLabels: map[string]string{
				"existing": "label",
				"app":      "test-app",
				"version":  "v1.0",
				"env":      "production",
			},
			expectedError: false,
		},
		{
			name: "update existing label value",
			initialLabels: map[string]string{
				"app":      "old-app",
				"existing": "label",
			},
			patchLabels: map[string]string{
				"app": "new-app",
			},
			expectedLabels: map[string]string{
				"app":      "new-app",
				"existing": "label",
			},
			expectedError: false,
		},
		{
			name: "patch upgrade state labels",
			initialLabels: map[string]string{
				"existing": "label",
			},
			patchLabels: map[string]string{
				common.LabelKeyUpgradeState:       string(common.LabelValueUpgradePromoted),
				common.LabelKeyUpgradeStateReason: "test-reason",
			},
			expectedLabels: map[string]string{
				"existing":                        "label",
				common.LabelKeyUpgradeState:       string(common.LabelValueUpgradePromoted),
				common.LabelKeyUpgradeStateReason: "test-reason",
			},
			expectedError: false,
		},
		{
			name:          "patch empty map (no-op)",
			initialLabels: map[string]string{"existing": "label"},
			patchLabels:   map[string]string{},
			expectedLabels: map[string]string{
				"existing": "label",
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a pipeline for testing
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

			pipelineObject := &unstructured.Unstructured{Object: make(map[string]interface{})}
			pipelineObject.SetGroupVersionKind(numaflowv1.PipelineGroupVersionKind)
			pipelineObject.SetName("test-pipeline-0")
			pipelineObject.SetNamespace(namespace)

			// Set initial labels if any
			if tt.initialLabels != nil {
				pipelineObject.SetLabels(tt.initialLabels)
			}

			var pipelineSpecMap map[string]interface{}
			err = util.StructToStruct(pipelineSpec, &pipelineSpecMap)
			assert.Nil(t, err)
			pipelineObject.Object["spec"] = pipelineSpecMap

			// Create the resource
			err = CreateResource(ctx, runtimeClient, pipelineObject)
			assert.Nil(t, err)

			// Get the resource to get the initial resource version
			pipelineObject, err = GetLiveResource(ctx, pipelineObject, "pipelines")
			assert.Nil(t, err)

			// Perform the patch
			err = PatchLabels(ctx, runtimeClient, pipelineObject, tt.patchLabels)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)

				// Get the updated resource
				pipelineObject, err = GetLiveResource(ctx, pipelineObject, "pipelines")
				assert.Nil(t, err)

				// Verify labels
				actualLabels := pipelineObject.GetLabels()
				assert.Equal(t, tt.expectedLabels, actualLabels, "labels should match expected")
			}

			// Clean up
			err = DeleteResource(ctx, runtimeClient, pipelineObject)
			assert.Nil(t, err)
		})
	}
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

	// Test ListResourcesOwnedBy by creating two pipelines owned by different PipelineRollouts
	ctx := context.Background()

	// Create two PipelineRollouts (owners)
	pipelineRollout1 := &unstructured.Unstructured{Object: make(map[string]interface{})}
	pipelineRollout1.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "numaplane.numaproj.io",
		Version: "v1alpha1",
		Kind:    "PipelineRollout",
	})
	pipelineRollout1.SetName("rollout-1")
	pipelineRollout1.SetNamespace(namespace)
	pipelineRollout1.Object["spec"] = map[string]interface{}{
		"pipeline": map[string]interface{}{
			"spec": pipelineSpecMap,
		},
	}
	err = CreateResource(ctx, runtimeClient, pipelineRollout1)
	assert.Nil(t, err)
	pipelineRollout1, err = GetResource(ctx, runtimeClient, pipelineRollout1.GroupVersionKind(),
		k8stypes.NamespacedName{Namespace: namespace, Name: "rollout-1"})
	assert.Nil(t, err)
	fmt.Printf("Created PipelineRollout1, UID=%s\n", pipelineRollout1.GetUID())

	pipelineRollout2 := &unstructured.Unstructured{Object: make(map[string]interface{})}
	pipelineRollout2.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "numaplane.numaproj.io",
		Version: "v1alpha1",
		Kind:    "PipelineRollout",
	})
	pipelineRollout2.SetName("rollout-2")
	pipelineRollout2.SetNamespace(namespace)
	pipelineRollout2.Object["spec"] = map[string]interface{}{
		"pipeline": map[string]interface{}{
			"spec": pipelineSpecMap,
		},
	}
	err = CreateResource(ctx, runtimeClient, pipelineRollout2)
	assert.Nil(t, err)
	pipelineRollout2, err = GetResource(ctx, runtimeClient, pipelineRollout2.GroupVersionKind(),
		k8stypes.NamespacedName{Namespace: namespace, Name: "rollout-2"})
	assert.Nil(t, err)
	fmt.Printf("Created PipelineRollout2, UID=%s\n", pipelineRollout2.GetUID())

	// Create pipeline1 owned by rollout1
	pipeline1 := &unstructured.Unstructured{Object: make(map[string]interface{})}
	pipeline1.SetGroupVersionKind(numaflowv1.PipelineGroupVersionKind)
	pipeline1.SetName(pipelineRollout1.GetName() + "-0")
	pipeline1.SetNamespace(namespace)
	pipeline1.SetLabels(map[string]string{"owner-test": "true"})
	pipeline1.Object["spec"] = pipelineSpecMap
	err = ApplyOwnerReference(pipeline1, pipelineRollout1)
	assert.Nil(t, err)
	err = CreateResource(ctx, runtimeClient, pipeline1)
	assert.Nil(t, err)

	// Create pipeline2 owned by rollout2
	pipeline2 := &unstructured.Unstructured{Object: make(map[string]interface{})}
	pipeline2.SetGroupVersionKind(numaflowv1.PipelineGroupVersionKind)
	pipeline2.SetName(pipelineRollout2.GetName() + "-0")
	pipeline2.SetNamespace(namespace)
	pipeline2.SetLabels(map[string]string{"owner-test": "true"})
	pipeline2.Object["spec"] = pipelineSpecMap
	err = ApplyOwnerReference(pipeline2, pipelineRollout2)
	assert.Nil(t, err)
	err = CreateResource(ctx, runtimeClient, pipeline2)
	assert.Nil(t, err)

	// ListResources should find both pipelines
	pipelineList, err = ListResources(ctx, runtimeClient, gvk, namespace, client.MatchingLabels{"owner-test": "true"})
	assert.Nil(t, err)
	assert.Len(t, pipelineList.Items, 2)

	// ListResourcesOwnedBy with rollout1 should find only pipeline1
	ownedByRollout1, err := ListResourcesOwnedBy(ctx, runtimeClient, gvk, namespace, pipelineRollout1, client.MatchingLabels{"owner-test": "true"})
	assert.Nil(t, err)
	assert.Len(t, ownedByRollout1.Items, 1)
	assert.Equal(t, pipelineRollout1.GetName()+"-0", ownedByRollout1.Items[0].GetName())

	// ListResourcesOwnedBy with rollout2 should find only pipeline2
	ownedByRollout2, err := ListResourcesOwnedBy(ctx, runtimeClient, gvk, namespace, pipelineRollout2, client.MatchingLabels{"owner-test": "true"})
	assert.Nil(t, err)
	assert.Len(t, ownedByRollout2.Items, 1)
	assert.Equal(t, pipelineRollout2.GetName()+"-0", ownedByRollout2.Items[0].GetName())

	// Clean up
	err = DeleteResource(ctx, runtimeClient, pipeline1)
	assert.Nil(t, err)
	err = DeleteResource(ctx, runtimeClient, pipeline2)
	assert.Nil(t, err)
	err = DeleteResource(ctx, runtimeClient, pipelineRollout1)
	assert.Nil(t, err)
	err = DeleteResource(ctx, runtimeClient, pipelineRollout2)
	assert.Nil(t, err)
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

func TestUnstructuredToRawExtension(t *testing.T) {
	tests := []struct {
		name           string
		input          *unstructured.Unstructured
		expectedFields map[string]interface{} // fields that should be present in the result
	}{
		{
			name: "pipeline with status - status and runtime metadata should be removed",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "numaflow.numaproj.io/v1alpha1",
					"kind":       "Pipeline",
					"metadata": map[string]interface{}{
						"name":              "test-pipeline",
						"namespace":         "test-namespace",
						"resourceVersion":   "12345",
						"uid":               "abc-123-def",
						"creationTimestamp": "2025-01-01T00:00:00Z",
						"generation":        int64(3),
						"ownerReferences": []interface{}{
							map[string]interface{}{
								"apiVersion": "numaplane.numaproj.io/v1alpha1",
								"kind":       "PipelineRollout",
								"name":       "my-rollout",
								"uid":        "owner-uid-123",
							},
						},
						"managedFields": []interface{}{
							map[string]interface{}{
								"manager":   "numaflow",
								"operation": "Update",
							},
						},
						"labels": map[string]interface{}{
							"app": "test",
						},
						"annotations": map[string]interface{}{
							"description": "test pipeline",
						},
					},
					"spec": map[string]interface{}{
						"vertices": []interface{}{
							map[string]interface{}{
								"name": "in",
								"source": map[string]interface{}{
									"generator": map[string]interface{}{},
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
						"observedGeneration": int64(3),
						"conditions": []interface{}{
							map[string]interface{}{
								"type":   "Ready",
								"status": "True",
							},
						},
					},
				},
			},
			expectedFields: map[string]interface{}{
				"apiVersion": "numaflow.numaproj.io/v1alpha1",
				"kind":       "Pipeline",
				"metadata": map[string]interface{}{
					"name":      "test-pipeline",
					"namespace": "test-namespace",
					"ownerReferences": []interface{}{
						map[string]interface{}{
							"apiVersion": "numaplane.numaproj.io/v1alpha1",
							"kind":       "PipelineRollout",
							"name":       "my-rollout",
							"uid":        "owner-uid-123",
						},
					},
					"labels": map[string]interface{}{
						"app": "test",
					},
					"annotations": map[string]interface{}{
						"description": "test pipeline",
					},
				},
				"spec": map[string]interface{}{
					"vertices": []interface{}{
						map[string]interface{}{
							"name": "in",
							"source": map[string]interface{}{
								"generator": map[string]interface{}{},
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
			},
		},
		{
			name: "configmap without status - should preserve all non-runtime fields",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]interface{}{
						"name":              "test-configmap",
						"namespace":         "test-namespace",
						"resourceVersion":   "67890",
						"uid":               "configmap-uid",
						"creationTimestamp": "2025-01-01T00:00:00Z",
						"generation":        int64(1),
						"managedFields": []interface{}{
							map[string]interface{}{
								"manager": "kubectl",
							},
						},
						"labels": map[string]interface{}{
							"env": "test",
						},
					},
					"data": map[string]interface{}{
						"key1": "value1",
						"key2": "value2",
					},
				},
			},
			expectedFields: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name":      "test-configmap",
					"namespace": "test-namespace",
					"labels": map[string]interface{}{
						"env": "test",
					},
				},
				"data": map[string]interface{}{
					"key1": "value1",
					"key2": "value2",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Store original to verify it's not modified
			originalJSON, err := json.Marshal(tt.input.Object)
			assert.Nil(t, err)

			result, err := UnstructuredToRawExtension(tt.input)
			assert.Nil(t, err)
			assert.NotNil(t, result)
			assert.NotNil(t, result.Raw)

			// Parse the result back to a map
			var resultMap map[string]interface{}
			err = json.Unmarshal(result.Raw, &resultMap)
			assert.Nil(t, err)

			// Verify exact match - this ensures:
			// - All expected fields are present with correct values
			// - No unexpected fields (like status or runtime metadata) are present
			assert.Equal(t, tt.expectedFields, resultMap)

			// Verify original object was not modified
			currentJSON, err := json.Marshal(tt.input.Object)
			assert.Nil(t, err)
			assert.Equal(t, string(originalJSON), string(currentJSON), "original object should not be modified")
		})
	}
}
