package kubernetes

import (
	"context"
	"fmt"
	"github.com/numaproj/numaplane/internal/util"
	"os"
	"testing"

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
	assert.Nil(t, SetDynamicClient(restConfig))

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
	pipelineList, err = ListResources(context.Background(), runtimeClient, gvk, client.InNamespace(namespace), client.MatchingLabels{"test": "value"})
	assert.Nil(t, err)
	assert.Len(t, pipelineList.Items, 1)

	err = DeleteResource(context.Background(), runtimeClient, pipelineObject)
	assert.Nil(t, err)
	pipelineList, err = ListLiveResource(context.Background(), common.NumaflowAPIGroup, common.NumaflowAPIVersion, "pipelines", namespace, "test=value", "")
	assert.Nil(t, err)
	assert.Len(t, pipelineList.Items, 0)
}
