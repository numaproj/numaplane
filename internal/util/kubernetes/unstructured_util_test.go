package kubernetes

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
