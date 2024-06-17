package kubernetes

import (
	"context"
	"os"
	"testing"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	"github.com/numaproj/numaplane/internal/common"
)

func TestIsValidKubernetesNamespace(t *testing.T) {
	testCases := []struct {
		name     string
		expected bool
	}{
		{"valid-namespace", true},
		{"INVALID", false},
		{"kubernetes-namespace", false},
		{"kube-system", false},
		{"1234", true},
		{"valid123", true},
		{"valid.namespace", false},
		{"-invalid", false},
		{"invalid-", false},
		{"valid-namespace-with-long-name-123456789012345678901234567890123456789012345678901234567890123", false},
		{"", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := IsValidKubernetesNamespace(tc.name)
			if actual != tc.expected {
				t.Errorf("For namespace '%s', expected %v but got %v", tc.name, tc.expected, actual)
			}
		})
	}
}

func TestGetNumaplaneInstanceLabel(t *testing.T) {
	yamlBytes, err := os.ReadFile("testdata/svc.yaml")
	assert.Nil(t, err)
	var obj unstructured.Unstructured
	err = yaml.Unmarshal(yamlBytes, &obj)
	assert.Nil(t, err)
	err = SetNumaplaneInstanceLabel(&obj, common.LabelKeyNumaplaneInstance, "my-example")
	assert.Nil(t, err)

	label, err := GetNumaplaneInstanceLabel(&obj, common.LabelKeyNumaplaneInstance)
	assert.Nil(t, err)
	assert.Equal(t, "my-example", label)
}

func TestGetNumaplaneInstanceLabelWithInvalidData(t *testing.T) {
	yamlBytes, err := os.ReadFile("testdata/svc-with-invalid-data.yaml")
	assert.Nil(t, err)
	var obj unstructured.Unstructured
	err = yaml.Unmarshal(yamlBytes, &obj)
	assert.Nil(t, err)

	_, err = GetNumaplaneInstanceLabel(&obj, "valid-label")
	assert.Error(t, err)
	assert.Equal(t, "failed to get labels from target object /v1, Kind=Service /my-service: .metadata.labels accessor error: contains non-string value in the map under key \"invalid-label\": <nil> is of the type <nil>, expected string", err.Error())
}

func TestGetSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	err := corev1.AddToScheme(scheme)
	assert.NoError(t, err)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-secret",
				Namespace: "test-namespace",
			},
			Data: map[string][]byte{
				"key": []byte("value"),
			},
		},
	).Build()

	ctx := context.TODO()

	secret, err := GetSecret(ctx, fakeClient, "test-namespace", "test-secret")

	assert.NoError(t, err)
	assert.NotNil(t, secret)
	assert.Equal(t, "value", string(secret.Data["key"]))
}
func TestIsValidKubernetesManifestFile(t *testing.T) {

	testCases := []struct {
		name         string
		resourceName string
		expected     bool
	}{
		{
			name:         "Invalid Name",
			resourceName: "data.md",
			expected:     false,
		},

		{
			name:         "valid name",
			resourceName: "my.yml",
			expected:     true,
		},

		{
			name:         "Valid Json file",
			resourceName: "pipeline.json",
			expected:     true,
		},

		{
			name:         "Valid name yaml",
			resourceName: "pipeline.yaml",
			expected:     true,
		},
		{
			name:         "Valid name yaml",
			resourceName: "pipeline.xyz.yaml",
			expected:     true,
		},
		{
			name:         "Valid name yaml",
			resourceName: "pipeline.xyz.hjk.json",
			expected:     true,
		},
		{
			name:         "Invalid File",
			resourceName: "main.go",
			expected:     false,
		},
		{
			name:         "Invalid File",
			resourceName: "main..json.go",
			expected:     false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ok := IsValidKubernetesManifestFile(tc.resourceName)
			assert.Equal(t, tc.expected, ok)
		})
	}

}

func TestSetAnnotation(t *testing.T) {
	key1, value1 := "some_key_1", "some_value_1"
	key2, value2 := "some_key_2", "some_value_2"

	pipelineRollout := &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pipeline-test",
			Namespace: "default",
		},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: runtime.RawExtension{Raw: []byte(`{"key":"value"}`)},
		},
	}

	// Invoke the method under test
	SetAnnotation(pipelineRollout, key1, value1)

	// Check if the annotation was correctly set
	annotations := pipelineRollout.GetAnnotations()
	assert.NotNil(t, annotations, "Expected annotations to be set on the pipelineRollout object")
	assert.Contains(t, annotations, key1, "Expected the key to be set in the annotations")
	assert.Equal(t, value1, annotations[key1], "Expected the value to be set for the key in the annotations")

	// Overwrite existing annotation
	newValue := "new_value"
	SetAnnotation(pipelineRollout, key1, newValue)

	// Check if the annotation was correctly updated
	annotations = pipelineRollout.GetAnnotations()
	assert.NotNil(t, annotations, "Expected annotations to be set on the pipelineRollout object")
	assert.Contains(t, annotations, key1, "Expected the key to be set in the annotations")
	assert.NotEqual(t, value1, annotations[key1], "Expected the old value to be replaced")
	assert.Equal(t, newValue, annotations[key1], "Expected the new value to be set for the key in the annotations")

	// Add one more annotation
	SetAnnotation(pipelineRollout, key2, value2)
	assert.NotNil(t, annotations, "Expected annotations to be set on the pipelineRollout object")
	assert.Len(t, annotations, 2, "Expected annotations to be of length 2")
	assert.Contains(t, annotations, key2, "Expected the key to be set in the annotations")
	assert.Equal(t, value2, annotations[key2], "Expected the value to be set for the key in the annotations")
}
