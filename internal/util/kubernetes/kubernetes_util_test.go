package kubernetes

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestParseResourceFilter(t *testing.T) {
	testCases := []struct {
		name              string
		rules             string
		expectedResources []ResourceType
		hasErr            bool
	}{
		{
			name:              "valid empty rule",
			rules:             "",
			expectedResources: []ResourceType{},
			hasErr:            false,
		},
		{
			name:  "valid rules",
			rules: "group=apps,kind=Deployment;group=rbac.authorization.k8s.io,kind=RoleBinding",
			expectedResources: []ResourceType{
				{
					Group: "apps",
					Kind:  "Deployment",
				},
				{
					Group: "rbac.authorization.k8s.io",
					Kind:  "RoleBinding",
				},
			},
			hasErr: false,
		},
		{
			name:  "valid rules with empty group",
			rules: "group=apps,kind=Deployment;group=,kind=ConfigMap",
			expectedResources: []ResourceType{
				{
					Group: "apps",
					Kind:  "Deployment",
				},
				{
					Group: "",
					Kind:  "ConfigMap",
				},
			},
			hasErr: false,
		},
		{
			name:   "invalid rules",
			rules:  "group=apps,kind=Deployment;grup=,kind=ConfigMap",
			hasErr: true,
		},
	}
	t.Parallel()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resources, err := ParseResourceFilter(tc.rules)
			if tc.hasErr {
				assert.NotNil(t, err)
				assert.Nil(t, resources)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tc.expectedResources, resources)
			}
		})
	}
}
