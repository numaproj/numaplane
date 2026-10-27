package pipelinerollout

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestAssessVertexReplicaReadiness(t *testing.T) {
	ready := vertexWithReplicas("in", 4, 4)
	shortIn := vertexWithReplicas("in", 4, 2)
	shortCat := vertexWithReplicas("cat", 3, 0)
	shortOut := vertexWithReplicas("out", 1, 0)

	tests := []struct {
		name            string
		vertices        []unstructured.Unstructured
		expectedReady   bool
		expectedReasons []string
		expectErr       bool
	}{
		{
			name:          "no vertices",
			vertices:      nil,
			expectedReady: true,
		},
		{
			name:          "all vertices ready",
			vertices:      []unstructured.Unstructured{ready, vertexWithReplicas("out", 1, 1)},
			expectedReady: true,
		},
		{
			name:          "one vertex short on replicas",
			vertices:      []unstructured.Unstructured{shortIn},
			expectedReady: false,
			expectedReasons: []string{
				"readyReplicas=2 is less than desiredReplicas=4 (vertex: in)",
			},
		},
		{
			name: "reasons accumulate across vertices and skip ready ones",
			vertices: []unstructured.Unstructured{
				shortIn,
				vertexWithReplicas("udf", 2, 2),
				shortCat,
				shortOut,
			},
			expectedReady: false,
			expectedReasons: []string{
				"readyReplicas=2 is less than desiredReplicas=4 (vertex: in)",
				"readyReplicas=0 is less than desiredReplicas=3 (vertex: cat)",
				"readyReplicas=0 is less than desiredReplicas=1 (vertex: out)",
			},
		},
		{
			name: "invalid replica status returns an error",
			vertices: []unstructured.Unstructured{{
				Object: map[string]any{
					"metadata": map[string]any{"name": "in"},
					"status":   map[string]any{"desiredReplicas": "not-a-number"},
				},
			}},
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ready, reasons, err := assessVertexReplicaReadiness(tc.vertices)
			if tc.expectErr {
				require.Error(t, err)
				assert.False(t, ready)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expectedReady, ready)
			assert.Equal(t, tc.expectedReasons, reasons)
		})
	}
}

func vertexWithReplicas(name string, desired, ready int64) unstructured.Unstructured {
	vertex := unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{
			"desiredReplicas": desired,
			"readyReplicas":   ready,
		},
	}}
	vertex.SetName(name)
	return vertex
}
