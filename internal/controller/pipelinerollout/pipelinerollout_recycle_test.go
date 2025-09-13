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

package pipelinerollout

import (
	"context"
	"testing"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func Test_calculateScaleForRecycle(t *testing.T) {
	ctx := context.Background()

	one := int64(1)
	two := int64(2)
	ten := int64(10)
	thirty := int64(30)
	fifty := int64(50)

	tests := []struct {
		name                     string
		historicalPodCount       map[string]int
		currentPipelineVertexMin map[string]*int64
		percent                  int32
		expectedResult           []apiv1.VertexScaleDefinition
		expectedError            bool
	}{
		{
			name: "vertex found in Historical Pod Count and in PipelineRollout",
			historicalPodCount: map[string]int{
				"vertex1": 3,
				"vertex2": 5,
			},
			currentPipelineVertexMin: map[string]*int64{
				"vertex1": &thirty,
				"vertex2": &fifty,
			},
			percent: 30,
			expectedResult: []apiv1.VertexScaleDefinition{
				{
					VertexName: "vertex1",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: &one,
						Max: &one,
					},
				},
				{
					VertexName: "vertex2",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: &two,
						Max: &two,
					},
				},
			},
			expectedError: false,
		},
		{
			name: "vertex not found in Historical Pod Count but found in PipelineRollout",
			historicalPodCount: map[string]int{
				"vertex1": 3,
				"vertex2": 5,
			},
			currentPipelineVertexMin: map[string]*int64{
				"vertex1": &thirty,
				"vertex3": &fifty,
			},
			percent: 30,
			expectedResult: []apiv1.VertexScaleDefinition{
				{
					VertexName: "vertex1",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: &one,
						Max: &one,
					},
				},
				{
					VertexName: "vertex3",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: &ten,
						Max: &ten,
					},
				},
			},
			expectedError: false,
		},
		{
			name: "vertex not found in Historical Pod Count but found in PipelineRollout (which has scale unset)",
			historicalPodCount: map[string]int{
				"vertex1": 3,
				"vertex2": 5,
			},
			currentPipelineVertexMin: map[string]*int64{
				"vertex1": &thirty,
				"vertex4": &fifty,
			},
			percent: 30,
			expectedResult: []apiv1.VertexScaleDefinition{
				{
					VertexName: "vertex1",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: &one,
						Max: &one,
					},
				},
				{
					VertexName: "vertex4",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: &one, // if Scale isn't set, it implies 1 for min
						Max: &one,
					},
				},
			},
			expectedError: false,
		},
		{
			name: "vertex not found in Historical Pod Count nor in PipelineRollout",
			historicalPodCount: map[string]int{
				"vertex1": 3,
				"vertex2": 5,
			},
			currentPipelineVertexMin: map[string]*int64{
				"vertex1": &thirty,
				"vertex5": &fifty,
			},
			percent: 30,
			expectedResult: []apiv1.VertexScaleDefinition{
				{
					VertexName: "vertex1",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: &one,
						Max: &one,
					},
				},
				{
					VertexName: "vertex5",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: &fifty,
						Max: &fifty,
					},
				},
			},
			expectedError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create a mock pipeline with vertices
			pipeline := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "numaflow.numaproj.io/v1alpha1",
					"kind":       "Pipeline",
					"metadata": map[string]interface{}{
						"name":      "test-pipeline",
						"namespace": "test-namespace",
					},
					"spec": map[string]interface{}{
						"vertices": []interface{}{},
					},
				},
			}

			// Add vertices to the current pipeline spec
			vertices := []interface{}{}
			for vertexName, min := range tc.currentPipelineVertexMin {
				if min == nil {
					vertices = append(vertices, map[string]interface{}{
						"name":  vertexName,
						"scale": nil,
					})
				} else {
					vertices = append(vertices, map[string]interface{}{
						"name": vertexName,
						"scale": map[string]interface{}{
							"min": *min,
							"max": *min + int64(10),
						},
					})
				}
			}
			pipeline.Object["spec"].(map[string]interface{})["vertices"] = vertices

			// Create a PipelineRollout with the test data
			pipelineRollout := &apiv1.PipelineRollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout",
					Namespace: "test-namespace",
				},
				Spec: apiv1.PipelineRolloutSpec{
					Pipeline: apiv1.Pipeline{
						Spec: runtime.RawExtension{
							Raw: []byte(`{
								"vertices": [
									{"name": "vertex1", "scale": {"min": 1}},
									{"name": "vertex2", "scale": {"min": 1}},
									{"name": "vertex3", "scale": {"min": 10}},
									{"name": "vertex4"}
								]
							}`),
						},
					},
				},
				Status: apiv1.PipelineRolloutStatus{
					ProgressiveStatus: apiv1.PipelineProgressiveStatus{
						HistoricalPodCount: tc.historicalPodCount,
					},
				},
			}

			// Call the function
			result, err := calculateScaleForRecycle(ctx, pipeline, pipelineRollout, tc.percent)

			// Check error expectations
			if tc.expectedError {
				assert.Error(t, err)
				assert.Nil(t, result)
				return
			}

			// Check successful case
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Len(t, result, len(tc.expectedResult))

			// Convert result to map for easier comparison (since order might vary)
			resultMap := make(map[string]apiv1.VertexScaleDefinition)
			for _, vsd := range result {
				resultMap[vsd.VertexName] = vsd
			}

			// Check each expected result
			for _, expected := range tc.expectedResult {
				actual, found := resultMap[expected.VertexName]
				assert.True(t, found, "Expected vertex %s not found in result", expected.VertexName)

				assert.Equal(t, expected.VertexName, actual.VertexName)
				assert.NotNil(t, actual.ScaleDefinition)
				assert.NotNil(t, expected.ScaleDefinition)

				if expected.ScaleDefinition.Min != nil {
					assert.NotNil(t, actual.ScaleDefinition.Min)
					assert.Equal(t, *expected.ScaleDefinition.Min, *actual.ScaleDefinition.Min)
				}

				if expected.ScaleDefinition.Max != nil {
					assert.NotNil(t, actual.ScaleDefinition.Max)
					assert.Equal(t, *expected.ScaleDefinition.Max, *actual.ScaleDefinition.Max)
				}
			}
		})
	}
}
