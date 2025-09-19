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
	"os"
	"path/filepath"
	"testing"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ctlrruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
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

// Test_Recycle tests the Recycle function with various input scenarios
func Test_Recycle(t *testing.T) {

	numaLogger := logger.New()
	numaLogger.SetLevel(4)
	restConfig, numaflowClientSet, client, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetClientSets(restConfig))

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig"))
	assert.NoError(t, err)

	ctx := context.Background()

	originalPauseGracePeriodSeconds := float64(60)
	paused := numaflowv1.PipelinePhasePaused

	tests := []struct {
		name                          string
		upgradeStateReason            string
		specHasBeenOverridden         bool
		desiredPhase                  *numaflowv1.PipelinePhase
		pipelinePhase                 numaflowv1.PipelinePhase
		isPromotedPipelineNew         bool
		initialVertexScaleDefinitions []apiv1.VertexScaleDefinition

		expectedDeleted                bool
		expectedError                  bool
		expectedDesiredPhase           numaflowv1.PipelinePhase
		expectSpecOverridden           bool
		expectedVertexScaleDefinitions []apiv1.VertexScaleDefinition
	}{
		/*{
			name:                  "Delete/Recreate - should delete immediately",
			upgradeStateReason:    string(common.LabelValueDeleteRecreateChild),
			specHasBeenOverridden: false,
			pipelinePhase:         numaflowv1.PipelinePhaseRunning,
			expectedDeleted:       true, // Delete recreate should delete immediately
			expectSpecOverridden:  false,
			expectedError:         false,
		},
		{
			name:                  "Progressive Replaced - first attempt - vertices already scaled down - should pause pipeline",
			upgradeStateReason:    string(common.LabelValueProgressiveReplaced),
			specHasBeenOverridden: false,
			pipelinePhase:         numaflowv1.PipelinePhaseRunning,
			// we've already scaled down so these match the expectedVertexScaleDefinitions below
			initialVertexScaleDefinitions: []apiv1.VertexScaleDefinition{
				{
					VertexName: "in",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: int64Ptr(0), // source=0 pods
						Max: int64Ptr(0), // source=0 pods
					},
				},
				{
					VertexName: "out",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: int64Ptr(2), // 50% of 3 historical pod = 1.5, rounded up to 2
						Max: int64Ptr(2),
					},
				},
			},

			expectedDeleted:      false, // Should not delete immediately, should pause first
			expectSpecOverridden: false,
			expectedError:        false,
			expectedDesiredPhase: numaflowv1.PipelinePhasePaused,
			expectedVertexScaleDefinitions: []apiv1.VertexScaleDefinition{
				{
					VertexName: "in",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: int64Ptr(0), // source=0 pods
						Max: int64Ptr(0), // source=0 pods
					},
				},
				{
					VertexName: "out",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: int64Ptr(2), // 50% of 3 historical pod = 1.5, rounded up to 2
						Max: int64Ptr(2),
					},
				},
			},
		},*/
		{
			name:                  "Progressive Replaced - second attempt (force drain) - first apply the new spec",
			upgradeStateReason:    string(common.LabelValueProgressiveReplaced),
			specHasBeenOverridden: false,
			desiredPhase:          &paused,
			pipelinePhase:         numaflowv1.PipelinePhasePaused,
			isPromotedPipelineNew: true,
			// pipeline was scaled to 0
			initialVertexScaleDefinitions: []apiv1.VertexScaleDefinition{
				{
					VertexName: "in",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: int64Ptr(0),
						Max: int64Ptr(0),
					},
				},
				{
					VertexName: "out",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: int64Ptr(0),
						Max: int64Ptr(0),
					},
				},
			},

			expectSpecOverridden: true,
			expectedError:        false,
			expectedDesiredPhase: numaflowv1.PipelinePhaseRunning,
			// pipeline scaled back up to PipelineRollout defined scale except Source is 0
			expectedVertexScaleDefinitions: []apiv1.VertexScaleDefinition{
				{
					VertexName: "in",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: int64Ptr(0), // source=0 pods
						Max: int64Ptr(0), // source=0 pods
					},
				},
				{
					VertexName: "out",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: int64Ptr(2), // original PipelineRollout scale
						Max: int64Ptr(4),
					},
				},
			},
		},
		/*{
			name: "Progressive Replace Failed - no new promoted pipeline available to use so scale to zero",
		},*/
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			// first delete any Pipelines or PipelineRollouts in case they already exist, in Kubernetes
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})

			_ = client.DeleteAllOf(ctx, &apiv1.PipelineRollout{}, &ctlrruntimeclient.DeleteAllOfOptions{ListOptions: ctlrruntimeclient.ListOptions{Namespace: ctlrcommon.DefaultTestNamespace}})

			pipelineRolloutName := "test-pipeline"

			newImage := "quay.io/repo/image:new"
			recyclableImage := "quay.io/repo/image:recyclable"
			oldImage := "quay.io/repo/image:old"

			newPromotedPipelineName := "test-pipeline-3"
			newPromotedPipelinePath := newImage
			originalPromotedPipelineName := "test-pipeline-0"
			originalPromotedPipelinePath := oldImage

			two := int64(2)
			three := int64(3)
			four := int64(4)
			five := int64(5)
			scaledUpVertexDefinitions := []apiv1.VertexScaleDefinition{
				{
					VertexName: "in",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: &three,
						Max: &five,
					},
				},
				{
					VertexName: "out",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: &two,
						Max: &four,
					},
				},
			}

			// Create a PipelineRollout
			pipelineRollout := &apiv1.PipelineRollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineRolloutName,
					Namespace: ctlrcommon.DefaultTestNamespace,
				},
				Spec: apiv1.PipelineRolloutSpec{
					Pipeline: apiv1.Pipeline{
						Spec: runtime.RawExtension{
							Raw: []byte(`{
								"lifecycle": {"pauseGracePeriodSeconds": 60 },
								"vertices": [
									{"name": "in", "source": {"generator": {"rpu": 5, "duration": "1s"}}, "scale": {"min": 3, "max": 5}},
									{"name": "out", "sink": {"log": {}}, "scale": {"min": 2, "max": 4}}
								],
								"edges": [{"from": "in", "to": "out"}]
							}`),
						},
					},
				},
				Status: apiv1.PipelineRolloutStatus{
					ProgressiveStatus: apiv1.PipelineProgressiveStatus{
						HistoricalPodCount: map[string]int{
							"in":  1,
							"out": 3,
						},
					},
				},
			}

			// Create the PipelineRollout in Kubernetes so the Recycle function can find it
			ctlrcommon.CreatePipelineRolloutInK8S(ctx, t, client, pipelineRollout)

			// Create the Pipeline we're recycling
			recyclablePipelineName := "test-pipeline-2"
			pipeline := createPipelineForRecycleTest(pipelineRolloutName, recyclablePipelineName, tc.desiredPhase, tc.pipelinePhase, "recyclable", tc.upgradeStateReason, recyclableImage, tc.specHasBeenOverridden, originalPauseGracePeriodSeconds, tc.initialVertexScaleDefinitions)
			ctlrcommon.CreatePipelineInK8S(ctx, t, numaflowClientSet, pipeline)
			// Convert it to Unstructured for the Recycle function
			var pipelineUnstructured unstructured.Unstructured
			err := util.StructToStruct(pipeline, &pipelineUnstructured.Object)
			assert.NoError(t, err)

			// Create the "promoted" Pipeline, which may either be old or new
			var promotedPipeline *numaflowv1.Pipeline
			if tc.isPromotedPipelineNew {
				promotedPipeline = createPipelineForRecycleTest(pipelineRolloutName, newPromotedPipelineName, nil, tc.pipelinePhase, "promoted", "", newPromotedPipelinePath, false, originalPauseGracePeriodSeconds, scaledUpVertexDefinitions)
			} else {
				promotedPipeline = createPipelineForRecycleTest(pipelineRolloutName, originalPromotedPipelineName, nil, tc.pipelinePhase, "promoted", "", originalPromotedPipelinePath, false, originalPauseGracePeriodSeconds, scaledUpVertexDefinitions)
			}
			ctlrcommon.CreatePipelineInK8S(ctx, t, numaflowClientSet, promotedPipeline)

			// Create a PipelineRolloutReconciler instance
			reconciler := &PipelineRolloutReconciler{}

			// Call the Recycle function
			deleted, err := reconciler.Recycle(ctx, &pipelineUnstructured, client)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedDeleted, deleted)

			// Retrieve the updated Pipeline from Kubernetes
			updatedPipeline, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).Get(ctx, recyclablePipelineName, metav1.GetOptions{})
			if tc.expectedDeleted {
				// If we expected deletion, the pipeline should not exist
				assert.Error(t, err, "Expected pipeline to be deleted")
			} else {
				// If we didn't expect deletion, verify the pipeline was updated correctly
				assert.NoError(t, err)
				assert.NotNil(t, updatedPipeline)

				if tc.expectedDesiredPhase != "" {
					// Verify desiredPhase was set correctly
					assert.Equal(t, tc.expectedDesiredPhase, getDesiredPhase(updatedPipeline))
					assert.Equal(t, int64(120), *updatedPipeline.Spec.Lifecycle.PauseGracePeriodSeconds)
				}

				if tc.expectSpecOverridden {
					assert.Contains(t, updatedPipeline.Annotations, common.AnnotationKeyOverriddenSpec)
					assert.Equal(t, "true", updatedPipeline.Annotations[common.AnnotationKeyOverriddenSpec])
				} else {
					assert.NotContains(t, updatedPipeline.Annotations, common.AnnotationKeyOverriddenSpec)
				}

				if tc.expectedVertexScaleDefinitions != nil && len(tc.expectedVertexScaleDefinitions) > 0 {
					// Verify vertex scale definitions were applied correctly
					/*vertices, found, err := unstructured.NestedSlice(updatedUnstructured.Object, "spec", "vertices")
					assert.NoError(t, err)
					assert.True(t, found, "Expected vertices to be found in pipeline spec")

					// Create a map of expected vertex scales for easier comparison
					expectedScales := make(map[string]apiv1.VertexScaleDefinition)
					for _, vsd := range tc.expectedVertexScaleDefinitions {
						expectedScales[vsd.VertexName] = vsd
					}

					// Check each vertex in the updated pipeline
					for _, vertex := range vertices {
						if vertexMap, ok := vertex.(map[string]interface{}); ok {
							vertexName, found, err := unstructured.NestedString(vertexMap, "name")
							assert.NoError(t, err)
							assert.True(t, found, "Vertex should have a name")

							if expectedScale, exists := expectedScales[vertexName]; exists {
								// Check the scale values
								scaleMap, found, err := unstructured.NestedMap(vertexMap, "scale")
								assert.NoError(t, err)
								assert.True(t, found, "Expected vertex %s to have scale definition", vertexName)

								if expectedScale.ScaleDefinition.Min != nil {
									actualMin, found, err := unstructured.NestedInt64(scaleMap, "min")
									assert.NoError(t, err)
									assert.True(t, found, "Expected vertex %s to have min scale", vertexName)
									assert.Equal(t, *expectedScale.ScaleDefinition.Min, actualMin, "Vertex %s min scale mismatch", vertexName)
								}

								if expectedScale.ScaleDefinition.Max != nil {
									actualMax, found, err := unstructured.NestedInt64(scaleMap, "max")
									assert.NoError(t, err)
									assert.True(t, found, "Expected vertex %s to have max scale", vertexName)
									assert.Equal(t, *expectedScale.ScaleDefinition.Max, actualMax, "Vertex %s max scale mismatch", vertexName)
								}
							}
						}
					}*/
				}
			}
		})
	}
}

func getDesiredPhase(pipeline *numaflowv1.Pipeline) numaflowv1.PipelinePhase {
	if pipeline.Spec.Lifecycle.DesiredPhase == "" {
		return numaflowv1.PipelinePhaseRunning
	}
	return pipeline.Spec.Lifecycle.DesiredPhase
}

// Helper function to create int64 pointer
func int64Ptr(i int64) *int64 {
	return &i
}

// Helper function to create int32 pointer
func int32Ptr(i int32) *int32 {
	return &i
}

/*
	func createRecyclablePipeline(pipelineRolloutName string, pipelineName string, phase, upgradeStateReason string, overriddenSpec bool, pauseGracePeriodSeconds float64) *numaflowv1.Pipeline {
		pauseGracePeriodSecondsInt64 := int64(pauseGracePeriodSeconds)
		pipelineSpecSourceRPU := int64(5)
		pipelineSpecSourceDuration := metav1.Duration{
			Duration: time.Second,
		}
		one := int32(1)
		five := int32(5)

		pipeline := &numaflowv1.Pipeline{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "numaflow.numaproj.io/v1alpha1",
				Kind:       "Pipeline",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      pipelineName,
				Namespace: ctlrcommon.DefaultTestNamespace,
				Labels: map[string]string{
					common.LabelKeyParentRollout:      pipelineRolloutName,
					common.LabelKeyUpgradeState:       string(common.LabelValueUpgradeRecyclable),
					common.LabelKeyUpgradeStateReason: upgradeStateReason,
				},
				Generation: 1,
			},
			Spec: numaflowv1.PipelineSpec{
				Lifecycle: numaflowv1.Lifecycle{
					PauseGracePeriodSeconds: &pauseGracePeriodSecondsInt64,
				},
				Vertices: []numaflowv1.AbstractVertex{
					{
						Name: "in",
						Source: &numaflowv1.Source{
							Generator: &numaflowv1.GeneratorSource{
								RPU:      &pipelineSpecSourceRPU,
								Duration: &pipelineSpecSourceDuration,
							},
						},
					},
					{
						Name: "out",
						Sink: &numaflowv1.Sink{
							AbstractSink: numaflowv1.AbstractSink{
								Log: &numaflowv1.Log{},
							},
						},
						Scale: numaflowv1.Scale{
							Min: &one,
							Max: &five,
						},
					},
				},
				Edges: []numaflowv1.Edge{
					{
						From: "in",
						To:   "out",
					},
				},
			},
			Status: numaflowv1.PipelineStatus{
				Phase:              numaflowv1.PipelinePhase(phase),
				ObservedGeneration: 1,
			},
		}

		// Set annotations if needed
		if overriddenSpec {
			pipeline.Annotations = map[string]string{
				common.AnnotationKeyOverriddenSpec: "true",
			}
		}

		return pipeline
	}
*/
func createPipelineForRecycleTest(pipelineRolloutName, pipelineName string, desiredPhase *numaflowv1.PipelinePhase, phase numaflowv1.PipelinePhase, upgradeState, upgradeStateReason, sinkImagePath string,
	overriddenSpec bool,
	pauseGracePeriodSeconds float64,
	vertexScaleDefinitions []apiv1.VertexScaleDefinition) *numaflowv1.Pipeline {

	pauseGracePeriodSecondsInt64 := int64(pauseGracePeriodSeconds)
	pipelineSpecSourceRPU := int64(5)
	pipelineSpecSourceDuration := metav1.Duration{
		Duration: time.Second,
	}

	pipeline := &numaflowv1.Pipeline{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "numaflow.numaproj.io/v1alpha1",
			Kind:       "Pipeline",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      pipelineName,
			Namespace: ctlrcommon.DefaultTestNamespace,
			Labels: map[string]string{
				common.LabelKeyParentRollout:      pipelineRolloutName,
				common.LabelKeyUpgradeState:       upgradeState,
				common.LabelKeyUpgradeStateReason: upgradeStateReason,
			},
			Generation: 1,
		},
		Spec: numaflowv1.PipelineSpec{
			Lifecycle: numaflowv1.Lifecycle{
				PauseGracePeriodSeconds: &pauseGracePeriodSecondsInt64,
			},
			Vertices: []numaflowv1.AbstractVertex{
				{
					Name: "in",
					Source: &numaflowv1.Source{
						Generator: &numaflowv1.GeneratorSource{
							RPU:      &pipelineSpecSourceRPU,
							Duration: &pipelineSpecSourceDuration,
						},
					},
				},
				{
					Name: "out",
					Sink: &numaflowv1.Sink{
						AbstractSink: numaflowv1.AbstractSink{
							UDSink: &numaflowv1.UDSink{
								Container: &numaflowv1.Container{
									Image: sinkImagePath,
								},
							},
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
		},
		Status: numaflowv1.PipelineStatus{
			Phase:              phase,
			ObservedGeneration: 1,
		},
	}

	// Set annotations if needed
	if overriddenSpec {
		pipeline.Annotations = map[string]string{
			common.AnnotationKeyOverriddenSpec: "true",
		}
	}

	if desiredPhase != nil {
		pipeline.Spec.Lifecycle.DesiredPhase = *desiredPhase
	}

	for index, scaleDefinition := range vertexScaleDefinitions {
		if scaleDefinition.ScaleDefinition != nil {
			if scaleDefinition.ScaleDefinition.Min != nil {
				min := int32(*scaleDefinition.ScaleDefinition.Min)
				pipeline.Spec.Vertices[index].Scale.Min = &min
			}
			if scaleDefinition.ScaleDefinition.Max != nil {
				max := int32(*scaleDefinition.ScaleDefinition.Max)
				pipeline.Spec.Vertices[index].Scale.Max = &max
			}

		}

	}

	return pipeline
}
