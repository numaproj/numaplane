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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ctlrruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"
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
	running := numaflowv1.PipelinePhaseRunning
	failed := numaflowv1.PipelinePhaseFailed
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
	// these vertex definitions represent our pipeline when it's scaled up to that of the PipelineRollout definition
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

	tests := []struct {
		name string
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		// INPUTS
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		// the "Upgrade State Reason" label on the Pipeline being recycled which explains the condition in which it was recycled
		upgradeStateReason string
		// if the spec has already been overridden on the pipeline for the purpose of force draining
		specHasBeenOverridden bool
		// if the pipeline annotation for "requires-drain" is set
		requiresDrain bool
		// the lifecycle.desiredPhase on the pipeline
		desiredPhase *numaflowv1.PipelinePhase
		// the status.phase on the pipeline
		pipelinePhase numaflowv1.PipelinePhase
		// if there's a new "promoted" pipeline which can be used for force draining (or really if there's a pipeline whose definition matches the PipelineRollout)
		isPromotedPipelineNew bool
		// if the "force drain failure time" annotation is set on the pipeline
		isForceDrainFailureTimeSet bool
		// the Vertex scale values of the Pipeline
		initialVertexScaleDefinitions []apiv1.VertexScaleDefinition

		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		// OUTPUTS TO VERIFY
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		// should this pipeline be deleted during the call to Recycle()?
		expectedDeleted bool
		// should Recycle() return an error?
		expectedError bool
		// expected value of lifecycle.desiredPhase; if set to nil, we don't care what it is
		expectedDesiredPhase *numaflowv1.PipelinePhase
		// do we expect the value of the pipeline's spec to be that of the "promoted" pipeline?
		expectSpecOverridden bool
		// expected pipeline vertex scale definitions at the end of the call
		expectedVertexScaleDefinitions []apiv1.VertexScaleDefinition
	}{
		{
			name:                  "Delete/Recreate - should delete immediately",
			upgradeStateReason:    string(common.LabelValueDeleteRecreateChild),
			specHasBeenOverridden: false,
			requiresDrain:         true,
			pipelinePhase:         numaflowv1.PipelinePhaseRunning,
			expectedDeleted:       true, // Delete recreate should delete immediately
			expectSpecOverridden:  false,
			expectedError:         false,
		},
		{
			name:                       "Progressive Replace - second attempt (force drain failed)",
			upgradeStateReason:         string(common.LabelValueProgressiveReplaced),
			specHasBeenOverridden:      true,
			requiresDrain:              true,
			desiredPhase:               &paused,
			pipelinePhase:              failed,
			isPromotedPipelineNew:      true,
			isForceDrainFailureTimeSet: true,
			expectSpecOverridden:       true,
			expectedDesiredPhase:       &running,
		},
		{
			name:                  "Progressive Replaced - second attempt (force drain) - first apply the new spec",
			upgradeStateReason:    string(common.LabelValueProgressiveReplaced),
			specHasBeenOverridden: false,
			requiresDrain:         true,
			desiredPhase:          &paused,
			pipelinePhase:         paused,
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
			expectedDesiredPhase: &running,
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
		{
			name: "Progressive Replaced - second attempt (force drain) - new spec was applied - now scale it down before pausing",
			// preconditions:
			// - desiredPhase=Running, phase=Running, initialScale=matching pipelinerollout
			upgradeStateReason:    string(common.LabelValueProgressiveReplaced),
			specHasBeenOverridden: true,
			requiresDrain:         true,
			desiredPhase:          &running,
			pipelinePhase:         running,
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
						Min: int64Ptr(2),
						Max: int64Ptr(4),
					},
				},
			},

			expectSpecOverridden: true,
			expectedError:        false,
			expectedDesiredPhase: &running,
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
						Min: int64Ptr(2), // 50% of 3 historical pod = 1.5, rounded up to 2
						Max: int64Ptr(2),
					},
				},
			},
		},
		{
			name: "Progressive Replace Failed - no new promoted pipeline available to use so scale to zero",
			// preconditions:
			// - desiredPhase=Paused, phase=Paused, initialScale=previous test's expected scale
			upgradeStateReason:    string(common.LabelValueProgressiveReplaced),
			specHasBeenOverridden: true,
			requiresDrain:         true,
			desiredPhase:          &paused,
			pipelinePhase:         paused,
			isPromotedPipelineNew: false,
			// pipeline was scaled down to prepare for pausing
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

			expectSpecOverridden: true,
			expectedError:        false,
			expectedDesiredPhase: nil,
			// vertex definitions scaled to 0
			expectedVertexScaleDefinitions: []apiv1.VertexScaleDefinition{
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
						Min: int64Ptr(0), // scaled down to 0
						Max: int64Ptr(0), // scaled down to 0
					},
				},
			},
		},
		{
			name:                  "Pipeline never had any data - just delete it",
			upgradeStateReason:    string(common.LabelValueProgressiveSuccess),
			requiresDrain:         false,
			desiredPhase:          &running, // it could be running but not have data if the Source Vertex had max=0
			pipelinePhase:         running,
			isPromotedPipelineNew: true,
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
						Min: int64Ptr(0),
						Max: int64Ptr(0),
					},
				},
			},
			expectedDeleted: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			// first delete any Pipelines or PipelineRollouts in case they already exist, in Kubernetes
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = client.DeleteAllOf(ctx, &apiv1.PipelineRollout{}, &ctlrruntimeclient.DeleteAllOfOptions{ListOptions: ctlrruntimeclient.ListOptions{Namespace: ctlrcommon.DefaultTestNamespace}})

			// Create a definition for a Promoted Pipeline
			// We will use it below for our "promoted" pipeline if the test calls for a new "promoted" pipeline
			// We will also use this for our PipelineRollout's definition
			newPromotedPipelineDefinition := createPipelineForRecycleTest(pipelineRolloutName, newPromotedPipelineName, nil, tc.pipelinePhase, "promoted", "", newPromotedPipelinePath, false, originalPauseGracePeriodSeconds, scaledUpVertexDefinitions, true)
			var newPromotedDefUnstructured unstructured.Unstructured
			err := util.StructToStruct(newPromotedPipelineDefinition, &newPromotedDefUnstructured.Object)
			assert.NoError(t, err)

			// Extract spec from the newPromotedPipelineDefinition and set it to PipelineRollout
			specData := newPromotedDefUnstructured.Object["spec"]
			specBytes, err := json.Marshal(specData)
			assert.NoError(t, err)

			// Create a PipelineRollout
			pipelineRollout := &apiv1.PipelineRollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineRolloutName,
					Namespace: ctlrcommon.DefaultTestNamespace,
				},
				Spec: apiv1.PipelineRolloutSpec{
					Pipeline: apiv1.Pipeline{
						Spec: runtime.RawExtension{
							Raw: specBytes,
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
			pipeline := createPipelineForRecycleTest(pipelineRolloutName, recyclablePipelineName, tc.desiredPhase, tc.pipelinePhase, "recyclable", tc.upgradeStateReason, recyclableImage, tc.specHasBeenOverridden, originalPauseGracePeriodSeconds, tc.initialVertexScaleDefinitions, tc.requiresDrain)
			ctlrcommon.CreatePipelineInK8S(ctx, t, numaflowClientSet, pipeline)
			// Convert it to Unstructured for the Recycle function
			var pipelineUnstructured unstructured.Unstructured
			err = util.StructToStruct(pipeline, &pipelineUnstructured.Object)
			assert.NoError(t, err)

			// Create the "promoted" Pipeline, which may either be old or new
			var promotedPipeline *numaflowv1.Pipeline
			if tc.isPromotedPipelineNew {
				promotedPipeline = newPromotedPipelineDefinition
			} else {
				// this doesn't match that of our PipelineRollout, which means it will be seen as "old"
				promotedPipeline = createPipelineForRecycleTest(pipelineRolloutName, originalPromotedPipelineName, nil, tc.pipelinePhase, "promoted", "", originalPromotedPipelinePath, false, originalPauseGracePeriodSeconds, scaledUpVertexDefinitions, true)
			}
			ctlrcommon.CreatePipelineInK8S(ctx, t, numaflowClientSet, promotedPipeline)

			reconciler := &PipelineRolloutReconciler{}

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

				if tc.expectedDesiredPhase != nil && *tc.expectedDesiredPhase == numaflowv1.PipelinePhasePaused {
					// Verify desiredPhase was set correctly
					assert.Equal(t, *tc.expectedDesiredPhase, getDesiredPhase(updatedPipeline))
					assert.Equal(t, int64(120), *updatedPipeline.Spec.Lifecycle.PauseGracePeriodSeconds)
				}

				// Verify if the force drain failure time annotation is set
				if tc.isForceDrainFailureTimeSet {
					assert.Contains(t, updatedPipeline.Annotations, common.AnnotationKeyForceDrainFailureStartTime)
				}

				// Should the spec be overridden with the "promoted" spec?
				if tc.expectSpecOverridden {
					assert.Contains(t, updatedPipeline.Annotations, common.AnnotationKeyOverriddenSpec)
					assert.Equal(t, "true", updatedPipeline.Annotations[common.AnnotationKeyOverriddenSpec])
				} else {
					assert.NotContains(t, updatedPipeline.Annotations, common.AnnotationKeyOverriddenSpec)
				}

				if len(tc.expectedVertexScaleDefinitions) > 0 {
					// Verify vertex scale definitions were applied correctly using the structured updatedPipeline
					assert.Len(t, updatedPipeline.Spec.Vertices, len(tc.expectedVertexScaleDefinitions), "Number of vertices should match expected scale definitions")

					// Check each vertex in the updated pipeline (assuming same order as expectedVertexScaleDefinitions)
					for i, expectedScale := range tc.expectedVertexScaleDefinitions {
						vertex := updatedPipeline.Spec.Vertices[i]
						assert.Equal(t, expectedScale.VertexName, vertex.Name, "Vertex name should match at index %d", i)

						if expectedScale.ScaleDefinition != nil {
							if expectedScale.ScaleDefinition.Min != nil {
								assert.NotNil(t, vertex.Scale.Min, "Expected vertex %s to have min scale", vertex.Name)
								expectedMin := int32(*expectedScale.ScaleDefinition.Min)
								assert.Equal(t, expectedMin, *vertex.Scale.Min, "Vertex %s min scale mismatch", vertex.Name)
							}

							if expectedScale.ScaleDefinition.Max != nil {
								assert.NotNil(t, vertex.Scale.Max, "Expected vertex %s to have max scale", vertex.Name)
								expectedMax := int32(*expectedScale.ScaleDefinition.Max)
								assert.Equal(t, expectedMax, *vertex.Scale.Max, "Vertex %s max scale mismatch", vertex.Name)
							}
						}
					}
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

func createPipelineForRecycleTest(pipelineRolloutName, pipelineName string, desiredPhase *numaflowv1.PipelinePhase, phase numaflowv1.PipelinePhase, upgradeState, upgradeStateReason, sinkImagePath string,
	overriddenSpec bool,
	pauseGracePeriodSeconds float64,
	vertexScaleDefinitions []apiv1.VertexScaleDefinition,
	requiresDrain bool) *numaflowv1.Pipeline {

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
			InterStepBufferServiceName: "default",
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

	pipeline.Annotations = map[string]string{}

	// Set annotations if needed
	if overriddenSpec {
		pipeline.Annotations[common.AnnotationKeyOverriddenSpec] = "true"
	}

	if requiresDrain {
		pipeline.Annotations[common.AnnotationKeyRequiresDrain] = "true"
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
			pipeline.Spec.Vertices[index].Scale.Disabled = false
		}

	}

	return pipeline
}
