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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctlrruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"
)

// createTestMetrics creates a minimal CustomMetrics instance for testing
func createTestMetrics() *metrics.CustomMetrics {
	return &metrics.CustomMetrics{
		ProgressivePipelineDrains: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_progressive_pipeline_drains",
			Help: "Test metric",
		}, []string{metrics.LabelNamespace, metrics.LabelPipelineRollout, metrics.LabelPipeline, metrics.LabelDrainComplete, metrics.LabelDrainResult}),
		NumaflowControllerRolloutRunningVersion: make(map[string]map[string]string),
	}
}

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

func Test_getForceAppliedSpec(t *testing.T) {
	ctx := context.Background()

	zero := int32(0)

	tests := []struct {
		name                  string
		currentPipelineISBSvc string
		newPipelineISBSvc     string
		newPipelineLifecycle  *numaflowv1.Lifecycle
		expectError           bool
		expectedISBSvcName    string
	}{
		{
			name:                  "preserves current pipeline ISBService name",
			currentPipelineISBSvc: "current-isbsvc",
			newPipelineISBSvc:     "new-isbsvc",
			newPipelineLifecycle:  &numaflowv1.Lifecycle{DesiredPhase: numaflowv1.PipelinePhasePaused},
			expectedISBSvcName:    "current-isbsvc",
		},
		{
			name:                  "uses default ISBService name when current pipeline has none set",
			currentPipelineISBSvc: "",
			newPipelineISBSvc:     "new-isbsvc",
			newPipelineLifecycle:  &numaflowv1.Lifecycle{DesiredPhase: numaflowv1.PipelinePhaseRunning},
			expectedISBSvcName:    "default",
		},
		{
			name:                  "sets desiredPhase to Running when new pipeline lifecycle is unset",
			currentPipelineISBSvc: "current-isbsvc",
			newPipelineISBSvc:     "new-isbsvc",
			newPipelineLifecycle:  nil,
			expectedISBSvcName:    "current-isbsvc",
		},
		{
			name:                  "sets desiredPhase to Running when new pipeline is already Running",
			currentPipelineISBSvc: "shared-isbsvc",
			newPipelineISBSvc:     "shared-isbsvc",
			newPipelineLifecycle:  &numaflowv1.Lifecycle{DesiredPhase: numaflowv1.PipelinePhaseRunning},
			expectedISBSvcName:    "shared-isbsvc",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			currentPipeline := pipelineUnstructuredForForceAppliedSpecTest(
				"current", tc.currentPipelineISBSvc, &numaflowv1.Lifecycle{DesiredPhase: numaflowv1.PipelinePhasePaused},
			)
			newPipeline := pipelineUnstructuredForForceAppliedSpecTest(
				"new", tc.newPipelineISBSvc, tc.newPipelineLifecycle,
			)
			newPipelineBefore := newPipeline.DeepCopy()

			result, err := getForceAppliedSpec(ctx, currentPipeline, newPipeline)
			if tc.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			isbsvcName, err := numaflowtypes.GetPipelineISBSVCName(result)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedISBSvcName, isbsvcName)

			desiredPhase, err := numaflowtypes.GetPipelineDesiredPhase(result)
			assert.NoError(t, err)
			assert.Equal(t, string(numaflowv1.PipelinePhaseRunning), desiredPhase)

			var pipelineSpec numaflowv1.Pipeline
			err = runtime.DefaultUnstructuredConverter.FromUnstructured(result.Object, &pipelineSpec)
			assert.NoError(t, err)

			for _, vertex := range pipelineSpec.Spec.Vertices {
				if vertex.Source != nil {
					assert.NotNil(t, vertex.Scale.Min)
					assert.NotNil(t, vertex.Scale.Max)
					assert.Equal(t, zero, *vertex.Scale.Min)
					assert.Equal(t, zero, *vertex.Scale.Max)
				}
			}

			for _, vertex := range pipelineSpec.Spec.Vertices {
				if vertex.Sink != nil {
					assert.NotNil(t, vertex.Scale.Min)
					assert.NotNil(t, vertex.Scale.Max)
					assert.Equal(t, int32(2), *vertex.Scale.Min)
					assert.Equal(t, int32(5), *vertex.Scale.Max)
				}
			}

			assert.Equal(t, newPipelineBefore.Object, newPipeline.Object)
		})
	}
}

func pipelineUnstructuredForForceAppliedSpecTest(name, isbsvcName string, lifecycle *numaflowv1.Lifecycle) *unstructured.Unstructured {
	vertices := []numaflowv1.AbstractVertex{
		{
			Name: "in",
			Source: &numaflowv1.Source{
				Generator: &numaflowv1.GeneratorSource{},
			},
			Scale: numaflowv1.Scale{
				Min: int32Ptr(3),
				Max: int32Ptr(6),
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
				Min: int32Ptr(2),
				Max: int32Ptr(5),
			},
		},
	}

	spec := numaflowv1.PipelineSpec{
		InterStepBufferServiceName: isbsvcName,
		Vertices:                   vertices,
	}
	if lifecycle != nil {
		spec.Lifecycle = *lifecycle
	}

	pipeline := &numaflowv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ctlrcommon.DefaultTestNamespace,
		},
		Spec: spec,
	}

	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pipeline)
	if err != nil {
		panic(err)
	}
	return &unstructured.Unstructured{Object: obj}
}

func int32Ptr(i int32) *int32 {
	return &i
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
		// initial value of the force-drain-specs-started annotation (comma-delimited pipeline names)
		initialForceDrainPipelinesStarted string
		// initial value of the force-drain-specs-completed annotation (comma-delimited pipeline names)
		initialForceDrainPipelinesCompleted string
		// if the pipeline annotation for "requires-drain" is set
		requiresDrain bool
		// the lifecycle.desiredPhase on the pipeline
		desiredPhase *numaflowv1.PipelinePhase
		// the status.phase on the pipeline
		pipelinePhase numaflowv1.PipelinePhase
		// if there's a new "promoted" pipeline which can be used for force draining (or really if there's a pipeline whose definition matches the PipelineRollout)
		isPromotedPipelineNew bool
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
		// expected pipeline name in force-drain-specs-started annotation (empty string means not checked)
		expectForceDrainPipelinesStarted string
		// expected pipeline name in force-drain-specs-completed annotation (empty string means not checked)
		expectedForceDrainPipelinesCompleted string
		// expected pipeline vertex scale definitions at the end of the call
		expectedVertexScaleDefinitions []apiv1.VertexScaleDefinition
		// expected whether the force drain failure time annotation is set
		expectForceDrainFailureTimeSet bool
	}{
		{
			name:               "Delete/Recreate - should delete immediately",
			upgradeStateReason: string(common.LabelValueDeleteRecreateChild),
			requiresDrain:      true,
			pipelinePhase:      numaflowv1.PipelinePhaseRunning,
			expectedDeleted:    true, // Delete recreate should delete immediately
			expectedError:      false,
		},
		{
			name:                           "Progressive Replace - original drain failed, wait before force drain",
			upgradeStateReason:             string(common.LabelValueProgressiveReplaced),
			requiresDrain:                  true,
			desiredPhase:                   &paused,
			pipelinePhase:                  failed,
			isPromotedPipelineNew:          true,
			expectForceDrainFailureTimeSet: true,
		},
		{
			name:                              "Progressive Replace - second attempt (force drain caused pipeline to have phase=Failed)",
			upgradeStateReason:                string(common.LabelValueProgressiveReplaced),
			initialForceDrainPipelinesStarted: newPromotedPipelineName + ",",
			requiresDrain:                     true,
			desiredPhase:                      &paused,
			pipelinePhase:                     failed,
			isPromotedPipelineNew:             true,
			expectForceDrainFailureTimeSet:    true,
			expectForceDrainPipelinesStarted:  newPromotedPipelineName + ",",
			expectedDesiredPhase:              &running,
		},
		{
			name:                  "Progressive Replaced - second attempt (force drain) - first apply the new spec",
			upgradeStateReason:    string(common.LabelValueProgressiveReplaced),
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

			expectForceDrainPipelinesStarted: newPromotedPipelineName + ",",
			expectedError:                    false,
			expectedDesiredPhase:             &running,
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
			upgradeStateReason:                string(common.LabelValueProgressiveReplaced),
			initialForceDrainPipelinesStarted: newPromotedPipelineName + ",",
			requiresDrain:                     true,
			desiredPhase:                      &running,
			pipelinePhase:                     running,
			isPromotedPipelineNew:             true,
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

			expectForceDrainPipelinesStarted: newPromotedPipelineName + ",",
			expectedError:                    false,
			expectedDesiredPhase:             &running,
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
			upgradeStateReason:                string(common.LabelValueProgressiveReplaced),
			initialForceDrainPipelinesStarted: originalPromotedPipelineName + ",",
			requiresDrain:                     true,
			desiredPhase:                      &paused,
			pipelinePhase:                     paused,
			isPromotedPipelineNew:             false,
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

			expectForceDrainPipelinesStarted: originalPromotedPipelineName + ",",
			expectedError:                    false,
			expectedDesiredPhase:             nil,
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
			newPromotedPipelineDefinition := createPipelineForRecycleTest(pipelineRolloutName, newPromotedPipelineName, nil, tc.pipelinePhase, "promoted", "", newPromotedPipelinePath, "", "", originalPauseGracePeriodSeconds, scaledUpVertexDefinitions, true)
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
			pipeline := createPipelineForRecycleTest(pipelineRolloutName, recyclablePipelineName, tc.desiredPhase, tc.pipelinePhase, "recyclable", tc.upgradeStateReason, recyclableImage, tc.initialForceDrainPipelinesStarted, tc.initialForceDrainPipelinesCompleted, originalPauseGracePeriodSeconds, tc.initialVertexScaleDefinitions, tc.requiresDrain)
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
				promotedPipeline = createPipelineForRecycleTest(pipelineRolloutName, originalPromotedPipelineName, nil, tc.pipelinePhase, "promoted", "", originalPromotedPipelinePath, "", "", originalPauseGracePeriodSeconds, scaledUpVertexDefinitions, true)
			}
			ctlrcommon.CreatePipelineInK8S(ctx, t, numaflowClientSet, promotedPipeline)

			reconciler := &PipelineRolloutReconciler{
				client:        client,
				customMetrics: createTestMetrics(),
				recorder:      record.NewFakeRecorder(100),
			}

			deleted, err := reconciler.Recycle(ctx, &pipelineUnstructured)
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
				if tc.expectForceDrainFailureTimeSet {
					assert.Contains(t, updatedPipeline.Annotations, common.AnnotationKeyDrainFailureStartTime)
				}

				// Check if force-drain-specs-started annotation matches expected value
				if tc.expectForceDrainPipelinesStarted != "" {
					assert.Contains(t, updatedPipeline.Annotations, common.AnnotationKeyForceDrainSpecsStarted)
					assert.Equal(t, tc.expectForceDrainPipelinesStarted, updatedPipeline.Annotations[common.AnnotationKeyForceDrainSpecsStarted],
						"force-drain-specs-started annotation mismatch")
				}

				// Check if force-drain-specs-completed annotation matches expected value
				if tc.expectedForceDrainPipelinesCompleted != "" {
					assert.Contains(t, updatedPipeline.Annotations, common.AnnotationKeyForceDrainSpecsCompleted)
					assert.Equal(t, tc.expectedForceDrainPipelinesCompleted, updatedPipeline.Annotations[common.AnnotationKeyForceDrainSpecsCompleted],
						"force-drain-specs-completed annotation mismatch")
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

func Test_markPipelineForceDrainStarted_clearsForceDrainFailureStartTime(t *testing.T) {
	ctx := context.Background()

	pipeline := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "numaflow.numaproj.io/v1alpha1",
			"kind":       "Pipeline",
			"metadata": map[string]interface{}{
				"name":      "test-pipeline",
				"namespace": ctlrcommon.DefaultTestNamespace,
			},
		},
	}
	pipeline.SetGroupVersionKind(numaflowv1.PipelineGroupVersionKind)
	pipeline.SetAnnotations(map[string]string{
		common.AnnotationKeyDrainFailureStartTime: time.Now().Add(-2 * time.Hour).Format(time.RFC3339),
	})

	scheme := runtime.NewScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pipeline).Build()

	err := markPipelineForceDrainStarted(ctx, fakeClient, pipeline, "promoted-pipeline-1")
	assert.NoError(t, err)
	assert.Equal(t, "promoted-pipeline-1,", pipeline.GetAnnotations()[common.AnnotationKeyForceDrainSpecsStarted])
	assert.Empty(t, pipeline.GetAnnotations()[common.AnnotationKeyDrainFailureStartTime])
}

func Test_checkForFailedPipeline(t *testing.T) {
	ctx := context.Background()
	waitDuration := time.Duration(config.GetForceDrainFailureWaitDuration()) * time.Second
	withinWait := waitDuration - time.Second
	pastWait := waitDuration + time.Second

	tests := []struct {
		name                      string
		failureStartTimeAgo       *time.Duration // nil = no annotation (first failure detection)
		expectNonTransientFailure bool
		expectAnnotationSet       bool
	}{
		{
			name:                "first failure sets start time and waits",
			expectAnnotationSet: true,
		},
		{
			name:                "failure within wait duration",
			failureStartTimeAgo: &withinWait,
		},
		{
			name:                      "failure past wait duration",
			failureStartTimeAgo:       &pastWait,
			expectNonTransientFailure: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pipeline := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "numaflow.numaproj.io/v1alpha1",
					"kind":       "Pipeline",
					"metadata": map[string]interface{}{
						"name":      "test-pipeline",
						"namespace": ctlrcommon.DefaultTestNamespace,
					},
				},
			}
			pipeline.SetGroupVersionKind(numaflowv1.PipelineGroupVersionKind)

			if tc.failureStartTimeAgo != nil {
				pipeline.SetAnnotations(map[string]string{
					common.AnnotationKeyDrainFailureStartTime: time.Now().Add(-*tc.failureStartTimeAgo).Format(time.RFC3339),
				})
			}

			scheme := runtime.NewScheme()
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pipeline).Build()
			reconciler := &PipelineRolloutReconciler{
				client:        fakeClient,
				customMetrics: createTestMetrics(),
				recorder:      record.NewFakeRecorder(100),
			}

			nonTransientFailure, err := reconciler.checkForFailedPipeline(ctx, pipeline)

			assert.NoError(t, err)
			assert.Equal(t, tc.expectNonTransientFailure, nonTransientFailure)

			if tc.expectAnnotationSet {
				assert.NotEmpty(t, pipeline.GetAnnotations()[common.AnnotationKeyDrainFailureStartTime])
			}
		})
	}
}

func Test_checkForValueInCommaDelimitedAnnotation(t *testing.T) {
	testAnnotationKey := "test-annotation-key"

	tests := []struct {
		name           string
		annotations    map[string]string
		value          string
		annotationKey  string
		expectedResult bool
	}{
		{
			name:           "nil annotations",
			annotations:    nil,
			value:          "abc",
			annotationKey:  testAnnotationKey,
			expectedResult: false,
		},
		{
			name:           "annotation key not present",
			annotations:    map[string]string{"other-key": "value"},
			value:          "abc",
			annotationKey:  testAnnotationKey,
			expectedResult: false,
		},
		{
			name:           "empty annotation value",
			annotations:    map[string]string{testAnnotationKey: ""},
			value:          "abc",
			annotationKey:  testAnnotationKey,
			expectedResult: false,
		},
		{
			name:           "value found - single value in annotation",
			annotations:    map[string]string{testAnnotationKey: "abc,"},
			value:          "abc",
			annotationKey:  testAnnotationKey,
			expectedResult: true,
		},
		{
			name:           "value found - first of multiple values",
			annotations:    map[string]string{testAnnotationKey: "abc,def,ghi,"},
			value:          "abc",
			annotationKey:  testAnnotationKey,
			expectedResult: true,
		},
		{
			name:           "value found - middle of multiple values",
			annotations:    map[string]string{testAnnotationKey: "abc,def,ghi,"},
			value:          "def",
			annotationKey:  testAnnotationKey,
			expectedResult: true,
		},
		{
			name:           "value found - last of multiple values",
			annotations:    map[string]string{testAnnotationKey: "abc,def,ghi,"},
			value:          "ghi",
			annotationKey:  testAnnotationKey,
			expectedResult: true,
		},
		{
			name:           "value not found",
			annotations:    map[string]string{testAnnotationKey: "abc,def,ghi,"},
			value:          "xyz",
			annotationKey:  testAnnotationKey,
			expectedResult: false,
		},
		{
			name:           "partial match should not match",
			annotations:    map[string]string{testAnnotationKey: "abcdef,"},
			value:          "abc",
			annotationKey:  testAnnotationKey,
			expectedResult: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pipeline := &unstructured.Unstructured{}
			pipeline.SetAnnotations(tc.annotations)

			result := checkForValueInCommaDelimitedAnnotation(pipeline, tc.value, tc.annotationKey)
			assert.Equal(t, tc.expectedResult, result)
		})
	}
}

func Test_shouldDeleteRecyclablePipeline(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name                    string
		deleteExpiredPipelines  bool
		forcePromoteStrategy    bool
		requiresDrainAnnotation string
		recyclableStartTime     string
		deletionAnnotation      string
		expectedShouldDelete    bool
		expectedExpired         bool
		expectedError           bool
	}{
		{
			name:                    "ForcePromote Strategy is set - should delete",
			deleteExpiredPipelines:  true,
			forcePromoteStrategy:    true,
			requiresDrainAnnotation: "true",
			recyclableStartTime:     "",
			expectedShouldDelete:    true,
			expectedExpired:         false,
			expectedError:           false,
		},
		{
			name:                    "No requires-drain annotation - should delete",
			deleteExpiredPipelines:  true,
			forcePromoteStrategy:    false,
			requiresDrainAnnotation: "",
			recyclableStartTime:     "",
			expectedShouldDelete:    true,
			expectedExpired:         false,
			expectedError:           false,
		},
		{
			name:                    "requires-drain is false - should delete",
			deleteExpiredPipelines:  true,
			forcePromoteStrategy:    false,
			requiresDrainAnnotation: "false",
			recyclableStartTime:     "",
			expectedShouldDelete:    true,
			expectedExpired:         false,
			expectedError:           false,
		},
		{
			name:                    "requires-drain is true, no recyclable start time - should delete by default",
			deleteExpiredPipelines:  true,
			forcePromoteStrategy:    false,
			requiresDrainAnnotation: "true",
			recyclableStartTime:     "",
			expectedShouldDelete:    true,
			expectedExpired:         true,
			expectedError:           false,
		},
		{
			name:                    "requires-drain is true, not yet expired - should not delete",
			deleteExpiredPipelines:  true,
			forcePromoteStrategy:    false,
			requiresDrainAnnotation: "true",
			recyclableStartTime:     time.Now().Format(time.RFC3339), // just started, not expired
			expectedShouldDelete:    false,
			expectedExpired:         false,
			expectedError:           false,
		},
		{
			name:                    "requires-drain is true, expired - should delete by default",
			deleteExpiredPipelines:  true,
			forcePromoteStrategy:    false,
			requiresDrainAnnotation: "true",
			recyclableStartTime:     time.Now().Add(-2 * time.Hour).Format(time.RFC3339), // 2 hours ago, should be expired
			expectedShouldDelete:    true,
			expectedExpired:         true,
			expectedError:           false,
		},
		{
			name:                    "requires-drain is true, invalid time format - should return error",
			deleteExpiredPipelines:  true,
			forcePromoteStrategy:    false,
			requiresDrainAnnotation: "true",
			recyclableStartTime:     "invalid-time-format",
			expectedShouldDelete:    false,
			expectedExpired:         false,
			expectedError:           true,
		},
		{
			name:                    "has deletion annotation",
			deleteExpiredPipelines:  true,
			forcePromoteStrategy:    false,
			requiresDrainAnnotation: "false",
			recyclableStartTime:     "",
			deletionAnnotation:      "true",
			expectedShouldDelete:    true,
			expectedExpired:         false,
			expectedError:           false,
		},
		{
			name:                    "expired recyclable pipeline is kept when deleteExpiredPipelines is false",
			deleteExpiredPipelines:  false,
			requiresDrainAnnotation: "true",
			recyclableStartTime:     time.Now().Add(-2 * time.Hour).Format(time.RFC3339),
			expectedShouldDelete:    false,
			expectedExpired:         true,
			expectedError:           false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			loadPipelineTestConfig(t, tc.deleteExpiredPipelines)

			pipelineRollout := &apiv1.PipelineRollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pipeline",
					Namespace: ctlrcommon.DefaultTestNamespace,
				},
				Spec: apiv1.PipelineRolloutSpec{},
			}

			if tc.forcePromoteStrategy {
				pipelineRollout.Spec.Strategy = &apiv1.PipelineStrategy{
					PipelineTypeRolloutStrategy: apiv1.PipelineTypeRolloutStrategy{
						PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
							Progressive: apiv1.ProgressiveStrategy{
								ForcePromote: true,
							},
						},
					},
				}
			}

			pipeline := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "numaflow.numaproj.io/v1alpha1",
					"kind":       "Pipeline",
					"metadata": map[string]interface{}{
						"name":      "test-pipeline-5",
						"namespace": ctlrcommon.DefaultTestNamespace,
					},
				},
			}
			pipeline.SetGroupVersionKind(numaflowv1.PipelineGroupVersionKind)
			pipeline.SetLabels(map[string]string{
				common.LabelKeyUpgradeState: string(common.LabelValueUpgradeRecyclable),
			})

			annotations := map[string]string{}
			if tc.requiresDrainAnnotation != "" {
				annotations[common.AnnotationKeyRequiresDrain] = tc.requiresDrainAnnotation
			}
			if tc.recyclableStartTime != "" {
				annotations[common.AnnotationKeyRecyclableStartTime] = tc.recyclableStartTime
			}
			if tc.deletionAnnotation != "" {
				annotations[common.AnnotationKeyMarkedForDeletion] = tc.deletionAnnotation
			}
			if len(annotations) > 0 {
				pipeline.SetAnnotations(annotations)
			}

			scheme := runtime.NewScheme()
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pipeline).Build()

			reconciler := &PipelineRolloutReconciler{
				client:        fakeClient,
				customMetrics: createTestMetrics(),
				recorder:      record.NewFakeRecorder(100),
			}

			shouldDelete, expired, err := reconciler.shouldDeleteRecyclablePipeline(ctx, pipelineRollout, pipeline)

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedShouldDelete, shouldDelete)
				assert.Equal(t, tc.expectedExpired, expired)
			}

			if expired && !shouldDelete {
				assert.Equal(t, string(common.LabelValueUpgradeRecyclableExpired), pipeline.GetLabels()[common.LabelKeyUpgradeState])
			}
		})
	}
}

func loadPipelineTestConfig(t *testing.T, deleteExpiredPipelines bool) {
	t.Helper()
	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configFile := "testconfig_keep_undrained"
	if deleteExpiredPipelines {
		configFile = "testconfig"
	}
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName(configFile))
	assert.NoError(t, err)
	assert.Equal(t, !deleteExpiredPipelines, config.GetKeepUndrainedPipelines())
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
	forceDrainPipelinesStarted string,
	forceDrainPipelinesCompleted string,
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
	if forceDrainPipelinesStarted != "" {
		pipeline.Annotations[common.AnnotationKeyForceDrainSpecsStarted] = forceDrainPipelinesStarted
	}

	if forceDrainPipelinesCompleted != "" {
		pipeline.Annotations[common.AnnotationKeyForceDrainSpecsCompleted] = forceDrainPipelinesCompleted
	}

	if requiresDrain {
		pipeline.Annotations[common.AnnotationKeyRequiresDrain] = "true"
	}

	// Set the recyclable start time annotation for recyclable pipelines to a recent time (30 minutes ago)
	// This ensures the pipeline won't be deleted due to expiration during the test
	if upgradeState == "recyclable" {
		pipeline.Annotations[common.AnnotationKeyRecyclableStartTime] = time.Now().Add(-30 * time.Minute).Format(time.RFC3339)
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
