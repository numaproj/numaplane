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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/controller/ppnd"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"
)

var (
	pipelineSpecSourceRPU      = int64(5)
	pipelineSpecSourceDuration = metav1.Duration{
		Duration: time.Second,
	}

	pipelineSpec = numaflowv1.PipelineSpec{
		InterStepBufferServiceName: "my-isbsvc",
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
				Name: "cat",
				UDF: &numaflowv1.UDF{
					Builtin: &numaflowv1.Function{
						Name: "cat",
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
			},
		},
		Edges: []numaflowv1.Edge{
			{
				From: "in",
				To:   "cat",
			},
			{
				From: "cat",
				To:   "out",
			},
		},
	}

	pipelineSpecWithWatermarkDisabled numaflowv1.PipelineSpec
	pipelineSpecWithTopologyChange    numaflowv1.PipelineSpec
)

func init() {
	pipelineSpecWithWatermarkDisabled = pipelineSpec
	pipelineSpecWithWatermarkDisabled.Watermark.Disabled = true

	pipelineSpecWithTopologyChange = pipelineSpec
	pipelineSpecWithTopologyChange.Vertices = append(pipelineSpecWithTopologyChange.Vertices, numaflowv1.AbstractVertex{})
	pipelineSpecWithTopologyChange.Vertices[3] = pipelineSpecWithTopologyChange.Vertices[2]
	pipelineSpecWithTopologyChange.Vertices[2] = numaflowv1.AbstractVertex{
		Name: "cat-2",
		UDF: &numaflowv1.UDF{
			Builtin: &numaflowv1.Function{
				Name: "cat",
			},
		},
	}
	pipelineSpecWithTopologyChange.Edges[1].To = "cat-2"
	pipelineSpecWithTopologyChange.Edges = append(pipelineSpecWithTopologyChange.Edges, numaflowv1.Edge{From: "cat-2", To: "out"})
}

var yamlHasDesiredPhase = `
{
	  "interStepBufferServiceName": "default",
	  "lifecycle": {
		"desiredPhase": "Paused"
	  },
	  "vertices": [
		{
		  "name": "in",
		  "source": {
			"generator": {
			  "rpu": 5,
			  "duration": "1s"
			}
		  }
		},
		{
		  "name": "cat",
		  "udf": {
			"builtin": {
			  "name": "cat"
			}
		  }
		},
		{
		  "name": "out",
		  "sink": {
			"log": {}
		  }
		}
	  ],
	  "edges": [
		{
		  "from": "in",
		  "to": "cat"
		},
		{
		  "from": "cat",
		  "to": "out"
		}
	  ]
	
}
`

var yamlHasDesiredPhaseDifferentUDF = `
{
	  "interStepBufferServiceName": "default",
	  "lifecycle": {
		"desiredPhase": "Paused"
	  },
	  "vertices": [
		{
		  "name": "in",
		  "source": {
			"generator": {
			  "rpu": 5,
			  "duration": "1s"
			}
		  }
		},
		{
		  "name": "cat",
		  "udf": {
			"builtin": {
			  "name": "SOMETHING_ELSE"
			}
		  }
		},
		{
		  "name": "out",
		  "sink": {
			"log": {}
		  }
		}
	  ],
	  "edges": [
		{
		  "from": "in",
		  "to": "cat"
		},
		{
		  "from": "cat",
		  "to": "out"
		}
	  ]
	
}
`

var yamlHasDesiredPhaseAndOtherLifecycleField = `
{
	  "interStepBufferServiceName": "default",
	  "lifecycle": {
		"desiredPhase": "Paused",
		"anotherField": 1
	  },
	  "vertices": [
		{
		  "name": "in",
		  "source": {
			"generator": {
			  "rpu": 5,
			  "duration": "1s"
			}
		  }
		},
		{
		  "name": "cat",
		  "udf": {
			"builtin": {
			  "name": "cat"
			}
		  }
		},
		{
		  "name": "out",
		  "sink": {
			"log": {}
		  }
		}
	  ],
	  "edges": [
		{
		  "from": "in",
		  "to": "cat"
		},
		{
		  "from": "cat",
		  "to": "out"
		}
	  ]
	
}
`

var yamlNoLifecycle = `
{
	  "interStepBufferServiceName": "default",
	  "vertices": [
		{
		  "name": "in",
		  "source": {
			"generator": {
			  "rpu": 5,
			  "duration": "1s"
			}
		  }
		},
		{
		  "name": "cat",
		  "udf": {
			"builtin": {
			  "name": "cat"
			}
		  }
		},
		{
		  "name": "out",
		  "sink": {
			"log": {}
		  }
		}
	  ],
	  "edges": [
		{
		  "from": "in",
		  "to": "cat"
		},
		{
		  "from": "cat",
		  "to": "out"
		}
	  ]
	
}
`

var yamlNoDesiredPhase = `
{
	  "interStepBufferServiceName": "default",
	  "lifecycle": {
	  },
	  "vertices": [
		{
		  "name": "in",
		  "source": {
			"generator": {
			  "rpu": 5,
			  "duration": "1s"
			}
		  }
		},
		{
		  "name": "cat",
		  "udf": {
			"builtin": {
			  "name": "cat"
			}
		  }
		},
		{
		  "name": "out",
		  "sink": {
			"log": {}
		  }
		}
	  ],
	  "edges": [
		{
		  "from": "in",
		  "to": "cat"
		},
		{
		  "from": "cat",
		  "to": "out"
		}
	  ]
	
}
`

func Test_pipelineSpecNeedsUpdating(t *testing.T) {

	_, _, numaplaneClient, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)

	recorder := record.NewFakeRecorder(64)

	r := NewPipelineRolloutReconciler(
		numaplaneClient,
		scheme.Scheme,
		ctlrcommon.TestCustomMetrics,
		recorder)

	testCases := []struct {
		name                  string
		specYaml1             string
		specYaml2             string
		expectedNeedsUpdating bool
		expectedError         bool
	}{
		{
			name:                  "Not Equal",
			specYaml1:             yamlHasDesiredPhase,
			specYaml2:             yamlHasDesiredPhaseDifferentUDF,
			expectedNeedsUpdating: true,
			expectedError:         false,
		},
		{
			name:                  "Not Equal - another lifecycle field",
			specYaml1:             yamlHasDesiredPhase,
			specYaml2:             yamlHasDesiredPhaseAndOtherLifecycleField,
			expectedNeedsUpdating: true,
			expectedError:         false,
		},
		{
			name:                  "Equal - just desiredPhase different",
			specYaml1:             yamlHasDesiredPhase,
			specYaml2:             yamlNoDesiredPhase,
			expectedNeedsUpdating: false,
			expectedError:         false,
		},
		{
			name:                  "Equal - just lifecycle different",
			specYaml1:             yamlHasDesiredPhase,
			specYaml2:             yamlNoLifecycle,
			expectedNeedsUpdating: false,
			expectedError:         false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			obj1 := &kubernetes.GenericObject{}
			obj1.Spec.Raw = []byte(tc.specYaml1)
			obj2 := &kubernetes.GenericObject{}
			obj2.Spec.Raw = []byte(tc.specYaml2)
			needsUpdating, err := r.ChildNeedsUpdating(context.Background(), obj1, obj2)
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedNeedsUpdating, needsUpdating)
			}

		})
	}
}

func TestBasePipelineMetadata(t *testing.T) {

	pipelineRolloutName := "my-pipeline"

	tests := []struct {
		name                     string
		jsonInput                string
		rolloutSpecifiedMetadata apiv1.Metadata
		expectedPipelineMetadata apiv1.Metadata
		expectError              bool
	}{
		{
			name:      "Valid Input",
			jsonInput: `{"interStepBufferServiceName": "buffer-service"}`,
			rolloutSpecifiedMetadata: apiv1.Metadata{
				Labels:      map[string]string{"key": "val"},
				Annotations: map[string]string{"key": "val"},
			},
			expectedPipelineMetadata: apiv1.Metadata{
				Labels: map[string]string{
					"key":                                    "val",
					common.LabelKeyISBServiceNameForPipeline: "buffer-service",
					common.LabelKeyParentRollout:             pipelineRolloutName,
				},
				Annotations: map[string]string{"key": "val"},
			},
			expectError: false,
		},
		{
			name:                     "Unspecified InterStepBufferServiceName",
			jsonInput:                `{}`,
			rolloutSpecifiedMetadata: apiv1.Metadata{},
			expectedPipelineMetadata: apiv1.Metadata{
				Labels: map[string]string{
					common.LabelKeyISBServiceNameForPipeline: "default",
					common.LabelKeyParentRollout:             pipelineRolloutName,
				},
			},
			expectError: false,
		},
		{
			name:        "Invalid JSON",
			jsonInput:   `{"interStepBufferServiceName": "buffer-service"`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipelineRollout := &apiv1.PipelineRollout{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ctlrcommon.DefaultTestNamespace,
					Name:      pipelineRolloutName,
				},
				Spec: apiv1.PipelineRolloutSpec{
					Pipeline: apiv1.Pipeline{
						Spec:     runtime.RawExtension{Raw: []byte(tt.jsonInput)},
						Metadata: tt.rolloutSpecifiedMetadata,
					},
				},
			}

			metadata, err := getBasePipelineMetadata(pipelineRollout)
			if (err != nil) != tt.expectError {
				t.Errorf("pipelineLabels() error = %v, expectError %v", err, tt.expectError)
				return
			}
			if err == nil {
				assert.Equal(t, tt.expectedPipelineMetadata.Labels, metadata.Labels)
				assert.Equal(t, tt.expectedPipelineMetadata.Annotations, metadata.Annotations)
			}
		})
	}
}

func createDefaultTestPipeline(phase numaflowv1.PipelinePhase) *numaflowv1.Pipeline {
	return ctlrcommon.CreateTestPipelineOfSpec(pipelineSpec, ctlrcommon.DefaultTestPipelineName, phase, numaflowv1.Status{}, false, map[string]string{})
}

func createPipeline(phase numaflowv1.PipelinePhase, status numaflowv1.Status, drainedOnPause bool, labels map[string]string) *numaflowv1.Pipeline {
	return ctlrcommon.CreateTestPipelineOfSpec(pipelineSpec, ctlrcommon.DefaultTestPipelineName, phase, status, drainedOnPause, labels)
}

// process an existing pipeline
// in this test, the user preferred strategy is PPND
func Test_processExistingPipeline_PPND(t *testing.T) {
	restConfig, numaflowClientSet, numaplaneClient, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetDynamicClient(restConfig))

	config.GetConfigManagerInstance().UpdateUSDEConfig(config.USDEConfig{
		DefaultUpgradeStrategy:    config.PPNDStrategyID,
		PipelineSpecExcludedPaths: []string{"watermark", "lifecycle"},
	})

	ctx := context.Background()

	// other tests may call this, but it fails if called more than once
	if ctlrcommon.TestCustomMetrics == nil {
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics()
	}

	recorder := record.NewFakeRecorder(64)

	falseValue := false
	trueValue := true

	r := NewPipelineRolloutReconciler(
		numaplaneClient,
		scheme.Scheme,
		ctlrcommon.TestCustomMetrics,
		recorder)

	testCases := []struct {
		name                           string
		newPipelineSpec                numaflowv1.PipelineSpec
		pipelineRolloutAnnotations     map[string]string
		existingPipelineDef            numaflowv1.Pipeline
		initialRolloutPhase            apiv1.Phase
		initialInProgressStrategy      apiv1.UpgradeStrategy
		numaflowControllerPauseRequest *bool
		isbServicePauseRequest         *bool

		expectedInProgressStrategy apiv1.UpgradeStrategy
		expectedRolloutPhase       apiv1.Phase
		// require these Conditions to be set (note that in real life, previous reconciliations may have set other Conditions from before which are still present)
		expectedPipelineSpecResult func(numaflowv1.PipelineSpec) bool
	}{
		{
			name:                           "nothing to do",
			newPipelineSpec:                pipelineSpec,
			existingPipelineDef:            *createDefaultTestPipeline(numaflowv1.PipelinePhaseRunning),
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialInProgressStrategy:      apiv1.UpgradeStrategyNoOp,
			numaflowControllerPauseRequest: &falseValue,
			isbServicePauseRequest:         &falseValue,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:           apiv1.PhaseDeployed,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return reflect.DeepEqual(pipelineSpec, spec)
			},
		},
		{
			name:                           "direct apply",
			newPipelineSpec:                pipelineSpecWithWatermarkDisabled,
			existingPipelineDef:            *createDefaultTestPipeline(numaflowv1.PipelinePhaseRunning),
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialInProgressStrategy:      apiv1.UpgradeStrategyNoOp,
			numaflowControllerPauseRequest: &falseValue,
			isbServicePauseRequest:         &falseValue,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:           apiv1.PhaseDeployed,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return reflect.DeepEqual(pipelineSpecWithWatermarkDisabled, spec)
			},
		},
		{
			name:                           "spec difference results in PPND",
			newPipelineSpec:                pipelineSpecWithTopologyChange,
			existingPipelineDef:            *createDefaultTestPipeline(numaflowv1.PipelinePhaseRunning),
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialInProgressStrategy:      apiv1.UpgradeStrategyNoOp,
			numaflowControllerPauseRequest: &falseValue,
			isbServicePauseRequest:         &falseValue,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyPPND,
			expectedRolloutPhase:           apiv1.PhasePending,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return reflect.DeepEqual(ctlrcommon.PipelineWithDesiredPhase(pipelineSpec, numaflowv1.PipelinePhasePaused), spec)
			},
		},
		{
			name:                           "external pause request at the same time as a DirectApply change",
			newPipelineSpec:                pipelineSpecWithWatermarkDisabled,
			existingPipelineDef:            *createDefaultTestPipeline(numaflowv1.PipelinePhaseRunning),
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialInProgressStrategy:      apiv1.UpgradeStrategyNoOp,
			numaflowControllerPauseRequest: &trueValue,
			isbServicePauseRequest:         &falseValue,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyPPND,
			expectedRolloutPhase:           apiv1.PhasePending,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return reflect.DeepEqual(ctlrcommon.PipelineWithDesiredPhase(pipelineSpec, numaflowv1.PipelinePhasePaused), spec)
			},
		},
		{
			name:                           "user sets desiredPhase=Paused",
			newPipelineSpec:                ctlrcommon.PipelineWithDesiredPhase(pipelineSpec, numaflowv1.PipelinePhasePaused),
			existingPipelineDef:            *createDefaultTestPipeline(numaflowv1.PipelinePhaseRunning),
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialInProgressStrategy:      apiv1.UpgradeStrategyNoOp,
			numaflowControllerPauseRequest: &falseValue,
			isbServicePauseRequest:         &falseValue,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:           apiv1.PhaseDeployed,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return reflect.DeepEqual(ctlrcommon.PipelineWithDesiredPhase(pipelineSpec, numaflowv1.PipelinePhasePaused), spec)
			},
		},
		{
			name:            "user sets desiredPhase=Running",
			newPipelineSpec: ctlrcommon.PipelineWithDesiredPhase(pipelineSpec, numaflowv1.PipelinePhaseRunning),
			existingPipelineDef: *ctlrcommon.CreateTestPipelineOfSpec(
				ctlrcommon.PipelineWithDesiredPhase(pipelineSpec, numaflowv1.PipelinePhaseRunning),
				ctlrcommon.DefaultTestPipelineName, numaflowv1.PipelinePhasePaused, numaflowv1.Status{},
				false, map[string]string{}),
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialInProgressStrategy:      apiv1.UpgradeStrategyNoOp,
			numaflowControllerPauseRequest: &falseValue,
			isbServicePauseRequest:         &falseValue,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:           apiv1.PhaseDeployed,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return reflect.DeepEqual(ctlrcommon.PipelineWithDesiredPhase(pipelineSpec, numaflowv1.PipelinePhaseRunning), spec)
			},
		},
		{
			name:                           "PPND in progress, spec not yet applied, pipeline not paused",
			newPipelineSpec:                pipelineSpecWithTopologyChange,
			existingPipelineDef:            *createDefaultTestPipeline(numaflowv1.PipelinePhaseRunning),
			initialRolloutPhase:            apiv1.PhasePending,
			initialInProgressStrategy:      apiv1.UpgradeStrategyPPND,
			numaflowControllerPauseRequest: &falseValue,
			isbServicePauseRequest:         &falseValue,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyPPND,
			expectedRolloutPhase:           apiv1.PhasePending,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return reflect.DeepEqual(ctlrcommon.PipelineWithDesiredPhase(pipelineSpec, numaflowv1.PipelinePhasePaused), spec)
			},
		},
		{
			name:            "PPND in progress, spec applied",
			newPipelineSpec: pipelineSpecWithTopologyChange,
			existingPipelineDef: *ctlrcommon.CreateTestPipelineOfSpec(
				ctlrcommon.PipelineWithDesiredPhase(pipelineSpecWithTopologyChange, numaflowv1.PipelinePhasePaused),
				ctlrcommon.DefaultTestPipelineName, numaflowv1.PipelinePhasePaused, numaflowv1.Status{},
				false, map[string]string{}),
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialInProgressStrategy:      apiv1.UpgradeStrategyPPND,
			numaflowControllerPauseRequest: &falseValue,
			isbServicePauseRequest:         &falseValue,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:           apiv1.PhaseDeployed,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return reflect.DeepEqual(ctlrcommon.PipelineWithDesiredPhase(pipelineSpecWithTopologyChange, numaflowv1.PipelinePhaseRunning), spec)
			},
		},
		{
			name:                           "Pipeline stuck pausing, allow-data-loss annotation applied",
			newPipelineSpec:                pipelineSpecWithTopologyChange,
			pipelineRolloutAnnotations:     map[string]string{common.LabelKeyAllowDataLoss: "true"},
			existingPipelineDef:            *createDefaultTestPipeline(numaflowv1.PipelinePhasePausing),
			initialRolloutPhase:            apiv1.PhasePending,
			initialInProgressStrategy:      apiv1.UpgradeStrategyPPND,
			numaflowControllerPauseRequest: &trueValue,
			isbServicePauseRequest:         &trueValue,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:           apiv1.PhaseDeployed,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return reflect.DeepEqual(pipelineSpecWithTopologyChange, spec)
			},
		},
	}

	for _, tc := range testCases {

		t.Run(tc.name, func(t *testing.T) {

			// first delete Pipeline and PipelineRollout in case they already exist, in Kubernetes
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).Delete(ctx, ctlrcommon.DefaultTestPipelineName, metav1.DeleteOptions{})

			pipelineList, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Len(t, pipelineList.Items, 0)

			if tc.pipelineRolloutAnnotations == nil {
				tc.pipelineRolloutAnnotations = map[string]string{}
			}

			rollout := ctlrcommon.CreateTestPipelineRollout(tc.newPipelineSpec, tc.pipelineRolloutAnnotations, map[string]string{})
			_ = numaplaneClient.Delete(ctx, rollout)

			rollout.Status.Phase = tc.initialRolloutPhase
			rollout.Status.UpgradeInProgress = tc.initialInProgressStrategy
			r.inProgressStrategyMgr.Store.SetStrategy(k8stypes.NamespacedName{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestPipelineRolloutName}, tc.initialInProgressStrategy)

			// the Reconcile() function does this, so we need to do it before calling reconcile() as well
			rollout.Status.Init(rollout.Generation)

			err = numaplaneClient.Create(ctx, rollout)
			assert.NoError(t, err)

			// create the already-existing Pipeline in Kubernetes
			// this updates everything but the Status subresource
			existingPipelineDef := &tc.existingPipelineDef
			existingPipelineDef.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rollout.GetObjectMeta(), apiv1.PipelineRolloutGroupVersionKind)}
			ctlrcommon.CreatePipelineInK8S(ctx, t, numaflowClientSet, &tc.existingPipelineDef)

			// external pause requests
			ppnd.GetPauseModule().PauseRequests = map[string]*bool{}
			if tc.numaflowControllerPauseRequest != nil {
				ppnd.GetPauseModule().PauseRequests[ppnd.GetPauseModule().GetNumaflowControllerKey(ctlrcommon.DefaultTestNamespace)] = tc.numaflowControllerPauseRequest
			}
			if tc.isbServicePauseRequest != nil {
				ppnd.GetPauseModule().PauseRequests[ppnd.GetPauseModule().GetISBServiceKey(ctlrcommon.DefaultTestNamespace, "my-isbsvc")] = tc.isbServicePauseRequest
			}

			_, _, err = r.reconcile(context.Background(), rollout, time.Now())
			assert.NoError(t, err)

			////// check results:
			// Check Phase of Rollout:
			assert.Equal(t, tc.expectedRolloutPhase, rollout.Status.Phase)
			// Check In-Progress Strategy
			assert.Equal(t, tc.expectedInProgressStrategy, rollout.Status.UpgradeInProgress)

			// Check Pipeline spec
			resultPipeline, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).Get(ctx, ctlrcommon.DefaultTestPipelineName, metav1.GetOptions{})
			assert.NoError(t, err)
			assert.NotNil(t, resultPipeline)
			assert.True(t, tc.expectedPipelineSpecResult(resultPipeline.Spec), "result spec", resultPipeline.Spec)
		})
	}
}

// process an existing pipeline in this test, the user preferred strategy is Progressive
func Test_processExistingPipeline_Progressive(t *testing.T) {
	restConfig, numaflowClientSet, numaplaneClient, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetDynamicClient(restConfig))

	config.GetConfigManagerInstance().UpdateUSDEConfig(config.USDEConfig{
		DefaultUpgradeStrategy:    config.ProgressiveStrategyID,
		PipelineSpecExcludedPaths: []string{"watermark", "lifecycle"},
	})
	ctx := context.Background()

	// other tests may call this, but it fails if called more than once
	if ctlrcommon.TestCustomMetrics == nil {
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics()
	}

	recorder := record.NewFakeRecorder(64)

	r := NewPipelineRolloutReconciler(
		numaplaneClient,
		scheme.Scheme,
		ctlrcommon.TestCustomMetrics,
		recorder)

	progressiveUpgradeStrategy := apiv1.UpgradeStrategyProgressive
	paused := numaflowv1.PipelinePhasePaused

	testCases := []struct {
		name                       string
		newPipelineSpec            numaflowv1.PipelineSpec
		existingPipelineDef        numaflowv1.Pipeline
		existingUpgradePipelineDef *numaflowv1.Pipeline
		initialRolloutPhase        apiv1.Phase
		initialInProgressStrategy  *apiv1.UpgradeStrategy

		expectedInProgressStrategy           apiv1.UpgradeStrategy
		expectedRolloutPhase                 apiv1.Phase
		expectedExistingPipelineDeleted      bool
		expectedExistingPipelineDesiredPhase *numaflowv1.PipelinePhase
		// require these Conditions to be set (note that in real life, previous reconciliations may have set other Conditions from before which are still present)
		expectedPipelineSpecResult func(numaflowv1.PipelineSpec) bool
	}{
		{
			name:            "spec difference results in Progressive",
			newPipelineSpec: pipelineSpecWithTopologyChange,
			existingPipelineDef: *createPipeline(
				numaflowv1.PipelinePhaseRunning,
				numaflowv1.Status{},
				false,
				map[string]string{
					common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
					common.LabelKeyParentRollout: ctlrcommon.DefaultTestPipelineRolloutName,
				}),
			existingUpgradePipelineDef:           nil,
			initialRolloutPhase:                  apiv1.PhaseDeployed,
			initialInProgressStrategy:            nil,
			expectedInProgressStrategy:           apiv1.UpgradeStrategyProgressive,
			expectedRolloutPhase:                 apiv1.PhasePending,
			expectedExistingPipelineDeleted:      false,
			expectedExistingPipelineDesiredPhase: nil,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return true
			},
		},
		{
			name:            "Progressive deployed successfully",
			newPipelineSpec: pipelineSpecWithTopologyChange,
			existingPipelineDef: *createPipeline(
				numaflowv1.PipelinePhaseRunning,
				numaflowv1.Status{},
				false,
				map[string]string{
					common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
					common.LabelKeyParentRollout: ctlrcommon.DefaultTestPipelineRolloutName,
				}),
			existingUpgradePipelineDef: ctlrcommon.CreateTestPipelineOfSpec(
				pipelineSpecWithTopologyChange, ctlrcommon.DefaultTestNewPipelineName,
				numaflowv1.PipelinePhaseRunning,
				numaflowv1.Status{
					Conditions: []metav1.Condition{
						{
							Type:   string(numaflowv1.PipelineConditionDaemonServiceHealthy),
							Status: metav1.ConditionTrue,
						},
					},
				},
				false,
				map[string]string{
					common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeInProgress),
					common.LabelKeyParentRollout: ctlrcommon.DefaultTestPipelineRolloutName,
				}),
			initialRolloutPhase:                  apiv1.PhasePending,
			initialInProgressStrategy:            &progressiveUpgradeStrategy,
			expectedInProgressStrategy:           apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:                 apiv1.PhaseDeployed,
			expectedExistingPipelineDeleted:      false,
			expectedExistingPipelineDesiredPhase: &paused,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return reflect.DeepEqual(pipelineSpecWithTopologyChange, spec)
			},
		},
		{
			name:            "Clean up after progressive upgrade",
			newPipelineSpec: pipelineSpecWithTopologyChange,
			existingPipelineDef: *createPipeline(
				numaflowv1.PipelinePhasePaused,
				numaflowv1.Status{},
				true,
				map[string]string{
					common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeRecyclable),
					common.LabelKeyParentRollout: ctlrcommon.DefaultTestPipelineRolloutName,
				}),
			existingUpgradePipelineDef: ctlrcommon.CreateTestPipelineOfSpec(
				pipelineSpecWithTopologyChange, ctlrcommon.DefaultTestNewPipelineName,
				numaflowv1.PipelinePhaseRunning,
				numaflowv1.Status{},
				false,
				map[string]string{
					common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
					common.LabelKeyParentRollout: ctlrcommon.DefaultTestPipelineRolloutName,
				}),
			initialRolloutPhase:                  apiv1.PhaseDeployed,
			initialInProgressStrategy:            nil,
			expectedInProgressStrategy:           apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:                 apiv1.PhaseDeployed,
			expectedExistingPipelineDeleted:      true,
			expectedExistingPipelineDesiredPhase: nil,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return true
			},
		},
		{
			name:            "Clean up after progressive upgrade do not delete not drained pipeline",
			newPipelineSpec: pipelineSpecWithTopologyChange,
			existingPipelineDef: *createPipeline(
				numaflowv1.PipelinePhasePaused,
				numaflowv1.Status{},
				false,
				map[string]string{
					common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeRecyclable),
					common.LabelKeyParentRollout: ctlrcommon.DefaultTestPipelineRolloutName,
				}),
			existingUpgradePipelineDef: ctlrcommon.CreateTestPipelineOfSpec(
				pipelineSpecWithTopologyChange, ctlrcommon.DefaultTestNewPipelineName,
				numaflowv1.PipelinePhaseRunning,
				numaflowv1.Status{},
				false,
				map[string]string{
					common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
					common.LabelKeyParentRollout: ctlrcommon.DefaultTestPipelineRolloutName,
				}),
			initialRolloutPhase:                  apiv1.PhaseDeployed,
			initialInProgressStrategy:            nil,
			expectedInProgressStrategy:           apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:                 apiv1.PhaseDeployed,
			expectedExistingPipelineDeleted:      false,
			expectedExistingPipelineDesiredPhase: nil,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return true
			},
		},
	}

	for _, tc := range testCases {

		t.Run(tc.name, func(t *testing.T) {

			// first delete Pipeline and PipelineRollout in case they already exist, in Kubernetes
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).Delete(ctx, ctlrcommon.DefaultTestPipelineName, metav1.DeleteOptions{})
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).Delete(ctx, ctlrcommon.DefaultTestNewPipelineName, metav1.DeleteOptions{})

			pipelineList, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Len(t, pipelineList.Items, 0)

			rollout := ctlrcommon.CreateTestPipelineRollout(tc.newPipelineSpec, map[string]string{}, map[string]string{})
			_ = numaplaneClient.Delete(ctx, rollout)

			rollout.Status.Phase = tc.initialRolloutPhase
			if rollout.Status.NameCount == nil {
				rollout.Status.NameCount = new(int32)
				*rollout.Status.NameCount++
			}
			if tc.initialInProgressStrategy != nil {
				rollout.Status.UpgradeInProgress = *tc.initialInProgressStrategy
				r.inProgressStrategyMgr.Store.SetStrategy(k8stypes.NamespacedName{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestPipelineRolloutName}, *tc.initialInProgressStrategy)
			} else {
				rollout.Status.UpgradeInProgress = apiv1.UpgradeStrategyNoOp
				r.inProgressStrategyMgr.Store.SetStrategy(k8stypes.NamespacedName{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestPipelineRolloutName}, apiv1.UpgradeStrategyNoOp)
			}

			// the Reconcile() function does this, so we need to do it before calling reconcile() as well
			rollout.Status.Init(rollout.Generation)

			rolloutCopy := *rollout
			err = numaplaneClient.Create(ctx, rollout)
			assert.NoError(t, err)
			// update Status subresource
			rollout.Status = rolloutCopy.Status
			err = numaplaneClient.Status().Update(ctx, rollout)
			assert.NoError(t, err)

			// create the already-existing Pipeline in Kubernetes
			// this updates everything but the Status subresource
			existingPipelineDef := &tc.existingPipelineDef
			existingPipelineDef.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rollout.GetObjectMeta(), apiv1.PipelineRolloutGroupVersionKind)}
			pipeline, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).Create(ctx, existingPipelineDef, metav1.CreateOptions{})
			assert.NoError(t, err)
			// update Status subresource
			pipeline.Status = tc.existingPipelineDef.Status
			_, err = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).UpdateStatus(ctx, pipeline, metav1.UpdateOptions{})
			assert.NoError(t, err)

			if tc.existingUpgradePipelineDef != nil {
				existingUpgradePipelineDef := tc.existingUpgradePipelineDef
				existingUpgradePipelineDef.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rollout.GetObjectMeta(), apiv1.PipelineRolloutGroupVersionKind)}
				pipeline, err = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).Create(ctx, existingUpgradePipelineDef, metav1.CreateOptions{})
				assert.NoError(t, err)

				// update Status subresource
				pipeline.Status = tc.existingUpgradePipelineDef.Status
				_, err = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).UpdateStatus(ctx, pipeline, metav1.UpdateOptions{})
				assert.NoError(t, err)
			}

			_, _, err = r.reconcile(context.Background(), rollout, time.Now())
			assert.NoError(t, err)

			////// check results:
			// Check Phase of Rollout:
			assert.Equal(t, tc.expectedRolloutPhase, rollout.Status.Phase)
			// Check In-Progress Strategy
			assert.Equal(t, tc.expectedInProgressStrategy, rollout.Status.UpgradeInProgress)

			// Check the new Pipeline spec
			resultPipeline, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).Get(ctx, ctlrcommon.DefaultTestNewPipelineName, metav1.GetOptions{})
			assert.NoError(t, err)
			assert.NotNil(t, resultPipeline)
			assert.True(t, tc.expectedPipelineSpecResult(resultPipeline.Spec), "result spec", fmt.Sprint(resultPipeline.Spec))

			// Check the existing Pipeline state
			resultExistingPipeline, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).Get(ctx, ctlrcommon.DefaultTestPipelineName, metav1.GetOptions{})
			if tc.expectedExistingPipelineDeleted {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, resultExistingPipeline)
			}

			if tc.expectedExistingPipelineDesiredPhase != nil {
				assert.True(t,
					reflect.DeepEqual(ctlrcommon.PipelineWithDesiredPhase(pipelineSpec, *tc.expectedExistingPipelineDesiredPhase), resultExistingPipeline.Spec),
					"result pipeline phase", fmt.Sprint(resultExistingPipeline.Status.Phase))
			}
		})
	}
}
