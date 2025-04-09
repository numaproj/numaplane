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

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctlrruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/controller/ppnd"
	"github.com/numaproj/numaplane/internal/util"
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
		InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName,
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

	// running pipelnes represent an actual running Pipeline and thus have resolved InterstepBufferService names
	// (as opposed to the spec defined in the PipelineRollout, which has the ISBServiceRollout's name)
	runningPipelineSpec                      numaflowv1.PipelineSpec
	runningPipelineSpecWithWatermarkDisabled numaflowv1.PipelineSpec
	runningPipelineSpecWithTopologyChange    numaflowv1.PipelineSpec
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

	// running pipelnes represent an actual running Pipeline and thus have resolved InterstepBufferService names
	runningPipelineSpec = withInterstepBufferService(pipelineSpec, ctlrcommon.DefaultTestISBSvcName)
	runningPipelineSpecWithWatermarkDisabled = withInterstepBufferService(pipelineSpecWithWatermarkDisabled, ctlrcommon.DefaultTestISBSvcName)

	runningPipelineSpecWithTopologyChange = withInterstepBufferService(pipelineSpecWithTopologyChange, ctlrcommon.DefaultTestISBSvcName)
}

var specHasDesiredPhase = `
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

var specHasDesiredPhaseDifferentUDF = `
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

var specHasDesiredPhaseAndOtherLifecycleField = `
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

var specNoLifecycle = `
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

var specNoDesiredPhase = `
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

var specNoScale = `
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

var specWithEmptyScale = `
{
	  "interStepBufferServiceName": "default",
	  "vertices": [
		{
		  "name": "in",
	  	  "scale": {},
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

var specWithNonEmptyScale = `
{
	  "interStepBufferServiceName": "default",
	  "vertices": [
		{
		  "name": "in",
	      "scale": {
		    "min": 3,
	        "max": 5
	      },
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

// TODO: update to account for Labels/Annotations differences
func Test_ChildNeedsUpdating(t *testing.T) {

	_, _, client, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)

	recorder := record.NewFakeRecorder(64)

	r := NewPipelineRolloutReconciler(
		client,
		scheme.Scheme,
		ctlrcommon.TestCustomMetrics,
		recorder)

	testCases := []struct {
		name                  string
		spec1                 string
		spec2                 string
		expectedNeedsUpdating bool
		expectedError         bool
	}{
		{
			name:                  "Not Equal",
			spec1:                 specHasDesiredPhase,
			spec2:                 specHasDesiredPhaseDifferentUDF,
			expectedNeedsUpdating: true,
			expectedError:         false,
		},
		{
			name:                  "Not Equal - another lifecycle field",
			spec1:                 specHasDesiredPhase,
			spec2:                 specHasDesiredPhaseAndOtherLifecycleField,
			expectedNeedsUpdating: true,
			expectedError:         false,
		},
		{
			name:                  "Equal - just desiredPhase different",
			spec1:                 specHasDesiredPhase,
			spec2:                 specNoDesiredPhase,
			expectedNeedsUpdating: false,
			expectedError:         false,
		},
		{
			name:                  "Equal - just lifecycle different",
			spec1:                 specHasDesiredPhase,
			spec2:                 specNoLifecycle,
			expectedNeedsUpdating: false,
			expectedError:         false,
		},
		{
			name:                  "Equal - just scale different",
			spec1:                 specNoScale,
			spec2:                 specWithEmptyScale,
			expectedNeedsUpdating: false,
			expectedError:         false,
		},
		{
			name:                  "Equal - just scale min/max different",
			spec1:                 specWithEmptyScale,
			spec2:                 specWithNonEmptyScale,
			expectedNeedsUpdating: false,
			expectedError:         false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			obj1 := &unstructured.Unstructured{Object: make(map[string]interface{})}
			var yaml1Spec map[string]interface{}
			err := json.Unmarshal([]byte(tc.spec1), &yaml1Spec)
			assert.NoError(t, err)
			obj1.Object["spec"] = yaml1Spec

			obj2 := &unstructured.Unstructured{Object: make(map[string]interface{})}
			var yaml2Spec map[string]interface{}
			err = json.Unmarshal([]byte(tc.spec2), &yaml2Spec)
			assert.NoError(t, err)
			obj2.Object["spec"] = yaml2Spec

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
					"key": "val",
					common.LabelKeyISBServiceRONameForPipeline: "buffer-service",
					common.LabelKeyParentRollout:               pipelineRolloutName,
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
					common.LabelKeyISBServiceRONameForPipeline: "default",
					common.LabelKeyParentRollout:               pipelineRolloutName,
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
	return ctlrcommon.CreateTestPipelineOfSpec(runningPipelineSpec, ctlrcommon.DefaultTestPipelineName, phase, numaflowv1.Status{}, false,
		map[string]string{"numaplane.numaproj.io/isbsvc-name": ctlrcommon.DefaultTestISBSvcRolloutName, "numaplane.numaproj.io/parent-rollout-name": "pipelinerollout-test", "numaplane.numaproj.io/upgrade-state": "promoted"},
		map[string]string{})
}

func createPipeline(phase numaflowv1.PipelinePhase, status numaflowv1.Status, drainedOnPause bool, rolloutLabels map[string]string) *numaflowv1.Pipeline {
	return ctlrcommon.CreateTestPipelineOfSpec(runningPipelineSpec, ctlrcommon.DefaultTestPipelineName, phase, status, drainedOnPause, rolloutLabels, map[string]string{})
}

func withInterstepBufferService(origPipelineSpec numaflowv1.PipelineSpec, isbsvc string) numaflowv1.PipelineSpec {
	newPipelineSpec := origPipelineSpec.DeepCopy()
	newPipelineSpec.InterStepBufferServiceName = isbsvc
	return *newPipelineSpec
}

// process an existing pipeline
// in this test, the user preferred strategy is PPND
func Test_processExistingPipeline_PPND(t *testing.T) {
	restConfig, numaflowClientSet, client, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetClientSets(restConfig))

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig"))
	assert.NoError(t, err)

	config.GetConfigManagerInstance().UpdateUSDEConfig(config.USDEConfig{
		"pipeline": config.USDEResourceConfig{
			DataLoss: []config.SpecField{{Path: "spec.vertices", IncludeSubfields: true}},
		},
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
		client,
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
				return util.CompareStructNumTypeAgnostic(runningPipelineSpec, spec)
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
				return util.CompareStructNumTypeAgnostic(runningPipelineSpecWithWatermarkDisabled, spec)
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
				return util.CompareStructNumTypeAgnostic(ctlrcommon.PipelineWithDesiredPhase(runningPipelineSpec, numaflowv1.PipelinePhasePaused), spec)
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
				return util.CompareStructNumTypeAgnostic(ctlrcommon.PipelineWithDesiredPhase(runningPipelineSpec, numaflowv1.PipelinePhasePaused), spec)
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
				return util.CompareStructNumTypeAgnostic(ctlrcommon.PipelineWithDesiredPhase(runningPipelineSpec, numaflowv1.PipelinePhasePaused), spec)
			},
		},
		{
			name:            "user sets desiredPhase=Running",
			newPipelineSpec: ctlrcommon.PipelineWithDesiredPhase(pipelineSpec, numaflowv1.PipelinePhaseRunning),
			existingPipelineDef: *ctlrcommon.CreateTestPipelineOfSpec(
				ctlrcommon.PipelineWithDesiredPhase(runningPipelineSpec, numaflowv1.PipelinePhaseRunning),
				ctlrcommon.DefaultTestPipelineName, numaflowv1.PipelinePhasePaused, numaflowv1.Status{},
				false,
				map[string]string{
					common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
					common.LabelKeyISBServiceChildNameForPipeline: ctlrcommon.DefaultTestISBSvcName,
					common.LabelKeyParentRollout:                  "pipelinerollout-test",
					common.LabelKeyUpgradeState:                   "promoted"},
				map[string]string{}),
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialInProgressStrategy:      apiv1.UpgradeStrategyNoOp,
			numaflowControllerPauseRequest: &falseValue,
			isbServicePauseRequest:         &falseValue,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:           apiv1.PhaseDeployed,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return util.CompareStructNumTypeAgnostic(ctlrcommon.PipelineWithDesiredPhase(runningPipelineSpec, numaflowv1.PipelinePhaseRunning), spec)
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
				return util.CompareStructNumTypeAgnostic(ctlrcommon.PipelineWithDesiredPhase(runningPipelineSpec, numaflowv1.PipelinePhasePaused), spec)
			},
		},
		{
			name:            "PPND in progress, spec applied",
			newPipelineSpec: pipelineSpecWithTopologyChange,
			existingPipelineDef: *ctlrcommon.CreateTestPipelineOfSpec(
				ctlrcommon.PipelineWithDesiredPhase(runningPipelineSpecWithTopologyChange, numaflowv1.PipelinePhasePaused),
				ctlrcommon.DefaultTestPipelineName, numaflowv1.PipelinePhasePaused, numaflowv1.Status{},
				false, map[string]string{
					common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
					common.LabelKeyISBServiceChildNameForPipeline: ctlrcommon.DefaultTestISBSvcName,
					common.LabelKeyParentRollout:                  "pipelinerollout-test",
					common.LabelKeyUpgradeState:                   "promoted"},
				map[string]string{}),
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialInProgressStrategy:      apiv1.UpgradeStrategyPPND,
			numaflowControllerPauseRequest: &falseValue,
			isbServicePauseRequest:         &falseValue,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:           apiv1.PhaseDeployed,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return util.CompareStructNumTypeAgnostic(ctlrcommon.PipelineWithDesiredPhase(runningPipelineSpecWithTopologyChange, numaflowv1.PipelinePhaseRunning), spec)
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
				return util.CompareStructNumTypeAgnostic(runningPipelineSpecWithTopologyChange, spec)
			},
		},
	}

	for _, tc := range testCases {

		t.Run(tc.name, func(t *testing.T) {

			// first delete resources (Pipeline, InterstepBufferService, PipelineRollout, ISBServiceRollout) in case they already exist, in Kubernetes
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = numaflowClientSet.NumaflowV1alpha1().InterStepBufferServices(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})

			_ = client.DeleteAllOf(ctx, &apiv1.PipelineRollout{}, &ctlrruntimeclient.DeleteAllOfOptions{ListOptions: ctlrruntimeclient.ListOptions{Namespace: ctlrcommon.DefaultTestNamespace}})
			_ = client.DeleteAllOf(ctx, &apiv1.ISBServiceRollout{}, &ctlrruntimeclient.DeleteAllOfOptions{ListOptions: ctlrruntimeclient.ListOptions{Namespace: ctlrcommon.DefaultTestNamespace}})

			if tc.pipelineRolloutAnnotations == nil {
				tc.pipelineRolloutAnnotations = map[string]string{}
			}

			// Create our PipelineRollout
			rollout := ctlrcommon.CreateTestPipelineRollout(tc.newPipelineSpec, tc.pipelineRolloutAnnotations, map[string]string{}, map[string]string{}, map[string]string{}, nil)
			_ = client.Delete(ctx, rollout)

			rollout.Status.Phase = tc.initialRolloutPhase

			// Set the In-Progress Strategy
			rollout.Status.UpgradeInProgress = tc.initialInProgressStrategy
			r.inProgressStrategyMgr.Store.SetStrategy(k8stypes.NamespacedName{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestPipelineRolloutName}, tc.initialInProgressStrategy)

			// the Reconcile() function does this, so we need to do it before calling reconcile() as well
			rollout.Status.Init(rollout.Generation)

			err = client.Create(ctx, rollout)
			assert.NoError(t, err)

			// create the already-existing Pipeline in Kubernetes
			existingPipelineDef := &tc.existingPipelineDef
			existingPipelineDef.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rollout.GetObjectMeta(), apiv1.PipelineRolloutGroupVersionKind)}
			ctlrcommon.CreatePipelineInK8S(ctx, t, numaflowClientSet, &tc.existingPipelineDef)

			// external pause requests
			ppnd.GetPauseModule().PauseRequests = map[string]*bool{}
			if tc.numaflowControllerPauseRequest != nil {
				ppnd.GetPauseModule().PauseRequests[ppnd.GetPauseModule().GetNumaflowControllerKey(ctlrcommon.DefaultTestNamespace)] = tc.numaflowControllerPauseRequest
			}
			if tc.isbServicePauseRequest != nil {
				ppnd.GetPauseModule().PauseRequests[ppnd.GetPauseModule().GetISBServiceKey(ctlrcommon.DefaultTestNamespace, ctlrcommon.DefaultTestISBSvcRolloutName)] = tc.isbServicePauseRequest
			}

			// create ISBServiceRollout and isbsvc in Kubernetes
			isbServiceRollout := ctlrcommon.CreateISBServiceRollout(ctlrcommon.CreateDefaultISBServiceSpec("2.10.11"), nil)
			ctlrcommon.CreateISBServiceRolloutInK8S(ctx, t, client, isbServiceRollout)
			isbsvc := ctlrcommon.CreateDefaultISBService("2.10.11", numaflowv1.ISBSvcPhaseRunning, true)
			ctlrcommon.CreateISBSvcInK8S(ctx, t, numaflowClientSet, isbsvc)

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
	restConfig, numaflowClientSet, client, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetClientSets(restConfig))

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig2"))
	assert.NoError(t, err)

	config.GetConfigManagerInstance().UpdateUSDEConfig(config.USDEConfig{
		"pipeline": config.USDEResourceConfig{
			DataLoss: []config.SpecField{{Path: "spec.vertices", IncludeSubfields: true}},
		},
	})
	ctx := context.Background()

	// other tests may call this, but it fails if called more than once
	if ctlrcommon.TestCustomMetrics == nil {
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics()
	}

	recorder := record.NewFakeRecorder(64)

	r := NewPipelineRolloutReconciler(
		client,
		scheme.Scheme,
		ctlrcommon.TestCustomMetrics,
		recorder)

	progressiveUpgradeStrategy := apiv1.UpgradeStrategyProgressive

	testCases := []struct {
		name                        string
		newPipelineSpec             numaflowv1.PipelineSpec
		existingPromotedPipelineDef numaflowv1.Pipeline
		existingUpgradePipelineDef  *numaflowv1.Pipeline
		initialRolloutPhase         apiv1.Phase
		initialRolloutNameCount     int
		initialInProgressStrategy   *apiv1.UpgradeStrategy
		initialUpgradingChildStatus *apiv1.UpgradingPipelineStatus
		initialPromotedChildStatus  *apiv1.PromotedPipelineStatus

		expectedInProgressStrategy apiv1.UpgradeStrategy
		expectedRolloutPhase       apiv1.Phase

		expectedPipelines map[string]common.UpgradeState // after reconcile(), these are the only pipelines we expect to exist along with their expected UpgradeState

	}{
		{
			name:            "spec difference results in Progressive",
			newPipelineSpec: pipelineSpecWithTopologyChange,
			existingPromotedPipelineDef: *createPipeline(
				numaflowv1.PipelinePhaseRunning,
				numaflowv1.Status{},
				false,
				map[string]string{
					common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
					common.LabelKeyISBServiceChildNameForPipeline: ctlrcommon.DefaultTestISBSvcName,
					common.LabelKeyUpgradeState:                   string(common.LabelValueUpgradePromoted),
					common.LabelKeyParentRollout:                  ctlrcommon.DefaultTestPipelineRolloutName,
				}),
			existingUpgradePipelineDef:  nil,
			initialRolloutPhase:         apiv1.PhaseDeployed,
			initialRolloutNameCount:     1,
			initialInProgressStrategy:   nil,
			initialUpgradingChildStatus: nil,
			initialPromotedChildStatus:  nil,
			expectedInProgressStrategy:  apiv1.UpgradeStrategyProgressive,
			expectedRolloutPhase:        apiv1.PhasePending,

			expectedPipelines: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestPipelineRolloutName + "-0": common.LabelValueUpgradePromoted,
				ctlrcommon.DefaultTestPipelineRolloutName + "-1": common.LabelValueUpgradeInProgress,
			},
		},
		{
			name:            "Progressive deployed successfully",
			newPipelineSpec: pipelineSpecWithTopologyChange,
			existingPromotedPipelineDef: *createPipeline(
				numaflowv1.PipelinePhaseRunning,
				numaflowv1.Status{},
				false,
				map[string]string{
					common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
					common.LabelKeyISBServiceChildNameForPipeline: ctlrcommon.DefaultTestISBSvcName,
					common.LabelKeyUpgradeState:                   string(common.LabelValueUpgradePromoted),
					common.LabelKeyParentRollout:                  ctlrcommon.DefaultTestPipelineRolloutName,
				}),
			existingUpgradePipelineDef: ctlrcommon.CreateTestPipelineOfSpec(
				pipelineSpecWithTopologyChange, ctlrcommon.DefaultTestPipelineRolloutName+"-1",
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
					common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
					common.LabelKeyISBServiceChildNameForPipeline: ctlrcommon.DefaultTestISBSvcName,
					common.LabelKeyUpgradeState:                   string(common.LabelValueUpgradeInProgress),
					common.LabelKeyParentRollout:                  ctlrcommon.DefaultTestPipelineRolloutName,
				},
				map[string]string{}),
			initialRolloutPhase:       apiv1.PhasePending,
			initialRolloutNameCount:   2,
			initialInProgressStrategy: &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: &apiv1.UpgradingPipelineStatus{
				UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                ctlrcommon.DefaultTestPipelineRolloutName + "-1",
						AssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
						AssessmentEndTime:   &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
						AssessmentResult:    apiv1.AssessmentResultSuccess,
					},
				},
				OriginalScaleMinMax: []apiv1.VertexScale{
					{VertexName: "in", ScaleMinMax: "null"},
					{VertexName: "cat", ScaleMinMax: "null"},
					{VertexName: "cat-2", ScaleMinMax: "null"},
					{VertexName: "out", ScaleMinMax: "null"},
				},
			},
			initialPromotedChildStatus: &apiv1.PromotedPipelineStatus{
				PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
					PromotedChildStatus: apiv1.PromotedChildStatus{
						Name: ctlrcommon.DefaultTestPipelineRolloutName + "-0",
					},
					AllVerticesScaledDown: true,
				},
			},
			expectedInProgressStrategy: apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:       apiv1.PhaseDeployed,

			expectedPipelines: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestPipelineRolloutName + "-0": common.LabelValueUpgradeRecyclable,
				ctlrcommon.DefaultTestPipelineRolloutName + "-1": common.LabelValueUpgradePromoted,
			},
		},
		{
			name:            "Progressive deployment failed",
			newPipelineSpec: pipelineSpecWithTopologyChange,
			existingPromotedPipelineDef: *createPipeline(
				numaflowv1.PipelinePhaseRunning,
				numaflowv1.Status{},
				false,
				map[string]string{
					common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
					common.LabelKeyISBServiceChildNameForPipeline: ctlrcommon.DefaultTestISBSvcName,
					common.LabelKeyUpgradeState:                   string(common.LabelValueUpgradePromoted),
					common.LabelKeyParentRollout:                  ctlrcommon.DefaultTestPipelineRolloutName,
				}),
			existingUpgradePipelineDef: ctlrcommon.CreateTestPipelineOfSpec(
				runningPipelineSpecWithTopologyChange, ctlrcommon.DefaultTestPipelineRolloutName+"-1",
				numaflowv1.PipelinePhaseFailed,
				numaflowv1.Status{
					Conditions: []metav1.Condition{
						{
							Type:   string(numaflowv1.PipelineConditionDaemonServiceHealthy),
							Status: metav1.ConditionFalse,
						},
					},
				},
				false,
				map[string]string{
					common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
					common.LabelKeyISBServiceChildNameForPipeline: ctlrcommon.DefaultTestISBSvcName,
					common.LabelKeyUpgradeState:                   string(common.LabelValueUpgradeInProgress),
					common.LabelKeyParentRollout:                  ctlrcommon.DefaultTestPipelineRolloutName,
				},
				map[string]string{}),
			initialRolloutPhase:       apiv1.PhasePending,
			initialRolloutNameCount:   2,
			initialInProgressStrategy: &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: &apiv1.UpgradingPipelineStatus{
				UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                ctlrcommon.DefaultTestPipelineRolloutName + "-1",
						AssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
						AssessmentEndTime:   &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
						AssessmentResult:    apiv1.AssessmentResultFailure,
					},
				},
				OriginalScaleMinMax: []apiv1.VertexScale{
					{VertexName: "in", ScaleMinMax: "null"},
					{VertexName: "cat", ScaleMinMax: "null"},
					{VertexName: "cat-2", ScaleMinMax: "null"},
					{VertexName: "out", ScaleMinMax: "null"},
				},
			},
			initialPromotedChildStatus: &apiv1.PromotedPipelineStatus{
				PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
					PromotedChildStatus: apiv1.PromotedChildStatus{
						Name: ctlrcommon.DefaultTestPipelineRolloutName + "-0",
					},
					AllVerticesScaledDown: true,
					ScaleValues: map[string]apiv1.ScaleValues{
						"in":  {OriginalScaleMinMax: ctlrcommon.DefaultScaleJSONString, ScaleTo: ctlrcommon.DefaultScaleTo},
						"cat": {OriginalScaleMinMax: ctlrcommon.DefaultScaleJSONString, ScaleTo: ctlrcommon.DefaultScaleTo},
						"out": {OriginalScaleMinMax: ctlrcommon.DefaultScaleJSONString, ScaleTo: ctlrcommon.DefaultScaleTo},
					},
				},
			},
			expectedInProgressStrategy: apiv1.UpgradeStrategyProgressive,
			expectedRolloutPhase:       apiv1.PhasePending,

			expectedPipelines: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestPipelineRolloutName + "-0": common.LabelValueUpgradePromoted,
				ctlrcommon.DefaultTestPipelineRolloutName + "-1": common.LabelValueUpgradeInProgress,
			},
		},
		{
			name:            "Progressive deployment failed - going back to original spec",
			newPipelineSpec: pipelineSpec, // this matches the original spec
			existingPromotedPipelineDef: *createPipeline(
				numaflowv1.PipelinePhaseRunning,
				numaflowv1.Status{},
				false,
				map[string]string{
					common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
					common.LabelKeyISBServiceChildNameForPipeline: ctlrcommon.DefaultTestISBSvcName,
					common.LabelKeyUpgradeState:                   string(common.LabelValueUpgradePromoted),
					common.LabelKeyParentRollout:                  ctlrcommon.DefaultTestPipelineRolloutName,
				}),
			existingUpgradePipelineDef: ctlrcommon.CreateTestPipelineOfSpec(
				runningPipelineSpecWithTopologyChange, ctlrcommon.DefaultTestPipelineRolloutName+"-1", // the one that's currently "upgrading" is the one with the topology change
				numaflowv1.PipelinePhaseFailed,
				numaflowv1.Status{
					Conditions: []metav1.Condition{
						{
							Type:   string(numaflowv1.PipelineConditionDaemonServiceHealthy),
							Status: metav1.ConditionFalse,
						},
					},
				},
				false,
				map[string]string{
					common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
					common.LabelKeyISBServiceChildNameForPipeline: ctlrcommon.DefaultTestISBSvcName,
					common.LabelKeyUpgradeState:                   string(common.LabelValueUpgradeInProgress),
					common.LabelKeyParentRollout:                  ctlrcommon.DefaultTestPipelineRolloutName,
				},
				map[string]string{}),
			initialRolloutPhase:       apiv1.PhasePending,
			initialRolloutNameCount:   2,
			initialInProgressStrategy: &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: &apiv1.UpgradingPipelineStatus{
				UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                ctlrcommon.DefaultTestPipelineRolloutName + "-1",
						AssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
						AssessmentEndTime:   &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
						AssessmentResult:    apiv1.AssessmentResultFailure,
					},
				},
				OriginalScaleMinMax: []apiv1.VertexScale{
					{VertexName: "in", ScaleMinMax: "null"},
					{VertexName: "cat", ScaleMinMax: "null"},
					{VertexName: "cat-2", ScaleMinMax: "null"},
					{VertexName: "out", ScaleMinMax: "null"},
				},
			},
			initialPromotedChildStatus: &apiv1.PromotedPipelineStatus{
				PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
					PromotedChildStatus: apiv1.PromotedChildStatus{
						Name: ctlrcommon.DefaultTestPipelineRolloutName + "-0",
					},
					AllVerticesScaledDown: true,
					ScaleValues: map[string]apiv1.ScaleValues{
						"in":  {OriginalScaleMinMax: ctlrcommon.DefaultScaleJSONString, ScaleTo: ctlrcommon.DefaultScaleTo},
						"cat": {OriginalScaleMinMax: ctlrcommon.DefaultScaleJSONString, ScaleTo: ctlrcommon.DefaultScaleTo},
						"out": {OriginalScaleMinMax: ctlrcommon.DefaultScaleJSONString, ScaleTo: ctlrcommon.DefaultScaleTo},
					},
				},
			},
			expectedInProgressStrategy: apiv1.UpgradeStrategyProgressive,
			expectedRolloutPhase:       apiv1.PhaseDeployed,

			// the Failed Pipeline which is marked "recyclable" gets deleted right away due to the fact that it's in "Failed" state and therefore can't pause
			expectedPipelines: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestPipelineRolloutName + "-0": common.LabelValueUpgradePromoted,
				ctlrcommon.DefaultTestPipelineRolloutName + "-2": common.LabelValueUpgradeInProgress,
			},
		},
		{
			name:            "Clean up after progressive upgrade",
			newPipelineSpec: pipelineSpecWithTopologyChange,
			existingPromotedPipelineDef: *createPipeline(
				numaflowv1.PipelinePhasePaused,
				numaflowv1.Status{},
				true,
				map[string]string{
					common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
					common.LabelKeyISBServiceChildNameForPipeline: ctlrcommon.DefaultTestISBSvcName,
					common.LabelKeyUpgradeState:                   string(common.LabelValueUpgradeRecyclable),
					common.LabelKeyUpgradeStateReason:             string(common.LabelValueProgressiveSuccess),
					common.LabelKeyParentRollout:                  ctlrcommon.DefaultTestPipelineRolloutName,
				}),
			existingUpgradePipelineDef: ctlrcommon.CreateTestPipelineOfSpec(
				runningPipelineSpecWithTopologyChange, ctlrcommon.DefaultTestPipelineRolloutName+"-1",
				numaflowv1.PipelinePhaseRunning,
				numaflowv1.Status{},
				false,
				map[string]string{
					common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
					common.LabelKeyISBServiceChildNameForPipeline: ctlrcommon.DefaultTestISBSvcName,
					common.LabelKeyUpgradeState:                   string(common.LabelValueUpgradePromoted),
					common.LabelKeyParentRollout:                  ctlrcommon.DefaultTestPipelineRolloutName,
				},
				map[string]string{}),
			initialRolloutPhase:         apiv1.PhaseDeployed,
			initialRolloutNameCount:     2,
			initialInProgressStrategy:   nil,
			initialUpgradingChildStatus: nil,
			expectedInProgressStrategy:  apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:        apiv1.PhaseDeployed,
			expectedPipelines: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestPipelineRolloutName + "-1": common.LabelValueUpgradePromoted,
			},
		},
		{
			name:            "Clean up after progressive upgrade: pipeline still pausing",
			newPipelineSpec: pipelineSpecWithTopologyChange,
			existingPromotedPipelineDef: *createPipeline(
				numaflowv1.PipelinePhasePausing,
				numaflowv1.Status{},
				false,
				map[string]string{
					common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
					common.LabelKeyISBServiceChildNameForPipeline: ctlrcommon.DefaultTestISBSvcName,
					common.LabelKeyUpgradeState:                   string(common.LabelValueUpgradeRecyclable),
					common.LabelKeyUpgradeStateReason:             string(common.LabelValueProgressiveSuccess),
					common.LabelKeyParentRollout:                  ctlrcommon.DefaultTestPipelineRolloutName,
				}),
			existingUpgradePipelineDef: ctlrcommon.CreateTestPipelineOfSpec(
				runningPipelineSpecWithTopologyChange, ctlrcommon.DefaultTestPipelineRolloutName+"-1",
				numaflowv1.PipelinePhaseRunning,
				numaflowv1.Status{},
				false,
				map[string]string{
					common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
					common.LabelKeyISBServiceChildNameForPipeline: ctlrcommon.DefaultTestISBSvcName,
					common.LabelKeyUpgradeState:                   string(common.LabelValueUpgradePromoted),
					common.LabelKeyParentRollout:                  ctlrcommon.DefaultTestPipelineRolloutName,
				},
				map[string]string{}),
			initialRolloutPhase:         apiv1.PhaseDeployed,
			initialRolloutNameCount:     2,
			initialInProgressStrategy:   nil,
			initialUpgradingChildStatus: nil,
			expectedInProgressStrategy:  apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:        apiv1.PhaseDeployed,

			expectedPipelines: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestPipelineRolloutName + "-0": common.LabelValueUpgradeRecyclable,
				ctlrcommon.DefaultTestPipelineRolloutName + "-1": common.LabelValueUpgradePromoted,
			},
		},
	}

	for _, tc := range testCases {

		t.Run(tc.name, func(t *testing.T) {

			// first delete resources (Pipeline, InterstepBufferService, PipelineRollout, ISBServiceRollout) in case they already exist, in Kubernetes
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = numaflowClientSet.NumaflowV1alpha1().InterStepBufferServices(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})

			_ = client.DeleteAllOf(ctx, &apiv1.PipelineRollout{}, &ctlrruntimeclient.DeleteAllOfOptions{ListOptions: ctlrruntimeclient.ListOptions{Namespace: ctlrcommon.DefaultTestNamespace}})
			_ = client.DeleteAllOf(ctx, &apiv1.ISBServiceRollout{}, &ctlrruntimeclient.DeleteAllOfOptions{ListOptions: ctlrruntimeclient.ListOptions{Namespace: ctlrcommon.DefaultTestNamespace}})

			// Create our PipelineRollout
			rollout := ctlrcommon.CreateTestPipelineRollout(tc.newPipelineSpec, map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{},
				&apiv1.PipelineRolloutStatus{ProgressiveStatus: apiv1.PipelineProgressiveStatus{UpgradingPipelineStatus: tc.initialUpgradingChildStatus, PromotedPipelineStatus: tc.initialPromotedChildStatus}})

			rollout.Status.Phase = tc.initialRolloutPhase
			if rollout.Status.NameCount == nil {
				rollout.Status.NameCount = new(int32)
			}
			*rollout.Status.NameCount = int32(tc.initialRolloutNameCount)

			// the Reconcile() function does this, so we need to do it before calling reconcile() as well
			rollout.Status.Init(rollout.Generation)
			ctlrcommon.CreatePipelineRolloutInK8S(ctx, t, client, rollout)

			// Set the In-Progress Strategy
			if tc.initialInProgressStrategy != nil {
				rollout.Status.UpgradeInProgress = *tc.initialInProgressStrategy
				r.inProgressStrategyMgr.Store.SetStrategy(k8stypes.NamespacedName{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestPipelineRolloutName}, *tc.initialInProgressStrategy)
			} else {
				rollout.Status.UpgradeInProgress = apiv1.UpgradeStrategyNoOp
				r.inProgressStrategyMgr.Store.SetStrategy(k8stypes.NamespacedName{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestPipelineRolloutName}, apiv1.UpgradeStrategyNoOp)
			}

			// create the already-existing Pipeline in Kubernetes
			existingPipelineDef := &tc.existingPromotedPipelineDef
			existingPipelineDef.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rollout.GetObjectMeta(), apiv1.PipelineRolloutGroupVersionKind)}
			ctlrcommon.CreatePipelineInK8S(ctx, t, numaflowClientSet, existingPipelineDef)

			if tc.existingUpgradePipelineDef != nil {
				existingUpgradePipelineDef := tc.existingUpgradePipelineDef
				existingUpgradePipelineDef.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rollout.GetObjectMeta(), apiv1.PipelineRolloutGroupVersionKind)}
				ctlrcommon.CreatePipelineInK8S(ctx, t, numaflowClientSet, existingUpgradePipelineDef)
			}

			// create ISBServiceRollout and isbsvc in Kubernetes
			isbServiceRollout := ctlrcommon.CreateISBServiceRollout(ctlrcommon.CreateDefaultISBServiceSpec("2.10.11"), nil)
			ctlrcommon.CreateISBServiceRolloutInK8S(ctx, t, client, isbServiceRollout)
			isbsvc := ctlrcommon.CreateDefaultISBService("2.10.11", numaflowv1.ISBSvcPhaseRunning, true)
			ctlrcommon.CreateISBSvcInK8S(ctx, t, numaflowClientSet, isbsvc)

			_, _, err = r.reconcile(context.Background(), rollout, time.Now())
			assert.NoError(t, err)

			////// check results:
			// Check Phase of Rollout:
			assert.Equal(t, tc.expectedRolloutPhase, rollout.Status.Phase)
			// Check In-Progress Strategy
			assert.Equal(t, tc.expectedInProgressStrategy, rollout.Status.UpgradeInProgress)

			resultPipelineList, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Equal(t, len(tc.expectedPipelines), len(resultPipelineList.Items), resultPipelineList.Items)

			for _, pipeline := range resultPipelineList.Items {
				expectedPipelineUpgradeState, found := tc.expectedPipelines[pipeline.Name]
				assert.True(t, found)
				resultUpgradeState, found := pipeline.Labels[common.LabelKeyUpgradeState]
				assert.True(t, found)
				assert.Equal(t, string(expectedPipelineUpgradeState), resultUpgradeState)
			}
		})
	}
}

func TestGetScalePatchesFromPipelineSpec(t *testing.T) {
	tests := []struct {
		name           string
		pipelineDef    string
		expectedResult []apiv1.VertexScale
		expectError    bool
	}{
		{
			name: "Valid pipeline spec with various scale definitions",
			pipelineDef: `
{
	  "vertices": [
		{
		  "name": "in",
		  "scale": {
			"min": 1,
			"max": 5
		  },
		  "source": {
			"generator": {
			  "rpu": 5,
			  "duration": "1s"
			}
		  }
		},
		{
		  "name": "cat",
		  "scale": {
		  },
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
	  ]
	
}
			`,
			expectedResult: []apiv1.VertexScale{
				{
					VertexName:  "in",
					ScaleMinMax: `{"min": 1, "max": 5}`,
				},
				{
					VertexName:  "cat",
					ScaleMinMax: `{"min": null, "max": null}`,
				},
				{
					VertexName:  "out",
					ScaleMinMax: "null",
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()

			obj := &unstructured.Unstructured{Object: make(map[string]interface{})}
			var spec map[string]interface{}
			err := json.Unmarshal([]byte(tt.pipelineDef), &spec)
			assert.NoError(t, err)

			obj.Object["spec"] = spec

			result, err := getScalePatchesFromPipelineSpec(ctx, obj)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result)
			}
		})
	}
}
