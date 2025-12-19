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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctlrruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/common/riders"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/controller/ppnd"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
)

var (
	pullPolicyAlways           = corev1.PullAlways
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
					Container: &numaflowv1.Container{
						Image:           "quay.io/numaio/numaflow-go/map-cat:stable",
						ImagePullPolicy: &pullPolicyAlways,
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
			Container: &numaflowv1.Container{
				Image:           "quay.io/numaio/numaflow-go/map-cat:stable",
				ImagePullPolicy: &pullPolicyAlways,
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
			"container": {
				"image": "quay.io/numaio/numaflow-go/map-cat:stable",
				"imagePullPolicy": "Always,"
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
			"container": {
				"image": "SOMETHING_ELSE",
				"imagePullPolicy": "Always,"
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
			"container": {
				"image": "quay.io/numaio/numaflow-go/map-cat:stable",
				"imagePullPolicy": "Always,"
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
			"container": {
				"image": "quay.io/numaio/numaflow-go/map-cat:stable",
				"imagePullPolicy": "Always,"
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
			"container": {
				"image": "quay.io/numaio/numaflow-go/map-cat:stable",
				"imagePullPolicy": "Always,"
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
			"container": {
				"image": "quay.io/numaio/numaflow-go/map-cat:stable",
				"imagePullPolicy": "Always,"
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

var specWithNonEmptyNonZeroScale = `
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
			"container": {
				"image": "quay.io/numaio/numaflow-go/map-cat:stable",
				"imagePullPolicy": "Always,"
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

var specWithZeroScale = `
{
	  "interStepBufferServiceName": "default",
	  "vertices": [
		{
		  "name": "in",
	      "scale": {
		    "min": 0,
	        "max": 0
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
		    "min": 0,
	        "max": 0
	      },
		  "udf": {
			"container": {
				"image": "quay.io/numaio/numaflow-go/map-cat:stable",
				"imagePullPolicy": "Always,"
			}
		  }
		},
		{
		  "name": "out",
	      "scale": {
		    "min": 0,
	        "max": 0
	      },
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

func Test_CheckForDifferences(t *testing.T) {

	_, _, client, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)

	recorder := record.NewFakeRecorder(64)

	r := NewPipelineRolloutReconciler(
		client,
		scheme.Scheme,
		ctlrcommon.TestCustomMetrics,
		recorder)

	testCases := []struct {
		name                      string
		from                      string // existing pipeline spec
		to                        string // rollout definition spec
		labels1                   map[string]string
		labels2                   map[string]string
		annotations1              map[string]string
		annotations2              map[string]string
		existingChildUpgradeState common.UpgradeState
		originalScaleDefinitions  []string // only used when existingChildUpgradeState=trial
		expectedNeedsUpdating     bool
		expectedError             bool
	}{
		{
			name:                      "Equal specs",
			from:                      specNoScale,
			to:                        specNoScale,
			existingChildUpgradeState: common.LabelValueUpgradePromoted,
			expectedNeedsUpdating:     false,
			expectedError:             false,
		},
		{
			name:                      "Scales differ - comparison to a promoted child",
			from:                      specNoScale,
			to:                        specWithNonEmptyNonZeroScale,
			existingChildUpgradeState: common.LabelValueUpgradePromoted,
			expectedNeedsUpdating:     false, // scale fields are excluded from comparison for promoted children
			expectedError:             false,
		},
		{
			name:                      "Required Labels not Present",
			from:                      specNoScale,
			to:                        specNoScale,
			labels1:                   map[string]string{"app": "test1"},
			labels2:                   map[string]string{"app": "test2"},
			existingChildUpgradeState: common.LabelValueUpgradePromoted,
			expectedNeedsUpdating:     true,
			expectedError:             false,
		},
		{
			name:                      "Required Annotations Present",
			from:                      specNoScale,
			to:                        specNoScale,
			annotations1:              map[string]string{"key1": "test1"},
			annotations2:              nil,
			existingChildUpgradeState: common.LabelValueUpgradePromoted,
			expectedNeedsUpdating:     false,
			expectedError:             false,
		},
		{
			name:                      "Comparison to Upgrading child - original scale matches rollout",
			from:                      specWithNonEmptyNonZeroScale, // current upgrading pipeline (modified by progressive)
			to:                        specNoScale,                  // rollout definition (no scale)
			existingChildUpgradeState: common.LabelValueUpgradeTrial,
			// original scale definitions (one per vertex: in, cat, out) - all had no scale originally
			originalScaleDefinitions: []string{"null", "null", "null"},
			expectedNeedsUpdating:    false, // no difference because original matches rollout
			expectedError:            false,
		},
		{
			name:                      "Upgrading - original scale differs from rollout",
			from:                      specWithNonEmptyNonZeroScale, // current upgrading pipeline (modified by progressive)
			to:                        specWithZeroScale,            // rollout definition (zero scale)
			existingChildUpgradeState: common.LabelValueUpgradeTrial,
			// original scale definitions - had non-zero scale
			originalScaleDefinitions: []string{`{"min":3,"max":5}`, "null", "null"},
			expectedNeedsUpdating:    true, // difference because original scale != new rollout scale
			expectedError:            false,
		},
		{
			name:                      "Upgrading - original scale null, rollout has scale",
			from:                      specWithNonEmptyNonZeroScale, // current upgrading pipeline (modified by progressive)
			to:                        specWithNonEmptyNonZeroScale, // rollout definition (has scale)
			existingChildUpgradeState: common.LabelValueUpgradeTrial,
			// original scale definitions - all had no scale
			originalScaleDefinitions: []string{"null", "null", "null"},
			expectedNeedsUpdating:    true, // difference because original (null) != rollout (has scale)
			expectedError:            false,
		},
		{
			name:                      "Upgrading - original scale matches rollout with scale",
			from:                      specWithZeroScale,            // current upgrading pipeline (modified by progressive)
			to:                        specWithNonEmptyNonZeroScale, // rollout definition (has scale)
			existingChildUpgradeState: common.LabelValueUpgradeTrial,
			// original scale definitions match the rollout
			originalScaleDefinitions: []string{`{"min":3,"max":5}`, "null", "null"},
			expectedNeedsUpdating:    false, // no difference because original matches rollout
			expectedError:            false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			obj1 := &unstructured.Unstructured{Object: make(map[string]interface{})}
			var yaml1Spec map[string]interface{}
			err := json.Unmarshal([]byte(tc.from), &yaml1Spec)
			assert.NoError(t, err)
			obj1.Object["spec"] = yaml1Spec
			obj1.SetName("test-pipeline")
			if tc.labels1 != nil {
				obj1.SetLabels(tc.labels1)
			}
			if tc.annotations1 != nil {
				obj1.SetAnnotations(tc.annotations1)
			}

			// Create the RolloutObject with the defined spec and metadata
			pipelineRollout := &apiv1.PipelineRollout{
				Spec: apiv1.PipelineRolloutSpec{
					Pipeline: apiv1.Pipeline{
						Spec: runtime.RawExtension{
							Raw: []byte(tc.to),
						},
						Metadata: apiv1.Metadata{
							Annotations: tc.annotations2,
							Labels:      tc.labels2,
						},
					},
				},
			}

			// If comparing to an upgrading child, set up the UpgradingPipelineStatus
			if tc.existingChildUpgradeState == common.LabelValueUpgradeTrial {
				pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus = &apiv1.UpgradingPipelineStatus{
					OriginalScaleDefinitions: tc.originalScaleDefinitions,
				}
				pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus.Name = obj1.GetName()
			}

			needsUpdating, err := r.CheckForDifferencesWithRolloutDef(context.Background(), obj1, pipelineRollout, tc.existingChildUpgradeState)
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedNeedsUpdating, needsUpdating)
			}
		})
	}
}

func Test_pipelineNeedsUpdatingForPPND(t *testing.T) {

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
		labels1               map[string]string
		labels2               map[string]string
		annotations1          map[string]string
		annotations2          map[string]string
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
			name:                  "Different Labels",
			spec1:                 specHasDesiredPhase,
			spec2:                 specHasDesiredPhase,
			labels1:               map[string]string{"app": "test1"},
			labels2:               map[string]string{"app": "test2"},
			expectedNeedsUpdating: false,
			expectedError:         false,
		},
		{
			name:                  "Different Annotations",
			spec1:                 specHasDesiredPhase,
			spec2:                 specHasDesiredPhase,
			annotations1:          map[string]string{"key1": "test1"},
			annotations2:          nil,
			expectedNeedsUpdating: false,
			expectedError:         false,
		},
		{
			name:                  "Numaflow Instance Annotation should not be ignored",
			spec1:                 specHasDesiredPhase,
			spec2:                 specHasDesiredPhase,
			annotations1:          map[string]string{common.AnnotationKeyNumaflowInstanceID: "2"},
			annotations2:          nil,
			expectedNeedsUpdating: true,
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
			if tc.labels1 != nil {
				obj1.SetLabels(tc.labels1)
			}
			if tc.annotations1 != nil {
				obj1.SetAnnotations(tc.annotations1)
			}

			obj2 := &unstructured.Unstructured{Object: make(map[string]interface{})}
			var yaml2Spec map[string]interface{}
			err = json.Unmarshal([]byte(tc.spec2), &yaml2Spec)
			assert.NoError(t, err)
			obj2.Object["spec"] = yaml2Spec
			if tc.labels2 != nil {
				obj2.SetLabels(tc.labels2)
			}
			if tc.annotations2 != nil {
				obj2.SetAnnotations(tc.annotations2)
			}

			needsUpdating, err := r.pipelineNeedsUpdatingForPPND(context.Background(), obj1, obj2)
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

func createPipeline(phase numaflowv1.PipelinePhase, status numaflowv1.Status, drainedOnPause bool, labels map[string]string, annotations map[string]string) *numaflowv1.Pipeline {
	return ctlrcommon.CreateTestPipelineOfSpec(runningPipelineSpec, ctlrcommon.DefaultTestPipelineName, phase, status, drainedOnPause, labels, annotations)
}

func withInterstepBufferService(origPipelineSpec numaflowv1.PipelineSpec, isbsvc string) numaflowv1.PipelineSpec {
	newPipelineSpec := origPipelineSpec.DeepCopy()
	newPipelineSpec.InterStepBufferServiceName = isbsvc
	return *newPipelineSpec
}

// process an existing pipeline
// in this test, the user preferred strategy is PPND
func Test_processExistingPipeline_PPND(t *testing.T) {
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

	config.GetConfigManagerInstance().UpdateUSDEConfig(config.USDEConfig{
		"pipeline": config.USDEResourceConfig{
			DataLoss: []config.SpecField{{Path: "spec.vertices", IncludeSubfields: true}},
		},
	})

	ctx := context.Background()

	// other tests may call this, but it fails if called more than once
	if ctlrcommon.TestCustomMetrics == nil {
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics(numaLogger)
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
			_ = numaflowClientSet.NumaflowV1alpha1().Vertices(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
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

			// create the already-existing Vertices in Kubernetes
			for _, vertexDef := range existingPipelineDef.Spec.Vertices {
				vertex := numaflowv1.Vertex{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: ctlrcommon.DefaultTestNamespace,
						Name:      fmt.Sprintf("%s-%s", tc.existingPipelineDef.Name, vertexDef.Name),
						Labels: map[string]string{
							common.LabelKeyNumaflowPipelineName:       tc.existingPipelineDef.GetName(),
							common.LabelKeyNumaflowPipelineVertexName: vertexDef.Name,
						},
					},
				}

				//vertex.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(tc.existingPipelineDef.GetObjectMeta(), numaflowv1.PipelineGroupVersionKind)}
				fmt.Printf("creating vertex named %q\n", vertex.Name)
				ctlrcommon.CreateVertexInK8S(ctx, t, numaflowClientSet, &vertex)
			}

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
	numaLogger := logger.New()
	numaLogger.SetLevel(4)
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
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics(numaLogger)
	}

	recorder := record.NewFakeRecorder(64)

	r := NewPipelineRolloutReconciler(
		client,
		scheme.Scheme,
		ctlrcommon.TestCustomMetrics,
		recorder)

	progressiveUpgradeStrategy := apiv1.UpgradeStrategyProgressive

	defaultPromotedPipelineDef := createPipeline(
		numaflowv1.PipelinePhaseRunning,
		numaflowv1.Status{},
		false,
		map[string]string{
			common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
			common.LabelKeyISBServiceChildNameForPipeline: ctlrcommon.DefaultTestISBSvcName,
			common.LabelKeyUpgradeState:                   string(common.LabelValueUpgradePromoted),
			common.LabelKeyParentRollout:                  ctlrcommon.DefaultTestPipelineRolloutName,
			common.LabelKeyProgressiveResultState:         string(common.LabelValueResultStateSucceeded),
		},
		map[string]string{
			common.AnnotationKeyRequiresDrain: "true",
		})

	defaultUpgradingPipelineDef := ctlrcommon.CreateTestPipelineOfSpec(
		runningPipelineSpecWithTopologyChange, ctlrcommon.DefaultTestPipelineRolloutName+"-1",
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
			common.LabelKeyUpgradeState:                   string(common.LabelValueUpgradeTrial),
			common.LabelKeyParentRollout:                  ctlrcommon.DefaultTestPipelineRolloutName,
		},
		map[string]string{
			common.AnnotationKeyRequiresDrain: "true",
		})

	defaultFailedUpgradingPipelineDef := defaultUpgradingPipelineDef.DeepCopy()
	defaultFailedUpgradingPipelineDef.Status.Conditions = []metav1.Condition{
		{
			Type:   string(numaflowv1.PipelineConditionDaemonServiceHealthy),
			Status: metav1.ConditionFalse,
		},
	}
	defaultPromotedChildStatus := &apiv1.PromotedPipelineStatus{
		PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
			PromotedChildStatus: apiv1.PromotedChildStatus{
				Name: ctlrcommon.DefaultTestPipelineRolloutName + "-0",
			},
			ScaleValues: map[string]apiv1.ScaleValues{
				"in":  {OriginalScaleMinMax: ctlrcommon.DefaultScaleJSONString, ScaleTo: ctlrcommon.DefaultScaleTo},
				"cat": {OriginalScaleMinMax: ctlrcommon.DefaultScaleJSONString, ScaleTo: ctlrcommon.DefaultScaleTo},
				"out": {OriginalScaleMinMax: ctlrcommon.DefaultScaleJSONString, ScaleTo: ctlrcommon.DefaultScaleTo},
			},
		},
	}

	successfulUpgradingChildStatus := &apiv1.UpgradingPipelineStatus{
		UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
			UpgradingChildStatus: apiv1.UpgradingChildStatus{
				Name:                     ctlrcommon.DefaultTestPipelineRolloutName + "-1",
				BasicAssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
				BasicAssessmentEndTime:   &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
				AssessmentResult:         apiv1.AssessmentResultSuccess,
				InitializationComplete:   true,
			},
		},
		OriginalScaleMinMax: []apiv1.VertexScaleDefinition{
			{VertexName: "in", ScaleDefinition: nil},
			{VertexName: "cat", ScaleDefinition: nil},
			{VertexName: "cat-2", ScaleDefinition: nil},
			{VertexName: "out", ScaleDefinition: nil},
		},
	}
	failedUpgradingChildStatus := successfulUpgradingChildStatus.DeepCopy()
	failedUpgradingChildStatus.UpgradingChildStatus.AssessmentResult = apiv1.AssessmentResultFailure

	testCases := []struct {
		name                        string
		newPipelineSpec             numaflowv1.PipelineSpec
		existingPromotedPipelineDef *numaflowv1.Pipeline
		existingUpgradePipelineDef  *numaflowv1.Pipeline
		initialRolloutPhase         apiv1.Phase
		initialRolloutNameCount     int
		initialInProgressStrategy   *apiv1.UpgradeStrategy
		initialUpgradingChildStatus *apiv1.UpgradingPipelineStatus
		initialPromotedChildStatus  *apiv1.PromotedPipelineStatus

		expectedInProgressStrategy apiv1.UpgradeStrategy
		expectedRolloutPhase       apiv1.Phase

		expectedPipelines            map[string]common.UpgradeState // after reconcile(), these are the only pipelines we expect to exist along with their expected UpgradeState
		expectedPipelinesResultState map[string]common.ResultState
	}{
		{
			name:                        "Progressive deployed successfully",
			newPipelineSpec:             pipelineSpecWithTopologyChange,
			existingPromotedPipelineDef: defaultPromotedPipelineDef,
			existingUpgradePipelineDef:  defaultUpgradingPipelineDef,
			initialRolloutPhase:         apiv1.PhasePending,
			initialRolloutNameCount:     2,
			initialInProgressStrategy:   &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: successfulUpgradingChildStatus,
			initialPromotedChildStatus:  defaultPromotedChildStatus,
			expectedInProgressStrategy:  apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:        apiv1.PhaseDeployed,

			expectedPipelines: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestPipelineRolloutName + "-0": common.LabelValueUpgradeRecyclable,
				ctlrcommon.DefaultTestPipelineRolloutName + "-1": common.LabelValueUpgradePromoted,
			},

			expectedPipelinesResultState: map[string]common.ResultState{
				ctlrcommon.DefaultTestPipelineRolloutName + "-0": common.LabelValueResultStateSucceeded,
				ctlrcommon.DefaultTestPipelineRolloutName + "-1": common.LabelValueResultStateSucceeded,
			},
		},
		{
			name:                        "Progressive deployment failed",
			newPipelineSpec:             pipelineSpecWithTopologyChange,
			existingPromotedPipelineDef: defaultPromotedPipelineDef,
			existingUpgradePipelineDef:  defaultFailedUpgradingPipelineDef,
			initialRolloutPhase:         apiv1.PhasePending,
			initialRolloutNameCount:     2,
			initialInProgressStrategy:   &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: failedUpgradingChildStatus,
			initialPromotedChildStatus:  defaultPromotedChildStatus,
			expectedInProgressStrategy:  apiv1.UpgradeStrategyProgressive,
			expectedRolloutPhase:        apiv1.PhasePending,

			expectedPipelines: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestPipelineRolloutName + "-0": common.LabelValueUpgradePromoted,
				ctlrcommon.DefaultTestPipelineRolloutName + "-1": common.LabelValueUpgradeTrial,
			},

			expectedPipelinesResultState: map[string]common.ResultState{
				ctlrcommon.DefaultTestPipelineRolloutName + "-0": common.LabelValueResultStateSucceeded,
				ctlrcommon.DefaultTestPipelineRolloutName + "-1": common.LabelValueResultStateFailed,
			},
		},
		{
			name:                        "Progressive deployment failed - going back to original spec",
			newPipelineSpec:             pipelineSpec, // this matches the original spec
			existingPromotedPipelineDef: defaultPromotedPipelineDef,
			existingUpgradePipelineDef:  defaultFailedUpgradingPipelineDef,
			initialRolloutPhase:         apiv1.PhasePending,
			initialRolloutNameCount:     2,
			initialInProgressStrategy:   &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: failedUpgradingChildStatus,
			initialPromotedChildStatus:  defaultPromotedChildStatus,
			expectedInProgressStrategy:  apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:        apiv1.PhaseDeployed,

			expectedPipelines: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestPipelineRolloutName + "-0": common.LabelValueUpgradePromoted,
				ctlrcommon.DefaultTestPipelineRolloutName + "-1": common.LabelValueUpgradeRecyclable,
			},

			expectedPipelinesResultState: map[string]common.ResultState{
				ctlrcommon.DefaultTestPipelineRolloutName + "-0": common.LabelValueResultStateSucceeded,
			},
		},
		{
			// this one is a weird case in which we've just updated our latest Pipeline (what's referred to below as the "existingUpgradePipelineDef") as "promoted" but maybe we had some resource version conflict failure
			// trying to set the previous "promoted" one to "recyclable" so now there are two "promoted" Pipelines in there
			name:                        "Clean up after progressive upgrade",
			newPipelineSpec:             pipelineSpecWithTopologyChange,
			existingPromotedPipelineDef: defaultPromotedPipelineDef,
			existingUpgradePipelineDef: ctlrcommon.CreateTestPipelineOfSpec(
				runningPipelineSpecWithTopologyChange, ctlrcommon.DefaultTestPipelineRolloutName+"-1",
				numaflowv1.PipelinePhaseRunning,
				numaflowv1.Status{},
				false,
				map[string]string{
					common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
					common.LabelKeyISBServiceChildNameForPipeline: ctlrcommon.DefaultTestISBSvcName,
					common.LabelKeyUpgradeState:                   string(common.LabelValueUpgradePromoted), // note: this is now "promoted"
					common.LabelKeyParentRollout:                  ctlrcommon.DefaultTestPipelineRolloutName,
					common.LabelKeyProgressiveResultState:         string(common.LabelValueResultStateSucceeded),
				},
				map[string]string{}),
			initialRolloutPhase:         apiv1.PhaseDeployed,
			initialRolloutNameCount:     2,
			initialInProgressStrategy:   nil, // TODO: why is this nil?
			initialUpgradingChildStatus: nil,
			expectedInProgressStrategy:  apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:        apiv1.PhaseDeployed,
			expectedPipelines: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestPipelineRolloutName + "-1": common.LabelValueUpgradePromoted,
			},
			expectedPipelinesResultState: map[string]common.ResultState{
				ctlrcommon.DefaultTestPipelineRolloutName + "-1": common.LabelValueResultStateSucceeded,
			},
		},
		{
			// this is the case of somebody deleting their "promoted" Pipeline during Progressive Rollout
			// we make sure that we create a new "promoted" one in its place with the latest and greatest spec, and also delete the "upgrading" one
			name:                        "Handle user deletion of promoted pipeline during Progressive",
			newPipelineSpec:             pipelineSpec, // this matches the original spec
			existingPromotedPipelineDef: nil,          // somebody just deleted their promoted pipeline
			existingUpgradePipelineDef:  defaultFailedUpgradingPipelineDef,
			initialRolloutPhase:         apiv1.PhasePending,
			initialRolloutNameCount:     2,
			initialInProgressStrategy:   &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: failedUpgradingChildStatus,
			initialPromotedChildStatus:  defaultPromotedChildStatus,
			expectedInProgressStrategy:  apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:        apiv1.PhaseDeployed,

			expectedPipelines: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestPipelineRolloutName + "-1": common.LabelValueUpgradeRecyclable,
				ctlrcommon.DefaultTestPipelineRolloutName + "-2": common.LabelValueUpgradePromoted,
			},
			expectedPipelinesResultState: map[string]common.ResultState{},
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
			if tc.existingPromotedPipelineDef != nil {
				existingPipelineDef := tc.existingPromotedPipelineDef
				existingPipelineDef.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rollout.GetObjectMeta(), apiv1.PipelineRolloutGroupVersionKind)}
				ctlrcommon.CreatePipelineInK8S(ctx, t, numaflowClientSet, existingPipelineDef)
			}

			time.Sleep(time.Second * 15) // this is for the "Clean up after progressive upgrade" test case in which there are two "promoted" Pipelines (due to a failure) and we must know which one was created most recently (therefore, we need the CreationTimestamps differentiated enough)

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

			// check results:
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

				if len(tc.expectedPipelinesResultState) > 0 {
					expectedPipelineResultState, found := tc.expectedPipelinesResultState[pipeline.Name]
					if found {
						resultState, labelFound := pipeline.Labels[common.LabelKeyProgressiveResultState]
						assert.True(t, labelFound)
						assert.Equal(t, string(expectedPipelineResultState), resultState)
					}
				}
			}
		})
	}
}

func TestProgressiveUnsupported(t *testing.T) {
	ctx := context.Background()

	// Create a test reconciler
	reconciler := &PipelineRolloutReconciler{}

	tests := []struct {
		name     string
		riders   []apiv1.PipelineRider
		expected bool
	}{
		{
			name:     "No riders",
			riders:   []apiv1.PipelineRider{},
			expected: false,
		},
		{
			name: "ConfigMap rider only",
			riders: []apiv1.PipelineRider{
				{
					Rider: apiv1.Rider{
						Progressive: true,
						Definition: runtime.RawExtension{
							Raw: createConfigMapRawExtension(t),
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "HPA rider - should return true",
			riders: []apiv1.PipelineRider{
				{
					Rider: apiv1.Rider{
						Progressive: true,
						Definition: runtime.RawExtension{
							Raw: createHPARawExtension(t),
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "Mixed riders with HPA - should return true",
			riders: []apiv1.PipelineRider{
				{
					Rider: apiv1.Rider{
						Progressive: true,
						Definition: runtime.RawExtension{
							Raw: createConfigMapRawExtension(t),
						},
					},
				},
				{
					Rider: apiv1.Rider{
						Progressive: true,
						Definition: runtime.RawExtension{
							Raw: createHPARawExtension(t),
						},
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a pipelineRollout with the test riders
			pipelineRollout := &apiv1.PipelineRollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout",
					Namespace: "test-namespace",
				},
				Spec: apiv1.PipelineRolloutSpec{
					Riders: tt.riders,
				},
			}

			result := reconciler.progressiveUnsupported(ctx, pipelineRollout)
			assert.Equal(t, tt.expected, result, "ProgressiveUnsupported should return %v for test case: %s", tt.expected, tt.name)
		})
	}
}

// Helper functions to create RawExtension objects for different resource types

func createConfigMapRawExtension(t *testing.T) []byte {
	t.Helper()
	configMap := corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-configmap",
		},
		Data: map[string]string{
			"key": "value",
		},
	}
	raw, err := json.Marshal(configMap)
	assert.NoError(t, err)
	return raw
}

func createHPARawExtension(t *testing.T) []byte {
	t.Helper()
	hpa := autoscalingv2.HorizontalPodAutoscaler{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "autoscaling/v2",
			Kind:       "HorizontalPodAutoscaler",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-hpa",
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			MinReplicas: ptr.To(int32(1)),
			MaxReplicas: 10,
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "numaflow.numaproj.io/v1alpha1",
				Kind:       "MonoVertex",
				Name:       "test-monovertex",
			},
		},
	}
	raw, err := json.Marshal(hpa)
	assert.NoError(t, err)
	return raw
}

// Technically, IsUpgradeReplacementRequired() function is in progressive.go file, but we test it here because we can take advantage of also testing code specific to the PipelineRollout controller.
func Test_PipelineRollout_IsUpgradeReplacementRequired(t *testing.T) {
	restConfig, numaflowClientSet, client, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetClientSets(restConfig))

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig2"))
	assert.NoError(t, err)

	ctx := context.Background()

	// Create a real PipelineRolloutReconciler
	scheme := scheme.Scheme
	reconciler := NewPipelineRolloutReconciler(client, scheme, nil, nil)

	// Create a PipelineSpec
	// For rollout spec, pass "{{.pipeline-name}}" as nameTemplateValue and use the ISBServiceRollout name as isbServiceName
	// For child specs, pass the evaluated name like "my-pipeline-0" as nameTemplateValue and use the resolved ISBService name
	createPipelineSpec := func(image string, nameTemplateValue string, isbServiceName string) numaflowv1.PipelineSpec {
		return numaflowv1.PipelineSpec{
			InterStepBufferServiceName: isbServiceName,
			Vertices: []numaflowv1.AbstractVertex{
				{
					Name: "in",
					Source: &numaflowv1.Source{
						Generator: &numaflowv1.GeneratorSource{
							RPU:      ptr.To(int64(5)),
							Duration: ptr.To(metav1.Duration{Duration: time.Second}),
						},
					},
				},
				{
					Name: "cat",
					UDF: &numaflowv1.UDF{
						Container: &numaflowv1.Container{
							Image: image,
							Env: []corev1.EnvVar{
								{
									Name:  "my-key",
									Value: nameTemplateValue,
								},
							},
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
		}
	}

	testCases := []struct {
		name                      string
		upgradingISBSvcExists     bool
		rolloutSpec               numaflowv1.PipelineSpec
		rolloutLabels             map[string]string
		rolloutAnnotations        map[string]string
		promotedChildSpec         numaflowv1.PipelineSpec
		promotedChildName         string
		promotedChildLabels       map[string]string
		promotedChildAnnotations  map[string]string
		upgradingChildSpec        numaflowv1.PipelineSpec
		upgradingChildName        string
		upgradingChildLabels      map[string]string
		upgradingChildAnnotations map[string]string
		upgradingPipelineStatus   *apiv1.UpgradingPipelineStatus
		expectedDiffFromUpgrading bool
		expectedDiffFromPromoted  bool
	}{

		{
			name:        "different from Upgrading only (different image)- rollout matches Promoted",
			rolloutSpec: createPipelineSpec("quay.io/numaio/numaflow-go/map-cat:v2.0.0", "{{.pipeline-name}}", ctlrcommon.DefaultTestISBSvcRolloutName),
			rolloutLabels: map[string]string{
				"my-label": "{{.pipeline-name}}",
			},
			rolloutAnnotations: map[string]string{
				"my-annotation": "{{.pipeline-name}}",
			},
			promotedChildSpec: createPipelineSpec("quay.io/numaio/numaflow-go/map-cat:v2.0.0", "my-pipeline-0", ctlrcommon.DefaultTestISBSvcRolloutName+"-0"),
			promotedChildName: "my-pipeline-0",
			promotedChildLabels: map[string]string{
				"my-label":    "my-pipeline-0",
				"extra-label": "extra-value", // this is no problem to have an extra label
			},
			promotedChildAnnotations: map[string]string{
				"my-annotation":    "my-pipeline-0",
				"extra-annotation": "extra-value", // this is no problem to have an extra annotation
			},
			upgradingChildSpec: createPipelineSpec("quay.io/numaio/numaflow-go/map-cat:v1.5.0", "my-pipeline-1", ctlrcommon.DefaultTestISBSvcRolloutName+"-0"),
			upgradingChildName: "my-pipeline-1",
			upgradingChildLabels: map[string]string{
				"my-label":    "my-pipeline-1",
				"extra-label": "extra-value", // this is no problem to have an extra label
			},
			upgradingChildAnnotations: map[string]string{
				"my-annotation":    "my-pipeline-1",
				"extra-annotation": "extra-value", // this is no problem to have an extra annotation
			},
			expectedDiffFromUpgrading: true,
			expectedDiffFromPromoted:  false,
		},
		{
			name:                  "same spec as Promoted pipeline, but a new (different) upgrading ISBService exists which Upgrading Pipeline should use",
			upgradingISBSvcExists: true,
			rolloutSpec:           createPipelineSpec("quay.io/numaio/numaflow-go/map-cat:v2.0.0", "{{.pipeline-name}}", ctlrcommon.DefaultTestISBSvcRolloutName),
			rolloutLabels: map[string]string{
				"my-label": "{{.pipeline-name}}",
			},
			rolloutAnnotations: map[string]string{
				"my-annotation": "{{.pipeline-name}}",
			},
			promotedChildSpec: createPipelineSpec("quay.io/numaio/numaflow-go/map-cat:v2.0.0", "my-pipeline-0", ctlrcommon.DefaultTestISBSvcRolloutName+"-0"),
			promotedChildName: "my-pipeline-0",
			promotedChildLabels: map[string]string{
				"my-label":    "my-pipeline-0",
				"extra-label": "extra-value", // this is no problem to have an extra label
			},
			promotedChildAnnotations: map[string]string{
				"my-annotation":    "my-pipeline-0",
				"extra-annotation": "extra-value", // this is no problem to have an extra annotation
			},
			upgradingChildSpec: createPipelineSpec("quay.io/numaio/numaflow-go/map-cat:v1.5.0", "my-pipeline-1", ctlrcommon.DefaultTestISBSvcRolloutName+"-0"),
			upgradingChildName: "my-pipeline-1",
			upgradingChildLabels: map[string]string{
				"my-label":    "my-pipeline-1",
				"extra-label": "extra-value", // this is no problem to have an extra label
			},
			upgradingChildAnnotations: map[string]string{
				"my-annotation":    "my-pipeline-1",
				"extra-annotation": "extra-value", // this is no problem to have an extra annotation
			},
			expectedDiffFromUpgrading: true, // image different
			expectedDiffFromPromoted:  true, // because we need to use a different ISBService, we consider the "target spec"to be different from the one we have
		},
		{
			name:        "different from Promoted only (different image)- rollout matches Upgrading",
			rolloutSpec: createPipelineSpec("quay.io/numaio/numaflow-go/map-cat:v1.5.0", "{{.pipeline-name}}", ctlrcommon.DefaultTestISBSvcRolloutName),
			rolloutLabels: map[string]string{
				"my-label": "{{.pipeline-name}}",
			},
			rolloutAnnotations: map[string]string{
				"my-annotation": "{{.pipeline-name}}",
			},
			promotedChildSpec: createPipelineSpec("quay.io/numaio/numaflow-go/map-cat:v1.0.0", "my-pipeline-0", ctlrcommon.DefaultTestISBSvcRolloutName+"-0"),
			promotedChildName: "my-pipeline-0",
			promotedChildLabels: map[string]string{
				"my-label":    "my-pipeline-0",
				"extra-label": "extra-value", // this is no problem to have an extra label
			},
			promotedChildAnnotations: map[string]string{
				"my-annotation":    "my-pipeline-0",
				"extra-annotation": "extra-value", // this is no problem to have an extra annotation
			},
			upgradingChildSpec: createPipelineSpec("quay.io/numaio/numaflow-go/map-cat:v1.5.0", "my-pipeline-1", ctlrcommon.DefaultTestISBSvcRolloutName+"-0"),
			upgradingChildName: "my-pipeline-1",
			upgradingChildLabels: map[string]string{
				"my-label":    "my-pipeline-1",
				"extra-label": "extra-value", // this is no problem to have an extra label
			},
			upgradingChildAnnotations: map[string]string{
				"my-annotation":    "my-pipeline-1",
				"extra-annotation": "extra-value", // this is no problem to have an extra annotation
			},
			expectedDiffFromUpgrading: false,
			expectedDiffFromPromoted:  true,
		},
		{
			name:                  "same spec as Upgrading pipeline, but a new (different) upgrading ISBService exists which Upgrading Pipeline should use",
			upgradingISBSvcExists: true,
			rolloutSpec:           createPipelineSpec("quay.io/numaio/numaflow-go/map-cat:v1.5.0", "{{.pipeline-name}}", ctlrcommon.DefaultTestISBSvcRolloutName),
			rolloutLabels: map[string]string{
				"my-label": "{{.pipeline-name}}",
			},
			rolloutAnnotations: map[string]string{
				"my-annotation": "{{.pipeline-name}}",
			},
			promotedChildSpec: createPipelineSpec("quay.io/numaio/numaflow-go/map-cat:v1.0.0", "my-pipeline-0", ctlrcommon.DefaultTestISBSvcRolloutName+"-0"),
			promotedChildName: "my-pipeline-0",
			promotedChildLabels: map[string]string{
				"my-label":    "my-pipeline-0",
				"extra-label": "extra-value", // this is no problem to have an extra label
			},
			promotedChildAnnotations: map[string]string{
				"my-annotation":    "my-pipeline-0",
				"extra-annotation": "extra-value", // this is no problem to have an extra annotation
			},
			upgradingChildSpec: createPipelineSpec("quay.io/numaio/numaflow-go/map-cat:v1.5.0", "my-pipeline-1", ctlrcommon.DefaultTestISBSvcRolloutName+"-0"),
			upgradingChildName: "my-pipeline-1",
			upgradingChildLabels: map[string]string{
				"my-label":    "my-pipeline-1",
				"extra-label": "extra-value", // this is no problem to have an extra label
			},
			upgradingChildAnnotations: map[string]string{
				"my-annotation":    "my-pipeline-1",
				"extra-annotation": "extra-value", // this is no problem to have an extra annotation
			},
			expectedDiffFromUpgrading: true, // because we need to use a different ISBService, we consider the "target spec"to be different from the one we have
			expectedDiffFromPromoted:  true, // different image
		},
		{
			name:        "different from both - required metadata not present",
			rolloutSpec: createPipelineSpec("quay.io/numaio/numaflow-go/map-cat:v1.0.0", "{{.pipeline-name}}", ctlrcommon.DefaultTestISBSvcRolloutName),
			rolloutLabels: map[string]string{
				"my-label":       "{{.pipeline-name}}",
				"required-label": "important-value",
			},
			rolloutAnnotations: map[string]string{
				"my-annotation":       "{{.pipeline-name}}",
				"required-annotation": "important-value",
			},
			promotedChildSpec: createPipelineSpec("quay.io/numaio/numaflow-go/map-cat:v1.0.0", "my-pipeline-0", ctlrcommon.DefaultTestISBSvcRolloutName+"-0"),
			promotedChildName: "my-pipeline-0",
			promotedChildLabels: map[string]string{
				"my-label":       "my-pipeline-0",
				"required-label": "important-value",
			},
			promotedChildAnnotations: map[string]string{
				"my-annotation": "my-pipeline-0",
				// Missing "required-annotation"
			},
			upgradingChildSpec: createPipelineSpec("quay.io/numaio/numaflow-go/map-cat:v1.0.0", "my-pipeline-1", ctlrcommon.DefaultTestISBSvcRolloutName+"-0"),
			upgradingChildName: "my-pipeline-1",
			upgradingChildLabels: map[string]string{
				"my-label":       "my-pipeline-1",
				"required-label": "different-important-value", // label present but value is different
			},
			upgradingChildAnnotations: map[string]string{
				"my-annotation":       "my-pipeline-1",
				"required-annotation": "important-value",
			},
			expectedDiffFromUpgrading: true,
			expectedDiffFromPromoted:  true,
		},
	}

	// Helper function to create a Pipeline child with required labels/annotations
	createPipelineChild := func(spec numaflowv1.PipelineSpec, name string, labels, annotations map[string]string, upgradeState common.UpgradeState) *unstructured.Unstructured {
		pipeline := ctlrcommon.CreateTestPipelineOfSpec(
			spec,
			name,
			numaflowv1.PipelinePhaseRunning,
			numaflowv1.Status{
				Conditions: []metav1.Condition{
					{
						Type:               string(numaflowv1.PipelineConditionDaemonServiceHealthy),
						Status:             metav1.ConditionTrue,
						Reason:             "healthy",
						LastTransitionTime: metav1.NewTime(time.Now()),
					},
				},
			},
			false,
			labels,
			annotations,
		)
		// Add required labels
		if pipeline.Labels == nil {
			pipeline.Labels = make(map[string]string)
		}
		pipeline.Labels[common.LabelKeyParentRollout] = ctlrcommon.DefaultTestPipelineRolloutName
		pipeline.Labels[common.LabelKeyUpgradeState] = string(upgradeState)
		if pipeline.Annotations == nil {
			pipeline.Annotations = make(map[string]string)
		}
		pipeline.Annotations[common.AnnotationKeyNumaflowInstanceID] = "1"

		// Convert to unstructured
		unstructMap, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(pipeline)
		return &unstructured.Unstructured{Object: unstructMap}
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up resources from previous test cases
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = numaflowClientSet.NumaflowV1alpha1().InterStepBufferServices(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = client.DeleteAllOf(ctx, &apiv1.PipelineRollout{}, &ctlrruntimeclient.DeleteAllOfOptions{ListOptions: ctlrruntimeclient.ListOptions{Namespace: ctlrcommon.DefaultTestNamespace}})
			_ = client.DeleteAllOf(ctx, &apiv1.ISBServiceRollout{}, &ctlrruntimeclient.DeleteAllOfOptions{ListOptions: ctlrruntimeclient.ListOptions{Namespace: ctlrcommon.DefaultTestNamespace}})

			// Create the base ISBServiceRollout and promoted ISBService
			isbServiceRollout := ctlrcommon.CreateISBServiceRollout(ctlrcommon.CreateDefaultISBServiceSpec("2.10.11"), nil)
			ctlrcommon.CreateISBServiceRolloutInK8S(ctx, t, client, isbServiceRollout)
			isbsvc := ctlrcommon.CreateDefaultISBService("2.10.11", numaflowv1.ISBSvcPhaseRunning, true)
			ctlrcommon.CreateISBSvcInK8S(ctx, t, numaflowClientSet, isbsvc)

			// Create upgrading ISBService if needed
			if tc.upgradingISBSvcExists {
				upgradingISBSvc := ctlrcommon.CreateTestISBService(
					"2.10.11",
					ctlrcommon.DefaultTestISBSvcRolloutName+"-1",
					numaflowv1.ISBSvcPhaseRunning,
					true,
					map[string]string{
						common.LabelKeyParentRollout: ctlrcommon.DefaultTestISBSvcRolloutName,
						common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeTrial),
					},
					map[string]string{},
				)
				ctlrcommon.CreateISBSvcInK8S(ctx, t, numaflowClientSet, upgradingISBSvc)
			}

			// Create PipelineRollout with template values
			upgradingStatus := tc.upgradingPipelineStatus
			if upgradingStatus == nil {
				upgradingStatus = &apiv1.UpgradingPipelineStatus{}
			}
			upgradingStatus.Name = tc.upgradingChildName
			pipelineRollout := ctlrcommon.CreateTestPipelineRollout(
				tc.rolloutSpec,
				map[string]string{}, // rollout annotations
				map[string]string{}, // rollout labels
				tc.rolloutAnnotations,
				tc.rolloutLabels,
				&apiv1.PipelineRolloutStatus{
					ProgressiveStatus: apiv1.PipelineProgressiveStatus{
						UpgradingPipelineStatus: upgradingStatus,
						PromotedPipelineStatus:  &apiv1.PromotedPipelineStatus{},
					},
				},
			)

			// Create Promoted and Upgrading Pipelines
			promotedChildUnstruct := createPipelineChild(
				tc.promotedChildSpec,
				tc.promotedChildName,
				tc.promotedChildLabels,
				tc.promotedChildAnnotations,
				common.LabelValueUpgradePromoted,
			)
			upgradingChildUnstruct := createPipelineChild(
				tc.upgradingChildSpec,
				tc.upgradingChildName,
				tc.upgradingChildLabels,
				tc.upgradingChildAnnotations,
				common.LabelValueUpgradeTrial,
			)

			// Call progressive.IsUpgradeReplacementRequired with the PipelineRollout controller
			differentFromUpgrading, differentFromPromoted, err := progressive.IsUpgradeReplacementRequired(
				ctx,
				pipelineRollout,
				reconciler,
				promotedChildUnstruct,
				upgradingChildUnstruct,
				client,
			)

			// Verify results
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedDiffFromUpgrading, differentFromUpgrading,
				"differentFromUpgrading mismatch")
			assert.Equal(t, tc.expectedDiffFromPromoted, differentFromPromoted,
				"differentFromPromoted mismatch")
		})
	}
}

// setRiderHashAnnotation calculates and sets the hash annotation on a rider.
// This must be done BEFORE adding runtime labels for proper comparison.
func setRiderHashAnnotation(ctx context.Context, t *testing.T, rider *unstructured.Unstructured) {
	hash, err := kubernetes.CalculateHash(ctx, *rider)
	assert.NoError(t, err, "Failed to calculate hash for rider")
	if rider.GetAnnotations() == nil {
		rider.SetAnnotations(make(map[string]string))
	}
	annotations := rider.GetAnnotations()
	annotations[common.AnnotationKeyHash] = hash
	rider.SetAnnotations(annotations)
}

func Test_SkipProgressiveAssessment(t *testing.T) {
	restConfig, _, client, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetClientSets(restConfig))

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig2"))
	assert.NoError(t, err)

	ctx := context.Background()

	recorder := record.NewFakeRecorder(64)
	r := NewPipelineRolloutReconciler(
		client,
		scheme.Scheme,
		ctlrcommon.TestCustomMetrics,
		recorder)

	// Helper function to create a pipeline spec
	createPipelineSpecWithLifecycle := func(desiredPhase string) numaflowv1.PipelineSpec {
		spec := pipelineSpec.DeepCopy()
		if desiredPhase != "" {
			spec.Lifecycle.DesiredPhase = numaflowv1.PipelinePhase(desiredPhase)
		}
		return *spec
	}

	// Helper function to create a pipeline spec with source vertex scaled to 0
	createPipelineSpecScaledToZero := func() numaflowv1.PipelineSpec {
		spec := pipelineSpec.DeepCopy()
		zero := int32(0)
		spec.Vertices[0].Scale = numaflowv1.Scale{Max: &zero}
		return *spec
	}

	testCases := []struct {
		name                   string
		pipelineSpec           numaflowv1.PipelineSpec
		forcePromoteConfigured bool
		riders                 []apiv1.PipelineRider
		expectedSkip           bool
	}{
		{
			name:                   "Pipeline can ingest data, no ForcePromote, no HPA rider - should NOT skip",
			pipelineSpec:           pipelineSpec,
			forcePromoteConfigured: false,
			riders:                 nil,
			expectedSkip:           false,
		},
		{
			name:                   "Pipeline paused - should skip",
			pipelineSpec:           createPipelineSpecWithLifecycle("Paused"),
			forcePromoteConfigured: false,
			riders:                 nil,
			expectedSkip:           true,
		},
		{
			name:                   "Pipeline source vertex scaled to 0 - should skip",
			pipelineSpec:           createPipelineSpecScaledToZero(),
			forcePromoteConfigured: false,
			riders:                 nil,
			expectedSkip:           true,
		},
		{
			name:                   "ForcePromote set to true - should skip",
			pipelineSpec:           pipelineSpec,
			forcePromoteConfigured: true,
			riders:                 nil,
			expectedSkip:           true,
		},
		{
			name:                   "HPA rider present - should skip",
			pipelineSpec:           pipelineSpec,
			forcePromoteConfigured: false,
			riders: []apiv1.PipelineRider{
				{
					Rider: apiv1.Rider{
						Progressive: true,
						Definition: runtime.RawExtension{
							Raw: createHPARawExtension(t),
						},
					},
				},
			},
			expectedSkip: true,
		},
		{
			name:                   "ConfigMap rider only (no HPA) - should NOT skip",
			pipelineSpec:           pipelineSpec,
			forcePromoteConfigured: false,
			riders: []apiv1.PipelineRider{
				{
					Rider: apiv1.Rider{
						Progressive: true,
						Definition: runtime.RawExtension{
							Raw: createConfigMapRawExtension(t),
						},
					},
				},
			},
			expectedSkip: false,
		},
		{
			name:                   "Mixed riders including HPA - should skip",
			pipelineSpec:           pipelineSpec,
			forcePromoteConfigured: false,
			riders: []apiv1.PipelineRider{
				{
					Rider: apiv1.Rider{
						Progressive: true,
						Definition: runtime.RawExtension{
							Raw: createConfigMapRawExtension(t),
						},
					},
				},
				{
					Rider: apiv1.Rider{
						Progressive: true,
						Definition: runtime.RawExtension{
							Raw: createHPARawExtension(t),
						},
					},
				},
			},
			expectedSkip: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Marshal pipelineSpec to RawExtension
			pipelineSpecBytes, err := json.Marshal(tc.pipelineSpec)
			assert.NoError(t, err)

			// Create PipelineRollout
			pipelineRollout := &apiv1.PipelineRollout{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ctlrcommon.DefaultTestNamespace,
					Name:      "test-pipeline-rollout",
				},
				Spec: apiv1.PipelineRolloutSpec{
					Pipeline: apiv1.Pipeline{
						Spec: runtime.RawExtension{Raw: pipelineSpecBytes},
					},
					Riders: tc.riders,
				},
			}

			// Set ForcePromote if needed
			if tc.forcePromoteConfigured {
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

			skip, _, err := r.SkipProgressiveAssessment(ctx, pipelineRollout)

			// Verify results
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedSkip, skip, "skip result mismatch")
		})
	}
}

// Technically, CheckRidersForDifferences() function is in progressive.go file, but we test it here because we can take advantage of also testing code specific to the PipelineRollout controller.
func Test_PipelineRollout_CheckRidersForDifferences(t *testing.T) {
	restConfig, _, client, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetClientSets(restConfig))

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig2"))
	assert.NoError(t, err)

	// Create a real PipelineRolloutReconciler
	scheme := scheme.Scheme
	reconciler := NewPipelineRolloutReconciler(client, scheme, nil, nil)

	// Define common rider definitions
	defaultVPARiderDef := map[string]interface{}{
		"apiVersion": "autoscaling.k8s.io/v1",
		"kind":       "VerticalPodAutoscaler",
		"metadata": map[string]interface{}{
			"name": "vpa",
		},
		"spec": map[string]interface{}{
			"targetRef": map[string]interface{}{
				"apiVersion": "numaflow.numaproj.io/v1alpha1",
				"kind":       "Vertex",
				"name":       "{{.pipeline-name}}-{{.vertex-name}}",
			},
			"updatePolicy": map[string]interface{}{
				"updateMode": "Auto",
			},
		},
	}

	testCases := []struct {
		name                        string
		rolloutRiderDef             map[string]interface{} // Rider definition with templates
		perVertex                   bool
		rolloutSpecVertices         []string // Vertex names in the rollout spec
		existingPipelineDefVertices []string // Vertex names in the actual existing pipeline definition (may differ from rollout)
		newPipelineName             string   // Name of the new pipeline we want to create (e.g., "my-pipeline-2")
		existingPipelineName        string   // Name of the existing pipeline (e.g., "my-pipeline-1")
		expectedRiderCount          int
		expectedRiderNames          []string
		verifyTemplateEval          func(t *testing.T, riders []riders.Rider) // Custom verification function
		existingVPADefinition       *map[string]interface{}                   // If nil, no existing VPA riders; if set, create existing VPA riders from this definition
		existingConfigMapDefinition *map[string]interface{}                   // If nil, no existing ConfigMap riders; if set, create existing ConfigMap riders from this definition
		existingChildUpgradeState   common.UpgradeState                       // Upgrade state of the existing child (e.g., "promoted", "in-progress")
		expectedDifferencesFound    bool                                      // Expected result from CheckRidersForDifferences
	}{
		{
			name:                        "Compare VPA per-vertex rider to Upgrading Pipeline riders - no differences",
			rolloutRiderDef:             defaultVPARiderDef,
			perVertex:                   true,
			rolloutSpecVertices:         []string{"in", "cat", "out"},
			existingPipelineDefVertices: []string{"in", "cat", "out"}, // Existing pipeline has the same vertices as rollout
			newPipelineName:             "my-pipeline-2",
			existingPipelineName:        "my-pipeline-1",
			expectedRiderCount:          3,
			expectedRiderNames: []string{
				"vpa-my-pipeline-2-in",
				"vpa-my-pipeline-2-cat",
				"vpa-my-pipeline-2-out",
			},
			verifyTemplateEval: func(t *testing.T, riders []riders.Rider) {
				vertexNames := []string{"in", "cat", "out"}
				for i, rider := range riders {
					// Verify the targetRef has templates evaluated
					targetRef, found, err := unstructured.NestedMap(rider.Definition.Object, "spec", "targetRef")
					assert.NoError(t, err)
					assert.True(t, found)
					expectedTargetName := fmt.Sprintf("my-pipeline-2-%s", vertexNames[i])
					assert.Equal(t, expectedTargetName, targetRef["name"],
						"TargetRef name should have templates evaluated")
				}
			},
			existingVPADefinition:       &defaultVPARiderDef,
			existingConfigMapDefinition: nil,                           // No existing ConfigMaps
			existingChildUpgradeState:   common.LabelValueUpgradeTrial, // Existing child is Upgrading
			expectedDifferencesFound:    false,                         // No differences expected since existing riders match rollout
		},
		{
			name:                        "Compare VPA per-vertex rider to Promoted Pipeline riders - no differences",
			rolloutRiderDef:             defaultVPARiderDef,
			perVertex:                   true,
			rolloutSpecVertices:         []string{"in", "cat", "out"},
			existingPipelineDefVertices: []string{"in", "cat", "out"}, // Existing pipeline has the same vertices as rollout
			newPipelineName:             "my-pipeline-1",              // New upgrading child
			existingPipelineName:        "my-pipeline-0",              // Existing promoted child
			expectedRiderCount:          3,
			expectedRiderNames: []string{
				"vpa-my-pipeline-1-in",
				"vpa-my-pipeline-1-cat",
				"vpa-my-pipeline-1-out",
			},
			verifyTemplateEval: func(t *testing.T, riders []riders.Rider) {
				vertexNames := []string{"in", "cat", "out"}
				for i, rider := range riders {
					// Verify the targetRef has templates evaluated
					targetRef, found, err := unstructured.NestedMap(rider.Definition.Object, "spec", "targetRef")
					assert.NoError(t, err)
					assert.True(t, found)
					expectedTargetName := fmt.Sprintf("my-pipeline-1-%s", vertexNames[i])
					assert.Equal(t, expectedTargetName, targetRef["name"],
						"TargetRef name should have templates evaluated")
				}
			},
			existingVPADefinition:       &defaultVPARiderDef,
			existingConfigMapDefinition: nil,                              // No existing ConfigMaps
			existingChildUpgradeState:   common.LabelValueUpgradePromoted, // Existing child is Promoted
			expectedDifferencesFound:    false,                            // No differences expected since existing riders match rollout
		},
		{
			name:                        "Compare VPA per-vertex rider to Upgrading Pipeline riders - the Vertex names have changed",
			rolloutRiderDef:             defaultVPARiderDef,
			perVertex:                   true,
			rolloutSpecVertices:         []string{"in", "cat", "out"},
			existingPipelineDefVertices: []string{"in", "transform", "out"}, // Different vertices - "cat" vs "transform"
			newPipelineName:             "my-pipeline-2",
			existingPipelineName:        "my-pipeline-1",
			expectedRiderCount:          3,
			expectedRiderNames: []string{
				"vpa-my-pipeline-2-in",
				"vpa-my-pipeline-2-cat",
				"vpa-my-pipeline-2-out",
			},
			verifyTemplateEval: func(t *testing.T, riders []riders.Rider) {
				vertexNames := []string{"in", "cat", "out"}
				for i, rider := range riders {
					// Verify the targetRef has templates evaluated
					targetRef, found, err := unstructured.NestedMap(rider.Definition.Object, "spec", "targetRef")
					assert.NoError(t, err)
					assert.True(t, found)
					expectedTargetName := fmt.Sprintf("my-pipeline-2-%s", vertexNames[i])
					assert.Equal(t, expectedTargetName, targetRef["name"],
						"TargetRef name should have templates evaluated")
				}
			},
			existingVPADefinition:       &defaultVPARiderDef,
			existingConfigMapDefinition: nil,                           // No existing ConfigMaps
			existingChildUpgradeState:   common.LabelValueUpgradeTrial, // Existing child is Upgrading
			expectedDifferencesFound:    true,                          // Differences expected - existing has "transform" rider, rollout wants "cat" rider
		},
		{
			name: "Compare ConfigMap per-Pipeline Rider to Upgrading Pipeline rider: spec has changed",
			rolloutRiderDef: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name": "my-config",
				},
				"data": map[string]interface{}{
					"pipeline-name": "{{.pipeline-name}}",
					"namespace":     "{{.pipeline-namespace}}",
				},
			},
			perVertex:                   false,
			rolloutSpecVertices:         []string{"in", "out"},
			existingPipelineDefVertices: []string{"in", "cat", "out"}, // Existing pipeline has different vertices than rollout (doesn't affect non-perVertex riders)
			newPipelineName:             "my-pipeline-2",
			existingPipelineName:        "my-pipeline-1",
			expectedRiderCount:          1,
			expectedRiderNames: []string{
				"my-config-my-pipeline-2",
			},
			verifyTemplateEval: func(t *testing.T, riders []riders.Rider) {
				// Verify data field has templates evaluated
				data, found, err := unstructured.NestedStringMap(riders[0].Definition.Object, "data")
				assert.NoError(t, err)
				assert.True(t, found)
				assert.Equal(t, "my-pipeline-2", data["pipeline-name"])
				assert.Equal(t, ctlrcommon.DefaultTestNamespace, data["namespace"])
			},
			existingVPADefinition: nil, // No existing VPAs
			existingConfigMapDefinition: &map[string]interface{}{
				// Existing ConfigMap has different data than rollout
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name": "my-config",
				},
				"data": map[string]interface{}{
					"pipeline-name": "old-value",               // Different from rollout ("{{.pipeline-name}}")
					"namespace":     "{{.pipeline-namespace}}", // Same as rollout
				},
			},
			existingChildUpgradeState: common.LabelValueUpgradeTrial, // Existing child is Upgrading
			expectedDifferencesFound:  true,                          // Differences expected since existing ConfigMap has different data
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create rider definition
			riderDefRaw, err := json.Marshal(tc.rolloutRiderDef)
			assert.NoError(t, err)

			// Create PipelineRollout with the rider (using rollout spec vertices)
			rolloutVertices := make([]numaflowv1.AbstractVertex, len(tc.rolloutSpecVertices))
			for i, name := range tc.rolloutSpecVertices {
				rolloutVertices[i] = numaflowv1.AbstractVertex{Name: name}
			}
			pipelineSpec := numaflowv1.PipelineSpec{
				InterStepBufferServiceName: "my-isbsvc-0",
				Vertices:                   rolloutVertices,
			}
			pipelineSpecRaw, err := json.Marshal(pipelineSpec)
			assert.NoError(t, err)

			pipelineRollout := &apiv1.PipelineRollout{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ctlrcommon.DefaultTestNamespace,
					Name:      ctlrcommon.DefaultTestPipelineRolloutName,
				},
				Spec: apiv1.PipelineRolloutSpec{
					Pipeline: apiv1.Pipeline{
						Spec: runtime.RawExtension{Raw: pipelineSpecRaw},
					},
					Riders: []apiv1.PipelineRider{
						{
							Rider: apiv1.Rider{
								Definition: runtime.RawExtension{Raw: riderDefRaw},
							},
							PerVertex: tc.perVertex,
						},
					},
				},
				Status: apiv1.PipelineRolloutStatus{
					ProgressiveStatus: apiv1.PipelineProgressiveStatus{
						UpgradingPipelineStatus: &apiv1.UpgradingPipelineStatus{},
						PromotedPipelineStatus:  &apiv1.PromotedPipelineStatus{},
					},
				},
			}

			// Create new pipeline definition from rollout spec (what we want to create)
			newPipelineDefVertices := make([]interface{}, len(tc.rolloutSpecVertices))
			for i, name := range tc.rolloutSpecVertices {
				newPipelineDefVertices[i] = map[string]interface{}{"name": name}
			}
			newPipelineDef := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"name":      tc.newPipelineName,
						"namespace": ctlrcommon.DefaultTestNamespace,
					},
					"spec": map[string]interface{}{
						"interStepBufferServiceName": "my-isbsvc-0",
						"vertices":                   newPipelineDefVertices,
					},
				},
			}

			// Create existing pipeline definition (which may have different vertices than the rollout spec)
			existingPipelineDefVertices := make([]interface{}, len(tc.existingPipelineDefVertices))
			for i, name := range tc.existingPipelineDefVertices {
				existingPipelineDefVertices[i] = map[string]interface{}{"name": name}
			}
			existingPipelineDef := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"name":      tc.existingPipelineName,
						"namespace": ctlrcommon.DefaultTestNamespace,
					},
					"spec": map[string]interface{}{
						"interStepBufferServiceName": "my-isbsvc-0",
						"vertices":                   existingPipelineDefVertices,
					},
				},
			}

			// Call GetDesiredRiders with the new pipeline definition
			desiredRiders, err := reconciler.GetDesiredRiders(pipelineRollout, tc.newPipelineName, newPipelineDef)
			assert.NoError(t, err)

			// Verify rider count
			assert.Equal(t, tc.expectedRiderCount, len(desiredRiders),
				"Should create expected number of riders")

			// Verify rider names
			for i, expectedName := range tc.expectedRiderNames {
				assert.Equal(t, expectedName, desiredRiders[i].Definition.GetName(),
					"Rider name should match expected name")
			}

			// Run custom verification if provided
			if tc.verifyTemplateEval != nil {
				tc.verifyTemplateEval(t, desiredRiders)
			}

			// Test CheckRidersForDifferences
			ctx := context.Background()

			// Create existing riders in the cluster based on custom definitions
			var existingRiders []unstructured.Unstructured
			var riderStatusList []apiv1.RiderStatus

			riderKind, _ := tc.rolloutRiderDef["kind"].(string)
			var existingRiderDefinition *map[string]interface{}
			var riderNamePrefix string
			if riderKind == "VerticalPodAutoscaler" {
				existingRiderDefinition = tc.existingVPADefinition
			} else if riderKind == "ConfigMap" {
				existingRiderDefinition = tc.existingConfigMapDefinition
			}

			// Extract the base name from the rollout rider definition
			if metadata, ok := tc.rolloutRiderDef["metadata"].(map[string]interface{}); ok {
				if name, ok := metadata["name"].(string); ok {
					riderNamePrefix = name
				}
			}

			// Create existing riders
			if existingRiderDefinition != nil {
				// Determine how many riders to create based on whether it's per-vertex
				ridersToCreate := []string{tc.existingPipelineName} // Default: one rider (per-pipeline)
				if tc.perVertex {
					// Per-vertex: create one rider per vertex in existing pipeline
					ridersToCreate = tc.existingPipelineDefVertices
				}

				for _, riderIdentifier := range ridersToCreate {
					// Use custom definition and evaluate templates
					templateArgs := map[string]interface{}{
						common.TemplatePipelineName:      tc.existingPipelineName,
						common.TemplatePipelineNamespace: ctlrcommon.DefaultTestNamespace,
					}
					if tc.perVertex {
						templateArgs[common.TemplateVertexName] = riderIdentifier
					}

					resolvedMap, err := util.ResolveTemplatedSpec(*existingRiderDefinition, templateArgs)
					assert.NoError(t, err)
					existingRider := &unstructured.Unstructured{Object: resolvedMap}
					existingRider.SetNamespace(ctlrcommon.DefaultTestNamespace)

					// Generate the rider name
					if tc.perVertex {
						existingRider.SetName(fmt.Sprintf("%s-%s-%s", riderNamePrefix, tc.existingPipelineName, riderIdentifier))
					} else {
						existingRider.SetName(fmt.Sprintf("%s-%s", riderNamePrefix, tc.existingPipelineName))
					}

					// Set hash annotation for the existing rider (matches production behavior)
					setRiderHashAnnotation(ctx, t, existingRider)

					// Create the rider in the cluster
					err = client.Create(ctx, existingRider)
					assert.NoError(t, err, "Failed to create existing rider")
					existingRiders = append(existingRiders, *existingRider)

					// Add to rider status list
					riderStatusList = append(riderStatusList, apiv1.RiderStatus{
						GroupVersionKind: kubernetes.SchemaGVKToMetaGVK(existingRider.GroupVersionKind()),
						Name:             existingRider.GetName(),
					})
				}
			}

			// Update the PipelineRollout status to include the rider list
			// Set riders in the appropriate status based on whether existing is Upgrading or Promoted
			if tc.existingChildUpgradeState == common.LabelValueUpgradeTrial {
				pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus.Riders = riderStatusList
			} else {
				// For promoted child, riders are stored at the top-level Status
				pipelineRollout.Status.Riders = riderStatusList
			}

			// Call CheckRidersForDifferences
			// Compare new desired riders (from newPipelineDef) against existing riders (associated with existingPipelineDef)
			differencesFound, err := progressive.CheckRidersForDifferences(
				ctx,
				reconciler,
				pipelineRollout,
				existingPipelineDef,          // existing child definition
				tc.existingChildUpgradeState, // upgrade state of existing child
				newPipelineDef,               // new upgrading child definition
			)
			assert.NoError(t, err, "CheckRidersForDifferences should not return an error")
			assert.Equal(t, tc.expectedDifferencesFound, differencesFound,
				"CheckRidersForDifferences result mismatch")

			// Clean up: delete the existing riders
			for _, rider := range existingRiders {
				err = client.Delete(ctx, &rider)
				assert.NoError(t, err, "Failed to delete rider")
			}
		})
	}
}
