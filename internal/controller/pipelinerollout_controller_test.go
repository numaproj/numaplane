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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"
)

var (
	defaultPipelineRolloutName = "pipelinerollout-test"
	defaultPipelineName        = defaultPipelineRolloutName + "-0"
	newPipelineName            = defaultPipelineRolloutName + "-1"

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

var _ = Describe("PipelineRollout Controller", Ordered, func() {

	ctx := context.Background()

	pipelineSpecRaw, err := json.Marshal(pipelineSpec)
	Expect(err).ToNot(HaveOccurred())

	pipelineRollout := &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: defaultNamespace,
			Name:      defaultPipelineRolloutName,
		},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: apiv1.Pipeline{
				Spec: runtime.RawExtension{
					Raw: pipelineSpecRaw,
				},
			},
		},
	}

	Context("When applying a PipelineRollout spec", func() {
		It("Should create the PipelineRollout if it does not exist or it should update existing PipelineRollout and Numaflow Pipeline", func() {
			Expect(k8sClient.Create(ctx, pipelineRollout)).Should(Succeed())

			resourceLookupKey := types.NamespacedName{Name: defaultPipelineRolloutName, Namespace: defaultNamespace}
			createdResource := &apiv1.PipelineRollout{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, createdResource)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			createdPipelineRolloutPipelineSpec := numaflowv1.PipelineSpec{}
			Expect(json.Unmarshal(createdResource.Spec.Pipeline.Spec.Raw, &createdPipelineRolloutPipelineSpec)).ToNot(HaveOccurred())

			By("Verifying the content of the pipeline spec field")
			Expect(createdPipelineRolloutPipelineSpec).Should(Equal(pipelineSpec))
		})

		It("Should create a Numaflow Pipeline", func() {
			createdResource := &numaflowv1.Pipeline{}
			resourceLookupKey := types.NamespacedName{Name: defaultPipelineName, Namespace: defaultNamespace}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, createdResource)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			By("Verifying the content of the pipeline spec")
			Expect(createdResource.Spec).Should(Equal(pipelineSpec))

			By("Verifying the label of the pipeline")
			Expect(createdResource.Labels[common.LabelKeyParentRollout]).Should(Equal(pipelineRollout.Name))
			Expect(createdResource.Labels[common.LabelKeyUpgradeState]).Should(Equal(string(common.LabelValueUpgradePromoted)))
		})

		It("Should have the PipelineRollout Status Phase has Deployed and ObservedGeneration matching Generation", func() {
			verifyStatusPhase(ctx, apiv1.PipelineRolloutGroupVersionKind, defaultNamespace, defaultPipelineRolloutName, apiv1.PhaseDeployed)
		})

		It("Should have the metrics updated", func() {
			By("Verifying the PipelineRollout metric")
			Expect(testutil.ToFloat64(customMetrics.PipelinesRunning.WithLabelValues(defaultNamespace))).Should(Equal(float64(1)))
			Expect(testutil.ToFloat64(customMetrics.PipelinesSynced.WithLabelValues())).Should(BeNumerically(">", 1))
		})

		Context("When applying a PipelineRollout spec where the Pipeline with same name already exists", func() {
			It("Should be automatically create another one with different naming", func() {
				pipelineRolloutName := "test"
				pipelineName := pipelineRolloutName + "-0"
				Expect(k8sClient.Create(ctx, &numaflowv1.Pipeline{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: defaultNamespace,
						Name:      pipelineName,
					},
					Spec: pipelineSpec,
				})).Should(Succeed())
				Expect(k8sClient.Create(ctx, &apiv1.PipelineRollout{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: defaultNamespace,
						Name:      pipelineRolloutName,
					},
					Spec: apiv1.PipelineRolloutSpec{
						Pipeline: apiv1.Pipeline{
							Spec: runtime.RawExtension{
								Raw: pipelineSpecRaw,
							},
						},
					},
				})).Should(Succeed())
				time.Sleep(5 * time.Second)

				newPipelineName := pipelineRolloutName + "-1"
				resourceLookupKey := types.NamespacedName{Name: newPipelineName, Namespace: defaultNamespace}
				createdResource := &numaflowv1.Pipeline{}
				Eventually(func() bool {
					err := k8sClient.Get(ctx, resourceLookupKey, createdResource)
					return err == nil
				}, timeout, interval).Should(BeTrue())

				// Still expect the intermediate failed state trigger the metric
				Expect(testutil.ToFloat64(customMetrics.PipelinesSyncFailed.WithLabelValues())).Should(BeNumerically(">", 1))
			})
		})

		It("Should update the PipelineRollout and Numaflow Pipeline", func() {
			By("updating the PipelineRollout")

			resourceLookupKey := types.NamespacedName{Name: defaultPipelineRolloutName, Namespace: defaultNamespace}
			currentPipelineRollout := &apiv1.PipelineRollout{}
			Expect(k8sClient.Get(ctx, resourceLookupKey, currentPipelineRollout)).ToNot(HaveOccurred())

			pipelineSpec.InterStepBufferServiceName = "my-isbsvc-updated"
			pipelineSpecRaw, err := json.Marshal(pipelineSpec)
			Expect(err).ToNot(HaveOccurred())

			currentPipelineRollout.Spec.Pipeline.Spec.Raw = pipelineSpecRaw

			Expect(k8sClient.Update(ctx, currentPipelineRollout)).ToNot(HaveOccurred())

			By("Verifying the content of the pipeline field of the PipelineRollout")
			Eventually(func() (numaflowv1.PipelineSpec, error) {
				updatedResource := &apiv1.PipelineRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, updatedResource)
				if err != nil {
					return numaflowv1.PipelineSpec{}, err
				}

				updatedPipelineRolloutPipelineSpec := numaflowv1.PipelineSpec{}
				Expect(json.Unmarshal(updatedResource.Spec.Pipeline.Spec.Raw, &updatedPipelineRolloutPipelineSpec)).ToNot(HaveOccurred())

				return updatedPipelineRolloutPipelineSpec, nil
			}, timeout, interval).Should(Equal(pipelineSpec))

			By("Verifying the content of the spec field of the Numaflow Pipeline")
			Eventually(func() (numaflowv1.PipelineSpec, error) {
				resourceLookupKey := types.NamespacedName{Name: defaultPipelineName, Namespace: defaultNamespace}
				updatedChildResource := &numaflowv1.Pipeline{}
				err := k8sClient.Get(ctx, resourceLookupKey, updatedChildResource)
				if err != nil {
					return numaflowv1.PipelineSpec{}, err
				}
				return updatedChildResource.Spec, nil
			}, timeout, interval).Should(Equal(pipelineSpec))

			By("Verifying that the PipelineRollout Status Phase is Deployed and ObservedGeneration matches Generation")
			verifyStatusPhase(ctx, apiv1.PipelineRolloutGroupVersionKind, defaultNamespace, defaultPipelineRolloutName, apiv1.PhaseDeployed)

		})

		It("Should auto heal the Numaflow Pipeline with the PipelineRollout pipeline spec when the Numaflow Pipeline spec is changed", func() {
			By("updating the Numaflow Pipeline and verifying the changed field is the same as the original and not the modified version")
			verifyAutoHealing(ctx, numaflowv1.PipelineGroupVersionKind, defaultNamespace, defaultPipelineName, "spec.interStepBufferServiceName", "someotherisbsname")
		})

		It("Should delete the PipelineRollout and Numaflow Pipeline", func() {
			Expect(k8sClient.Delete(ctx, &apiv1.PipelineRollout{
				ObjectMeta: pipelineRollout.ObjectMeta,
			})).Should(Succeed())

			deletedResource := &apiv1.PipelineRollout{}
			Eventually(func() bool {
				resourceLookupKey := types.NamespacedName{Name: defaultPipelineRolloutName, Namespace: defaultNamespace}
				err := k8sClient.Get(ctx, resourceLookupKey, deletedResource)
				return errors.IsNotFound(err)
			}, timeout, interval).Should(BeTrue())

			deletingChildResource := &numaflowv1.Pipeline{}
			Eventually(func() bool {
				resourceLookupKey := types.NamespacedName{Name: defaultPipelineName, Namespace: defaultNamespace}
				err := k8sClient.Get(ctx, resourceLookupKey, deletingChildResource)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(deletingChildResource.OwnerReferences).Should(HaveLen(1))
			Expect(deletedResource.UID).Should(Equal(deletingChildResource.OwnerReferences[0].UID))
		})
	})

	Context("When applying an invalid PipelineRollout spec", func() {
		It("Should not create the PipelineRollout", func() {
			Expect(k8sClient.Create(ctx, &apiv1.PipelineRollout{
				Spec: pipelineRollout.Spec,
			})).ShouldNot(Succeed())

			Expect(k8sClient.Create(ctx, &apiv1.PipelineRollout{
				ObjectMeta: pipelineRollout.ObjectMeta,
			})).ShouldNot(Succeed())

			Expect(k8sClient.Create(ctx, &apiv1.PipelineRollout{
				ObjectMeta: pipelineRollout.ObjectMeta,
				Spec:       apiv1.PipelineRolloutSpec{},
			})).ShouldNot(Succeed())
		})
	})

})

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

	restConfig, _, numaplaneClient, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)

	recorder := record.NewFakeRecorder(64)

	r := NewPipelineRolloutReconciler(
		numaplaneClient,
		scheme.Scheme,
		restConfig,
		customMetrics,
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
			needsUpdating, err := r.childNeedsUpdating(context.Background(), obj1, obj2)
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
					Namespace: defaultNamespace,
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

func createPipelineRollout(isbsvcSpec numaflowv1.PipelineSpec, annotations map[string]string, labels map[string]string) *apiv1.PipelineRollout {
	pipelineRaw, _ := json.Marshal(isbsvcSpec)
	return &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         defaultNamespace,
			Name:              defaultPipelineRolloutName,
			UID:               "uid",
			CreationTimestamp: metav1.NewTime(time.Now()),
			Generation:        1,
			Annotations:       annotations,
			Labels:            labels,
		},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: apiv1.Pipeline{
				Spec: runtime.RawExtension{
					Raw: pipelineRaw,
				},
			},
		},
	}
}

func createPipelineOfSpec(
	spec numaflowv1.PipelineSpec,
	name string,
	phase numaflowv1.PipelinePhase,
	innerStatus numaflowv1.Status,
	drainedOnPause bool,
	labels map[string]string,
) *numaflowv1.Pipeline {
	status := numaflowv1.PipelineStatus{
		Phase:          phase,
		Status:         innerStatus,
		DrainedOnPause: drainedOnPause,
	}
	return &numaflowv1.Pipeline{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pipeline",
			APIVersion: "numaflow.numaproj.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: defaultNamespace,
			Labels:    labels,
		},
		Spec:   spec,
		Status: status,
	}

}

func createDefaultPipeline(phase numaflowv1.PipelinePhase) *numaflowv1.Pipeline {
	return createPipelineOfSpec(pipelineSpec, defaultPipelineName, phase, numaflowv1.Status{}, false, map[string]string{})
}

func createPipeline(phase numaflowv1.PipelinePhase, status numaflowv1.Status, drainedOnPause bool, labels map[string]string) *numaflowv1.Pipeline {
	return createPipelineOfSpec(pipelineSpec, defaultPipelineName, phase, status, drainedOnPause, labels)
}

func pipelineWithDesiredPhase(spec numaflowv1.PipelineSpec, phase numaflowv1.PipelinePhase) numaflowv1.PipelineSpec {
	spec.Lifecycle.DesiredPhase = phase
	return spec
}

// process an existing pipeline
// in this test, the user preferred strategy is PPND
func Test_processExistingPipeline_PPND(t *testing.T) {
	restConfig, numaflowClientSet, numaplaneClient, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)

	config.GetConfigManagerInstance().UpdateUSDEConfig(config.USDEConfig{
		DefaultUpgradeStrategy:    config.PPNDStrategyID,
		PipelineSpecExcludedPaths: []string{"watermark", "lifecycle"},
	})

	ctx := context.Background()

	// other tests may call this, but it fails if called more than once
	if customMetrics == nil {
		customMetrics = metrics.RegisterCustomMetrics()
	}

	recorder := record.NewFakeRecorder(64)

	falseValue := false
	trueValue := true
	ppndUpgradeStrategy := apiv1.UpgradeStrategyPPND

	r := NewPipelineRolloutReconciler(
		numaplaneClient,
		scheme.Scheme,
		restConfig,
		customMetrics,
		recorder)

	testCases := []struct {
		name                           string
		newPipelineSpec                numaflowv1.PipelineSpec
		pipelineRolloutAnnotations     map[string]string
		existingPipelineDef            numaflowv1.Pipeline
		initialRolloutPhase            apiv1.Phase
		initialInProgressStrategy      *apiv1.UpgradeStrategy
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
			existingPipelineDef:            *createDefaultPipeline(numaflowv1.PipelinePhaseRunning),
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialInProgressStrategy:      nil,
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
			existingPipelineDef:            *createDefaultPipeline(numaflowv1.PipelinePhaseRunning),
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialInProgressStrategy:      nil,
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
			existingPipelineDef:            *createDefaultPipeline(numaflowv1.PipelinePhaseRunning),
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialInProgressStrategy:      nil,
			numaflowControllerPauseRequest: &falseValue,
			isbServicePauseRequest:         &falseValue,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyPPND,
			expectedRolloutPhase:           apiv1.PhasePending,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return reflect.DeepEqual(pipelineWithDesiredPhase(pipelineSpec, numaflowv1.PipelinePhasePaused), spec)
			},
		},
		{
			name:                           "external pause request at the same time as a DirectApply change",
			newPipelineSpec:                pipelineSpecWithWatermarkDisabled,
			existingPipelineDef:            *createDefaultPipeline(numaflowv1.PipelinePhaseRunning),
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialInProgressStrategy:      nil,
			numaflowControllerPauseRequest: &trueValue,
			isbServicePauseRequest:         &falseValue,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyPPND,
			expectedRolloutPhase:           apiv1.PhasePending,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return reflect.DeepEqual(pipelineWithDesiredPhase(pipelineSpec, numaflowv1.PipelinePhasePaused), spec)
			},
		},
		{
			name:                           "user sets desiredPhase=Paused",
			newPipelineSpec:                pipelineWithDesiredPhase(pipelineSpec, numaflowv1.PipelinePhasePaused),
			existingPipelineDef:            *createDefaultPipeline(numaflowv1.PipelinePhaseRunning),
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialInProgressStrategy:      nil,
			numaflowControllerPauseRequest: &falseValue,
			isbServicePauseRequest:         &falseValue,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:           apiv1.PhaseDeployed,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return reflect.DeepEqual(pipelineWithDesiredPhase(pipelineSpec, numaflowv1.PipelinePhasePaused), spec)
			},
		},
		{
			name:            "user sets desiredPhase=Running",
			newPipelineSpec: pipelineWithDesiredPhase(pipelineSpec, numaflowv1.PipelinePhaseRunning),
			existingPipelineDef: *createPipelineOfSpec(
				pipelineWithDesiredPhase(pipelineSpec, numaflowv1.PipelinePhaseRunning),
				defaultPipelineName, numaflowv1.PipelinePhasePaused, numaflowv1.Status{},
				false, map[string]string{}),
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialInProgressStrategy:      nil,
			numaflowControllerPauseRequest: &falseValue,
			isbServicePauseRequest:         &falseValue,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:           apiv1.PhaseDeployed,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return reflect.DeepEqual(pipelineWithDesiredPhase(pipelineSpec, numaflowv1.PipelinePhaseRunning), spec)
			},
		},
		{
			name:                           "PPND in progress, spec not yet applied, pipeline not paused",
			newPipelineSpec:                pipelineSpecWithTopologyChange,
			existingPipelineDef:            *createDefaultPipeline(numaflowv1.PipelinePhaseRunning),
			initialRolloutPhase:            apiv1.PhasePending,
			initialInProgressStrategy:      &ppndUpgradeStrategy,
			numaflowControllerPauseRequest: &falseValue,
			isbServicePauseRequest:         &falseValue,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyPPND,
			expectedRolloutPhase:           apiv1.PhasePending,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return reflect.DeepEqual(pipelineWithDesiredPhase(pipelineSpec, numaflowv1.PipelinePhasePaused), spec)
			},
		},
		{
			name:            "PPND in progress, spec applied",
			newPipelineSpec: pipelineSpecWithTopologyChange,
			existingPipelineDef: *createPipelineOfSpec(
				pipelineWithDesiredPhase(pipelineSpecWithTopologyChange, numaflowv1.PipelinePhasePaused),
				defaultPipelineName, numaflowv1.PipelinePhasePaused, numaflowv1.Status{},
				false, map[string]string{}),
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialInProgressStrategy:      &ppndUpgradeStrategy,
			numaflowControllerPauseRequest: &falseValue,
			isbServicePauseRequest:         &falseValue,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:           apiv1.PhaseDeployed,
			expectedPipelineSpecResult: func(spec numaflowv1.PipelineSpec) bool {
				return reflect.DeepEqual(pipelineWithDesiredPhase(pipelineSpecWithTopologyChange, numaflowv1.PipelinePhaseRunning), spec)
			},
		},
		{
			name:                           "Pipeline stuck pausing, allow-data-loss annotation applied",
			newPipelineSpec:                pipelineSpecWithTopologyChange,
			pipelineRolloutAnnotations:     map[string]string{common.LabelKeyAllowDataLoss: "true"},
			existingPipelineDef:            *createDefaultPipeline(numaflowv1.PipelinePhasePausing),
			initialRolloutPhase:            apiv1.PhasePending,
			initialInProgressStrategy:      &ppndUpgradeStrategy,
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
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).Delete(ctx, defaultPipelineName, metav1.DeleteOptions{})

			pipelineList, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Len(t, pipelineList.Items, 0)

			if tc.pipelineRolloutAnnotations == nil {
				tc.pipelineRolloutAnnotations = map[string]string{}
			}

			rollout := createPipelineRollout(tc.newPipelineSpec, tc.pipelineRolloutAnnotations, map[string]string{})
			_ = numaplaneClient.Delete(ctx, rollout)

			rollout.Status.Phase = tc.initialRolloutPhase
			if tc.initialInProgressStrategy != nil {
				rollout.Status.UpgradeInProgress = *tc.initialInProgressStrategy
				r.inProgressStrategyMgr.store.setStrategy(k8stypes.NamespacedName{Namespace: defaultNamespace, Name: defaultPipelineRolloutName}, *tc.initialInProgressStrategy)
			} else {
				rollout.Status.UpgradeInProgress = apiv1.UpgradeStrategyNoOp
				r.inProgressStrategyMgr.store.setStrategy(k8stypes.NamespacedName{Namespace: defaultNamespace, Name: defaultPipelineRolloutName}, apiv1.UpgradeStrategyNoOp)
			}

			// the Reconcile() function does this, so we need to do it before calling reconcile() as well
			rollout.Status.Init(rollout.Generation)

			err = numaplaneClient.Create(ctx, rollout)
			assert.NoError(t, err)

			// create the already-existing Pipeline in Kubernetes
			// this updates everything but the Status subresource
			existingPipelineDef := &tc.existingPipelineDef
			existingPipelineDef.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rollout.GetObjectMeta(), apiv1.PipelineRolloutGroupVersionKind)}
			createPipelineInK8S(ctx, t, numaflowClientSet, &tc.existingPipelineDef)

			// external pause requests
			GetPauseModule().pauseRequests = map[string]*bool{}
			if tc.numaflowControllerPauseRequest != nil {
				GetPauseModule().pauseRequests[GetPauseModule().getNumaflowControllerKey(defaultNamespace)] = tc.numaflowControllerPauseRequest
			}
			if tc.isbServicePauseRequest != nil {
				GetPauseModule().pauseRequests[GetPauseModule().getISBServiceKey(defaultNamespace, "my-isbsvc")] = tc.isbServicePauseRequest
			}

			_, err = r.reconcile(context.Background(), rollout, time.Now())
			assert.NoError(t, err)

			////// check results:
			// Check Phase of Rollout:
			assert.Equal(t, tc.expectedRolloutPhase, rollout.Status.Phase)
			// Check In-Progress Strategy
			assert.Equal(t, tc.expectedInProgressStrategy, rollout.Status.UpgradeInProgress)

			// Check Pipeline spec
			resultPipeline, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).Get(ctx, defaultPipelineName, metav1.GetOptions{})
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

	config.GetConfigManagerInstance().UpdateUSDEConfig(config.USDEConfig{
		DefaultUpgradeStrategy:    config.ProgressiveStrategyID,
		PipelineSpecExcludedPaths: []string{"watermark", "lifecycle"},
	})
	ctx := context.Background()

	// other tests may call this, but it fails if called more than once
	if customMetrics == nil {
		customMetrics = metrics.RegisterCustomMetrics()
	}

	recorder := record.NewFakeRecorder(64)

	r := NewPipelineRolloutReconciler(
		numaplaneClient,
		scheme.Scheme,
		restConfig,
		customMetrics,
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
					common.LabelKeyParentRollout: defaultPipelineRolloutName,
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
					common.LabelKeyParentRollout: defaultPipelineRolloutName,
				}),
			existingUpgradePipelineDef: createPipelineOfSpec(
				pipelineSpecWithTopologyChange, newPipelineName,
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
					common.LabelKeyParentRollout: defaultPipelineRolloutName,
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
					common.LabelKeyParentRollout: defaultPipelineRolloutName,
				}),
			existingUpgradePipelineDef: createPipelineOfSpec(
				pipelineSpecWithTopologyChange, newPipelineName,
				numaflowv1.PipelinePhaseRunning,
				numaflowv1.Status{},
				false,
				map[string]string{
					common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
					common.LabelKeyParentRollout: defaultPipelineRolloutName,
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
					common.LabelKeyParentRollout: defaultPipelineRolloutName,
				}),
			existingUpgradePipelineDef: createPipelineOfSpec(
				pipelineSpecWithTopologyChange, newPipelineName,
				numaflowv1.PipelinePhaseRunning,
				numaflowv1.Status{},
				false,
				map[string]string{
					common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
					common.LabelKeyParentRollout: defaultPipelineRolloutName,
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
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).Delete(ctx, defaultPipelineName, metav1.DeleteOptions{})
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).Delete(ctx, newPipelineName, metav1.DeleteOptions{})

			pipelineList, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Len(t, pipelineList.Items, 0)

			rollout := createPipelineRollout(tc.newPipelineSpec, map[string]string{}, map[string]string{})
			_ = numaplaneClient.Delete(ctx, rollout)

			rollout.Status.Phase = tc.initialRolloutPhase
			if rollout.Status.NameCount == nil {
				rollout.Status.NameCount = new(int32)
				*rollout.Status.NameCount++
			}
			if tc.initialInProgressStrategy != nil {
				rollout.Status.UpgradeInProgress = *tc.initialInProgressStrategy
				r.inProgressStrategyMgr.store.setStrategy(k8stypes.NamespacedName{Namespace: defaultNamespace, Name: defaultPipelineRolloutName}, *tc.initialInProgressStrategy)
			} else {
				rollout.Status.UpgradeInProgress = apiv1.UpgradeStrategyNoOp
				r.inProgressStrategyMgr.store.setStrategy(k8stypes.NamespacedName{Namespace: defaultNamespace, Name: defaultPipelineRolloutName}, apiv1.UpgradeStrategyNoOp)
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
			pipeline, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).Create(ctx, existingPipelineDef, metav1.CreateOptions{})
			assert.NoError(t, err)
			// update Status subresource
			pipeline.Status = tc.existingPipelineDef.Status
			_, err = numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).UpdateStatus(ctx, pipeline, metav1.UpdateOptions{})
			assert.NoError(t, err)

			if tc.existingUpgradePipelineDef != nil {
				existingUpgradePipelineDef := tc.existingUpgradePipelineDef
				existingUpgradePipelineDef.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rollout.GetObjectMeta(), apiv1.PipelineRolloutGroupVersionKind)}
				pipeline, err = numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).Create(ctx, existingUpgradePipelineDef, metav1.CreateOptions{})
				assert.NoError(t, err)

				// update Status subresource
				pipeline.Status = tc.existingUpgradePipelineDef.Status
				_, err = numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).UpdateStatus(ctx, pipeline, metav1.UpdateOptions{})
				assert.NoError(t, err)
			}

			_, err = r.reconcile(context.Background(), rollout, time.Now())
			assert.NoError(t, err)

			////// check results:
			// Check Phase of Rollout:
			assert.Equal(t, tc.expectedRolloutPhase, rollout.Status.Phase)
			// Check In-Progress Strategy
			assert.Equal(t, tc.expectedInProgressStrategy, rollout.Status.UpgradeInProgress)

			// Check the new Pipeline spec
			resultPipeline, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).Get(ctx, newPipelineName, metav1.GetOptions{})
			assert.NoError(t, err)
			assert.NotNil(t, resultPipeline)
			assert.True(t, tc.expectedPipelineSpecResult(resultPipeline.Spec), "result spec", fmt.Sprint(resultPipeline.Spec))

			// Check the existing Pipeline state
			resultExistingPipeline, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).Get(ctx, defaultPipelineName, metav1.GetOptions{})
			if tc.expectedExistingPipelineDeleted {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, resultExistingPipeline)
			}

			if tc.expectedExistingPipelineDesiredPhase != nil {
				assert.True(t,
					reflect.DeepEqual(pipelineWithDesiredPhase(pipelineSpec, *tc.expectedExistingPipelineDesiredPhase), resultExistingPipeline.Spec),
					"result pipeline phase", fmt.Sprint(resultExistingPipeline.Status.Phase))
			}
		})
	}
}
