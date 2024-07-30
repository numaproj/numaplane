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
	"strings"
	"time"

	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	numaflowversioned "github.com/numaproj/numaflow/pkg/client/clientset/versioned"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	numaplaneversioned "github.com/numaproj/numaplane/pkg/client/clientset/versioned"
)

const (
	testNamespace           = "default"
	testPipelineRolloutName = "pipelinerollout-test"
	testPipelineName        = testPipelineRolloutName
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

	testPipeline = numaflowv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testPipelineName,
			Namespace: testNamespace,
		},
		Spec: pipelineSpec,
	}
)

func createPipelineRollout(pipelineSpec *numaflowv1.PipelineSpec) *apiv1.PipelineRollout {

	pipelineSpecRaw, err := json.Marshal(pipelineSpec)
	if err != nil {
		panic(err)
	}

	return &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testPipelineRolloutName,
		},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: apiv1.Pipeline{
				Spec: runtime.RawExtension{
					Raw: pipelineSpecRaw,
				},
			},
		},
	}
}

var _ = Describe("PipelineRollout Controller", Ordered, func() {
	const (
		namespace           = "default"
		pipelineRolloutName = "pipelinerollout-test"
	)

	ctx := context.Background()

	pipelineRollout := createPipelineRollout(&pipelineSpec)

	resourceLookupKey := types.NamespacedName{Name: pipelineRolloutName, Namespace: namespace}

	Context("When applying a PipelineRollout spec", func() {
		It("Should create the PipelineRollout if it does not exist or it should update existing PipelineRollout and Numaflow Pipeline", func() {
			Expect(k8sClient.Create(ctx, pipelineRollout)).Should(Succeed())

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
			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, createdResource)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			By("Verifying the content of the pipeline spec")
			Expect(createdResource.Spec).Should(Equal(pipelineSpec))
		})

		It("Should have the PipelineRollout Status Phase has Deployed and ObservedGeneration matching Generation", func() {
			verifyStatusPhase(ctx, apiv1.PipelineRolloutGroupVersionKind, namespace, pipelineRolloutName, apiv1.PhaseDeployed)
		})

		It("Should have the metrics updated", func() {
			By("Verifying the PipelineRollout metric")
			Expect(len(customMetrics.GetPipelineCounterMap())).Should(Equal(1))
		})

		It("Should update the PipelineRollout and Numaflow Pipeline", func() {
			By("updating the PipelineRollout")

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
				updatedChildResource := &numaflowv1.Pipeline{}
				err := k8sClient.Get(ctx, resourceLookupKey, updatedChildResource)
				if err != nil {
					return numaflowv1.PipelineSpec{}, err
				}
				return updatedChildResource.Spec, nil
			}, timeout, interval).Should(Equal(pipelineSpec))

			/*By("Verifying the Numaflow Pipeline lifecycle desiredPhase is 'Paused'")
			Eventually(func() (string, error) {
				updatedChildResource := &numaflowv1.Pipeline{}
				err := k8sClient.Get(ctx, resourceLookupKey, updatedChildResource)
				if err != nil {
					return "", err
				}
				return string(updatedChildResource.Spec.Lifecycle.DesiredPhase), nil
			}, timeout, interval).Should(Equal("Paused"))

			By("Verifying that the PipelineRollout Status Phase is Deployed and ObservedGeneration matches Generation")
			verifyStatusPhase(ctx, apiv1.PipelineRolloutGroupVersionKind, namespace, pipelineRolloutName, apiv1.PhaseDeployed)*/

		})

		It("Should auto heal the Numaflow Pipeline with the PipelineRollout pipeline spec when the Numaflow Pipeline spec is changed", func() {
			By("updating the Numaflow Pipeline and verifying the changed field is the same as the original and not the modified version")
			verifyAutoHealing(ctx, numaflowv1.PipelineGroupVersionKind, namespace, pipelineRolloutName, "spec.interStepBufferServiceName", "someotherisbsname")
		})

		It("Should delete the PipelineRollout and Numaflow Pipeline", func() {
			Expect(k8sClient.Delete(ctx, &apiv1.PipelineRollout{
				ObjectMeta: pipelineRollout.ObjectMeta,
			})).Should(Succeed())

			deletedResource := &apiv1.PipelineRollout{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, deletedResource)
				return errors.IsNotFound(err)
			}, timeout, interval).Should(BeTrue())

			deletingChildResource := &numaflowv1.Pipeline{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, deletingChildResource)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(deletingChildResource.OwnerReferences).Should(HaveLen(1))
			Expect(deletedResource.UID).Should(Equal(deletingChildResource.OwnerReferences[0].UID))

			// TODO: use this on real cluster for e2e tests
			// NOTE: it's necessary to run on existing cluster to allow for deletion of child resources.
			// See https://book.kubebuilder.io/reference/envtest#testing-considerations for more details.
			// Could also reuse the env var used to set useExistingCluster to skip or perform the deletion based on CI settings.
			// Eventually(func() bool {
			// 	deletedChildResource := &numaflowv1.Pipeline{}
			// 	err := k8sClient.Get(ctx, resourceLookupKey, deletedChildResource)
			// 	return errors.IsNotFound(err)
			// }, timeout, interval).Should(BeTrue())
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

// this tests reconcile() with "DataLossPrevention" feature flag on
// test cases:
// - Normal cases:
// - PipelineRollout told to pause: verify pipeline lifecycle gets changed, verify Phase == Pending, Conditions.PipelinePausedOrPausing set, Conditions.Deployed not set
// - PipelineRollout needs to update and isn't paused: verify pipeline lifecycle gets changed, verify Phase == Pending, Conditions.PipelinePausedOrPausing set, Conditions.Deployed not set
// - PipelineRollout needs to update and is paused: verify pipeline lifecycle set to Running and pipeline updated, verify Phase == Deployed, Conditions.PipelinePausedOrPausing still set, Conditions.Deployed set
// - PipelineRollout allowed to run from paused state, doesn't need to update: verify pipeline lifecycle gets changed, verify Phase still equals Deployed, Conditions.PipelinePausedOrPausing not set, Conditions.Deployed was already set from before
// - auto-healing - somebody modified Pipeline spec - need to set to pause to overwrite safely
// - Just after Numaplane starts up: Controller-directed pause and ISBService-directed pause information not available
// - Failure cases:
// - PipelineRollout's Pipeline spec is invalid - TBD
// - Pipeline gets into a failed state for some other reason - TBD
func Test_reconcile(t *testing.T) {
	restConfig, err := kubernetes.K8sRestConfig()
	assert.Nil(t, err)

	err = numaflowv1.AddToScheme(scheme.Scheme)
	assert.Nil(t, err)
	err = apiv1.AddToScheme(scheme.Scheme)
	assert.Nil(t, err)

	numaflowClientSet := numaflowversioned.NewForConfigOrDie(restConfig)
	numaplaneClientSet := numaplaneversioned.NewForConfigOrDie(restConfig)
	numaplaneClient, err := client.New(restConfig, client.Options{}) //todo: actually, if we have this, do we need numaplaneClientSet?
	assert.Nil(t, err)

	// Make sure "dataLossPrevention" feature flag is turned on
	//configManager := config.GetConfigManagerInstance()
	// todo: make sure this works when called from root level too
	//err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath("./testdata/"), config.WithConfigFileName("config"))
	//assert.NoError(t, err)
	common.DataLossPrevention = true

	numaLogger := logger.GetBaseLogger()
	numaLogger.LogLevel = logger.DebugLevel // TODO: why isn't this working?
	logger.SetBaseLogger(numaLogger)
	ctx := logger.WithLogger(context.Background(), numaLogger)

	r := &PipelineRolloutReconciler{
		client:     numaplaneClient,
		scheme:     scheme.Scheme,
		restConfig: restConfig,
	}

	trueVal := true
	truePtr := &trueVal
	falseVal := false
	falsePtr := &falseVal

	modifiedPipelineSpec := pipelineSpec.DeepCopy()
	modifiedPipelineSpec.Vertices[1].UDF.Builtin.Args = []string{"something"}

	testCases := []struct {
		name                         string
		newPipelineSpec              numaflowv1.PipelineSpec
		controllerDirectedPause      *bool
		isbServiceDirectedPause      *bool
		expectedPipelineDesiredPhase string
		expectedPhase                apiv1.Phase
		//expectedConditionsSet        map[apiv1.ConditionType]bool // these are the ones we expect to have been set from before, as well as whether they're set to true or false
	}{
		{
			name:                         "PipelineRollout told to pause",
			newPipelineSpec:              pipelineSpec,
			controllerDirectedPause:      truePtr,
			isbServiceDirectedPause:      falsePtr,
			expectedPipelineDesiredPhase: PipelinePhasePaused,
			expectedPhase:                apiv1.PhaseDeployed,
		},
		{
			name:                         "PipelineRollout needs to update and isn't paused",
			newPipelineSpec:              *modifiedPipelineSpec,
			controllerDirectedPause:      falsePtr,
			isbServiceDirectedPause:      falsePtr,
			expectedPipelineDesiredPhase: PipelinePhasePaused,
			expectedPhase:                apiv1.PhasePending,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create PipelineRollout and Pipeline first, and then reconcile() the PipelineRollout
			//pipelineRollout := createPipelineRollout(&pipelineSpec)
			//pipelineRollout, err := numaplaneClientSet.NumaplaneV1alpha1().PipelineRollouts(testNamespace).Create(ctx, pipelineRollout, metav1.CreateOptions{})
			//assert.Nil(t, err)

			pipelineRollout := createPipelineRollout(&tc.newPipelineSpec)

			pipeline := numaflowv1.Pipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineRollout.Name,
					Namespace: pipelineRollout.Namespace,
				},
				Spec: pipelineSpec,
			}
			numaflowClientSet.NumaflowV1alpha1().Pipelines(testNamespace).Create(ctx, &pipeline, metav1.CreateOptions{})
			assert.Nil(t, err)

			// set Controller-Directed pause and ISBService-directed pause appropriately
			GetPauseModule().controllerRequestedPause[pipelineRollout.Namespace] = tc.controllerDirectedPause
			GetPauseModule().isbSvcRequestedPause[fmt.Sprintf("%s/%s", pipelineRollout.Namespace, pipelineSpec.InterStepBufferServiceName)] = tc.isbServiceDirectedPause

			//preReconciliationTime := time.Now()

			needsRequeue, err := r.reconcile(ctx, pipelineRollout)
			assert.Nil(t, err)
			assert.Equal(t, false, needsRequeue)

			// get Pipeline definition
			retrievedPipeline, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(testNamespace).Get(ctx, pipelineRollout.Name, metav1.GetOptions{})
			assert.Nil(t, err)
			// check DesiredPhase
			assert.Equal(t, tc.expectedPipelineDesiredPhase, string(retrievedPipeline.Spec.Lifecycle.DesiredPhase))

			// check PipelineRollout Phase
			assert.Equal(t, tc.expectedPhase, pipelineRollout.Status.Phase)

			// get the conditions that have LastTransitionTime since reconciliation
			/*fmt.Printf("Conditions: %+v\n", pipelineRollout.Status.Conditions)
			conditions := conditionsSince(pipelineRollout.Status.Conditions, preReconciliationTime)

			assert.Equal(t, len(tc.expectedConditionsSet), len(conditions))
			for conditionType, conditionValue := range tc.expectedConditionsSet {
				foundCondition := false
				for _, c := range conditions {
					if c.Type == string(conditionType) {
						foundCondition = true
						assert.Equal(t, conditionValue, c.Status)
					}
				}
				assert.True(t, foundCondition)
			}*/

			// Delete the PipelineRollout and Pipeline at the end
			numaplaneClientSet.NumaplaneV1alpha1().PipelineRollouts(testNamespace).Delete(ctx, pipelineRollout.Name, metav1.DeleteOptions{}) // todo: if this test fails, will the garbage be left on the cluster?
			numaflowClientSet.NumaflowV1alpha1().Pipelines(testNamespace).Delete(ctx, pipeline.Name, metav1.DeleteOptions{})

		})
	}
}

// return the Conditions whose LastTransitionTime are after the time passed in
/*func conditionsSince(conditions []metav1.Condition, t time.Time) []metav1.Condition {
	returnConditions := []metav1.Condition{}
	for _, c := range conditions {
		if c.LastTransitionTime.After(t) {
			returnConditions = append(returnConditions, c)
		}
	}
	return returnConditions
}*/

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

var yamlDesiredPhaseWrongType = `
{
	  "interStepBufferServiceName": "default",
	  "lifecycle": {
		"desiredPhase": 3
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

var yamlNoLifecycle = `
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

var yamlNoLifecycleWithNulls = `
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
			"log": {},
			"RANDOM_KEY":
			{
				"RANDOM_INNER_KEY": {}
			}
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

func Test_pipelineWithoutLifecycle(t *testing.T) {
	testCases := []struct {
		name          string
		specYaml      string
		expectedError bool
	}{
		{
			name:          "desiredPhase set to Paused",
			specYaml:      yamlHasDesiredPhase,
			expectedError: false,
		},
		{
			name:          "desiredPhase set to wrong type",
			specYaml:      yamlDesiredPhaseWrongType,
			expectedError: true,
		},
		{
			name:          "desiredPhase not present",
			specYaml:      yamlNoDesiredPhase,
			expectedError: false,
		},
		{
			name:          "lifecycle not present",
			specYaml:      yamlNoLifecycle,
			expectedError: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			obj := &kubernetes.GenericObject{}
			obj.Spec.Raw = []byte(tc.specYaml)
			withoutLifecycle, err := pipelineWithoutLifecycle(obj)
			if tc.expectedError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				bytes, _ := json.Marshal(withoutLifecycle)
				fmt.Printf("Test case %q: final yaml=%s\n", tc.name, string(bytes))
				assert.False(t, strings.Contains(string(bytes), "desiredPhase"))
			}
		})
	}
}

func Test_pipelineSpecEqual(t *testing.T) {
	testCases := []struct {
		name          string
		specYaml1     string
		specYaml2     string
		expectedEqual bool
		expectedError bool
	}{
		{
			name:          "Equal Except for Lifecycle and null values",
			specYaml1:     yamlHasDesiredPhase,
			specYaml2:     yamlNoLifecycleWithNulls,
			expectedEqual: true,
			expectedError: false,
		},
		{
			name:          "Not Equal",
			specYaml1:     yamlHasDesiredPhase,
			specYaml2:     yamlHasDesiredPhaseDifferentUDF,
			expectedEqual: false,
			expectedError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			obj1 := &kubernetes.GenericObject{}
			obj1.Spec.Raw = []byte(tc.specYaml1)
			obj2 := &kubernetes.GenericObject{}
			obj2.Spec.Raw = []byte(tc.specYaml2)
			equal, err := pipelineSpecEqual(context.Background(), obj1, obj2)
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedEqual, equal)
			}

		})
	}
}

func TestPipelineLabels(t *testing.T) {
	tests := []struct {
		name          string
		jsonInput     string
		expectedLabel string
		expectError   bool
	}{
		{
			name:          "Valid Input",
			jsonInput:     `{"interStepBufferServiceName": "buffer-service"}`,
			expectedLabel: "buffer-service",
			expectError:   false,
		},
		{
			name:          "Missing InterStepBufferServiceName",
			jsonInput:     `{}`,
			expectedLabel: "default",
			expectError:   false,
		},
		{
			name:          "Invalid JSON",
			jsonInput:     `{"interStepBufferServiceName": "buffer-service"`,
			expectedLabel: "",
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipelineRollout := &apiv1.PipelineRollout{
				Spec: apiv1.PipelineRolloutSpec{
					Pipeline: apiv1.Pipeline{
						Spec: runtime.RawExtension{Raw: []byte(tt.jsonInput)},
					},
				},
			}

			labels, err := pipelineLabels(pipelineRollout)
			if (err != nil) != tt.expectError {
				t.Errorf("pipelineLabels() error = %v, expectError %v", err, tt.expectError)
				return
			}
			if err == nil && labels[common.LabelKeyISBServiceNameForPipeline] != tt.expectedLabel {
				t.Errorf("pipelineLabels() = %v, expected %v", common.LabelKeyISBServiceNameForPipeline, tt.expectedLabel)
			}
		})
	}
}
