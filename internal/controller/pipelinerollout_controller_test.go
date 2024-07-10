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
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var _ = Describe("PipelineRollout Controller", Ordered, func() {
	const (
		namespace           = "default"
		pipelineRolloutName = "pipelinerollout-test"
	)

	ctx := context.Background()

	pipelineSpecSourceRPU := int64(5)
	pipelineSpecSourceDuration := metav1.Duration{
		Duration: time.Second,
	}

	pipelineSpec := numaflowv1.PipelineSpec{
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

	pipelineSpecRaw, err := json.Marshal(pipelineSpec)
	Expect(err).ToNot(HaveOccurred())

	pipelineRollout := &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pipelineRolloutName,
		},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: runtime.RawExtension{
				Raw: pipelineSpecRaw,
			},
		},
	}

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
			Expect(json.Unmarshal(createdResource.Spec.Pipeline.Raw, &createdPipelineRolloutPipelineSpec)).ToNot(HaveOccurred())

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

		It("Should have the PipelineRollout Status Phase has Deployed", func() {
			Consistently(func() (apiv1.Phase, error) {
				createdResource := &apiv1.PipelineRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, createdResource)
				if err != nil {
					return apiv1.Phase(""), err
				}
				return createdResource.Status.Phase, nil
			}, duration, interval).Should(Equal(apiv1.PhaseDeployed))
		})

		It("Should update the PipelineRollout and Numaflow Pipeline", func() {
			By("updating the PipelineRollout")

			currentPipelineRollout := &apiv1.PipelineRollout{}
			Expect(k8sClient.Get(ctx, resourceLookupKey, currentPipelineRollout)).ToNot(HaveOccurred())

			var lastTransitionTime time.Time
			Eventually(func() (time.Time, error) {
				currentResource := &apiv1.PipelineRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, currentResource)
				if err != nil {
					return time.Time{}, err
				}

				for _, cond := range currentPipelineRollout.Status.Conditions {
					if cond.Type == string(apiv1.ConditionChildResourceDeployed) {
						lastTransitionTime = cond.LastTransitionTime.Time
						return lastTransitionTime, nil
					}
				}

				return time.Time{}, nil
			}, timeout, interval).Should(Not(Equal(time.Time{})))

			pipelineSpec.InterStepBufferServiceName = "my-isbsvc-updated"
			pipelineSpecRaw, err := json.Marshal(pipelineSpec)
			Expect(err).ToNot(HaveOccurred())

			currentPipelineRollout.Spec.Pipeline.Raw = pipelineSpecRaw

			Expect(k8sClient.Update(ctx, currentPipelineRollout)).ToNot(HaveOccurred())

			By("Verifying the content of the pipeline field of the PipelineRollout")
			Eventually(func() (numaflowv1.PipelineSpec, error) {
				updatedResource := &apiv1.PipelineRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, updatedResource)
				if err != nil {
					return numaflowv1.PipelineSpec{}, err
				}

				updatedPipelineRolloutPipelineSpec := numaflowv1.PipelineSpec{}
				Expect(json.Unmarshal(updatedResource.Spec.Pipeline.Raw, &updatedPipelineRolloutPipelineSpec)).ToNot(HaveOccurred())

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

			By("Verifying the LastTransitionTime of the Deployed condition of the PipelineRollout is after the time of the initial configuration")
			Eventually(func() (bool, error) {
				updatedResource := &apiv1.PipelineRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, updatedResource)
				if err != nil {
					return false, err
				}

				for _, cond := range updatedResource.Status.Conditions {
					if cond.Type == string(apiv1.ConditionChildResourceDeployed) {
						isAfter := cond.LastTransitionTime.Time.After(lastTransitionTime)
						lastTransitionTime = cond.LastTransitionTime.Time
						return isAfter, nil
					}
				}

				return false, nil
			}, timeout, interval).Should(BeTrue())

			By("Verifying that the PipelineRollout Status Phase is Deployed")
			Consistently(func() (apiv1.Phase, error) {
				updatedResource := &apiv1.PipelineRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, updatedResource)
				if err != nil {
					return apiv1.Phase(""), err
				}
				return updatedResource.Status.Phase, nil
			}, duration, interval).Should(Equal(apiv1.PhaseDeployed))

			By("Verifying that the same PipelineRollout should not perform and update (no Configuration condition LastTransitionTime change)")
			Expect(k8sClient.Get(ctx, resourceLookupKey, currentPipelineRollout)).ToNot(HaveOccurred())
			Expect(k8sClient.Update(ctx, currentPipelineRollout)).ToNot(HaveOccurred())
			Eventually(func() (bool, error) {
				updatedResource := &apiv1.PipelineRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, updatedResource)
				if err != nil {
					return false, err
				}

				for _, cond := range updatedResource.Status.Conditions {
					if cond.Type == string(apiv1.ConditionChildResourceDeployed) {
						return cond.LastTransitionTime.Time.Equal(lastTransitionTime), nil
					}
				}

				return false, nil
			}, timeout, interval).Should(BeTrue())
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

func TestPipeLinLabels(t *testing.T) {
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
					Pipeline: runtime.RawExtension{Raw: []byte(tt.jsonInput)},
				},
			}
			labels, err := pipeLinLabels(pipelineRollout)
			if (err != nil) != tt.expectError {
				t.Errorf("pipeLinLabels() error = %v, expectError %v", err, tt.expectError)
				return
			}
			if err == nil && labels["isbsvc-name"] != tt.expectedLabel {
				t.Errorf("pipeLinLabels() = %v, expected %v", labels["isbsvc-name"], tt.expectedLabel)
			}
		})
	}
}
