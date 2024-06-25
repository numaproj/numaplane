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
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var _ = Describe("PipelineRollout Controller", func() {
	const (
		namespace           = "default"
		pipelineRolloutName = "pipelinerollout-test"
		timeout             = 10 * time.Second
		duration            = 10 * time.Second
		interval            = 250 * time.Millisecond
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

			By("Verifying the spec hash stored in the PipelineRollout annotations after creation")
			var pipelineSpecAsMap map[string]any
			Expect(json.Unmarshal(pipelineSpecRaw, &pipelineSpecAsMap)).ToNot(HaveOccurred())
			pipelineSpecHash := util.MustHash(pipelineSpecAsMap)
			Eventually(func() (string, error) {
				createdResource := &apiv1.PipelineRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, createdResource)
				if err != nil {
					return "", err
				}
				return createdResource.Annotations[apiv1.KeyHash], nil
			}, timeout, interval).Should(Equal(pipelineSpecHash))
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

		It("Should have the PipelineRollout Status Phase has Running", func() {
			Consistently(func() (apiv1.Phase, error) {
				createdResource := &apiv1.PipelineRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, createdResource)
				if err != nil {
					return apiv1.Phase(""), err
				}
				return createdResource.Status.Phase, nil
			}, duration, interval).Should(Equal(apiv1.PhaseRunning))
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
					if cond.Type == string(apiv1.ConditionConfigured) {
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

			By("Verifying the spec hash stored in the PipelineRollout annotations after update")
			var pipelineSpecAsMap map[string]any
			Expect(json.Unmarshal(pipelineSpecRaw, &pipelineSpecAsMap)).ToNot(HaveOccurred())
			pipelineSpecHash := util.MustHash(pipelineSpecAsMap)
			Eventually(func() (string, error) {
				updatedResource := &apiv1.PipelineRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, updatedResource)
				if err != nil {
					return "", err
				}
				return updatedResource.Annotations[apiv1.KeyHash], nil
			}, timeout, interval).Should(Equal(pipelineSpecHash))

			By("Verifying the LastTransitionTime of the Configured condition of the PipelineRollout is after the time of the initial configuration")
			Eventually(func() (bool, error) {
				updatedResource := &apiv1.PipelineRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, updatedResource)
				if err != nil {
					return false, err
				}

				for _, cond := range updatedResource.Status.Conditions {
					if cond.Type == string(apiv1.ConditionConfigured) {
						isAfter := cond.LastTransitionTime.Time.After(lastTransitionTime)
						lastTransitionTime = cond.LastTransitionTime.Time
						return isAfter, nil
					}
				}

				return false, nil
			}, time.Second, interval).Should(BeTrue())

			By("Verifying that the same PipelineRollout should not perform and update (no Configuration condition LastTransitionTime change) and the hash spec annotation should not change")
			Expect(k8sClient.Get(ctx, resourceLookupKey, currentPipelineRollout)).ToNot(HaveOccurred())
			Expect(k8sClient.Update(ctx, currentPipelineRollout)).ToNot(HaveOccurred())
			Eventually(func() (bool, error) {
				updatedResource := &apiv1.PipelineRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, updatedResource)
				if err != nil {
					return false, err
				}

				equalHash := updatedResource.Annotations[apiv1.KeyHash] == pipelineSpecHash

				for _, cond := range updatedResource.Status.Conditions {
					if cond.Type == string(apiv1.ConditionConfigured) {
						equalTime := cond.LastTransitionTime.Time.Equal(lastTransitionTime)
						return equalTime && equalHash, nil
					}
				}

				return false, nil
			}, timeout, interval).Should(BeTrue())
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

func Test_makeChildResourceFromRolloutAndUpdateSpecHash_InvalidType(t *testing.T) {
	ctx := context.Background()
	restConfig := &rest.Config{}

	invalidType := kubernetes.GenericObject{}

	_, _, err := makeChildResourceFromRolloutAndUpdateSpecHash(ctx, restConfig, &invalidType)

	assert.Error(t, err)
	assert.Equal(t, "invalid rollout type", err.Error())
}

func Test_makeChildResourceFromRolloutAndUpdateSpecHash_PipelineRollout_UnmarshalError(t *testing.T) {
	ctx := context.Background()
	restConfig := &rest.Config{}

	pipelineRolloutInvalidSpec := &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pipeline-test",
			Namespace: "default",
		},
		Spec: apiv1.PipelineRolloutSpec{
			// providing invalid JSON for unmarshal error
			Pipeline: runtime.RawExtension{Raw: []byte(`{"key":"value"`)},
		},
	}

	_, _, err := makeChildResourceFromRolloutAndUpdateSpecHash(ctx, restConfig, pipelineRolloutInvalidSpec)
	assert.Error(t, err)
}

func Test_setAnnotation(t *testing.T) {
	key1, value1 := "some_key_1", "some_value_1"
	key2, value2 := "some_key_2", "some_value_2"

	pipelineRollout := &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pipeline-test",
			Namespace: "default",
		},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: runtime.RawExtension{Raw: []byte(`{"key":"value"}`)},
		},
	}

	// Invoke the method under test
	setAnnotation(pipelineRollout, key1, value1)

	// Check if the annotation was correctly set
	annotations := pipelineRollout.GetAnnotations()
	assert.NotNil(t, annotations, "Expected annotations to be set on the pipelineRollout object")
	assert.Contains(t, annotations, key1, "Expected the key to be set in the annotations")
	assert.Equal(t, value1, annotations[key1], "Expected the value to be set for the key in the annotations")

	// Overwrite existing annotation
	newValue := "new_value"
	setAnnotation(pipelineRollout, key1, newValue)

	// Check if the annotation was correctly updated
	annotations = pipelineRollout.GetAnnotations()
	assert.NotNil(t, annotations, "Expected annotations to be set on the pipelineRollout object")
	assert.Contains(t, annotations, key1, "Expected the key to be set in the annotations")
	assert.NotEqual(t, value1, annotations[key1], "Expected the old value to be replaced")
	assert.Equal(t, newValue, annotations[key1], "Expected the new value to be set for the key in the annotations")

	// Add one more annotation
	setAnnotation(pipelineRollout, key2, value2)
	assert.NotNil(t, annotations, "Expected annotations to be set on the pipelineRollout object")
	assert.Len(t, annotations, 2, "Expected annotations to be of length 2")
	assert.Contains(t, annotations, key2, "Expected the key to be set in the annotations")
	assert.Equal(t, value2, annotations[key2], "Expected the value to be set for the key in the annotations")
}
