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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/util"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("ISBServiceRollout Controller", Ordered, func() {
	const (
		namespace             = "default"
		isbServiceRolloutName = "isbservicerollout-test"
	)

	ctx := context.Background()

	isbsSpec := numaflowv1.InterStepBufferServiceSpec{
		Redis: &numaflowv1.RedisBufferService{},
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: "latest",
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &numaflowv1.DefaultVolumeSize,
			},
		},
	}

	isbsSpecRaw, err := json.Marshal(isbsSpec)
	Expect(err).ToNot(HaveOccurred())

	isbServiceRollout := &apiv1.ISBServiceRollout{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      isbServiceRolloutName,
		},
		Spec: apiv1.ISBServiceRolloutSpec{
			InterStepBufferService: runtime.RawExtension{
				Raw: isbsSpecRaw,
			},
		},
	}

	resourceLookupKey := types.NamespacedName{Name: isbServiceRolloutName, Namespace: namespace}

	Context("When applying a ISBServiceRollout spec", func() {
		It("Should create the ISBServiceRollout if it does not exist", func() {
			Expect(k8sClient.Create(ctx, isbServiceRollout)).Should(Succeed())

			createdResource := &apiv1.ISBServiceRollout{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, createdResource)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			createdInterStepBufferServiceSpec := numaflowv1.InterStepBufferServiceSpec{}
			Expect(json.Unmarshal(createdResource.Spec.InterStepBufferService.Raw, &createdInterStepBufferServiceSpec)).ToNot(HaveOccurred())

			By("Verifying the content of the ISBServiceRollout spec field")
			Expect(createdInterStepBufferServiceSpec).Should(Equal(isbsSpec))

			By("Verifying the spec hash stored in the ISBServiceRollout annotations after creation")
			var isbServiceSpecAsMap map[string]any
			Expect(json.Unmarshal(isbsSpecRaw, &isbServiceSpecAsMap)).ToNot(HaveOccurred())
			isbServiceSpecHash := util.MustHash(isbServiceSpecAsMap)
			Eventually(func() (string, error) {
				createdResource := &apiv1.ISBServiceRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, createdResource)
				if err != nil {
					return "", err
				}
				return createdResource.Annotations[apiv1.KeyHash], nil
			}, timeout, interval).Should(Equal(isbServiceSpecHash))
		})

		It("Should have created an InterStepBufferService ", func() {
			createdISBResource := &numaflowv1.InterStepBufferService{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, createdISBResource)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			By("Verifying the content of the InterStepBufferService spec")
			Expect(createdISBResource.Spec).Should(Equal(isbsSpec))
		})

		It("Should have the ISBServiceRollout Status Phase as Running", func() {
			Consistently(func() (apiv1.Phase, error) {
				createdISBResource := &apiv1.ISBServiceRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, createdISBResource)
				if err != nil {
					return apiv1.Phase(""), err
				}
				return createdISBResource.Status.Phase, nil
			}, timeout, interval).Should(Equal(apiv1.PhaseRunning))
		})

		It("Should update the ISBServiceRollout and InterStepBufferService", func() {
			By("updating the ISBServiceRollout")

			currentISBServiceRollout := &apiv1.ISBServiceRollout{}
			Expect(k8sClient.Get(ctx, resourceLookupKey, currentISBServiceRollout)).ToNot(HaveOccurred())

			var lastTransitionTime time.Time
			Eventually(func() (time.Time, error) {
				currentResource := &apiv1.ISBServiceRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, currentResource)
				if err != nil {
					return time.Time{}, err
				}

				for _, cond := range currentISBServiceRollout.Status.Conditions {
					if cond.Type == string(apiv1.ConditionConfigured) {
						lastTransitionTime = cond.LastTransitionTime.Time
						return lastTransitionTime, nil
					}
				}

				return time.Time{}, nil
			}, timeout, interval).Should(Not(Equal(time.Time{})))

			// Prepare a new spec for update
			newIsbsSpec := numaflowv1.InterStepBufferServiceSpec{
				Redis: &numaflowv1.RedisBufferService{},
				JetStream: &numaflowv1.JetStreamBufferService{
					Version: "an updated version",
					Persistence: &numaflowv1.PersistenceStrategy{
						VolumeSize: &numaflowv1.DefaultVolumeSize,
					},
				},
			}

			newIsbsSpecRaw, err := json.Marshal(newIsbsSpec)
			Expect(err).ToNot(HaveOccurred())

			// Update the spec
			currentISBServiceRollout.Spec.InterStepBufferService.Raw = newIsbsSpecRaw //runtime.RawExtension{Raw: newIsbsSpecRaw}

			Expect(k8sClient.Update(ctx, currentISBServiceRollout)).ToNot(HaveOccurred())

			By("Verifying the content of the ISBServiceRollout")
			Eventually(func() (numaflowv1.InterStepBufferServiceSpec, error) {
				updatedResource := &apiv1.ISBServiceRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, updatedResource)
				if err != nil {
					return numaflowv1.InterStepBufferServiceSpec{}, err
				}

				createdInterStepBufferServiceSpec := numaflowv1.InterStepBufferServiceSpec{}
				Expect(json.Unmarshal(updatedResource.Spec.InterStepBufferService.Raw, &createdInterStepBufferServiceSpec)).ToNot(HaveOccurred())

				return createdInterStepBufferServiceSpec, nil
			}, timeout, interval).Should(Equal(newIsbsSpec))

			By("Verifying the content of the InterStepBufferService ")
			Eventually(func() (numaflowv1.InterStepBufferServiceSpec, error) {
				updatedChildResource := &numaflowv1.InterStepBufferService{}
				err := k8sClient.Get(ctx, resourceLookupKey, updatedChildResource)
				if err != nil {
					return numaflowv1.InterStepBufferServiceSpec{}, err
				}
				return updatedChildResource.Spec, nil
			}, timeout, interval).Should(Equal(newIsbsSpec))

			By("Verifying the spec hash stored in the ISBServiceRollout annotations after update")
			var isbServiceSpecAsMap map[string]any
			Expect(json.Unmarshal(newIsbsSpecRaw, &isbServiceSpecAsMap)).ToNot(HaveOccurred())
			isbServiceSpecHash := util.MustHash(isbServiceSpecAsMap)
			Eventually(func() (string, error) {
				updatedResource := &apiv1.ISBServiceRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, updatedResource)
				if err != nil {
					return "", err
				}
				return updatedResource.Annotations[apiv1.KeyHash], nil
			}, timeout, interval).Should(Equal(isbServiceSpecHash))

			By("Verifying the LastTransitionTime of the Configured condition of the ISBServiceRollout is after the time of the initial configuration")
			Eventually(func() (bool, error) {
				updatedResource := &apiv1.ISBServiceRollout{}
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

			By("Verifying that the ISBServiceRollout Status Phase is Running")
			Consistently(func() (apiv1.Phase, error) {
				updatedResource := &apiv1.ISBServiceRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, updatedResource)
				if err != nil {
					return apiv1.Phase(""), err
				}
				return updatedResource.Status.Phase, nil
			}, timeout, interval).Should(Equal(apiv1.PhaseRunning))

			By("Verifying that the same ISBServiceRollout should not perform and update (no Configuration condition LastTransitionTime change) and the hash spec annotation should not change")
			Expect(k8sClient.Get(ctx, resourceLookupKey, currentISBServiceRollout)).ToNot(HaveOccurred())
			Expect(k8sClient.Update(ctx, currentISBServiceRollout)).ToNot(HaveOccurred())
			Eventually(func() (bool, error) {
				updatedResource := &apiv1.ISBServiceRollout{}
				err := k8sClient.Get(ctx, resourceLookupKey, updatedResource)
				if err != nil {
					return false, err
				}

				equalHash := updatedResource.Annotations[apiv1.KeyHash] == isbServiceSpecHash

				for _, cond := range updatedResource.Status.Conditions {
					if cond.Type == string(apiv1.ConditionConfigured) {
						equalTime := cond.LastTransitionTime.Time.Equal(lastTransitionTime)
						return equalTime && equalHash, nil
					}
				}

				return false, nil
			}, timeout, interval).Should(BeTrue())

		})

		It("Should auto heal the InterStepBufferService with the ISBServiceRollout interstepbufferservice spec when the InterStepBufferService spec is changed", func() {
			By("updating the InterStepBufferService")
			currentISBService := &numaflowv1.InterStepBufferService{}
			Expect(k8sClient.Get(ctx, resourceLookupKey, currentISBService)).To(Succeed())

			originalJetstreamVersion := currentISBService.Spec.JetStream.Version
			newJetstreamVersion := "1.2.3"
			currentISBService.Spec.JetStream.Version = newJetstreamVersion

			Expect(k8sClient.Update(ctx, currentISBService)).ToNot(HaveOccurred())

			By("Verifying the changed field of the InterStepBufferService is the same as the original and not the modified version")
			e := Eventually(func() (string, error) {
				updatedResource := &numaflowv1.InterStepBufferService{}
				err := k8sClient.Get(ctx, resourceLookupKey, updatedResource)
				if err != nil {
					return "", err
				}

				return updatedResource.Spec.JetStream.Version, nil
			}, duration, interval)

			e.Should(Equal(originalJetstreamVersion))
			e.ShouldNot(Equal(newJetstreamVersion))
		})

		It("Should delete the ISBServiceRollout and InterStepBufferService", func() {
			Expect(k8sClient.Delete(ctx, &apiv1.ISBServiceRollout{
				ObjectMeta: isbServiceRollout.ObjectMeta,
			})).Should(Succeed())

			deletedResource := &apiv1.ISBServiceRollout{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, deletedResource)
				return errors.IsNotFound(err)
			}, timeout, interval).Should(BeTrue())

			deletingChildResource := &numaflowv1.InterStepBufferService{}
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
			// 	deletedChildResource := &apiv1.ISBServiceRollout{}
			// 	err := k8sClient.Get(ctx, resourceLookupKey, deletedChildResource)
			// 	return errors.IsNotFound(err)
			// }, timeout, interval).Should(BeTrue())
		})
	})

	Context("When applying an invalid ISBServiceRollout spec", func() {
		It("Should not create the ISBServiceRollout", func() {
			Expect(k8sClient.Create(ctx, &apiv1.ISBServiceRollout{
				Spec: isbServiceRollout.Spec,
			})).ShouldNot(Succeed())

			Expect(k8sClient.Create(ctx, &apiv1.ISBServiceRollout{
				ObjectMeta: isbServiceRollout.ObjectMeta,
			})).ShouldNot(Succeed())

			Expect(k8sClient.Create(ctx, &apiv1.ISBServiceRollout{
				ObjectMeta: isbServiceRollout.ObjectMeta,
				Spec:       apiv1.ISBServiceRolloutSpec{},
			})).ShouldNot(Succeed())
		})
	})
})
