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

package isbservicerollout

import (
	"context"
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var _ = Describe("ISBServiceRollout Controller", Ordered, func() {
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
			Namespace: ctlrcommon.DefaultTestNamespace,
			Name:      ctlrcommon.DefaultTestISBSvcRolloutName,
		},
		Spec: apiv1.ISBServiceRolloutSpec{
			InterStepBufferService: apiv1.InterStepBufferService{
				Spec: k8sruntime.RawExtension{
					Raw: isbsSpecRaw,
				},
			},
		},
	}

	resourceLookupKey := types.NamespacedName{Name: ctlrcommon.DefaultTestISBSvcRolloutName, Namespace: ctlrcommon.DefaultTestNamespace}

	Context("When applying a ISBServiceRollout spec", func() {
		It("Should create the ISBServiceRollout if it does not exist", func() {
			Expect(ctlrcommon.TestK8sClient.Create(ctx, isbServiceRollout)).Should(Succeed())

			createdResource := &apiv1.ISBServiceRollout{}
			Eventually(func() bool {
				err := ctlrcommon.TestK8sClient.Get(ctx, resourceLookupKey, createdResource)
				return err == nil
			}, ctlrcommon.TestDefaultTimeout, ctlrcommon.TestDefaultInterval).Should(BeTrue())

			createdInterStepBufferServiceSpec := numaflowv1.InterStepBufferServiceSpec{}
			Expect(json.Unmarshal(createdResource.Spec.InterStepBufferService.Spec.Raw, &createdInterStepBufferServiceSpec)).ToNot(HaveOccurred())

			By("Verifying the content of the ISBServiceRollout spec field")
			Expect(createdInterStepBufferServiceSpec).Should(Equal(isbsSpec))
		})

		It("Should have created an InterStepBufferService ", func() {
			createdISBResource := &numaflowv1.InterStepBufferService{}
			Eventually(func() bool {
				err := ctlrcommon.TestK8sClient.Get(ctx, resourceLookupKey, createdISBResource)
				return err == nil
			}, ctlrcommon.TestDefaultTimeout, ctlrcommon.TestDefaultInterval).Should(BeTrue())

			By("Verifying the content of the InterStepBufferService spec")
			Expect(createdISBResource.Spec).Should(Equal(isbsSpec))
		})

		It("Should have the ISBServiceRollout Status Phase as Deployed and ObservedGeneration matching Generation", func() {
			ctlrcommon.VerifyStatusPhase(ctx, apiv1.ISBServiceRolloutGroupVersionKind, ctlrcommon.DefaultTestNamespace, ctlrcommon.DefaultTestISBSvcRolloutName, apiv1.PhaseDeployed)
		})

		It("Should have created an PodDisruptionBudget for ISB ", func() {
			isbPDB := &policyv1.PodDisruptionBudget{}
			Eventually(func() bool {
				err := ctlrcommon.TestK8sClient.Get(ctx, resourceLookupKey, isbPDB)
				return err == nil
			}, ctlrcommon.TestDefaultTimeout, ctlrcommon.TestDefaultInterval).Should(BeTrue())

			By("Verifying the content of the InterStepBufferService spec")
			Expect(isbPDB.Spec.MaxUnavailable.IntVal).Should(Equal(int32(1)))
		})

		It("Should have the metrics updated", func() {
			By("Verifying the ISBService metric")
			Expect(testutil.ToFloat64(ctlrcommon.TestCustomMetrics.ISBServiceRolloutsRunning.WithLabelValues(ctlrcommon.DefaultTestNamespace))).Should(Equal(float64(1)))
			Expect(testutil.ToFloat64(ctlrcommon.TestCustomMetrics.ISBServiceROSyncs.WithLabelValues())).Should(BeNumerically(">", 1))
		})

		It("Should update the ISBServiceRollout and InterStepBufferService", func() {
			By("updating the ISBServiceRollout")

			currentISBServiceRollout := &apiv1.ISBServiceRollout{}
			Expect(ctlrcommon.TestK8sClient.Get(ctx, resourceLookupKey, currentISBServiceRollout)).ToNot(HaveOccurred())

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
			currentISBServiceRollout.Spec.InterStepBufferService.Spec.Raw = newIsbsSpecRaw //runtime.RawExtension{Raw: newIsbsSpecRaw}

			Expect(ctlrcommon.TestK8sClient.Update(ctx, currentISBServiceRollout)).ToNot(HaveOccurred())

			By("Verifying the content of the ISBServiceRollout")
			Eventually(func() (numaflowv1.InterStepBufferServiceSpec, error) {
				updatedResource := &apiv1.ISBServiceRollout{}
				err := ctlrcommon.TestK8sClient.Get(ctx, resourceLookupKey, updatedResource)
				if err != nil {
					return numaflowv1.InterStepBufferServiceSpec{}, err
				}

				createdInterStepBufferServiceSpec := numaflowv1.InterStepBufferServiceSpec{}
				Expect(json.Unmarshal(updatedResource.Spec.InterStepBufferService.Spec.Raw, &createdInterStepBufferServiceSpec)).ToNot(HaveOccurred())

				return createdInterStepBufferServiceSpec, nil
			}, ctlrcommon.TestDefaultTimeout, ctlrcommon.TestDefaultInterval).Should(Equal(newIsbsSpec))

			By("Verifying the content of the InterStepBufferService ")
			Eventually(func() (numaflowv1.InterStepBufferServiceSpec, error) {
				updatedChildResource := &numaflowv1.InterStepBufferService{}
				err := ctlrcommon.TestK8sClient.Get(ctx, resourceLookupKey, updatedChildResource)
				if err != nil {
					return numaflowv1.InterStepBufferServiceSpec{}, err
				}
				return updatedChildResource.Spec, nil
			}, ctlrcommon.TestDefaultTimeout, ctlrcommon.TestDefaultInterval).Should(Equal(newIsbsSpec))

			By("Verifying that the ISBServiceRollout Status Phase is Deployed and ObservedGeneration matches Generation")
			ctlrcommon.VerifyStatusPhase(ctx, apiv1.ISBServiceRolloutGroupVersionKind, ctlrcommon.DefaultTestNamespace, ctlrcommon.DefaultTestISBSvcRolloutName, apiv1.PhaseDeployed)
		})

		It("Should auto heal the InterStepBufferService with the ISBServiceRollout pipeline spec when the InterStepBufferService spec is changed", func() {
			By("updating the InterStepBufferService and verifying the changed field is the same as the original and not the modified version")
			ctlrcommon.VerifyAutoHealing(ctx, numaflowv1.ISBGroupVersionKind, ctlrcommon.DefaultTestNamespace, ctlrcommon.DefaultTestISBSvcRolloutName, "spec.jetstream.version", "1.2.3.4.5")
		})

		It("Should delete the ISBServiceRollout and InterStepBufferService", func() {
			Expect(ctlrcommon.TestK8sClient.Delete(ctx, &apiv1.ISBServiceRollout{
				ObjectMeta: isbServiceRollout.ObjectMeta,
			})).Should(Succeed())

			deletedResource := &apiv1.ISBServiceRollout{}
			Eventually(func() bool {
				err := ctlrcommon.TestK8sClient.Get(ctx, resourceLookupKey, deletedResource)
				return errors.IsNotFound(err)
			}, ctlrcommon.TestDefaultTimeout, ctlrcommon.TestDefaultInterval).Should(BeTrue())

			deletingChildResource := &numaflowv1.InterStepBufferService{}
			Eventually(func() bool {
				err := ctlrcommon.TestK8sClient.Get(ctx, resourceLookupKey, deletingChildResource)
				return err == nil
			}, ctlrcommon.TestDefaultTimeout, ctlrcommon.TestDefaultInterval).Should(BeTrue())

			Expect(deletingChildResource.OwnerReferences).Should(HaveLen(1))
			Expect(deletedResource.UID).Should(Equal(deletingChildResource.OwnerReferences[0].UID))

			// TODO: use this on real cluster for e2e tests
			// NOTE: it's necessary to run on existing cluster to allow for deletion of child resources.
			// See https://book.kubebuilder.io/reference/envtest#testing-considerations for more details.
			// Could also reuse the env var used to set useExistingCluster to skip or perform the deletion based on CI settings.
			// Eventually(func() bool {
			// 	deletedChildResource := &apiv1.ISBServiceRollout{}
			// 	err := ctlrcommon.TestK8sClient.Get(ctx, resourceLookupKey, deletedChildResource)
			// 	return errors.IsNotFound(err)
			// }, ctlrcommon.TestDefaultTimeout, ctlrcommon.TestDefaultInterval).Should(BeTrue())
		})
	})

	Context("When applying an invalid ISBServiceRollout spec", func() {
		It("Should not create the ISBServiceRollout", func() {
			Expect(ctlrcommon.TestK8sClient.Create(ctx, &apiv1.ISBServiceRollout{
				Spec: isbServiceRollout.Spec,
			})).ShouldNot(Succeed())

			Expect(ctlrcommon.TestK8sClient.Create(ctx, &apiv1.ISBServiceRollout{
				ObjectMeta: isbServiceRollout.ObjectMeta,
			})).ShouldNot(Succeed())

			Expect(ctlrcommon.TestK8sClient.Create(ctx, &apiv1.ISBServiceRollout{
				ObjectMeta: isbServiceRollout.ObjectMeta,
				Spec:       apiv1.ISBServiceRolloutSpec{},
			})).ShouldNot(Succeed())
		})
	})
})
