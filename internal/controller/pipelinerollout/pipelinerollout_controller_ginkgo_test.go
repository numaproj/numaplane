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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var _ = Describe("PipelineRollout Controller", Ordered, func() {

	ctx := context.Background()

	pipelineSpecRaw, err := json.Marshal(pipelineSpec)
	Expect(err).ToNot(HaveOccurred())

	pipelineRollout := &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ctlrcommon.DefaultTestNamespace,
			Name:      ctlrcommon.DefaultTestPipelineRolloutName,
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
			Expect(ctlrcommon.TestK8sClient.Create(ctx, pipelineRollout)).Should(Succeed())

			resourceLookupKey := types.NamespacedName{Name: ctlrcommon.DefaultTestPipelineRolloutName, Namespace: ctlrcommon.DefaultTestNamespace}
			createdResource := &apiv1.PipelineRollout{}
			Eventually(func() bool {
				err := ctlrcommon.TestK8sClient.Get(ctx, resourceLookupKey, createdResource)
				return err == nil
			}, ctlrcommon.TestDefaultTimeout, ctlrcommon.TestDefaultInterval).Should(BeTrue())

			createdPipelineRolloutPipelineSpec := numaflowv1.PipelineSpec{}
			Expect(json.Unmarshal(createdResource.Spec.Pipeline.Spec.Raw, &createdPipelineRolloutPipelineSpec)).ToNot(HaveOccurred())

			By("Verifying the content of the pipeline spec field")
			Expect(createdPipelineRolloutPipelineSpec).Should(Equal(pipelineSpec))
		})

		It("Should create a Numaflow Pipeline", func() {
			createdResource := &numaflowv1.Pipeline{}
			resourceLookupKey := types.NamespacedName{Name: ctlrcommon.DefaultTestPipelineName, Namespace: ctlrcommon.DefaultTestNamespace}
			Eventually(func() bool {
				err := ctlrcommon.TestK8sClient.Get(ctx, resourceLookupKey, createdResource)
				return err == nil
			}, ctlrcommon.TestDefaultTimeout, ctlrcommon.TestDefaultInterval).Should(BeTrue())

			By("Verifying the content of the pipeline spec")
			Expect(createdResource.Spec).Should(Equal(pipelineSpec))

			By("Verifying the label of the pipeline")
			Expect(createdResource.Labels[common.LabelKeyParentRollout]).Should(Equal(pipelineRollout.Name))
			Expect(createdResource.Labels[common.LabelKeyUpgradeState]).Should(Equal(string(common.LabelValueUpgradePromoted)))
		})

		It("Should have the PipelineRollout Status Phase has Deployed and ObservedGeneration matching Generation", func() {
			ctlrcommon.VerifyStatusPhase(ctx, apiv1.PipelineRolloutGroupVersionKind, ctlrcommon.DefaultTestNamespace, ctlrcommon.DefaultTestPipelineRolloutName, apiv1.PhaseDeployed)
		})

		It("Should have the metrics updated", func() {
			By("Verifying the PipelineRollout metric")
			Expect(testutil.ToFloat64(ctlrcommon.TestCustomMetrics.PipelineRolloutsRunning.WithLabelValues(ctlrcommon.DefaultTestNamespace))).Should(Equal(float64(1)))
			Expect(testutil.ToFloat64(ctlrcommon.TestCustomMetrics.PipelineROSyncs.WithLabelValues())).Should(BeNumerically(">", 1))
		})

		Context("When applying a PipelineRollout spec where the Pipeline with same name already exists", func() {
			It("Should be automatically create another one with different naming", func() {
				pipelineRolloutName := "test"
				pipelineName := pipelineRolloutName + "-0"
				Expect(ctlrcommon.TestK8sClient.Create(ctx, &numaflowv1.Pipeline{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: ctlrcommon.DefaultTestNamespace,
						Name:      pipelineName,
					},
					Spec: pipelineSpec,
				})).Should(Succeed())
				Expect(ctlrcommon.TestK8sClient.Create(ctx, &apiv1.PipelineRollout{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: ctlrcommon.DefaultTestNamespace,
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
				resourceLookupKey := types.NamespacedName{Name: newPipelineName, Namespace: ctlrcommon.DefaultTestNamespace}
				createdResource := &numaflowv1.Pipeline{}
				Eventually(func() bool {
					err := ctlrcommon.TestK8sClient.Get(ctx, resourceLookupKey, createdResource)
					return err == nil
				}, ctlrcommon.TestDefaultTimeout, ctlrcommon.TestDefaultInterval).Should(BeTrue())

				// Still expect the intermediate failed state trigger the metric
				Expect(testutil.ToFloat64(ctlrcommon.TestCustomMetrics.PipelineROSyncErrors.WithLabelValues())).Should(BeNumerically(">", 1))
			})
		})

		It("Should update the PipelineRollout and Numaflow Pipeline", func() {
			By("updating the PipelineRollout")

			resourceLookupKey := types.NamespacedName{Name: ctlrcommon.DefaultTestPipelineRolloutName, Namespace: ctlrcommon.DefaultTestNamespace}
			currentPipelineRollout := &apiv1.PipelineRollout{}
			Expect(ctlrcommon.TestK8sClient.Get(ctx, resourceLookupKey, currentPipelineRollout)).ToNot(HaveOccurred())

			pipelineSpec.InterStepBufferServiceName = "my-isbsvc-updated"
			pipelineSpecRaw, err := json.Marshal(pipelineSpec)
			Expect(err).ToNot(HaveOccurred())

			currentPipelineRollout.Spec.Pipeline.Spec.Raw = pipelineSpecRaw

			Expect(ctlrcommon.TestK8sClient.Update(ctx, currentPipelineRollout)).ToNot(HaveOccurred())

			By("Verifying the content of the pipeline field of the PipelineRollout")
			Eventually(func() (numaflowv1.PipelineSpec, error) {
				updatedResource := &apiv1.PipelineRollout{}
				err := ctlrcommon.TestK8sClient.Get(ctx, resourceLookupKey, updatedResource)
				if err != nil {
					return numaflowv1.PipelineSpec{}, err
				}

				updatedPipelineRolloutPipelineSpec := numaflowv1.PipelineSpec{}
				Expect(json.Unmarshal(updatedResource.Spec.Pipeline.Spec.Raw, &updatedPipelineRolloutPipelineSpec)).ToNot(HaveOccurred())

				return updatedPipelineRolloutPipelineSpec, nil
			}, ctlrcommon.TestDefaultTimeout, ctlrcommon.TestDefaultInterval).Should(Equal(pipelineSpec))

			By("Verifying the content of the spec field of the Numaflow Pipeline")
			Eventually(func() (numaflowv1.PipelineSpec, error) {
				resourceLookupKey := types.NamespacedName{Name: ctlrcommon.DefaultTestPipelineName, Namespace: ctlrcommon.DefaultTestNamespace}
				updatedChildResource := &numaflowv1.Pipeline{}
				err := ctlrcommon.TestK8sClient.Get(ctx, resourceLookupKey, updatedChildResource)
				if err != nil {
					return numaflowv1.PipelineSpec{}, err
				}
				return updatedChildResource.Spec, nil
			}, ctlrcommon.TestDefaultTimeout, ctlrcommon.TestDefaultInterval).Should(Equal(pipelineSpec))

			By("Verifying that the PipelineRollout Status Phase is Deployed and ObservedGeneration matches Generation")
			ctlrcommon.VerifyStatusPhase(ctx, apiv1.PipelineRolloutGroupVersionKind, ctlrcommon.DefaultTestNamespace, ctlrcommon.DefaultTestPipelineRolloutName, apiv1.PhaseDeployed)

		})

		It("Should auto heal the Numaflow Pipeline with the PipelineRollout pipeline spec when the Numaflow Pipeline spec is changed", func() {
			By("updating the Numaflow Pipeline and verifying the changed field is the same as the original and not the modified version")
			ctlrcommon.VerifyAutoHealing(ctx, numaflowv1.PipelineGroupVersionKind, ctlrcommon.DefaultTestNamespace, ctlrcommon.DefaultTestPipelineName, "spec.interStepBufferServiceName", "someotherisbsname")
		})

		It("Should delete the PipelineRollout and Numaflow Pipeline", func() {
			Expect(ctlrcommon.TestK8sClient.Delete(ctx, &apiv1.PipelineRollout{
				ObjectMeta: pipelineRollout.ObjectMeta,
			})).Should(Succeed())

			deletedResource := &apiv1.PipelineRollout{}
			Eventually(func() bool {
				resourceLookupKey := types.NamespacedName{Name: ctlrcommon.DefaultTestPipelineRolloutName, Namespace: ctlrcommon.DefaultTestNamespace}
				err := ctlrcommon.TestK8sClient.Get(ctx, resourceLookupKey, deletedResource)
				return errors.IsNotFound(err)
			}, ctlrcommon.TestDefaultTimeout, ctlrcommon.TestDefaultInterval).Should(BeTrue())

			deletingChildResource := &numaflowv1.Pipeline{}
			Eventually(func() bool {
				resourceLookupKey := types.NamespacedName{Name: ctlrcommon.DefaultTestPipelineName, Namespace: ctlrcommon.DefaultTestNamespace}
				err := ctlrcommon.TestK8sClient.Get(ctx, resourceLookupKey, deletingChildResource)
				return err == nil
			}, ctlrcommon.TestDefaultTimeout, ctlrcommon.TestDefaultInterval).Should(BeTrue())

			Expect(deletingChildResource.OwnerReferences).Should(HaveLen(1))
			Expect(deletedResource.UID).Should(Equal(deletingChildResource.OwnerReferences[0].UID))
		})
	})

	Context("When applying an invalid PipelineRollout spec", func() {
		It("Should not create the PipelineRollout", func() {
			Expect(ctlrcommon.TestK8sClient.Create(ctx, &apiv1.PipelineRollout{
				Spec: pipelineRollout.Spec,
			})).ShouldNot(Succeed())

			Expect(ctlrcommon.TestK8sClient.Create(ctx, &apiv1.PipelineRollout{
				ObjectMeta: pipelineRollout.ObjectMeta,
			})).ShouldNot(Succeed())

			Expect(ctlrcommon.TestK8sClient.Create(ctx, &apiv1.PipelineRollout{
				ObjectMeta: pipelineRollout.ObjectMeta,
				Spec:       apiv1.PipelineRolloutSpec{},
			})).ShouldNot(Succeed())
		})
	})

})
