/*
Copyright 2025.

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

package e2e

import (
	"testing"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/numaproj/numaplane/internal/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	pipelineRolloutName     = "test-pipeline-rollout"
	isbServiceRolloutName   = "test-isbservice-rollout"
	initialJetstreamVersion = "2.10.17"
	validJetstreamVersion   = "2.10.11"
	invalidJetstreamVersion = "0.0.0"
)

var (
	pullPolicyAlways = corev1.PullAlways
	defaultStrategy  = apiv1.PipelineStrategy{
		PipelineTypeRolloutStrategy: apiv1.PipelineTypeRolloutStrategy{
			PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
				Progressive: apiv1.ProgressiveStrategy{
					AssessmentSchedule: "60,300,30,10",
				},
			},
		},
	}

	pipelineSpecSourceRPU      = int64(5)
	pipelineSpecSourceDuration = metav1.Duration{
		Duration: time.Second,
	}
	sourceVertexScaleMin = int32(5)
	sourceVertexScaleMax = int32(9)
	numVertices          = int32(1)
	zeroReplicaSleepSec  = uint32(15)
	initialPipelineSpec  = numaflowv1.PipelineSpec{
		InterStepBufferServiceName: isbServiceRolloutName,
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: "in",
				Source: &numaflowv1.Source{
					Generator: &numaflowv1.GeneratorSource{
						RPU:      &pipelineSpecSourceRPU,
						Duration: &pipelineSpecSourceDuration,
					},
				},
				Scale: numaflowv1.Scale{Min: &sourceVertexScaleMin, Max: &sourceVertexScaleMax, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name: "cat",
				UDF: &numaflowv1.UDF{
					Container: &numaflowv1.Container{
						Image:           "quay.io/numaio/numaflow-go/map-cat:stable",
						ImagePullPolicy: &pullPolicyAlways,
					},
				},
				Scale: numaflowv1.Scale{Min: &numVertices, Max: &numVertices, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name: "out",
				Sink: &numaflowv1.Sink{
					AbstractSink: numaflowv1.AbstractSink{
						Log: &numaflowv1.Log{},
					},
				},
				Scale: numaflowv1.Scale{Min: &numVertices, Max: &numVertices, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
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

	volSize, _            = apiresource.ParseQuantity("10Mi")
	initialISBServiceSpec = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: initialJetstreamVersion,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &volSize,
			},
		},
	}
)

func TestProgressiveE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Progressive Pipeline E2E Suite")
}

var _ = Describe("Progressive Pipeline and ISBService E2E", Serial, func() {

	It("Should create initial rollout objects", func() {
		CreateNumaflowControllerRollout(PrimaryNumaflowControllerVersion)
		CreateISBServiceRollout(isbServiceRolloutName, initialISBServiceSpec)
	})

	It("Should validate Pipeline and ISBService upgrade failure followed by success using Progressive strategy", func() {
		CreateInitialPipelineRollout(pipelineRolloutName, GetInstanceName(isbServiceRolloutName, 0), initialPipelineSpec, defaultStrategy)

		By("Updating the Pipeline Topology to cause a Progressive change - Failure case")
		updatedPipelineSpec := initialPipelineSpec.DeepCopy()
		updatedPipelineSpec.Vertices[1].UDF = &numaflowv1.UDF{
			Container: &numaflowv1.Container{
				Image:           "badcat",
				ImagePullPolicy: &pullPolicyAlways,
			},
		}
		UpdatePipeline(pipelineRolloutName, *updatedPipelineSpec)

		updatedISBServiceSpec := updateISBServiceForFailure()

		VerifyPipelineProgressiveFailure(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 1), initialPipelineSpec, *updatedPipelineSpec)

		By("Updating the Pipeline Topology to cause a Progressive change - Successful case")
		updatedPipelineSpec = initialPipelineSpec.DeepCopy()
		UpdatePipeline(pipelineRolloutName, *updatedPipelineSpec)

		By("Updating the ISBService to cause a Progressive change - Successful case")
		updatedISBServiceSpec.JetStream.Version = validJetstreamVersion
		UpdateISBService(isbServiceRolloutName, *updatedISBServiceSpec)

		VerifyPipelineProgressiveSuccess(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 2), false, *updatedPipelineSpec)
		VerifyPipelineDeletion(GetInstanceName(pipelineRolloutName, 1))

		// Verify ISBServiceRollout Progressive Status
		VerifyISBServiceProgressiveSuccess(isbServiceRolloutName, GetInstanceName(isbServiceRolloutName, 0), GetInstanceName(isbServiceRolloutName, 2))

		// Verify in-progress-strategy no longer set
		VerifyISBServiceRolloutInProgressStrategy(isbServiceRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyISBServiceRolloutInProgressStrategyConsistently(isbServiceRolloutName, apiv1.UpgradeStrategyNoOp)

		DeletePipelineRollout(pipelineRolloutName)
	})

	It("Should validate ISBService as failure when Pipeline upgrade assesses in failure", func() {
		CreateInitialPipelineRollout(pipelineRolloutName, GetInstanceName(isbServiceRolloutName, 2), initialPipelineSpec, defaultStrategy)

		By("Updating the Pipeline Topology to cause a Progressive change - Invalid change causing failure")
		updatedPipelineSpec := initialPipelineSpec.DeepCopy()
		updatedPipelineSpec.Vertices[1].UDF = &numaflowv1.UDF{Builtin: &numaflowv1.Function{
			Name: "badcat",
		}}
		UpdatePipeline(pipelineRolloutName, *updatedPipelineSpec)

		By("Updating the ISBService to cause a Progressive change - Valid change")
		updatedISBServiceSpec := initialISBServiceSpec.DeepCopy()
		updatedISBServiceSpec.JetStream.Version = initialJetstreamVersion
		UpdateISBService(isbServiceRolloutName, *updatedISBServiceSpec)

		VerifyPipelineProgressiveFailure(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 1), initialPipelineSpec, *updatedPipelineSpec)

		// Verify ISBServiceRollout Progressive Status
		VerifyISBServiceProgressiveFailure(isbServiceRolloutName, GetInstanceName(isbServiceRolloutName, 2), GetInstanceName(isbServiceRolloutName, 3))

		By("Updating the Pipeline to set the 'force promote' Label")
		UpdatePipelineInK8S(GetInstanceName(pipelineRolloutName, 1), func(pipeline *unstructured.Unstructured) (*unstructured.Unstructured, error) {
			labels := pipeline.GetLabels()
			if labels == nil {
				labels = make(map[string]string)
			}
			labels[common.LabelKeyForcePromote] = "true"
			pipeline.SetLabels(labels)
			return pipeline, nil
		})

		VerifyPipelineProgressiveSuccess(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 1), true, initialPipelineSpec)

		By("Updating the ISBService to set the 'force promote' Label")
		UpdateISBServiceInK8S(GetInstanceName(isbServiceRolloutName, 3), func(isbservice *unstructured.Unstructured) (*unstructured.Unstructured, error) {
			labels := isbservice.GetLabels()
			if labels == nil {
				labels = make(map[string]string)
			}
			labels[common.LabelKeyForcePromote] = "true"
			isbservice.SetLabels(labels)
			return isbservice, nil
		})

		VerifyISBServiceProgressiveSuccess(isbServiceRolloutName, GetInstanceName(isbServiceRolloutName, 2), GetInstanceName(isbServiceRolloutName, 3))
		// Verify in-progress-strategy no longer set
		VerifyISBServiceRolloutInProgressStrategy(isbServiceRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyISBServiceRolloutInProgressStrategyConsistently(isbServiceRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyISBServiceDeletion(GetInstanceName(isbServiceRolloutName, 2))

		DeletePipelineRollout(pipelineRolloutName)
	})

	It("Should validate Pipeline as failure when ISBService upgrade assesses in failure", func() {
		CreateInitialPipelineRollout(pipelineRolloutName, GetInstanceName(isbServiceRolloutName, 3), initialPipelineSpec, defaultStrategy)

		promotedISBSvc, initialISBServiceSpec, _, err := GetPromotedISBServiceSpecAndStatus(Namespace, isbServiceRolloutName)
		Expect(err).ShouldNot(HaveOccurred())
		promotedPipelineName, err := GetPromotedPipelineName(Namespace, pipelineRolloutName)
		Expect(err).ShouldNot(HaveOccurred())

		_ = updateISBServiceForFailure()

		VerifyPipelineProgressiveFailure(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 1), initialPipelineSpec, initialPipelineSpec)
		// Verify ISBServiceRollout Progressive Status
		VerifyISBServiceProgressiveFailure(isbServiceRolloutName, GetInstanceName(isbServiceRolloutName, 3), GetInstanceName(isbServiceRolloutName, 4))

		// Now put the isbsvc spec back to what it was before; this will cause the isbsvc and pipeline to both go back to just the "promoted" one
		UpdateISBService(isbServiceRolloutName, initialISBServiceSpec)
		VerifyPipelineDeletion(GetInstanceName(pipelineRolloutName, 1))     // the "Upgrading" one
		VerifyISBServiceDeletion(GetInstanceName(isbServiceRolloutName, 4)) // the "Upgrading" one
		CheckConsistently("verifying just the original promoted Pipeline remains", func() bool {
			pipelineName, _ := GetPromotedPipelineName(Namespace, pipelineRolloutName)
			return GetNumberOfChildren(GetGVRForPipeline(), Namespace, pipelineRolloutName) == 1 && pipelineName == promotedPipelineName
		}).Should(BeTrue())

		CheckConsistently("verifying just the original promoted InterstepBufferService remains", func() bool {
			isbsvcName, _ := GetPromotedISBServiceName(Namespace, isbServiceRolloutName)
			return GetNumberOfChildren(GetGVRForISBService(), Namespace, isbServiceRolloutName) == 1 && isbsvcName == promotedISBSvc.GetName()
		}).Should(BeTrue())

		DeletePipelineRollout(pipelineRolloutName)
	})

	It("Should validate ISBServiceRollout and PipelineRollout ForcePromote Strategy works", func() {
		// Create a PipelineRollout with forcePromote=true
		CreateInitialPipelineRollout(pipelineRolloutName, GetInstanceName(isbServiceRolloutName, 3), initialPipelineSpec, apiv1.PipelineStrategy{
			PipelineTypeRolloutStrategy: apiv1.PipelineTypeRolloutStrategy{
				PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
					Progressive: apiv1.ProgressiveStrategy{
						ForcePromote: true,
					},
				},
			},
		})

		// Update ISBServiceRollout to set forcePromote=true
		UpdateISBServiceRolloutInK8S(isbServiceRolloutName, func(ir apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
			ir.Spec.Strategy = &apiv1.ISBServiceRolloutStrategy{
				Progressive: apiv1.ProgressiveStrategy{
					ForcePromote: true,
				},
			}
			return ir, nil
		})

		By("Updating the Pipeline Topology to cause a Progressive change - Invalid change causing failure")
		updatedPipelineSpec := initialPipelineSpec.DeepCopy()
		updatedPipelineSpec.Vertices[1].UDF = &numaflowv1.UDF{
			Container: &numaflowv1.Container{
				Image:           "badcat",
				ImagePullPolicy: &pullPolicyAlways,
			},
			//Builtin: &numaflowv1.Function{
			//	Name: "badcat",
			//}
		}
		UpdatePipeline(pipelineRolloutName, *updatedPipelineSpec)

		By("Updating the ISBService to cause a Progressive change - Valid change")
		updatedISBServiceSpec := initialISBServiceSpec.DeepCopy()
		updatedISBServiceSpec.JetStream.Version = UpdatedJetstreamVersion
		UpdateISBService(isbServiceRolloutName, *updatedISBServiceSpec)

		// Had to remove this check due to the fact that things can happen in a different order
		// See https://github.com/numaproj/numaplane/pull/865 for details
		//VerifyPipelineProgressiveSuccess(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 1), true, *updatedPipelineSpec)

		// Verify ISBServiceRollout Progressive Status shows success
		VerifyISBServiceRolloutProgressiveStatus(isbServiceRolloutName, GetInstanceName(isbServiceRolloutName, 3), GetInstanceName(isbServiceRolloutName, 5), apiv1.AssessmentResultSuccess)

		// check that the Pipeline and ISBSvc got promoted (specs are correct)
		VerifyPromotedISBServiceSpec(Namespace, isbServiceRolloutName, func(spec numaflowv1.InterStepBufferServiceSpec) bool {
			return spec.JetStream.Version == UpdatedJetstreamVersion
		})

		VerifyPromotedPipelineSpec(Namespace, pipelineRolloutName, func(spec numaflowv1.PipelineSpec) bool {
			return spec.Vertices[1].UDF.Container.Image == "badcat"
		})

		// make sure there's only 1 promoted pipeline and isbsvc and no upgrading ones anymore
		checkOnePromotedPipelineAndISBSvc := func() bool {
			promotedPipelines, err := GetChildrenOfUpgradeStrategy(GetGVRForPipeline(), Namespace, pipelineRolloutName, common.LabelValueUpgradePromoted)
			if err != nil {
				return false
			}
			promotedISBSvcs, err := GetChildrenOfUpgradeStrategy(GetGVRForISBService(), Namespace, isbServiceRolloutName, common.LabelValueUpgradePromoted)
			if err != nil {
				return false
			}
			return promotedPipelines != nil && len(promotedPipelines.Items) == 1 && promotedISBSvcs != nil && len(promotedISBSvcs.Items) == 1
		}

		checkNoUpgradingPipelineOrISBSvc := func() bool {
			upgradingPipelines, err := GetUpgradingPipelines(Namespace, pipelineRolloutName)
			if err != nil {
				return false
			}
			upgradingISBSvcs, err := GetUpgradingISBServices(Namespace, isbServiceRolloutName)
			if err != nil {
				return false
			}
			return upgradingPipelines != nil && len(upgradingPipelines.Items) == 0 && upgradingISBSvcs != nil && len(upgradingISBSvcs.Items) == 0
		}

		CheckEventually("verify only 1 Promoted Pipeline and 1 Promoted ISBSvc", checkOnePromotedPipelineAndISBSvc).Should(BeTrue())
		CheckEventually("verify no Upgrading Pipeline or ISBSvc", checkNoUpgradingPipelineOrISBSvc).Should(BeTrue())
		CheckConsistently("verify only 1 Promoted Pipeline and 1 Promoted ISBSvc consistently", checkOnePromotedPipelineAndISBSvc).Should(BeTrue())
		CheckConsistently("verify no Upgrading Pipeline or ISBSvc consistently", checkNoUpgradingPipelineOrISBSvc).Should(BeTrue())

		// Verify in-progress-strategy no longer set on both PipelineRollout and ISBServiceRollout (rollout completed)
		VerifyISBServiceRolloutInProgressStrategy(isbServiceRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyISBServiceRolloutInProgressStrategyConsistently(isbServiceRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyPipelineRolloutInProgressStrategyConsistently(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

		DeletePipelineRollout(pipelineRolloutName)

	})

	It("Should delete all remaining rollout objects", func() {
		DeleteISBServiceRollout(isbServiceRolloutName)
		DeleteNumaflowControllerRollout()
	})
})

func updateISBServiceForFailure() *numaflowv1.InterStepBufferServiceSpec {
	By("Updating the ISBService to cause a Progressive change - Invalid change causing failure")
	updatedISBServiceSpec := initialISBServiceSpec.DeepCopy()
	updatedISBServiceSpec.JetStream.Version = invalidJetstreamVersion
	UpdateISBService(isbServiceRolloutName, *updatedISBServiceSpec)

	return updatedISBServiceSpec
}
