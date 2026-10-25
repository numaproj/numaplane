/*
Copyright 2026.

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
	"fmt"
	"testing"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/numaproj/numaplane/internal/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	pipelineRolloutName   = "test-pipeline-rollout"
	isbServiceRolloutName = "test-isbservice-rollout"
	monoVertexRolloutName = "test-monovertex-rollout"

	pendingPipelineName   = "pending-pipeline-rollout"
	pendingMonoVertexName = "pending-monovertex-rollout"
	pendingISBName        = "pending-isbservice-rollout"
	pendingPairISBName    = "pending-pair-isbservice-rollout"
	pendingPairPipeline   = "pending-pair-pipeline-rollout"

	stagger = 30 * time.Second
)

func TestProgressiveNCFailureE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	BeforeSuite(func() {
		BeforeSuiteSetup()
	})
	RunSpecs(t, "Progressive NumaflowController failure E2E Suite")
}

var _ = Describe("Failed progressive NumaflowController rollout", Ordered, Serial, func() {

	It("creates the controller and its dependents", func() {
		SetupProgressiveControllerStack(InitialNumaflowControllerVersion, isbServiceRolloutName, pipelineRolloutName, monoVertexRolloutName, FailureWorkloadSchedule)
	})

	It("keeps a broken controller trial in place until the version is reverted", func() {
		SetNumaflowControllerVersion(BrokenNumaflowControllerVersion, ControllerFailureSchedule)
		trial := WaitForTrialNumaflowController(BrokenNumaflowControllerVersion)

		By("checking dependents stay bound to the failed trial")
		VerifyTrialWorkloadsOnInstance(pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName, trial.Spec.InstanceID)

		By("applying a Pipeline change on the current trial stack")
		UpdatePipeline(pipelineRolloutName, ProgressivePipelineSpecWithEnv(isbServiceRolloutName, "rollback"))
		VerifyTrialWorkloadsOnInstance(pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName, trial.Spec.InstanceID)

		VerifyNumaflowControllerAssessmentForVersion(apiv1.AssessmentResultFailure, BrokenNumaflowControllerVersion)
		VerifyNumaflowControllerAssessmentStaysForVersion(apiv1.AssessmentResultFailure, BrokenNumaflowControllerVersion)
		VerifyPromotedNumaflowControllerVersion(InitialNumaflowControllerVersion)

		By("reverting the controller version")
		SetNumaflowControllerVersion(InitialNumaflowControllerVersion, ControllerSuccessSchedule)
		VerifyNumaflowControllerDiscontinued(InitialNumaflowControllerVersion)
		VerifyNoLiveChildOnInstance(GetGVRForPipeline(), pipelineRolloutName, trial.Spec.InstanceID)
		VerifyNoLiveChildOnInstance(GetGVRForISBService(), isbServiceRolloutName, trial.Spec.InstanceID)
		VerifyNoLiveChildOnInstance(GetGVRForMonoVertex(), monoVertexRolloutName, trial.Spec.InstanceID)

		By("checking the Pipeline change is promoted on the previous controller")
		promoted := PromotedNumaflowController()
		Expect(promoted.Spec.Version).Should(Equal(InitialNumaflowControllerVersion))
		VerifyPromotedPipelineEnv(pipelineRolloutName, "rollback")
		VerifyPromotedWorkloadsOnInstance(pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName, promoted.Spec.InstanceID)
		VerifyPromotedPipelineRunning(Namespace, pipelineRolloutName)
	})

	It("ignores force-promote on a failed chain and promotes the chain when force-promote-chain is set", func() {
		SetNumaflowControllerVersion(BrokenNumaflowControllerVersion, ControllerFailureSchedule)
		trial := WaitForTrialNumaflowController(BrokenNumaflowControllerVersion)
		VerifyNumaflowControllerAssessmentForVersion(apiv1.AssessmentResultFailure, BrokenNumaflowControllerVersion)

		By("setting force-promote, which does not promote a failed chain")
		UpdateNumaflowControllerInK8S(trial.Name, func(controller *apiv1.NumaflowController) error {
			SetObjectLabel(controller, common.LabelKeyForcePromote, "true")
			return nil
		})
		VerifyNumaflowControllerAssessmentStaysForVersion(apiv1.AssessmentResultFailure, BrokenNumaflowControllerVersion)
		VerifyPromotedNumaflowControllerVersion(InitialNumaflowControllerVersion)

		By("setting force-promote-chain on the failed trial")
		UpdateNumaflowControllerInK8S(trial.Name, func(controller *apiv1.NumaflowController) error {
			SetObjectLabel(controller, common.LabelKeyForcePromoteChain, "true")
			return nil
		})
		VerifyNumaflowControllerForcePromoted(BrokenNumaflowControllerVersion)
		VerifyPromotedWorkloadsOnInstance(pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName, trial.Spec.InstanceID)

		By("returning to the healthy controller so later tests can run")
		// The one-minute failure window is for a bad image. A controller that just started needs
		// longer before the Pipeline daemon is available, and a recorded Failure is not cleared.
		SetPipelineAssessmentSchedule(pipelineRolloutName, RecoverWorkloadSchedule)
		SetMonoVertexAssessmentSchedule(monoVertexRolloutName, RecoverWorkloadSchedule)
		SetNumaflowControllerVersion(InitialNumaflowControllerVersion, ControllerSuccessSchedule)
		VerifySuccessfulControllerUpgrade(InitialNumaflowControllerVersion, pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName)
		SetPipelineAssessmentSchedule(pipelineRolloutName, FailureWorkloadSchedule)
		SetMonoVertexAssessmentSchedule(monoVertexRolloutName, FailureWorkloadSchedule)
	})

	It("fails the controller trial when the Pipeline and controller change together", func() {
		failPipelineAndControllerTogether()
		restoreHealthyPipelineAndController()
	})

	It("fails the controller trial when the controller changes and the Pipeline changes 30 seconds later", func() {
		SetNumaflowControllerVersion(UpdatedNumaflowControllerVersion, ControllerSuccessSchedule)
		time.Sleep(stagger)
		UpdatePipeline(pipelineRolloutName, ProgressivePipelineSpecWithBadImage(isbServiceRolloutName))
		verifyPipelineFailedTheController(InitialNumaflowControllerVersion, UpdatedNumaflowControllerVersion)
		restoreHealthyPipelineAndController()
	})

	It("fails the controller trial when the Pipeline changes and the controller changes 30 seconds later", func() {
		UpdatePipeline(pipelineRolloutName, ProgressivePipelineSpecWithBadImage(isbServiceRolloutName))
		time.Sleep(stagger)
		SetNumaflowControllerVersion(UpdatedNumaflowControllerVersion, ControllerSuccessSchedule)
		verifyPipelineFailedTheController(InitialNumaflowControllerVersion, UpdatedNumaflowControllerVersion)
		restoreHealthyPipelineAndController()
	})

	It("promotes the controller, ISBService, and Pipeline from one force-promote-chain label", func() {
		SetNumaflowControllerVersion(UpdatedNumaflowControllerVersion, ControllerSuccessSchedule)
		UpdateISBService(isbServiceRolloutName, ProgressiveISBSpec(UpdatedJetstreamVersion))
		UpdatePipeline(pipelineRolloutName, ProgressivePipelineSpecWithBadImage(isbServiceRolloutName))
		verifyPipelineFailedTheController(InitialNumaflowControllerVersion, UpdatedNumaflowControllerVersion)

		By("setting force-promote-chain on the Pipeline trial")
		labelTrialPipelineForcePromoteChain()

		VerifyNumaflowControllerForcePromoted(UpdatedNumaflowControllerVersion)
		VerifyPromotedISBVersion(isbServiceRolloutName, UpdatedJetstreamVersion)
		VerifyPromotedPipelineSpec(Namespace, pipelineRolloutName, func(spec numaflowv1.PipelineSpec) bool {
			return len(spec.Vertices) > 1 && spec.Vertices[1].UDF != nil && spec.Vertices[1].UDF.Container != nil &&
				spec.Vertices[1].UDF.Container.Image == "badcat"
		})

		By("restoring a Pipeline image the later tests can run")
		UpdatePipeline(pipelineRolloutName, ProgressivePipelineSpecWithEnv(isbServiceRolloutName, "restored"))
		VerifyPromotedPipelineEnv(pipelineRolloutName, "restored")
		VerifyPromotedPipelineRunning(Namespace, pipelineRolloutName)
	})

	It("fails the controller chain when a MonoVertex image cannot be pulled", func() {
		promotedVersion := PromotedNumaflowController().Spec.Version
		trialVersion := otherControllerVersion(promotedVersion)
		SetNumaflowControllerVersion(trialVersion, ControllerSuccessSchedule)
		UpdateMonoVertexSpec(monoVertexRolloutName, ProgressiveMonoVertexSpecWithBadImage())

		VerifyNumaflowControllerChainFailed()
		VerifyNumaflowControllerFailureIncludes("MonoVertexRollout " + monoVertexRolloutName + " failed")
		VerifyPromotedNumaflowControllerVersion(promotedVersion)

		SetNumaflowControllerVersion(promotedVersion, ControllerSuccessSchedule)
		VerifyNumaflowControllerDiscontinued(promotedVersion)
		UpdateMonoVertexSpec(monoVertexRolloutName, ProgressiveMonoVertexSpec())
		VerifyMonoVertexRolloutInProgressStrategy(monoVertexRolloutName, apiv1.UpgradeStrategyNoOp)
		Expect(VerifyPromotedMonoVertexRunning(Namespace, monoVertexRolloutName)).Should(Succeed())
	})

	It("promotes the chain only after every failed dependent is fixed", func() {
		promotedVersion, trialVersion, _ := failChainWithBadPipeline()
		UpdateMonoVertexSpec(monoVertexRolloutName, ProgressiveMonoVertexSpecWithBadImage())
		VerifyNumaflowControllerFailureIncludes("MonoVertexRollout " + monoVertexRolloutName + " failed")

		By("fixing only the Pipeline")
		UpdatePipeline(pipelineRolloutName, ProgressivePipelineSpecWithEnv(isbServiceRolloutName, "pipeline-fixed"))
		VerifyPipelineResourceAssessment(pipelineRolloutName, apiv1.AssessmentResultSuccess)
		VerifyNumaflowControllerChainFailed()
		VerifyPromotedNumaflowControllerVersion(promotedVersion)

		By("fixing the MonoVertex")
		UpdateMonoVertexSpec(monoVertexRolloutName, ProgressiveMonoVertexSpec())
		VerifySuccessfulControllerUpgrade(trialVersion, pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName)
		VerifyPromotedPipelineEnv(pipelineRolloutName, "pipeline-fixed")
	})

	It("creates the fixed ISBService on the trial controller and then promotes the chain", func() {
		promotedVersion := PromotedNumaflowController().Spec.Version
		trialVersion := otherControllerVersion(promotedVersion)
		SetNumaflowControllerVersion(trialVersion, ControllerSuccessSchedule)
		trial := WaitForTrialNumaflowController(trialVersion)

		SetISBServiceAssessmentSchedule(isbServiceRolloutName, ControllerFailureSchedule)
		UpdateISBService(isbServiceRolloutName, ProgressiveISBSpec(BrokenJetstreamVersion))
		failedISB := WaitForTrialChildOnInstance(GetGVRForISBService(), isbServiceRolloutName, trial.Spec.InstanceID)
		VerifyNumaflowControllerChainFailed()
		VerifyNumaflowControllerFailureIncludes("ISBServiceRollout " + isbServiceRolloutName + " failed")
		VerifyPromotedNumaflowControllerVersion(promotedVersion)

		By("replacing the failed ISBService on the trial controller")
		// The Pipeline trial on the new ISBService waits for JetStream before its daemon is available.
		SetPipelineAssessmentSchedule(pipelineRolloutName, RecoverWorkloadSchedule)
		SetISBServiceAssessmentSchedule(isbServiceRolloutName, ISBSchedule)
		UpdateISBService(isbServiceRolloutName, ProgressiveISBSpec(UpdatedJetstreamVersion))
		fixedISB := WaitForNewerTrialChildOnInstance(GetGVRForISBService(), isbServiceRolloutName, trial.Spec.InstanceID, failedISB.GetName())
		CheckEventually("verifying the trial Pipeline uses the fixed ISBService", func() bool {
			pipelines, err := GetUpgradingPipelines(Namespace, pipelineRolloutName)
			if err != nil {
				return false
			}
			for i := range pipelines.Items {
				if ControllerInstanceIDOf(&pipelines.Items[i]) == trial.Spec.InstanceID &&
					PipelineInterStepBufferServiceName(&pipelines.Items[i]) == fixedISB.GetName() {
					return true
				}
			}
			return false
		}).Should(BeTrue())

		VerifySuccessfulControllerUpgrade(trialVersion, pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName)
		SetPipelineAssessmentSchedule(pipelineRolloutName, FailureWorkloadSchedule)
		VerifyPromotedISBVersion(isbServiceRolloutName, UpdatedJetstreamVersion)
		VerifyPromotedPipelineSpec(Namespace, pipelineRolloutName, func(spec numaflowv1.PipelineSpec) bool {
			return spec.InterStepBufferServiceName == fixedISB.GetName()
		})
	})

	It("promotes the chain after the failed Pipeline is given a good image", func() {
		promotedVersion, trialVersion, trialInstance := failChainWithBadPipeline()
		failed := WaitForTrialChildOnInstance(GetGVRForPipeline(), pipelineRolloutName, trialInstance)
		isb := WaitForTrialChildOnInstance(GetGVRForISBService(), isbServiceRolloutName, trialInstance)

		UpdatePipeline(pipelineRolloutName, ProgressivePipelineSpecWithEnv(isbServiceRolloutName, "good-image"))
		replacement := WaitForNewerTrialChildOnInstance(GetGVRForPipeline(), pipelineRolloutName, trialInstance, failed.GetName())
		Expect(PipelineInterStepBufferServiceName(replacement)).Should(Equal(isb.GetName()))

		VerifySuccessfulControllerUpgrade(trialVersion, pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName)
		VerifyPromotedNumaflowControllerVersion(trialVersion)
		Expect(trialVersion).ShouldNot(Equal(promotedVersion))
		VerifyPromotedPipelineEnv(pipelineRolloutName, "good-image")
	})

	It("creates a new Pipeline trial on the current controller and ISBService when the Pipeline spec is reverted", func() {
		_, trialVersion, trialInstance := failChainWithBadPipeline()
		promotedName, err := GetPromotedPipelineName(Namespace, pipelineRolloutName)
		Expect(err).ShouldNot(HaveOccurred())
		failed := WaitForTrialChildOnInstance(GetGVRForPipeline(), pipelineRolloutName, trialInstance)
		isb := WaitForTrialChildOnInstance(GetGVRForISBService(), isbServiceRolloutName, trialInstance)

		UpdatePipeline(pipelineRolloutName, ProgressivePipelineSpec(isbServiceRolloutName))
		replacement := WaitForNewerTrialChildOnInstance(GetGVRForPipeline(), pipelineRolloutName, trialInstance, failed.GetName())
		Expect(replacement.GetName()).ShouldNot(Equal(promotedName))
		Expect(PipelineInterStepBufferServiceName(replacement)).Should(Equal(isb.GetName()))
		currentPromoted, err := GetPromotedPipelineName(Namespace, pipelineRolloutName)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(currentPromoted).Should(Equal(promotedName))

		VerifySuccessfulControllerUpgrade(trialVersion, pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName)
		VerifyPromotedPipelineRunning(Namespace, pipelineRolloutName)
	})

	It("creates held workloads on the controller that the chain promotes", func() {
		_, trialVersion, trialInstance := failChainWithBadPipeline()
		createHeldWorkloads()

		fixFailedPipelineOnTrial("chain-succeeded", trialInstance)
		By("checking the held workloads wait for the chain to succeed, not just for the failed Pipeline to be replaced")
		VerifyPipelineRolloutHeldPending(pendingPipelineName)
		VerifySuccessfulControllerUpgrade(trialVersion, pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName)
		SetPipelineAssessmentSchedule(pipelineRolloutName, FailureWorkloadSchedule)
		verifyHeldWorkloadsOnInstance(trialInstance)
		deleteHeldWorkloads()
	})

	It("creates held workloads on the previous controller when the controller is reverted", func() {
		promotedVersion, _, trialInstance := failChainWithBadPipeline()
		createHeldWorkloads()

		SetNumaflowControllerVersion(promotedVersion, ControllerSuccessSchedule)
		VerifyNumaflowControllerDiscontinued(promotedVersion)
		VerifyNoLiveChildOnInstance(GetGVRForPipeline(), pipelineRolloutName, trialInstance)

		promoted := PromotedNumaflowController()
		verifyHeldWorkloadsOnInstance(promoted.Spec.InstanceID)
		deleteHeldWorkloads()

		UpdatePipeline(pipelineRolloutName, ProgressivePipelineSpec(isbServiceRolloutName))
		VerifyPromotedPipelineRunning(Namespace, pipelineRolloutName)
	})

	It("creates held workloads on the controller that force-promote-chain promotes", func() {
		_, trialVersion, trialInstance := failChainWithBadPipeline()
		createHeldWorkloads()

		labelTrialPipelineForcePromoteChain()
		VerifyNumaflowControllerForcePromoted(trialVersion)
		verifyHeldWorkloadsOnInstance(trialInstance)
		deleteHeldWorkloads()

		UpdatePipeline(pipelineRolloutName, ProgressivePipelineSpecWithEnv(isbServiceRolloutName, "after-force-promote"))
		VerifyPromotedPipelineEnv(pipelineRolloutName, "after-force-promote")
		VerifyPromotedPipelineRunning(Namespace, pipelineRolloutName)
	})

	It("creates a new ISBService before the new Pipeline once the chain is resolved", func() {
		_, trialVersion, trialInstance := failChainWithBadPipeline()
		CreateISBServiceRolloutObject(pendingPairISBName, ProgressiveISBSpec(InitialJetstreamVersion))
		CreatePipelineRolloutObject(pendingPairPipeline, ProgressivePipelineSpec(pendingPairISBName), nil)
		VerifyISBServiceRolloutHeldPending(pendingPairISBName)
		VerifyPipelineRolloutHeldPending(pendingPairPipeline)

		fixFailedPipelineOnTrial("pair-released", trialInstance)
		VerifyISBServiceRolloutHeldPending(pendingPairISBName)
		VerifySuccessfulControllerUpgrade(trialVersion, pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName)
		SetPipelineAssessmentSchedule(pipelineRolloutName, FailureWorkloadSchedule)

		promoted := PromotedNumaflowController()
		VerifyFirstChildOnInstance(GetGVRForISBService(), pendingPairISBName, promoted.Spec.InstanceID)
		VerifyFirstChildOnInstance(GetGVRForPipeline(), pendingPairPipeline, promoted.Spec.InstanceID)

		isb, err := GetResource(GetGVRForISBService(), Namespace, pendingPairISBName+"-0")
		Expect(err).ShouldNot(HaveOccurred())
		pipeline, err := GetResource(GetGVRForPipeline(), Namespace, pendingPairPipeline+"-0")
		Expect(err).ShouldNot(HaveOccurred())
		Expect(pipeline.GetCreationTimestamp().Time.Before(isb.GetCreationTimestamp().Time)).Should(BeFalse())
		Expect(PipelineInterStepBufferServiceName(pipeline)).Should(Equal(isb.GetName()))

		DeletePipelineRollout(pendingPairPipeline)
		DeleteISBServiceRollout(pendingPairISBName)
	})

	It("deletes the remaining rollouts", func() {
		DeleteProgressiveControllerStack(
			[]string{pipelineRolloutName},
			[]string{monoVertexRolloutName},
			[]string{isbServiceRolloutName},
		)
	})
})

func otherControllerVersion(version string) string {
	if version == InitialNumaflowControllerVersion {
		return UpdatedNumaflowControllerVersion
	}
	return InitialNumaflowControllerVersion
}

func failPipelineAndControllerTogether() {
	SetNumaflowControllerVersion(UpdatedNumaflowControllerVersion, ControllerSuccessSchedule)
	UpdatePipeline(pipelineRolloutName, ProgressivePipelineSpecWithBadImage(isbServiceRolloutName))
	verifyPipelineFailedTheController(InitialNumaflowControllerVersion, UpdatedNumaflowControllerVersion)
}

func failChainWithBadPipeline() (promotedVersion, trialVersion, trialInstanceID string) {
	promoted := PromotedNumaflowController()
	promotedVersion = promoted.Spec.Version
	trialVersion = otherControllerVersion(promotedVersion)
	SetNumaflowControllerVersion(trialVersion, ControllerSuccessSchedule)
	UpdatePipeline(pipelineRolloutName, ProgressivePipelineSpecWithBadImage(isbServiceRolloutName))
	verifyPipelineFailedTheController(promotedVersion, trialVersion)
	trial := WaitForTrialNumaflowController(trialVersion)
	return promotedVersion, trialVersion, trial.Spec.InstanceID
}

func verifyPipelineFailedTheController(promotedVersion, trialVersion string) {
	VerifyNumaflowControllerChainFailed()
	VerifyNumaflowControllerFailureIncludes("PipelineRollout " + pipelineRolloutName + " failed")
	VerifyPromotedNumaflowControllerVersion(promotedVersion)
	trial := WaitForTrialNumaflowController(trialVersion)
	// The controller change replaces the Pipeline trial, so the total child count moves while the
	// old child is recycled. Once the new trial is on this controller, the live set should stay put.
	VerifyWorkloadsHaveChildOnInstance(pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName, trial.Spec.InstanceID)
	count := GetNumberOfNonRecyclableChildren(GetGVRForPipeline(), Namespace, pipelineRolloutName)
	CheckConsistently(fmt.Sprintf("verifying PipelineRollout %s keeps %d live children", pipelineRolloutName, count), func() int {
		return GetNumberOfNonRecyclableChildren(GetGVRForPipeline(), Namespace, pipelineRolloutName)
	}).WithTimeout(10 * time.Second).Should(Equal(count))
}

func restoreHealthyPipelineAndController() {
	SetNumaflowControllerVersion(InitialNumaflowControllerVersion, ControllerSuccessSchedule)
	VerifyNumaflowControllerDiscontinued(InitialNumaflowControllerVersion)
	UpdatePipeline(pipelineRolloutName, ProgressivePipelineSpecWithEnv(isbServiceRolloutName, "restored-initial"))
	VerifyPromotedPipelineEnv(pipelineRolloutName, "restored-initial")
	VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
	VerifyPromotedPipelineRunning(Namespace, pipelineRolloutName)
}

// fixFailedPipelineOnTrial replaces the failed Pipeline trial with a healthy one on the trial controller.
// FailureWorkloadSchedule records a sticky Failure if the new daemon is still rolling after one minute.
// Callers restore FailureWorkloadSchedule once the chain is promoted.
func fixFailedPipelineOnTrial(env, trialInstance string) {
	failed := WaitForTrialChildOnInstance(GetGVRForPipeline(), pipelineRolloutName, trialInstance)
	SetPipelineAssessmentSchedule(pipelineRolloutName, RecoverWorkloadSchedule)
	UpdatePipeline(pipelineRolloutName, ProgressivePipelineSpecWithEnv(isbServiceRolloutName, env))
	WaitForNewerTrialChildOnInstance(GetGVRForPipeline(), pipelineRolloutName, trialInstance, failed.GetName())
}

func createHeldWorkloads() {
	CreatePipelineRolloutObject(pendingPipelineName, ProgressivePipelineSpec(isbServiceRolloutName), nil)
	CreateMonoVertexRolloutObject(pendingMonoVertexName, ProgressiveMonoVertexSpec(), nil)
	CreateISBServiceRolloutObject(pendingISBName, ProgressiveISBSpec(InitialJetstreamVersion))
	VerifyPipelineRolloutHeldPending(pendingPipelineName)
	VerifyMonoVertexRolloutHeldPending(pendingMonoVertexName)
	VerifyISBServiceRolloutHeldPending(pendingISBName)
}

func verifyHeldWorkloadsOnInstance(instanceID string) {
	VerifyFirstChildOnInstance(GetGVRForPipeline(), pendingPipelineName, instanceID)
	VerifyFirstChildOnInstance(GetGVRForMonoVertex(), pendingMonoVertexName, instanceID)
	VerifyFirstChildOnInstance(GetGVRForISBService(), pendingISBName, instanceID)
}

func deleteHeldWorkloads() {
	DeletePipelineRollout(pendingPipelineName)
	DeleteMonoVertexRollout(pendingMonoVertexName)
	DeleteISBServiceRollout(pendingISBName)
}

func labelTrialPipelineForcePromoteChain() {
	var name string
	CheckEventually("verifying the trial Pipeline name", func() bool {
		status := GetPipelineRolloutProgressiveStatus(pipelineRolloutName)
		if status.UpgradingPipelineStatus == nil || status.UpgradingPipelineStatus.Name == "" {
			return false
		}
		name = status.UpgradingPipelineStatus.Name
		return true
	}).Should(BeTrue())
	UpdatePipelineInK8S(name, func(pipeline *unstructured.Unstructured) (*unstructured.Unstructured, error) {
		SetObjectLabel(pipeline, common.LabelKeyForcePromoteChain, "true")
		return pipeline, nil
	})
}
