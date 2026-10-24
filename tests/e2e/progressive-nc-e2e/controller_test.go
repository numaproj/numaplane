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
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	pipelineRolloutName   = "test-pipeline-rollout"
	isbServiceRolloutName = "test-isbservice-rollout"
	monoVertexRolloutName = "test-monovertex-rollout"

	slowPipelineRolloutName   = "slow-pipeline-rollout"
	slowISBServiceRolloutName = "slow-isbservice-rollout"
)

func TestProgressiveNCE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	BeforeSuite(func() {
		BeforeSuiteSetup()
	})
	RunSpecs(t, "Progressive NumaflowController E2E Suite")
}

var _ = Describe("Progressive NumaflowController rollout", Ordered, Serial, func() {

	It("creates the controller and its dependents", func() {
		SetupProgressiveControllerStack(InitialNumaflowControllerVersion, isbServiceRolloutName, pipelineRolloutName, monoVertexRolloutName, FastWorkloadSchedule)
	})

	It("promotes a healthy controller and moves the ISBService, Pipeline, and MonoVertex onto it", func() {
		promoted := PromotedNumaflowController()
		promotedResources := SnapshotControllerResources(promoted.Name)

		By("bumping only the controller version")
		SetNumaflowControllerVersion(UpdatedNumaflowControllerVersion, ControllerSuccessSchedule)
		trial := WaitForTrialNumaflowController(UpdatedNumaflowControllerVersion)
		Expect(trial.Spec.InstanceID).ShouldNot(BeEmpty())

		By("checking the two controllers leave each other's Deployments, ConfigMaps, and Roles alone")
		VerifyControllerResourceNamesDisjoint(promoted.Name, trial.Name)
		VerifyControllerResourcesUnchanged(promoted.Name, promotedResources)
		trialResources := SnapshotControllerResources(trial.Name)
		VerifyControllerResourcesUnchanged(trial.Name, trialResources)
		VerifyControllerResourcesUnchanged(promoted.Name, promotedResources)

		By("checking the trial Pipeline and ISBService share the trial controller")
		VerifyTrialWorkloadsOnInstance(pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName, trial.Spec.InstanceID)

		By("checking the trial Pipeline passes data")
		pipelines, err := GetUpgradingPipelines(Namespace, pipelineRolloutName)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(pipelines.Items).ShouldNot(BeEmpty())
		var trialPipelineName string
		for i := range pipelines.Items {
			if ControllerInstanceIDOf(&pipelines.Items[i]) == trial.Spec.InstanceID {
				trialPipelineName = pipelines.Items[i].GetName()
			}
		}
		Expect(trialPipelineName).ShouldNot(BeEmpty())
		VerifyPipelinePassesData(trialPipelineName)

		By("checking the controller and its chain succeed, and that result stays set")
		VerifySuccessfulControllerUpgrade(UpdatedNumaflowControllerVersion, pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName)
		VerifyPipelinePassesData(mustPromotedPipelineName(pipelineRolloutName))

		By("checking the previous controller is deleted once nothing uses it")
		VerifyNumaflowControllerDeleted(promoted.Name, promoted.Spec.InstanceID)
	})

	It("finishes a controller-only upgrade while the dependents are idle", func() {
		SetNumaflowControllerVersion(InitialNumaflowControllerVersion, ControllerSuccessSchedule)
		VerifySuccessfulControllerUpgrade(InitialNumaflowControllerVersion, pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName)
	})

	It("discontinues a trial when the version changes back to the promoted one", func() {
		SetNumaflowControllerVersion(UpdatedNumaflowControllerVersion, ControllerSuccessSchedule)
		WaitForTrialNumaflowController(UpdatedNumaflowControllerVersion)

		By("changing the version back while the trial is still running")
		SetNumaflowControllerVersion(InitialNumaflowControllerVersion, ControllerSuccessSchedule)
		VerifyNumaflowControllerDiscontinued(InitialNumaflowControllerVersion)
		VerifyPromotedPipelineRunning(Namespace, pipelineRolloutName)
	})

	It("lets a fast Pipeline and ISBService finish assessment while a slow pair is still waiting", func() {
		CreateISBServiceRollout(slowISBServiceRolloutName, ProgressiveISBSpec(InitialJetstreamVersion))
		SetISBServiceAssessmentSchedule(slowISBServiceRolloutName, SlowWorkloadSchedule)
		CreateInitialPipelineRollout(slowPipelineRolloutName, GetInstanceName(slowISBServiceRolloutName, 0),
			ProgressivePipelineSpec(slowISBServiceRolloutName), PipelineStrategyForSchedule(SlowWorkloadSchedule), apiv1.Metadata{})

		SetNumaflowControllerVersion(UpdatedNumaflowControllerVersion, ControllerSuccessSchedule)
		trial := WaitForTrialNumaflowController(UpdatedNumaflowControllerVersion)

		By("checking every Pipeline and ISBService has started moving onto the trial controller")
		VerifyWorkloadsHaveChildOnInstance(pipelineRolloutName, isbServiceRolloutName, "", trial.Spec.InstanceID)
		VerifyWorkloadsHaveChildOnInstance(slowPipelineRolloutName, slowISBServiceRolloutName, "", trial.Spec.InstanceID)

		By("checking the fast workloads are assessed while the slow workloads are still unresolved")
		VerifyPipelineResourceAssessment(pipelineRolloutName, apiv1.AssessmentResultSuccess)
		VerifyISBResourceAssessment(isbServiceRolloutName, apiv1.AssessmentResultSuccess)
		VerifyPipelineResourceAssessmentUnresolved(slowPipelineRolloutName)
		VerifyISBResourceAssessmentUnresolved(slowISBServiceRolloutName)

		By("checking the slow workloads finish afterwards and the controller is promoted")
		VerifySuccessfulControllerUpgrade(UpdatedNumaflowControllerVersion, pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName)
		VerifyPipelineRolloutInProgressStrategy(slowPipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyISBServiceRolloutInProgressStrategy(slowISBServiceRolloutName, apiv1.UpgradeStrategyNoOp)
	})

	It("deletes the remaining rollouts", func() {
		DeleteProgressiveControllerStack(
			[]string{pipelineRolloutName, slowPipelineRolloutName},
			[]string{monoVertexRolloutName},
			[]string{isbServiceRolloutName, slowISBServiceRolloutName},
		)
	})
})

func mustPromotedPipelineName(pipelineRolloutName string) string {
	name, err := GetPromotedPipelineName(Namespace, pipelineRolloutName)
	Expect(err).ShouldNot(HaveOccurred())
	return name
}
