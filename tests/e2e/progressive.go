package e2e

import (
	"fmt"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

type ExpectedPipelineTypeProgressiveStatus struct {
	Promoted                 apiv1.PromotedPipelineTypeStatus
	Upgrading                apiv1.UpgradingChildStatus
	PipelineSourceVertexName string
}

func GetInstanceName(rolloutName string, idx int) string {
	return fmt.Sprintf("%s-%d", rolloutName, idx)
}

func VerifyMonoVertexRolloutProgressiveStatus(
	monoVertexRolloutName string,
	expectedPromotedName string,
	expectedUpgradingName string,
	expectedScaleValuesRestoredToOriginal bool,
	expectedAssessmentResult apiv1.AssessmentResult,
	forcedPromotion bool,
) {
	CheckEventually("verifying the MonoVertexRollout Progressive Status", func() bool {
		mvrProgressiveStatus := GetMonoVertexRolloutProgressiveStatus(monoVertexRolloutName)

		if forcedPromotion {
			if mvrProgressiveStatus.UpgradingMonoVertexStatus == nil {
				return false
			}

			upgradingStatus := mvrProgressiveStatus.UpgradingMonoVertexStatus

			return upgradingStatus.Name == expectedUpgradingName &&
				upgradingStatus.AssessmentResult == expectedAssessmentResult &&
				upgradingStatus.ForcedSuccess
		}

		if mvrProgressiveStatus.UpgradingMonoVertexStatus == nil {
			return false
		}

		promotedStatus := mvrProgressiveStatus.PromotedMonoVertexStatus
		upgradingStatus := mvrProgressiveStatus.UpgradingMonoVertexStatus

		// NOTE: this function is used to perform checks during progressive upgrade and also at its completion.
		// When progressive is done, the promotedStatus gets set to nil, that is why we need this if-statement.
		if promotedStatus == nil {
			return upgradingStatus.Name == expectedUpgradingName &&
				upgradingStatus.AssessmentResult == expectedAssessmentResult &&
				upgradingStatus.AssessmentEndTime != nil
		} else {
			return promotedStatus.Name == expectedPromotedName &&
				promotedStatus.ScaleValuesRestoredToOriginal == expectedScaleValuesRestoredToOriginal &&
				upgradingStatus.Name == expectedUpgradingName &&
				upgradingStatus.AssessmentResult == expectedAssessmentResult
		}

	}).Should(BeTrue())
}

func VerifyPipelineRolloutProgressiveStatus(
	pipelineRolloutName string,
	expectedPromotedName string,
	expectedUpgradingName string,
	expectedScaleValuesRestoredToOriginal bool,
	expectedAssessmentResult apiv1.AssessmentResult,
	forcedPromotion bool,
) {
	CheckEventually("verifying the PipelineRollout Progressive Status", func() bool {
		prProgressiveStatus := GetPipelineRolloutProgressiveStatus(pipelineRolloutName)

		if forcedPromotion {
			if prProgressiveStatus.UpgradingPipelineStatus == nil {
				return false
			}

			upgradingStatus := prProgressiveStatus.UpgradingPipelineStatus

			return upgradingStatus.Name == expectedUpgradingName &&
				upgradingStatus.AssessmentResult == expectedAssessmentResult &&
				upgradingStatus.ForcedSuccess
		}

		if prProgressiveStatus.UpgradingPipelineStatus == nil {
			return false
		}

		promotedStatus := prProgressiveStatus.PromotedPipelineStatus
		upgradingStatus := prProgressiveStatus.UpgradingPipelineStatus

		if promotedStatus == nil {
			return upgradingStatus.Name == expectedUpgradingName &&
				upgradingStatus.AssessmentResult == expectedAssessmentResult &&
				upgradingStatus.AssessmentEndTime != nil
		} else {
			return promotedStatus.Name == expectedPromotedName &&
				promotedStatus.ScaleValuesRestoredToOriginal == expectedScaleValuesRestoredToOriginal &&
				upgradingStatus.Name == expectedUpgradingName &&
				upgradingStatus.AssessmentResult == expectedAssessmentResult
		}

	}).Should(BeTrue())
}

func VerifyMonoVertexRolloutScaledDownForProgressive(
	monoVertexRolloutName string,
	expectedPromotedName string,
	expectedCurrent int64,
	expectedOriginalScaleMinMaxAsJSONString string,
	expectedScaleTo int64,
) {
	CheckEventually("verifying that the MonoVertexRollout scaled down for Progressive upgrade", func() bool {
		mvrProgressiveStatus := GetMonoVertexRolloutProgressiveStatus(monoVertexRolloutName)

		if mvrProgressiveStatus.PromotedMonoVertexStatus == nil {
			return false
		}

		if _, exists := mvrProgressiveStatus.PromotedMonoVertexStatus.ScaleValues[expectedPromotedName]; !exists {
			return false
		}

		return mvrProgressiveStatus.PromotedMonoVertexStatus.AllVerticesScaledDown &&
			mvrProgressiveStatus.PromotedMonoVertexStatus.Name == expectedPromotedName &&
			mvrProgressiveStatus.PromotedMonoVertexStatus.ScaleValues != nil &&
			mvrProgressiveStatus.PromotedMonoVertexStatus.ScaleValues[expectedPromotedName].Current == expectedCurrent &&
			mvrProgressiveStatus.PromotedMonoVertexStatus.ScaleValues[expectedPromotedName].OriginalScaleMinMax == expectedOriginalScaleMinMaxAsJSONString &&
			mvrProgressiveStatus.PromotedMonoVertexStatus.ScaleValues[expectedPromotedName].ScaleTo == expectedScaleTo
	}).Should(BeTrue())
}

// NOTE: this function assumes that the pipeline only has one source vertex.
// This function should be modified if the E2E tests will be changed
// to have more than one source vertex.
func VerifyPipelineRolloutScaledDownForProgressive(
	pipelineRolloutName string,
	expectedPromotedName string,
	sourceVertexName string,
	expectedCurrent int64,
	expectedOriginalScaleMinMaxAsJSONString string,
	expectedScaleTo int64,
) {
	CheckEventually("verifying that the PipelineRollout scaled down for Progressive upgrade", func() bool {
		prProgressiveStatus := GetPipelineRolloutProgressiveStatus(pipelineRolloutName)

		if prProgressiveStatus.PromotedPipelineStatus == nil {
			return false
		}

		if _, exists := prProgressiveStatus.PromotedPipelineStatus.ScaleValues[sourceVertexName]; !exists {
			return false
		}

		return prProgressiveStatus.PromotedPipelineStatus.AllVerticesScaledDown &&
			prProgressiveStatus.PromotedPipelineStatus.Name == expectedPromotedName &&
			prProgressiveStatus.PromotedPipelineStatus.ScaleValues != nil &&
			prProgressiveStatus.PromotedPipelineStatus.ScaleValues[sourceVertexName].Current == expectedCurrent &&
			prProgressiveStatus.PromotedPipelineStatus.ScaleValues[sourceVertexName].OriginalScaleMinMax == expectedOriginalScaleMinMaxAsJSONString &&
			prProgressiveStatus.PromotedPipelineStatus.ScaleValues[sourceVertexName].ScaleTo == expectedScaleTo
	}).Should(BeTrue())
}

func VerifyMonoVertexPromotedScale(namespace, monoVertexRolloutName string, expectedMonoVertexScaleMap map[string]numaflowv1.Scale) {
	CheckEventually("verifying that the scale values are as expected for the Promoted MonoVertex", func() bool {
		unstructMonoVertex, err := GetMonoVertex(namespace, monoVertexRolloutName)
		Expect(err).ShouldNot(HaveOccurred())

		monoVertexSpec, err := getMonoVertexSpec(unstructMonoVertex)
		Expect(err).ShouldNot(HaveOccurred())

		actualMonoVertexScaleMap := map[string]numaflowv1.Scale{
			unstructMonoVertex.GetName(): monoVertexSpec.Scale,
		}

		return VerifyVerticesScale(actualMonoVertexScaleMap, expectedMonoVertexScaleMap)
	}).WithTimeout(TestTimeout).Should(BeTrue())
}

func MakeExpectedPipelineTypeProgressiveStatus(
	promotedName, upgradingName, sourceVertexName string,
	current, scaleTo int64,
	originalScaleMinMax string,
	assessmentResultInProgress, assessmentResultOnDone apiv1.AssessmentResult,
) (ExpectedPipelineTypeProgressiveStatus, ExpectedPipelineTypeProgressiveStatus) {
	expectedPipelineTypeProgressiveStatusInProgress := ExpectedPipelineTypeProgressiveStatus{
		Promoted: apiv1.PromotedPipelineTypeStatus{
			PromotedChildStatus: apiv1.PromotedChildStatus{
				Name: promotedName,
			},
			ScaleValues: map[string]apiv1.ScaleValues{
				sourceVertexName: {
					Current:             current,
					OriginalScaleMinMax: originalScaleMinMax,
					ScaleTo:             scaleTo,
				},
			},
		},
		Upgrading: apiv1.UpgradingChildStatus{
			Name:             upgradingName,
			AssessmentResult: assessmentResultInProgress,
		},
		PipelineSourceVertexName: sourceVertexName,
	}

	expectedPipelineTypeProgressiveStatusOnDone := ExpectedPipelineTypeProgressiveStatus{
		Promoted: apiv1.PromotedPipelineTypeStatus{
			PromotedChildStatus: apiv1.PromotedChildStatus{
				Name: promotedName,
			},
		},
		Upgrading: apiv1.UpgradingChildStatus{
			Name:             upgradingName,
			AssessmentResult: assessmentResultOnDone,
		},
	}

	return expectedPipelineTypeProgressiveStatusInProgress, expectedPipelineTypeProgressiveStatusOnDone
}

func PipelineProgressiveChecks(pipelineRolloutName string, pipelineSpec numaflowv1.PipelineSpec, expectedPipelineTypeProgressiveStatusInProgress, expectedPipelineTypeProgressiveStatusOnDone *ExpectedPipelineTypeProgressiveStatus) {

	// Check Progressive status while the assessment is in progress

	VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyProgressive)

	// Verify that the Pipeline is set to scale down
	VerifyPipelineRolloutScaledDownForProgressive(pipelineRolloutName, expectedPipelineTypeProgressiveStatusInProgress.Promoted.Name, expectedPipelineTypeProgressiveStatusInProgress.PipelineSourceVertexName,
		expectedPipelineTypeProgressiveStatusInProgress.Promoted.ScaleValues[expectedPipelineTypeProgressiveStatusInProgress.PipelineSourceVertexName].Current,
		expectedPipelineTypeProgressiveStatusInProgress.Promoted.ScaleValues[expectedPipelineTypeProgressiveStatusInProgress.PipelineSourceVertexName].OriginalScaleMinMax,
		expectedPipelineTypeProgressiveStatusInProgress.Promoted.ScaleValues[expectedPipelineTypeProgressiveStatusInProgress.PipelineSourceVertexName].ScaleTo)

	VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, expectedPipelineTypeProgressiveStatusInProgress.Promoted.Name, expectedPipelineTypeProgressiveStatusInProgress.Upgrading.Name,
		expectedPipelineTypeProgressiveStatusInProgress.Promoted.ScaleValuesRestoredToOriginal, expectedPipelineTypeProgressiveStatusInProgress.Upgrading.AssessmentResult, false)

	// Verify that the expected number of promoted Pipeline pods is running (only for source vertex)
	// NOTE: min is set same as max if the original min if greater than scaleTo
	prProgressiveStatus := GetPipelineRolloutProgressiveStatus(pipelineRolloutName)
	Expect(prProgressiveStatus.PromotedPipelineStatus).NotTo(BeNil())
	scaleTo := expectedPipelineTypeProgressiveStatusInProgress.Promoted.ScaleValues[expectedPipelineTypeProgressiveStatusInProgress.PipelineSourceVertexName].ScaleTo
	min := prProgressiveStatus.PromotedPipelineStatus.ScaleValues[expectedPipelineTypeProgressiveStatusInProgress.PipelineSourceVertexName].Initial
	if min > scaleTo {
		min = scaleTo
	}
	promotedScale := numaflowv1.Scale{Min: ptr.To(int32(min)), Max: ptr.To(int32(scaleTo))}
	VerifyVerticesPodsRunning(Namespace, expectedPipelineTypeProgressiveStatusInProgress.Promoted.Name,
		[]numaflowv1.AbstractVertex{{Name: expectedPipelineTypeProgressiveStatusInProgress.PipelineSourceVertexName, Scale: promotedScale}}, ComponentVertex)

	// Verify that the expected number of upgrading Pipeline pods is running (only for source vertex)
	// Min and max are set to the same value which is the scale.min of the pipeline.
	// TODO: when progressive scaling for pipeline is implemented similarly to monovertex, set this value to initial - scaleTo
	minMax := pipelineSpec.Vertices[0].Scale.Min
	VerifyVerticesPodsRunning(Namespace, expectedPipelineTypeProgressiveStatusInProgress.Upgrading.Name,
		[]numaflowv1.AbstractVertex{{Name: expectedPipelineTypeProgressiveStatusInProgress.PipelineSourceVertexName, Scale: numaflowv1.Scale{Min: minMax, Max: minMax}}}, ComponentVertex)

	// Check Progressive status post-assessment

	VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, expectedPipelineTypeProgressiveStatusOnDone.Promoted.Name, expectedPipelineTypeProgressiveStatusOnDone.Upgrading.Name,
		expectedPipelineTypeProgressiveStatusOnDone.Promoted.ScaleValuesRestoredToOriginal, expectedPipelineTypeProgressiveStatusOnDone.Upgrading.AssessmentResult, false)

	// Verify that the upgrading pipeline was promoted by checking that the expected number of pods are running with the correct pipeline name (only for source vertex)
	VerifyVerticesPodsRunning(Namespace, expectedPipelineTypeProgressiveStatusOnDone.Upgrading.Name,
		[]numaflowv1.AbstractVertex{{Name: expectedPipelineTypeProgressiveStatusInProgress.PipelineSourceVertexName, Scale: pipelineSpec.Vertices[0].Scale}}, ComponentVertex)

	// Verify that the previously promoted pipeline was deleted
	// NOTE: checking no pods are running for the source vertex only
	VerifyVerticesPodsRunning(Namespace, expectedPipelineTypeProgressiveStatusOnDone.Promoted.Name,
		[]numaflowv1.AbstractVertex{{Name: expectedPipelineTypeProgressiveStatusInProgress.PipelineSourceVertexName, Scale: numaflowv1.Scale{Min: ptr.To(int32(0)), Max: ptr.To(int32(0))}}}, ComponentVertex)
	VerifyPipelineDeletion(expectedPipelineTypeProgressiveStatusOnDone.Promoted.Name)
}

func GetMonoVertexRolloutProgressiveStatus(monoVertexRolloutName string) apiv1.MonoVertexProgressiveStatus {
	mvr, err := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	if mvr != nil {
		return mvr.Status.ProgressiveStatus
	}

	return apiv1.MonoVertexProgressiveStatus{}
}

func GetPipelineRolloutProgressiveStatus(pipelineRolloutName string) apiv1.PipelineProgressiveStatus {
	pr, err := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	if pr != nil {
		return pr.Status.ProgressiveStatus
	}

	return apiv1.PipelineProgressiveStatus{}
}
