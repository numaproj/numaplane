package e2e

import (
	"fmt"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

type ExpectedProgressiveStatus struct {
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
		mvr, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})

		if forcedPromotion {
			if mvr == nil || mvr.Status.ProgressiveStatus.UpgradingMonoVertexStatus == nil {
				return false
			}

			upgradingStatus := mvr.Status.ProgressiveStatus.UpgradingMonoVertexStatus

			return upgradingStatus.Name == expectedUpgradingName &&
				upgradingStatus.AssessmentResult == expectedAssessmentResult &&
				upgradingStatus.ForcedSuccess
		}

		if mvr == nil || mvr.Status.ProgressiveStatus.UpgradingMonoVertexStatus == nil {
			return false
		}

		promotedStatus := mvr.Status.ProgressiveStatus.PromotedMonoVertexStatus
		upgradingStatus := mvr.Status.ProgressiveStatus.UpgradingMonoVertexStatus

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
		mvr, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})

		if forcedPromotion {
			if mvr == nil || mvr.Status.ProgressiveStatus.UpgradingPipelineStatus == nil {
				return false
			}

			upgradingStatus := mvr.Status.ProgressiveStatus.UpgradingPipelineStatus

			return upgradingStatus.Name == expectedUpgradingName &&
				upgradingStatus.AssessmentResult == expectedAssessmentResult &&
				upgradingStatus.ForcedSuccess
		}

		if mvr == nil || mvr.Status.ProgressiveStatus.UpgradingPipelineStatus == nil {
			return false
		}

		promotedStatus := mvr.Status.ProgressiveStatus.PromotedPipelineStatus
		upgradingStatus := mvr.Status.ProgressiveStatus.UpgradingPipelineStatus

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
	expectedInitial int64,
	expectedOriginalScaleMinMaxAsJSONString string,
	expectedScaleTo int64,
) {
	CheckEventually("verifying that the MonoVertexRollout scaled down for Progressive upgrade", func() bool {
		mvr, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})

		if mvr == nil || mvr.Status.ProgressiveStatus.PromotedMonoVertexStatus == nil {
			return false
		}

		if _, exists := mvr.Status.ProgressiveStatus.PromotedMonoVertexStatus.ScaleValues[expectedPromotedName]; !exists {
			return false
		}

		return mvr.Status.ProgressiveStatus.PromotedMonoVertexStatus.AllSourceVerticesScaledDown &&
			mvr.Status.ProgressiveStatus.PromotedMonoVertexStatus.Name == expectedPromotedName &&
			mvr.Status.ProgressiveStatus.PromotedMonoVertexStatus.ScaleValues != nil &&
			mvr.Status.ProgressiveStatus.PromotedMonoVertexStatus.ScaleValues[expectedPromotedName].Current == expectedCurrent &&
			mvr.Status.ProgressiveStatus.PromotedMonoVertexStatus.ScaleValues[expectedPromotedName].Initial == expectedInitial &&
			mvr.Status.ProgressiveStatus.PromotedMonoVertexStatus.ScaleValues[expectedPromotedName].OriginalScaleMinMax == expectedOriginalScaleMinMaxAsJSONString &&
			mvr.Status.ProgressiveStatus.PromotedMonoVertexStatus.ScaleValues[expectedPromotedName].ScaleTo == expectedScaleTo
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
	expectedInitial int64,
	expectedOriginalScaleMinMaxAsJSONString string,
	expectedScaleTo int64,
) {
	CheckEventually("verifying that the PipelineRollout scaled down for Progressive upgrade", func() bool {
		mvr, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})

		if mvr == nil || mvr.Status.ProgressiveStatus.PromotedPipelineStatus == nil {
			return false
		}

		if _, exists := mvr.Status.ProgressiveStatus.PromotedPipelineStatus.ScaleValues[sourceVertexName]; !exists {
			return false
		}

		return mvr.Status.ProgressiveStatus.PromotedPipelineStatus.AllSourceVerticesScaledDown &&
			mvr.Status.ProgressiveStatus.PromotedPipelineStatus.Name == expectedPromotedName &&
			mvr.Status.ProgressiveStatus.PromotedPipelineStatus.ScaleValues != nil &&
			mvr.Status.ProgressiveStatus.PromotedPipelineStatus.ScaleValues[sourceVertexName].Current == expectedCurrent &&
			mvr.Status.ProgressiveStatus.PromotedPipelineStatus.ScaleValues[sourceVertexName].Initial == expectedInitial &&
			mvr.Status.ProgressiveStatus.PromotedPipelineStatus.ScaleValues[sourceVertexName].OriginalScaleMinMax == expectedOriginalScaleMinMaxAsJSONString &&
			mvr.Status.ProgressiveStatus.PromotedPipelineStatus.ScaleValues[sourceVertexName].ScaleTo == expectedScaleTo
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

func MakeExpectedProgressiveStatus(
	promotedName, upgradingName, sourceVertexName string,
	current, initial, scaleTo int64,
	originalScaleMinMax string,
	assessmentResultInProgress, assessmentResultOnDone apiv1.AssessmentResult,
) (ExpectedProgressiveStatus, ExpectedProgressiveStatus) {
	expectedProgressiveStatusInProgress := ExpectedProgressiveStatus{
		Promoted: apiv1.PromotedPipelineTypeStatus{
			PromotedChildStatus: apiv1.PromotedChildStatus{
				Name: promotedName,
			},
			ScaleValues: map[string]apiv1.ScaleValues{
				sourceVertexName: {
					Current:             current,
					Initial:             initial,
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

	expectedProgressiveStatusOnDone := ExpectedProgressiveStatus{
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

	return expectedProgressiveStatusInProgress, expectedProgressiveStatusOnDone
}

func PipelineProgressiveChecks(pipelineRolloutName string, pipelineSpec numaflowv1.PipelineSpec, expectedProgressiveStatusInProgress, expectedProgressiveStatusOnDone *ExpectedProgressiveStatus) {
	// Check Progressive status while the assessment is in progress

	VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyProgressive)

	// Verify that the Pipeline is set to scale down
	VerifyPipelineRolloutScaledDownForProgressive(pipelineRolloutName, expectedProgressiveStatusInProgress.Promoted.Name, expectedProgressiveStatusInProgress.PipelineSourceVertexName,
		expectedProgressiveStatusInProgress.Promoted.ScaleValues[expectedProgressiveStatusInProgress.PipelineSourceVertexName].Current,
		expectedProgressiveStatusInProgress.Promoted.ScaleValues[expectedProgressiveStatusInProgress.PipelineSourceVertexName].Initial,
		expectedProgressiveStatusInProgress.Promoted.ScaleValues[expectedProgressiveStatusInProgress.PipelineSourceVertexName].OriginalScaleMinMax,
		expectedProgressiveStatusInProgress.Promoted.ScaleValues[expectedProgressiveStatusInProgress.PipelineSourceVertexName].ScaleTo)

	VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, expectedProgressiveStatusInProgress.Promoted.Name, expectedProgressiveStatusInProgress.Upgrading.Name,
		expectedProgressiveStatusInProgress.Promoted.ScaleValuesRestoredToOriginal, expectedProgressiveStatusInProgress.Upgrading.AssessmentResult, false)

	// Verify that the expected number of promoted Pipeline pods is running (only for source vertex)
	// NOTE: min is set same as max if the original min if greater than scaleTo
	scaleTo := expectedProgressiveStatusInProgress.Promoted.ScaleValues[expectedProgressiveStatusInProgress.PipelineSourceVertexName].ScaleTo
	min := expectedProgressiveStatusInProgress.Promoted.ScaleValues[expectedProgressiveStatusInProgress.PipelineSourceVertexName].Initial
	if min > scaleTo {
		min = scaleTo
	}
	promotedScale := numaflowv1.Scale{Min: ptr.To(int32(min)), Max: ptr.To(int32(scaleTo))}
	VerifyVerticesPodsRunning(Namespace, expectedProgressiveStatusInProgress.Promoted.Name,
		[]numaflowv1.AbstractVertex{{Name: expectedProgressiveStatusInProgress.PipelineSourceVertexName, Scale: promotedScale}}, ComponentVertex)

	// Verify that the expected number of upgrading Pipeline pods is running (only for source vertex)
	// Min and max are set to the same value which is the scale.min of the pipeline.
	// TODO: when progressive scaling for pipeline is implemented similarly to monovertex, set this value to initial - scaleTo
	minMax := pipelineSpec.Vertices[0].Scale.Min
	VerifyVerticesPodsRunning(Namespace, expectedProgressiveStatusInProgress.Upgrading.Name,
		[]numaflowv1.AbstractVertex{{Name: expectedProgressiveStatusInProgress.PipelineSourceVertexName, Scale: numaflowv1.Scale{Min: minMax, Max: minMax}}}, ComponentVertex)

	// Check Progressive status post-assessment

	VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, expectedProgressiveStatusOnDone.Promoted.Name, expectedProgressiveStatusOnDone.Upgrading.Name,
		expectedProgressiveStatusOnDone.Promoted.ScaleValuesRestoredToOriginal, expectedProgressiveStatusOnDone.Upgrading.AssessmentResult, false)

	// Verify that the upgrading pipeline was promoted by checking that the expected number of pods are running with the correct pipeline name (only for source vertex)
	VerifyVerticesPodsRunning(Namespace, expectedProgressiveStatusOnDone.Upgrading.Name,
		[]numaflowv1.AbstractVertex{{Name: expectedProgressiveStatusInProgress.PipelineSourceVertexName, Scale: pipelineSpec.Vertices[0].Scale}}, ComponentVertex)

	// Verify that the previously promoted pipeline was deleted
	// NOTE: checking no pods are running for the source vertex only
	VerifyVerticesPodsRunning(Namespace, expectedProgressiveStatusOnDone.Promoted.Name,
		[]numaflowv1.AbstractVertex{{Name: expectedProgressiveStatusInProgress.PipelineSourceVertexName, Scale: numaflowv1.Scale{Min: ptr.To(int32(0)), Max: ptr.To(int32(0))}}}, ComponentVertex)
	VerifyPipelineDeletion(expectedProgressiveStatusOnDone.Promoted.Name)
}
