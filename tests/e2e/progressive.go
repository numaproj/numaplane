package e2e

import (
	"fmt"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

type ExpectedProgressiveStatus struct {
	Promoted  apiv1.PromotedPipelineTypeStatus
	Upgrading apiv1.UpgradingChildStatus
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

		if mvr == nil || mvr.Status.ProgressiveStatus.PromotedMonoVertexStatus == nil || mvr.Status.ProgressiveStatus.UpgradingMonoVertexStatus == nil {
			return false
		}

		promotedStatus := mvr.Status.ProgressiveStatus.PromotedMonoVertexStatus
		upgradingStatus := mvr.Status.ProgressiveStatus.UpgradingMonoVertexStatus

		return promotedStatus.Name == expectedPromotedName &&
			promotedStatus.ScaleValuesRestoredToOriginal == expectedScaleValuesRestoredToOriginal &&
			upgradingStatus.Name == expectedUpgradingName &&
			upgradingStatus.AssessmentResult == expectedAssessmentResult &&
			upgradingStatus.AssessmentEndTime != nil
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
