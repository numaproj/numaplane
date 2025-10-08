package e2e

import (
	"context"
	"fmt"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
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
	CheckEventually(fmt.Sprintf("verifying the MonoVertexRollout Progressive Status (promoted=%s, upgrading=%s)", expectedPromotedName, expectedUpgradingName), func() bool {
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
				upgradingStatus.BasicAssessmentEndTime != nil
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
	CheckEventually(fmt.Sprintf("verifying the PipelineRollout Progressive Status (promoted=%s, upgrading=%s)", expectedPromotedName, expectedUpgradingName), func() bool {
		prProgressiveStatus := GetPipelineRolloutProgressiveStatus(pipelineRolloutName)

		if prProgressiveStatus.UpgradingPipelineStatus == nil {
			return false
		}

		promotedStatus := prProgressiveStatus.PromotedPipelineStatus
		upgradingStatus := prProgressiveStatus.UpgradingPipelineStatus

		if expectedAssessmentResult == apiv1.AssessmentResultSuccess {
			success := promotedStatus == nil && upgradingStatus.Name == expectedUpgradingName &&
				upgradingStatus.AssessmentResult == expectedAssessmentResult
			if forcedPromotion {
				return success && upgradingStatus.ForcedSuccess
			} else {
				return success && upgradingStatus.BasicAssessmentEndTime != nil
			}
		} else {
			// still in the middle of progressive upgrade strategy
			if promotedStatus == nil {
				return false
			}
			return promotedStatus.Name == expectedPromotedName &&
				promotedStatus.ScaleValuesRestoredToOriginal == expectedScaleValuesRestoredToOriginal &&
				upgradingStatus.Name == expectedUpgradingName &&
				upgradingStatus.AssessmentResult == expectedAssessmentResult
		}

	}).Should(BeTrue())
}

func VerifyISBServiceRolloutProgressiveStatus(
	isbServiceRolloutName string,
	expectedPromotedName string,
	expectedUpgradingName string,
	expectedAssessmentResult apiv1.AssessmentResult,
) {
	CheckEventually(fmt.Sprintf("verifying the ISBServiceRollout Progressive Status (promoted=%s, upgrading=%s)", expectedPromotedName, expectedUpgradingName), func() bool {
		isbSvcRolloutProgressiveStatus := GetISBServiceRolloutProgressiveStatus(isbServiceRolloutName)

		if isbSvcRolloutProgressiveStatus.UpgradingISBServiceStatus == nil {
			return false
		}

		promotedStatus := isbSvcRolloutProgressiveStatus.PromotedISBServiceStatus
		upgradingStatus := isbSvcRolloutProgressiveStatus.UpgradingISBServiceStatus

		return promotedStatus.Name == expectedPromotedName &&
			upgradingStatus.Name == expectedUpgradingName &&
			upgradingStatus.AssessmentResult == expectedAssessmentResult
	}).Should(BeTrue())
}

func VerifyMonoVertexRolloutScaledDownForProgressive(
	monoVertexRolloutName string,
	expectedPromotedName string,
	expectedOriginalScaleDefinitionAsJSONString string,
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

		return mvrProgressiveStatus.PromotedMonoVertexStatus.Name == expectedPromotedName &&
			mvrProgressiveStatus.PromotedMonoVertexStatus.ScaleValues != nil &&
			mvrProgressiveStatus.PromotedMonoVertexStatus.ScaleValues[expectedPromotedName].OriginalScaleDefinition == expectedOriginalScaleDefinitionAsJSONString &&
			mvrProgressiveStatus.PromotedMonoVertexStatus.ScaleValues[expectedPromotedName].ScaleTo == expectedScaleTo
	}).Should(BeTrue())
}
func VerifyPromotedPipelineScaledDownForProgressive(
	pipelineRolloutName string,
	expectedPromotedPipelineName string,
) {
	CheckEventually("verifying that the PipelineRollout scaled down for Progressive upgrade", func() bool {
		prProgressiveStatus := GetPipelineRolloutProgressiveStatus(pipelineRolloutName)

		if prProgressiveStatus.PromotedPipelineStatus == nil {
			return false
		}

		return prProgressiveStatus.PromotedPipelineStatus.Name == expectedPromotedPipelineName

	}).Should(BeTrue())

	CheckEventually("verifying Promoted Pipeline Vertex Scale", func() bool {

		promotedPipeline, err := GetPromotedPipeline(Namespace, pipelineRolloutName)
		if err != nil {
			return false
		}
		if promotedPipeline.GetName() != expectedPromotedPipelineName {
			return false
		}

		prProgressiveStatus := GetPipelineRolloutProgressiveStatus(pipelineRolloutName)

		vertexScaleDefinitions, err := numaflowtypes.GetScaleValuesFromPipelineDefinition(context.TODO(), promotedPipeline)
		if err != nil {
			return false
		}

		// make sure the min/max from the promoted pipeline matches the Status
		// and make sure min=max
		if prProgressiveStatus.PromotedPipelineStatus == nil || len(vertexScaleDefinitions) != len(prProgressiveStatus.PromotedPipelineStatus.ScaleValues) {
			return false
		}

		for _, vertexScaleDef := range vertexScaleDefinitions {
			statusDefinedScale := prProgressiveStatus.PromotedPipelineStatus.ScaleValues[vertexScaleDef.VertexName]
			if vertexScaleDef.ScaleDefinition == nil || vertexScaleDef.ScaleDefinition.Min == nil || vertexScaleDef.ScaleDefinition.Max == nil ||
				*vertexScaleDef.ScaleDefinition.Min != *vertexScaleDef.ScaleDefinition.Max || statusDefinedScale.ScaleTo != *vertexScaleDef.ScaleDefinition.Min {
				return false
			}
		}

		// Verify the number of Pods for each Vertex
		VerifyPodsRunningForAllVertices(promotedPipeline.GetName(), vertexScaleDefinitions)

		return true
	}).Should(BeTrue())
}

func VerifyPodsRunningForAllVertices(pipelineName string, vertexScaleDefinitions []apiv1.VertexScaleDefinition) {
	for _, vertexScaleDef := range vertexScaleDefinitions {
		min := int32(1)
		if vertexScaleDef.ScaleDefinition != nil && vertexScaleDef.ScaleDefinition.Min != nil {
			min = int32(*vertexScaleDef.ScaleDefinition.Min)
		}
		max := int32(1)
		if vertexScaleDef.ScaleDefinition != nil && vertexScaleDef.ScaleDefinition.Max != nil {
			max = int32(*vertexScaleDef.ScaleDefinition.Max)
		}
		VerifyVerticesPodsRunning(Namespace, pipelineName,
			[]numaflowv1.AbstractVertex{{Name: vertexScaleDef.VertexName, Scale: numaflowv1.Scale{Min: &min, Max: &max}}}, ComponentVertex)
	}
}

func VerifyMonoVertexPromotedScale(namespace, monoVertexRolloutName string, expectedMonoVertexScaleMap map[string]numaflowv1.Scale) {
	CheckEventually("verifying that the scale values are as expected for the Promoted MonoVertex", func() bool {
		unstructMonoVertex, err := GetPromotedMonoVertex(namespace, monoVertexRolloutName)
		Expect(err).ShouldNot(HaveOccurred())

		monoVertexSpec, err := getMonoVertexSpec(unstructMonoVertex)
		Expect(err).ShouldNot(HaveOccurred())

		actualMonoVertexScaleMap := map[string]numaflowv1.Scale{
			unstructMonoVertex.GetName(): monoVertexSpec.Scale,
		}

		return VerifyVerticesScale(actualMonoVertexScaleMap, expectedMonoVertexScaleMap)
	}).WithTimeout(DefaultTestTimeout).Should(BeTrue())
}

// Make sure Upgrading Pipeline has min=max equal to difference between Initial and ScaleTo in promoted status
func VerifyUpgradingPipelineScaledDownForProgressive(
	pipelineRolloutName string,
	expectedUpgradingPipelineName string,
) {
	CheckEventually("verifying Upgrading Pipeline Vertex Scale", func() bool {

		upgradingPipeline, err := GetPipelineByName(Namespace, expectedUpgradingPipelineName)
		if err != nil {
			return false
		}
		upgradingScaleDefinitions, err := numaflowtypes.GetScaleValuesFromPipelineDefinition(context.TODO(), upgradingPipeline)
		if err != nil {
			return false
		}

		prProgressiveStatus := GetPipelineRolloutProgressiveStatus(pipelineRolloutName)
		promotedScaleValues := prProgressiveStatus.PromotedPipelineStatus.ScaleValues
		for _, upgradingScaleDef := range upgradingScaleDefinitions {
			expectedUpgradingScaleMinMax := 1

			// is this vertex in the promoted pipeline?
			promotedScale, vertexExistsInPromoted := promotedScaleValues[upgradingScaleDef.VertexName]
			if vertexExistsInPromoted {
				expectedUpgradingScaleMinMax = int(promotedScale.Initial - promotedScale.ScaleTo)
			}
			if upgradingScaleDef.ScaleDefinition == nil ||
				upgradingScaleDef.ScaleDefinition.Min == nil || *upgradingScaleDef.ScaleDefinition.Min != int64(expectedUpgradingScaleMinMax) ||
				upgradingScaleDef.ScaleDefinition.Max == nil || *upgradingScaleDef.ScaleDefinition.Max != int64(expectedUpgradingScaleMinMax) {
				return false
			}
		}

		// Verify the number of Pods for each Vertex
		VerifyPodsRunningForAllVertices(expectedUpgradingPipelineName, upgradingScaleDefinitions)

		return true
	}).Should(BeTrue())
}

// verify that when the upgrading Pipeline fails, its Vertices scale to 0 Pods
func VerifyUpgradingPipelineScaledToZeroForProgressive(
	pipelineRolloutName string,
	expectedUpgradingPipelineName string,
) {
	CheckEventually("verifying Upgrading Pipeline Vertex Scaled down to 0 Pods", func() bool {

		upgradingPipeline, err := GetPipelineByName(Namespace, expectedUpgradingPipelineName)
		if err != nil {
			return false
		}
		upgradingScaleDefinitions, err := numaflowtypes.GetScaleValuesFromPipelineDefinition(context.TODO(), upgradingPipeline)
		if err != nil {
			return false
		}

		for _, upgradingScaleDef := range upgradingScaleDefinitions {

			if upgradingScaleDef.ScaleDefinition == nil ||
				upgradingScaleDef.ScaleDefinition.Min == nil || *upgradingScaleDef.ScaleDefinition.Min != 0 ||
				upgradingScaleDef.ScaleDefinition.Max == nil || *upgradingScaleDef.ScaleDefinition.Max != 0 {
				return false
			}
		}

		// Verify the number of Pods for each Vertex
		VerifyPodsRunningForAllVertices(expectedUpgradingPipelineName, upgradingScaleDefinitions)

		return true
	}).Should(BeTrue())
}

func VerifyPromotedPipelineScaledUpForProgressive(
	pipelineRolloutName string,
	expectedPromotedPipelineName string,
	newPipelineSpec numaflowv1.PipelineSpec,
) {
	CheckEventually("verifying expected pipeline is promoted", func() bool {

		promotedPipeline, err := GetPromotedPipeline(Namespace, pipelineRolloutName)
		if err != nil {
			return false
		}
		if promotedPipeline.GetName() != expectedPromotedPipelineName {
			return false
		}

		return true
	}).Should(BeTrue())

	CheckEventually("verifying promoted pipeline matches PipelineRollout-defined scale definition", func() bool {

		promotedPipeline, err := GetPromotedPipeline(Namespace, pipelineRolloutName)
		if err != nil {
			return false
		}
		vertexScaleDefinitions, err := numaflowtypes.GetScaleValuesFromPipelineDefinition(context.TODO(), promotedPipeline)
		if err != nil {
			return false
		}
		if len(vertexScaleDefinitions) != len(newPipelineSpec.Vertices) {
			return false
		}
		for i, vertexScaleDef := range vertexScaleDefinitions {
			originalSpecVertex := newPipelineSpec.Vertices[i]
			if originalSpecVertex.Name != vertexScaleDef.VertexName {
				return false
			}
			// compare Min
			if vertexScaleDef.ScaleDefinition == nil || vertexScaleDef.ScaleDefinition.Min == nil {
				if originalSpecVertex.Scale.Min != nil {
					return false
				}
			} else {
				if originalSpecVertex.Scale.Min == nil || *vertexScaleDef.ScaleDefinition.Min != int64(*originalSpecVertex.Scale.Min) {
					return false
				}
			}
			// compare Max
			if vertexScaleDef.ScaleDefinition == nil || vertexScaleDef.ScaleDefinition.Max == nil {
				if originalSpecVertex.Scale.Max != nil {
					return false
				}
			} else {
				if originalSpecVertex.Scale.Max == nil || *vertexScaleDef.ScaleDefinition.Max != int64(*originalSpecVertex.Scale.Max) {
					return false
				}
			}
		}
		return true
	}).Should(BeTrue())
}

func MakeExpectedPipelineTypeProgressiveStatus(
	promotedName, upgradingName, sourceVertexName string,
	scaleTo int64,
	originalScaleDefinition string,
	assessmentResultInProgress, assessmentResultOnDone apiv1.AssessmentResult,
) (ExpectedPipelineTypeProgressiveStatus, ExpectedPipelineTypeProgressiveStatus) {
	expectedPipelineTypeProgressiveStatusInProgress := ExpectedPipelineTypeProgressiveStatus{
		Promoted: apiv1.PromotedPipelineTypeStatus{
			PromotedChildStatus: apiv1.PromotedChildStatus{
				Name: promotedName,
			},
			ScaleValues: map[string]apiv1.ScaleValues{
				sourceVertexName: {
					OriginalScaleDefinition: originalScaleDefinition,
					ScaleTo:                 scaleTo,
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

// Verify promoted pipeline and upgrading pipeline during Progressive Rollout
func PipelineTransientProgressiveChecks(pipelineRolloutName string, expectedPromotedPipelineName string, expectedUpgradingPipelineName string) {

	// Check Progressive status and pipelines while the assessment is in progress

	VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyProgressive)

	VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, expectedPromotedPipelineName, expectedUpgradingPipelineName,
		false, apiv1.AssessmentResultUnknown, false)

	VerifyPromotedPipelineScaledDownForProgressive(pipelineRolloutName, expectedPromotedPipelineName)
	VerifyUpgradingPipelineScaledDownForProgressive(pipelineRolloutName, expectedUpgradingPipelineName)

}

// Verify promoted pipeline and upgrading pipeline after Progressive Rollout
func PipelineFinalProgressiveChecks(pipelineRolloutName string, expectedPromotedPipelineName string, expectedUpgradingPipelineName string, expectedSuccess bool,
	newPipelineSpec numaflowv1.PipelineSpec) {

	expectedAssessment := apiv1.AssessmentResultSuccess
	if !expectedSuccess {
		expectedAssessment = apiv1.AssessmentResultFailure
	}

	VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, expectedPromotedPipelineName, expectedUpgradingPipelineName,
		expectedSuccess, expectedAssessment, false)

	// if expectedAssessmentResult==Success, check that new pipeline min and max match PipelineSpec
	// if expectedAssessmentResult==Failure, check that original pipeline min and max match PipelineSpec and that upgrading pipeline min=max=0
	if expectedSuccess {
		// this is the "new" promoted pipeline
		VerifyPromotedPipelineScaledUpForProgressive(pipelineRolloutName, expectedUpgradingPipelineName, newPipelineSpec)
		VerifyPipelineDeletion(expectedPromotedPipelineName)
	} else {
		VerifyPromotedPipelineScaledUpForProgressive(pipelineRolloutName, expectedPromotedPipelineName, newPipelineSpec)
		VerifyUpgradingPipelineScaledToZeroForProgressive(pipelineRolloutName, expectedUpgradingPipelineName)
	}

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

func GetISBServiceRolloutProgressiveStatus(isbSvcRolloutName string) apiv1.ISBServiceProgressiveStatus {
	isbSvcRollout, err := isbServiceRolloutClient.Get(ctx, isbSvcRolloutName, metav1.GetOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	if isbSvcRollout != nil {
		return isbSvcRollout.Status.ProgressiveStatus
	}

	return apiv1.ISBServiceProgressiveStatus{}
}
