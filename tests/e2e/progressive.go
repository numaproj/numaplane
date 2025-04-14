package e2e

import (
	"errors"
	"fmt"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/controller/progressive"
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
	CheckEventually(fmt.Sprintf("verifying the PipelineRollout Progressive Status (promoted=%s, upgrading=%s)", expectedPromotedName, expectedUpgradingName), func() bool {
		prProgressiveStatus := GetPipelineRolloutProgressiveStatus(pipelineRolloutName)

		/*if forcedPromotion {
			upgradingStatus := prProgressiveStatus.UpgradingPipelineStatus
			if upgradingStatus == nil {
				return false
			}

			// TODO: can't this just be incorporated into the "success" case below?
			return upgradingStatus.Name == expectedUpgradingName &&
				upgradingStatus.AssessmentResult == expectedAssessmentResult &&
				upgradingStatus.ForcedSuccess
		}*/

		if prProgressiveStatus.UpgradingPipelineStatus == nil {
			return false
		}

		promotedStatus := prProgressiveStatus.PromotedPipelineStatus
		upgradingStatus := prProgressiveStatus.UpgradingPipelineStatus

		/*if promotedStatus == nil { // this indicates that the upgrading pipeline was deemed successful and we're no longer in the middle of progressive upgrade strategy
			return upgradingStatus.Name == expectedUpgradingName &&
				upgradingStatus.AssessmentResult == expectedAssessmentResult &&
				upgradingStatus.AssessmentEndTime != nil
		} else { // still in the middle of progressive upgrade strategy
			return promotedStatus.Name == expectedPromotedName &&
				promotedStatus.ScaleValuesRestoredToOriginal == expectedScaleValuesRestoredToOriginal &&
				upgradingStatus.Name == expectedUpgradingName &&
				upgradingStatus.AssessmentResult == expectedAssessmentResult
		}*/
		if expectedAssessmentResult == apiv1.AssessmentResultSuccess {
			success := promotedStatus == nil && upgradingStatus.Name == expectedUpgradingName &&
				upgradingStatus.AssessmentResult == expectedAssessmentResult &&
				upgradingStatus.AssessmentEndTime != nil
			if forcedPromotion {
				return success && upgradingStatus.ForcedSuccess
			} else {
				return success
			}
		} else {
			// still in the middle of progressive upgrade strategy
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

func GetScaleValuesFromPipelineSpec(pipelineDef *unstructured.Unstructured) ([]apiv1.VertexScaleDefinition, error) {
	vertices, _, err := unstructured.NestedSlice(pipelineDef.Object, "spec", "vertices")
	if err != nil {
		return nil, fmt.Errorf("error while getting vertices of pipeline %s/%s: %w", pipelineDef.GetNamespace(), pipelineDef.GetName(), err)
	}

	scaleDefinitions := []apiv1.VertexScaleDefinition{}

	for _, vertex := range vertices {
		if vertexAsMap, ok := vertex.(map[string]any); ok {

			vertexName, foundVertexName, err := unstructured.NestedString(vertexAsMap, "name")
			if err != nil {
				return nil, err
			}
			if !foundVertexName {
				return nil, errors.New("a vertex must have a name")
			}

			vertexScaleDef, err := progressive.ExtractScaleMinMax(vertexAsMap, []string{"scale"})
			if err != nil {
				return nil, err
			}
			scaleDefinitions = append(scaleDefinitions, apiv1.VertexScaleDefinition{VertexName: vertexName, ScaleDefinition: vertexScaleDef})
		}
	}
	return scaleDefinitions, nil
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

		// first make sure that the Progressive status indicates that scale down happened
		return prProgressiveStatus.PromotedPipelineStatus.AllVerticesScaledDown &&
			prProgressiveStatus.PromotedPipelineStatus.Name == expectedPromotedPipelineName

	}).Should(BeTrue())

	CheckEventually("verifying Promoted Pipeline Vertex Scale", func() bool {

		promotedPipeline, err := GetPromotedPipeline(Namespace, pipelineRolloutName)
		if err != nil {
			return false
		}
		if promotedPipeline.GetName() != expectedPromotedPipelineName {
			return false
		}
		_, err = GetScaleValuesFromPipelineSpec(promotedPipeline)
		if err != nil {
			return false
		}

		prProgressiveStatus := GetPipelineRolloutProgressiveStatus(pipelineRolloutName)

		vertexScaleDefinitions, err := GetScaleValuesFromPipelineSpec(promotedPipeline)
		if err != nil {
			return false
		}

		// make sure the min/max from the promoted pipeline matches the Status
		// and make sure min=max
		if len(vertexScaleDefinitions) != len(prProgressiveStatus.PromotedPipelineStatus.ScaleValues) {
			return false
		}

		for _, vertexScaleDef := range vertexScaleDefinitions {
			statusDefinedScale := prProgressiveStatus.PromotedPipelineStatus.ScaleValues[vertexScaleDef.VertexName]
			if vertexScaleDef.ScaleDefinition == nil || vertexScaleDef.ScaleDefinition.Min == nil || vertexScaleDef.ScaleDefinition.Max == nil ||
				*vertexScaleDef.ScaleDefinition.Min != *vertexScaleDef.ScaleDefinition.Max || statusDefinedScale.ScaleTo != *vertexScaleDef.ScaleDefinition.Min {
				return false
			}
		}

		// TODO: look at actual number of Pods running

		return true
	}).Should(BeTrue())
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
	}).WithTimeout(TestTimeout).Should(BeTrue())
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
		upgradingScaleDefinitions, err := GetScaleValuesFromPipelineSpec(upgradingPipeline)
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

		// TODO: look at actual number of Pods running

		return true
	}).Should(BeTrue())
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

// what can we check?
// all promoted and upgrading vertices in actual Pipeline have min=max
// promoted status indicates allVerticesScaledDown
func PipelineTransientProgressiveChecks(pipelineRolloutName string, expectedPromotedPipelineName string, expectedUpgradingPipelineName string) {

	// Check Progressive status and pipelines while the assessment is in progress

	VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyProgressive)

	VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, expectedPromotedPipelineName, expectedUpgradingPipelineName,
		false, apiv1.AssessmentResultUnknown, false)

	VerifyPromotedPipelineScaledDownForProgressive(pipelineRolloutName, expectedPromotedPipelineName)
	VerifyUpgradingPipelineScaledDownForProgressive(pipelineRolloutName, expectedUpgradingPipelineName)

}

// if expectedAssessmentResult==Success, check that promoted pipeline min and max match PipelineSpec
// if expectedAssessmentResult==Failure, check that promoted pipeline min and max match PipelineSpec and that upgrading pipeline min=max=0
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

	// Verify that the previously promoted pipeline was deleted

}

// what could we pass in to perform the below for transient check?:
// - promotedName, upgradingName
// - do we really need ScaleValuesRestoredToOriginal? shouldn't they always be?
// check:
// - VerifyPipelineRolloutScaledDownForProgressive(pipelineRolloutName, promotedName)
// - VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, promoted.Name, upgrading.Name,
//		notRestoredToOriginal, AssessmentUnknown, false)
// - Get scaleTo value for each Promoted vertex and verify that that number of Pods is running
// - Get initial-scaleTo for each Upgrading vertex  and verify that that number of Pods is running

// what could we pass in to perform the below for final check?:
// - promotedName, upgradingName
// - expected pass vs fail
// - pipeline spec (to know that the promoted one scaled back up)
// check:
// - VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, promoted.Name, upgrading.Name,
//		scaleValuesRestoredToOriginal, assessmentResult, false)

/*	// Verify that the Pipeline is set to scale down
	VerifyPipelineRolloutScaledDownForProgressive(pipelineRolloutName, expectedPipelineTypeProgressiveStatusInProgress.Promoted.Name)

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
}*/

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
