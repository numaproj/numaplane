/*
Copyright 2024.

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

package v1alpha1

import (
	"time"

	argorolloutsv1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type AssessmentResult string

const (
	AssessmentResultSuccess = "Success"
	AssessmentResultFailure = "Failure"
	AssessmentResultUnknown = "Unknown"
)

// UpgradingChildStatus describes the status of an upgrading child
type UpgradingChildStatus struct {
	// Name of the upgrading child
	Name string `json:"name"`
	// AssessmentResult described whether it's failed or succeeded, or to be determined
	AssessmentResult AssessmentResult `json:"assessmentResult,omitempty"`
	// AssessmentStartTime indicates the time at/after which the assessment result will be computed
	AssessmentStartTime *metav1.Time `json:"assessmentStartTime,omitempty"`
	// AssessmentEndTime indicates the time after which no more assessments will be performed
	AssessmentEndTime *metav1.Time `json:"assessmentEndTime,omitempty"`
	// ForcedSuccess indicates if this promotion was forced to complete
	ForcedSuccess bool `json:"forcedSuccess,omitempty"`
	// FailureReason indicates the reason for the failure
	FailureReason string `json:"failureReason,omitempty"`
}

type UpgradingPipelineTypeStatus struct {
	UpgradingChildStatus `json:",inline"`

	Analysis AnalysisStatus `json:"analysis,omitempty"`
}

type AnalysisStatus struct {
	// AnalysisRunName is the name of the AnalysisRun, set after it's generated
	AnalysisRunName string `json:"analysisRunName,omitempty"`
	// StartTime is the time that the AnalysisRun is created
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// EndTime is the time that it completed
	EndTime *metav1.Time `json:"endTime,omitempty"`
	// Phase is the phase of the AnalysisRun when completed
	Phase argorolloutsv1.AnalysisPhase `json:"phase"`
}

// ScaleValues stores the original scale min and max values, scaleTo value, and actual scale value of a pipeline or monovertex vertex
type ScaleValues struct {
	// OriginalScaleMinMax stores the original scale min and max values as JSON string
	OriginalScaleMinMax string `json:"originalScaleMinMax"`
	// ScaleTo indicates how many pods to scale down to
	ScaleTo int64 `json:"scaleTo"`
	// Current indicates how many pods are currently running for the vertex
	Current int64 `json:"current"`
	// Initial indicates how many pods were initially running for the vertex at the beginning of the upgrade process
	Initial int64 `json:"initial"`
}

// PromotedChildStatus describes the status of the promoted child
type PromotedChildStatus struct {
	// Name of the promoted child
	Name string `json:"name"`
}

// PromotedPipelineTypeStatus describes the status of a promoted child, and applies to all pipeline types
// (Pipeline and MonoVertex)
type PromotedPipelineTypeStatus struct {
	PromotedChildStatus `json:",inline"`

	// ScaleValues is a map where the keys are the promoted child source vertices names
	// and the values are the scale values of the source vertices
	ScaleValues map[string]ScaleValues `json:"scaleValues,omitempty"`
	// AllSourceVerticesScaledDown indicates if ALL the promoted child source vertices have been scaled down
	AllSourceVerticesScaledDown bool `json:"allSourceVerticesScaledDown,omitempty"`
	// ScaleValuesRestoredToOriginal indicates if ALL the promoted child source vertices have been set back to the original min and max scale values.
	// This field being set to `true` invalidates the value(s) in the scaleValues.Actual field.
	ScaleValuesRestoredToOriginal bool `json:"scaleValuesRestoredToOriginal,omitempty"`
}

// IsAssessmentEndTimeSet checks if the AssessmentEndTime field is not nil.
func (ucs *UpgradingChildStatus) IsAssessmentEndTimeSet() bool {
	return ucs != nil && ucs.AssessmentEndTime != nil
}

// CanAssess determines if the UpgradingChildStatus instance is eligible for assessment.
// It checks that the current time is after the AssessmentStartTime and that it hasn't already previously failed
// (all checks within the time period must succeed, so if we previously failed, we maintain that failed status).
func (ucs *UpgradingChildStatus) CanAssess() bool {
	return ucs != nil &&
		ucs.AssessmentStartTime != nil && time.Now().After(ucs.AssessmentStartTime.Time) &&
		ucs.AssessmentResult != AssessmentResultFailure
}

// CanDeclareSuccess() determines if it's okay to declare success:
// We must have arrived at the AssessmentEndTime
// Note that if we've arrived at the AssessmentEndTime, then the assumption is that it never failed within the window;
// otherwise we would've stopped assessing
func (ucs *UpgradingChildStatus) CanDeclareSuccess() bool {
	return ucs != nil &&
		ucs.AssessmentEndTime != nil &&
		time.Now().After(ucs.AssessmentEndTime.Time)
}

func (ucs *UpgradingChildStatus) IsFailed() bool {
	return ucs != nil && ucs.AssessmentResult == AssessmentResultFailure
}

// AreAllSourceVerticesScaledDown checks if all source vertices have been scaled down for the named child.
func (pcs *PromotedPipelineTypeStatus) AreAllSourceVerticesScaledDown(name string) bool {
	return pcs != nil && pcs.Name == name && pcs.AllSourceVerticesScaledDown
}

// AreScaleValuesRestoredToOriginal checks if all source vertices have been restored to the original scale values.
func (pcs *PromotedPipelineTypeStatus) AreScaleValuesRestoredToOriginal(name string) bool {
	return pcs != nil && pcs.Name == name && pcs.ScaleValuesRestoredToOriginal
}

// MarkAllSourceVerticesScaledDown checks if all source vertices in the PromotedChildStatus
// have been scaled down by comparing their Actual and ScaleTo scale values.
// It updates the AllSourceVerticesScaledDown field to true if all vertices
// are scaled down, otherwise sets it to false.
func (pcs *PromotedPipelineTypeStatus) MarkAllSourceVerticesScaledDown() {
	if pcs == nil || len(pcs.ScaleValues) == 0 {
		return
	}

	allScaledDown := true
	for _, sv := range pcs.ScaleValues {
		if sv.Current > sv.ScaleTo {
			allScaledDown = false
			break
		}
	}

	pcs.AllSourceVerticesScaledDown = allScaledDown
}
