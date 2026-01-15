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
	"k8s.io/apimachinery/pkg/runtime"
)

type AssessmentResult string

const (
	AssessmentResultSuccess AssessmentResult = "Success"
	AssessmentResultFailure AssessmentResult = "Failure"
	AssessmentResultUnknown AssessmentResult = "Unknown"
)

// UpgradingChildStatus describes the status of an upgrading child
type UpgradingChildStatus struct {
	// Name of the upgrading child
	Name string `json:"name"`

	// AssessmentResult described whether it's failed or succeeded, or to be determined
	AssessmentResult AssessmentResult `json:"assessmentResult,omitempty"`

	// BasicAssessmentStartTime indicates the time at/after which the basic resource health check assessment result will be computed
	BasicAssessmentStartTime *metav1.Time `json:"basicAssessmentStartTime,omitempty"`

	// BasicAssessmentEndTime indicates the time after which no more basic resource health check assessments will be performed
	BasicAssessmentEndTime *metav1.Time `json:"basicAssessmentEndTime,omitempty"`

	// BasicAssessmentResult indicates the result of the basic resource health check assessment
	BasicAssessmentResult AssessmentResult `json:"basicAssessmentResult,omitempty"`

	// TrialWindowStartTime indicates the time at which the trial window starts
	TrialWindowStartTime *metav1.Time `json:"trialWindowStartTime,omitempty"`

	// ForcedSuccess indicates if this promotion was forced to complete
	ForcedSuccess bool `json:"forcedSuccess,omitempty"`

	// ForcedSuccessReason indicates the reason for the forced success
	ForcedSuccessReason string `json:"forcedSuccessReason,omitempty"`

	// Discontinued indicates if the upgrade was stopped prematurely.
	// This can happen if the upgrade gets preempted by a new change, or it can happen if user deletes their promoted pipeline
	// in the middle of an upgrade
	Discontinued bool `json:"discontinued,omitempty"`

	// FailureReason indicates the reason for the failure
	FailureReason string `json:"failureReason,omitempty"`

	// ChildStatus is the full dump of child status object
	ChildStatus runtime.RawExtension `json:"childStatus,omitempty"`

	// InitializationComplete determines if the upgrade process has completed (if it hasn't, we will come back and try it again)
	InitializationComplete bool `json:"initializationComplete,omitempty"`

	// Riders stores the list of Riders that have been deployed along with the "upgrading" child
	Riders []RiderStatus `json:"riders,omitempty"`
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

	// OriginalHPADefinition is set if there was an HPA Rider originally for the promoted child
	OriginalHPADefinition *runtime.RawExtension `json:"originalHPADefinition,omitempty"`

	// ScaleTo indicates how many pods to scale down to
	ScaleTo int64 `json:"scaleTo"`
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

	// ScaleValues is a map where the keys are the promoted child vertices' names
	// and the values are the scale values of the vertices
	ScaleValues map[string]ScaleValues `json:"scaleValues,omitempty"`
	// ScaleValuesRestoredToOriginal indicates if ALL the promoted child vertices have been set back to the original min and max scale values.
	// This field being set to `true` invalidates the value(s) in the scaleValues.Actual field.
	ScaleValuesRestoredToOriginal bool `json:"scaleValuesRestoredToOriginal,omitempty"`
}

// IsAssessmentEndTimeSet checks if the AssessmentEndTime field is not nil.
func (ucs *UpgradingChildStatus) IsAssessmentEndTimeSet() bool {
	return ucs != nil && ucs.BasicAssessmentEndTime != nil
}

// IsBasicAssessmentResultSet checks if the BasicAssessmentResult field is empty.
func (ucs *UpgradingChildStatus) IsBasicAssessmentResultSet() bool {
	return ucs != nil && ucs.BasicAssessmentResult != ""
}

// IsTrialWindowStartTimeSet checks if the TrialWindowStartTime field is not nil.
func (ucs *UpgradingChildStatus) IsTrialWindowStartTimeSet() bool {
	return ucs != nil && ucs.TrialWindowStartTime != nil
}

// CanAssess determines if the UpgradingChildStatus instance is eligible for assessment.
// It checks that the current time is after the AssessmentStartTime and that it hasn't already previously failed
// (all checks within the time period must succeed, so if we previously failed, we maintain that failed status).
func (ucs *UpgradingChildStatus) CanAssess() bool {
	return ucs != nil &&
		ucs.BasicAssessmentStartTime != nil && time.Now().After(ucs.BasicAssessmentStartTime.Time) &&
		ucs.AssessmentResult != AssessmentResultFailure
}

// BasicAssessmentEndTimeArrived determines if the BasicAssessmentEndTime has been set and is in the past
func (ucs *UpgradingChildStatus) BasicAssessmentEndTimeArrived() bool {
	return ucs != nil &&
		ucs.BasicAssessmentEndTime != nil &&
		time.Now().After(ucs.BasicAssessmentEndTime.Time)
}

func (ucs *UpgradingChildStatus) IsFailed() bool {
	return ucs != nil && ucs.AssessmentResult == AssessmentResultFailure
}

// AreScaleValuesRestoredToOriginal checks if all vertices have been restored to the original scale values.
func (pcs *PromotedPipelineTypeStatus) AreScaleValuesRestoredToOriginal(name string) bool {
	return pcs != nil && pcs.Name == name && pcs.ScaleValuesRestoredToOriginal
}
