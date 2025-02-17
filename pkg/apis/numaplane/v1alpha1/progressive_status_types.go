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
	// NextAssessmentTime indicates the time at/after which the assessment result will be computed
	NextAssessmentTime *metav1.Time `json:"nextAssessmentTime,omitempty"`
	// AssessUntil indicates the time after which no more assessments will be performed
	AssessUntil *metav1.Time `json:"assessUntil,omitempty"`
}

// ScaleValues stores the desired min and max, scaleTo, and actual scale values of a pipeline or monovertex vertex
type ScaleValues struct {
	// IsDesiredScaleSet indicates if the original child spec scale field is set (true) or nil (false)
	IsDesiredScaleSet bool `json:"isDesiredScaleSet"`
	// DesiredMin is the min scale value of the original child spec
	DesiredMin *int64 `json:"desiredMin"`
	// DesiredMax is the max scale value of the original child spec
	DesiredMax *int64 `json:"desiredMax"`
	// ScaleTo indicates how many pods to scale down to
	ScaleTo int64 `json:"scaleTo"`
	// Actual indicates how many pods are actually running for the vertex
	Actual int64 `json:"actual"`
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
	// ScaleValuesRestoredToDesired indicates if ALL the promoted child source vertices have been set back to the original min and max scale values (desiredMin and desiredMax).
	// This field being set to `true` invalidates the value(s) in the scaleValues.Actual field.
	ScaleValuesRestoredToDesired bool `json:"scaleValuesRestoredToDesired,omitempty"`
}

// assessUntilInitValue is an arbitrary value in the far future to use as a maximum value for AssessUntil
var assessUntilInitValue = time.Date(2222, 2, 2, 2, 2, 2, 0, time.UTC)

// IsAssessUntilSet checks if the AssessUntil field is not nil nor set to a maximum arbitrary value in the far future.
func (ucs *UpgradingChildStatus) IsAssessUntilSet() bool {
	return ucs != nil && ucs.AssessUntil != nil && !ucs.AssessUntil.Time.Equal(assessUntilInitValue)
}

// CanAssess determines if the UpgradingChildStatus instance is eligible for assessment.
// It checks that the current time is after the NextAssessmentTime and that it hasn't already previously failed
// (all checks within the time period must succeed, so if we previously failed, we maintain that failed status).
func (ucs *UpgradingChildStatus) CanAssess() bool {
	return ucs != nil &&
		ucs.NextAssessmentTime != nil && time.Now().After(ucs.NextAssessmentTime.Time) &&
		//ucs.AssessUntil != nil && time.Now().Before(ucs.AssessUntil.Time) &&
		ucs.AssessmentResult != AssessmentResultFailure
}

// CanDeclareSuccess() determines if it's okay to declare success:
// We must have arrived at the AssessUntil time
// Note that if we've arrived at the AssessUntil time, then the assumption is that it never failed within the window;
// otherwise we would've stopped assessing
func (ucs *UpgradingChildStatus) CanDeclareSuccess() bool {
	return ucs != nil &&
		ucs.AssessUntil != nil &&
		time.Now().After(ucs.AssessUntil.Time)
}

func (ucs *UpgradingChildStatus) IsFailed() bool {
	return ucs != nil && ucs.AssessmentResult == AssessmentResultFailure
}

// AreAllSourceVerticesScaledDown checks if all source vertices have been scaled down for the named child.
func (pcs *PromotedPipelineTypeStatus) AreAllSourceVerticesScaledDown(name string) bool {
	return pcs != nil && pcs.Name == name && pcs.AllSourceVerticesScaledDown
}

// AreScaleValuesRestoredToDesired checks if all source vertices have been restored to the desired min and max values.
func (pcs *PromotedPipelineTypeStatus) AreScaleValuesRestoredToDesired(name string) bool {
	return pcs != nil && pcs.Name == name && pcs.ScaleValuesRestoredToDesired
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
		if sv.Actual > sv.ScaleTo {
			allScaledDown = false
			break
		}
	}

	pcs.AllSourceVerticesScaledDown = allScaledDown
}
