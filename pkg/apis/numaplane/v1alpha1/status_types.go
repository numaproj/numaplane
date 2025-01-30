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
	"reflect"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConditionType is a valid value of Condition.Type
type ConditionType string

// +kubebuilder:validation:Enum="";Pending;Deployed;Failed
type Phase string

const (
	// PhasePending indicates that a reconciliation operation on the rollout spec has started.
	// In this phase, the reconciliation process could take some time and/or happen with multiple reconciliation calls.
	PhasePending Phase = "Pending"

	// PhaseDeployed indicates that the child resource has been applied to the cluster.
	PhaseDeployed Phase = "Deployed"

	// PhaseFailed indicates that one or more errors have occurred during reconciliation.
	PhaseFailed Phase = "Failed"

	// ConditionChildResourceHealthy indicates if the child resource is in a healthy state.
	ConditionChildResourceHealthy ConditionType = "ChildResourcesHealthy"

	// ConditionChildResourceDeployed indicates that the child resource was deployed.
	ConditionChildResourceDeployed ConditionType = "ChildResourceDeployed"

	// ConditionPausingPipelines applies to ISBServiceRollout or NumaflowControllerRollout for when they are in the process
	// of pausing pipelines
	ConditionPausingPipelines ConditionType = "PausingPipelines"

	// ConditionProgressiveUpgradeSucceeded indicates that whether the progressive upgrade succeeded.
	ConditionProgressiveUpgradeSucceeded ConditionType = "ProgressiveUpgradeSucceeded"

	// ProgressingReasonString indicates the status condition reason as Progressing
	ProgressingReasonString = "Progressing"
)

// Status is a common structure which can be used for Status field.
type Status struct {
	// Conditions are the latest available observations of a resource's current state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`

	// Message is added if Phase is PhaseFailed.
	Message string `json:"message,omitempty"`

	// Phase indicates the current phase of the resource.
	Phase Phase `json:"phase,omitempty"`

	// LastFailureTime records the timestamp of the Last Failure (PhaseFailed)
	LastFailureTime metav1.Time `json:"lastFailureTime,omitempty"`

	// ObservedGeneration stores the generation value observed when setting the current Phase
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// UpgradeInProgress indicates the upgrade strategy currently being used and affecting the resource state or empty if no upgrade is in progress
	UpgradeInProgress UpgradeStrategy `json:"upgradeInProgress,omitempty"`

	// ProgressiveStatus stores fields related to the Progressive strategy
	ProgressiveStatus ProgressiveStatus `json:"progressiveStatus,omitempty"`
}

// PauseStatus is a common structure used to communicate how long Pipelines are paused.
type PauseStatus struct {
	// The begin timestamp for the last pause of the Pipeline.
	LastPauseBeginTime metav1.Time `json:"lastPauseBeginTime,omitempty"`

	// The transition timestamp from Pausing to Paused for the last pause of the Pipeline.
	LastPauseTransitionTime metav1.Time `json:"lastPausePhaseChangeTime,omitempty"`

	// The end timestamp for the last pause of the Pipeline.
	LastPauseEndTime metav1.Time `json:"lastPauseEndTime,omitempty"`
}

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

// PromotedChildStatus describes the status of the promoted child
type PromotedChildStatus struct {
	// Name of the promoted child
	Name string `json:"name"`
	// ScaledDown indicates if ALL the promoted child source vertices have been scaled down
	AllSourceVerticesScaledDown bool `json:"allSourceVerticesScaledDown,omitempty"`
}

type ProgressiveStatus struct {
	// UpgradingChildStatus represents either the current or otherwise the most recent "upgrading" child
	UpgradingChildStatus *UpgradingChildStatus `json:"upgradingChildStatus,omitempty"`
	// PromotedChild stores information regarding the current "promoted" child status
	PromotedChildStatus *PromotedChildStatus `json:"promotedChildStatus,omitempty"`
}

func (status *Status) SetPhase(phase Phase, msg string) {
	if phase == PhaseFailed {
		status.LastFailureTime = metav1.NewTime(time.Now())
	}

	status.Phase = phase
	status.Message = msg
}

// IsHealthy indicates whether the resource is healthy
func (s *Status) IsHealthy() bool {
	return s.Phase == PhaseDeployed || s.Phase == PhasePending
}

func (status *Status) SetObservedGeneration(generation int64) {
	status.ObservedGeneration = generation
}

// MarkTrue sets the status of t to true
func (s *Status) MarkTrue(t ConditionType, generation int64) {
	s.markTypeStatus(t, metav1.ConditionTrue, "Successful", "Successful", generation)
}

// MarkTrueWithReason sets the status of t to true with reason
func (s *Status) MarkTrueWithReason(t ConditionType, reason, message string, generation int64) {
	s.markTypeStatus(t, metav1.ConditionTrue, reason, message, generation)
}

// MarkFalse sets the status of t to fasle
func (s *Status) MarkFalse(t ConditionType, reason, message string, generation int64) {
	s.markTypeStatus(t, metav1.ConditionFalse, reason, message, generation)
}

// MarkUnknown sets the status of t to unknown
func (s *Status) MarkUnknown(t ConditionType, reason, message string, generation int64) {
	s.markTypeStatus(t, metav1.ConditionUnknown, reason, message, generation)
}

// GetCondition returns the condition of a condition type
func (s *Status) GetCondition(t ConditionType) *metav1.Condition {
	for _, c := range s.Conditions {
		if c.Type == string(t) {
			return &c
		}
	}
	return nil
}

// Init sets certain Status parameters to a default initial state
func (status *Status) Init(generation int64) {
	status.SetObservedGeneration(generation)
	// rationale for commenting this out:
	// "Pending" is now something we indicate when a rollout has been updated and we are trying to deploy it,
	// as opposed to meaning that we're "pending reconciliation"
	//status.MarkPending()
}

// MarkPending sets Phase to Pending
func (status *Status) MarkPending() {
	status.SetPhase(PhasePending, "Progressing")
}

// MarkDeployed sets Phase to Deployed
func (status *Status) MarkDeployed(generation int64) {
	status.SetPhase(PhaseDeployed, "Deployed")
	status.MarkTrue(ConditionChildResourceDeployed, generation)
}

// MarkFailed sets Phase to Failed
func (status *Status) MarkFailed(message string) {
	status.SetPhase(PhaseFailed, message)
}

func (status *Status) MarkChildResourcesHealthy(generation int64) {
	status.MarkTrue(ConditionChildResourceHealthy, generation)
}

func (status *Status) MarkChildResourcesUnhealthy(reason, message string, generation int64) {
	status.MarkFalse(ConditionChildResourceHealthy, reason, message, generation)
}

func (status *Status) MarkChildResourcesHealthUnknown(reason, message string, generation int64) {
	status.MarkUnknown(ConditionChildResourceHealthy, reason, message, generation)
}

func (status *Status) MarkPausingPipelines(generation int64) {
	status.MarkTrueWithReason(ConditionPausingPipelines, "Pause", "pause needed for update", generation)
}

func (status *Status) MarkUnpausingPipelines(generation int64) {
	status.MarkFalse(ConditionPausingPipelines, "NoPause", "no need for pause", generation)
}

func (status *Status) MarkProgressiveUpgradeSucceeded(message string, generation int64) {
	status.MarkTrueWithReason(ConditionProgressiveUpgradeSucceeded, "Succeeded", message, generation)
}

func (status *Status) MarkProgressiveUpgradeFailed(message string, generation int64) {
	status.MarkFalse(ConditionProgressiveUpgradeSucceeded, "Failed", message, generation)
}

func (status *Status) SetUpgradeInProgress(upgradeStrategy UpgradeStrategy) {
	status.UpgradeInProgress = upgradeStrategy
}

func (status *Status) ClearUpgradeInProgress() {
	status.UpgradeInProgress = ""
}

// assessUntilInitValue is an arbitrary value in the far future to use as a maximum value for AssessUntil
var assessUntilInitValue = time.Date(2222, 2, 2, 2, 2, 2, 0, time.UTC)

// InitAssessUntil initializes the AssessUntil field to a large value.
func (ucs *UpgradingChildStatus) InitAssessUntil() {
	if ucs == nil {
		ucs = &UpgradingChildStatus{}
	}

	assessUntil := metav1.NewTime(assessUntilInitValue)
	ucs.AssessUntil = &assessUntil
}

// IsAssessUntilSet checks if the AssessUntil field is not nil nor set to a maximum arbitrary value in the far future.
func (ucs *UpgradingChildStatus) IsAssessUntilSet() bool {
	return ucs != nil && ucs.AssessUntil != nil && !ucs.AssessUntil.Time.Equal(assessUntilInitValue)
}

// CanAssess determines if the UpgradingChildStatus instance is eligible for assessment.
// It checks that the current time is after the NextAssessmentTime and
// before the AssessUntil time, and that it hasn't already previously failed
// (all checks within the time period must succeed, so if we previously failed, we maintain that failed status).
func (ucs *UpgradingChildStatus) CanAssess() bool {
	return ucs != nil &&
		ucs.NextAssessmentTime != nil && time.Now().After(ucs.NextAssessmentTime.Time) &&
		ucs.AssessUntil != nil && time.Now().Before(ucs.AssessUntil.Time) &&
		ucs.AssessmentResult != AssessmentResultFailure
}

// AreAllSourceVerticesScaledDown checks if all source vertices have been scaled down for the named child.
func (pcs *PromotedChildStatus) AreAllSourceVerticesScaledDown(name string) bool {
	return pcs != nil && pcs.Name == name && pcs.AllSourceVerticesScaledDown
}

// MarkAllSourceVerticesScaledDownTrue sets the AllSourceVerticesScaledDown field
// of the named PromotedChildStatus to true. If the PromotedChildStatus is nil, it initializes
// a new instance before setting the field.
func (pcs *PromotedChildStatus) MarkAllSourceVerticesScaledDownTrue(name string) {
	if pcs == nil {
		pcs = &PromotedChildStatus{}
	}

	pcs.Name = name
	pcs.AllSourceVerticesScaledDown = true
}

// setCondition sets a condition
func (s *Status) setCondition(condition metav1.Condition) {
	var conditions []metav1.Condition
	for _, currCondition := range s.Conditions {
		if currCondition.Type != condition.Type {
			conditions = append(conditions, currCondition)
		} else {
			// Do not update lastTransitionTime if the status nor the reason of the condition change
			if currCondition.Status == condition.Status && currCondition.Reason == condition.Reason {
				condition.LastTransitionTime = currCondition.LastTransitionTime
			}

			if reflect.DeepEqual(&condition, &currCondition) {
				return
			}
		}
	}

	if condition.LastTransitionTime == metav1.NewTime(time.Time{}) {
		condition.LastTransitionTime = metav1.NewTime(time.Now())
	}

	conditions = append(conditions, condition)

	// Sort for easy read
	sort.Slice(conditions, func(i, j int) bool { return conditions[i].Type < conditions[j].Type })
	s.Conditions = conditions
}

func (s *Status) markTypeStatus(t ConditionType, status metav1.ConditionStatus, reason, message string, generation int64) {
	s.setCondition(metav1.Condition{
		Type:               string(t),
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}
