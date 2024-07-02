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

	// ConditionChildResourceHealthy indicates if the child resource is in a healthy phase (ex: not pending, not paused, not deleting, etc.).
	// Child health is set to True only if the child resource generation is equal to its own observedGeneration.
	ConditionChildResourceHealthy ConditionType = "ChildResourcesHealthy"

	// ConditionChildResourceDeployed indicates that the child resource was deployed.
	ConditionChildResourceDeployed ConditionType = "ChildResourceDeployed"
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

	// ObservedGeneration stores the generation value observed when setting the current Phase
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// InitializeConditions initializes the conditions to Unknown
func (s *Status) InitializeConditions(conditionTypes ...ConditionType) {
	for _, t := range conditionTypes {
		c := metav1.Condition{
			Type:   string(t),
			Status: metav1.ConditionUnknown,
			Reason: "Unknown",
		}
		s.setCondition(c)
	}
}

func (status *Status) SetPhase(phase Phase, msg string) {
	status.Phase = phase
	status.Message = msg
}

// MarkTrue sets the status of t to true
func (s *Status) MarkTrue(t ConditionType, observedGeneration int64) {
	s.markTypeStatus(t, metav1.ConditionTrue, "Successful", "Successful", observedGeneration)
}

// MarkTrueWithReason sets the status of t to true with reason
func (s *Status) MarkTrueWithReason(t ConditionType, reason, message string, observedGeneration int64) {
	s.markTypeStatus(t, metav1.ConditionTrue, reason, message, observedGeneration)
}

// MarkFalse sets the status of t to fasle
func (s *Status) MarkFalse(t ConditionType, reason, message string, observedGeneration int64) {
	s.markTypeStatus(t, metav1.ConditionFalse, reason, message, observedGeneration)
}

// MarkUnknown sets the status of t to unknown
func (s *Status) MarkUnknown(t ConditionType, reason, message string, observedGeneration int64) {
	s.markTypeStatus(t, metav1.ConditionUnknown, reason, message, observedGeneration)
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

// Init sets various Status parameters (Conditions, Phase, etc.) to a default initial state
func (status *Status) Init(generation int64) {
	status.InitializeConditions(ConditionChildResourceDeployed, ConditionChildResourceHealthy)

	if generation != status.ObservedGeneration {
		status.SetObservedGeneration(generation)
		status.MarkPending()
	}
}

func (status *Status) SetObservedGeneration(generation int64) {
	status.ObservedGeneration = generation
}

// MarkPending sets Phase to Pending
func (status *Status) MarkPending() {
	status.SetPhase(PhasePending, "")
}

// MarkDeployed sets Phase to Deployed
func (status *Status) MarkDeployed(observedGeneration int64) {
	status.SetPhase(PhaseDeployed, "")
	status.MarkTrue(ConditionChildResourceDeployed, observedGeneration)
}

// MarkFailed sets Phase to Failed
func (status *Status) MarkFailed(reason, message string) {
	status.SetPhase(PhaseFailed, message)
}

func (status *Status) MarkChildResourcesHealthy(observedGeneration int64) {
	status.MarkTrue(ConditionChildResourceHealthy, observedGeneration)
}

func (status *Status) MarkChildResourcesUnhealthy(reason, message string, observedGeneration int64) {
	status.MarkFalse(ConditionChildResourceHealthy, reason, message, observedGeneration)
}

func (status *Status) MarkChildResourcesHealthUnknown(reason, message string, observedGeneration int64) {
	status.MarkUnknown(ConditionChildResourceHealthy, reason, message, observedGeneration)
}

// setCondition sets a condition
func (s *Status) setCondition(condition metav1.Condition) {
	var conditions []metav1.Condition
	for _, c := range s.Conditions {
		if c.Type != condition.Type {
			conditions = append(conditions, c)
		} else {
			condition.LastTransitionTime = c.LastTransitionTime
			if reflect.DeepEqual(&condition, &c) {
				return
			}
		}
	}
	condition.LastTransitionTime = metav1.NewTime(time.Now())
	conditions = append(conditions, condition)
	// Sort for easy read
	sort.Slice(conditions, func(i, j int) bool { return conditions[i].Type < conditions[j].Type })
	s.Conditions = conditions
}

func (s *Status) markTypeStatus(t ConditionType, status metav1.ConditionStatus, reason, message string, observedGeneration int64) {
	s.setCondition(metav1.Condition{
		Type:               string(t),
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: observedGeneration,
	})
}
