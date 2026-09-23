/*
Copyright 2023.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

type Controller struct {
	// NOTE: keeping the instanceID also in the NumaflowControllerRollout in case users want to
	// create multiple Numaflow controllers within the same namespace
	InstanceID string `json:"instanceID,omitempty"`
	Version    string `json:"version"`
}

// NumaflowControllerRolloutSpec defines the desired state of NumaflowControllerRollout
type NumaflowControllerRolloutSpec struct {
	Controller Controller          `json:"controller"`
	Strategy   *ControllerStrategy `json:"strategy,omitempty"`
}

// ControllerStrategy defines the strategy to use when upgrading the NumaflowController
type ControllerStrategy struct {
	Progressive *ControllerProgressiveStrategy `json:"progressive,omitempty"`
}

// ControllerProgressiveStrategy defines the configuration for a progressive upgrade of the NumaflowController
type ControllerProgressiveStrategy struct {
	// AssessmentSchedule describes the schedule for how often we assess the health of the upgrading controller instance,
	// in the format "delay,end,period,interval" (see config.AssessmentSchedule)
	AssessmentSchedule string `json:"assessmentSchedule,omitempty"`
}

// NumaflowControllerRolloutStatus defines the observed state of NumaflowControllerRollout
type NumaflowControllerRolloutStatus struct {
	Status             `json:",inline"`
	PauseRequestStatus PauseStatus `json:"pauseRequestStatus,omitempty"`

	// ControllerInstances holds one entry for each live NumaflowController child, whether promoted or trial.
	ControllerInstances []ControllerInstanceRef `json:"controllerInstances,omitempty"`
}

// ControllerInstanceRef represents a live NumaflowController child, either promoted or trial.
type ControllerInstanceRef struct {
	// Name of the child NumaflowController CR, e.g. "numaflow-controller-2".
	Name string `json:"name"`

	// InstanceID is stamped into the child controller's spec and into every Pipeline or MonoVertex bound to this
	// controller instance.
	InstanceID string `json:"instanceID"`

	Version string `json:"version"`

	// State reuses the existing common.UpgradeState values: "promoted" or "trial".
	State string `json:"state"`

	// ReferencingInterStepBufferServices and ReferencingMonovertices are live counts of ISBServices and MonoVertices,
	// respectively, currently bound to this instance. Pipelines are not counted directly: a Pipeline's ISBSvc is not
	// deleted while that Pipeline still references it, so counting ISBServices already transitively captures
	// Pipeline usage. Both fields are consulted exclusively by Recycle() as a deletion-safety gate, once a controller
	// instance has already been marked recyclable.
	ReferencingInterStepBufferServices int32 `json:"referencingInterStepBufferServices"`
	ReferencingMonovertices            int32 `json:"referencingMonovertices"`
}

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="The current phase"
// +kubebuilder:printcolumn:name="Version",type="string",JSONPath=".spec.controller.version",description="The desired Numaflow Controller version"
// NumaflowControllerRollout is the Schema for the numaflowcontrollerrollouts API
type NumaflowControllerRollout struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NumaflowControllerRolloutSpec   `json:"spec,omitempty"`
	Status NumaflowControllerRolloutStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// NumaflowControllerRolloutList contains a list of NumaflowControllerRollout
type NumaflowControllerRolloutList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NumaflowControllerRollout `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NumaflowControllerRollout{}, &NumaflowControllerRolloutList{})
}
