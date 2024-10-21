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
	InstanceID string `json:"instanceID,omitempty"`
	Version    string `json:"version"`
}

// NumaflowControllerRolloutSpec defines the desired state of NumaflowControllerRollout
type NumaflowControllerRolloutSpec struct {
	Controller Controller `json:"controller"`
}

// NumaflowControllerRolloutStatus defines the observed state of NumaflowControllerRollout
type NumaflowControllerRolloutStatus struct {
	Status             `json:",inline"`
	PauseRequestStatus PauseStatus `json:"pauseRequestStatus,omitempty"`
}

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:validation:XValidation:rule="matches(self.metadata.name, '^numaflow-controller.*')",message="The metadata name must start with 'numaflow-controller'"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="The current phase"
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

// IsHealthy indicates whether the NumaflowController rollout is healthy or not
func (nc *NumaflowControllerRolloutStatus) IsHealthy() bool {
	return nc.Phase == PhaseDeployed || nc.Phase == PhasePending
}
