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
	"k8s.io/apimachinery/pkg/runtime"
)

// MonoVertexRolloutSpec defines the desired state of MonoVertexRollout
type MonoVertexRolloutSpec struct {
	MonoVertex MonoVertex `json:"monoVertex"`
}

// MonoVertex includes the spec of MonoVertex in Numaflow
type MonoVertex struct {
	Metadata `json:"metadata,omitempty"`
	Spec     runtime.RawExtension `json:"spec"`
}

// MonoVertexRolloutStatus defines the observed state of MonoVertexRollout
type MonoVertexRolloutStatus struct {
	Status `json:",inline"`
}

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="The current phase"
// MonoVertexRollout is the Schema for the monovertexrollouts API
type MonoVertexRollout struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MonoVertexRolloutSpec   `json:"spec"`
	Status MonoVertexRolloutStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// MonoVertexRolloutList contains a list of MonoVertexRollout
type MonoVertexRolloutList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MonoVertexRollout `json:"items"`
}

func (monoVertexRollout *MonoVertexRollout) GetTypeMeta() *metav1.TypeMeta {
	return &monoVertexRollout.TypeMeta
}

func (monoVertexRollout *MonoVertexRollout) GetObjectMeta() *metav1.ObjectMeta {
	return &monoVertexRollout.ObjectMeta
}

func (monoVertexRollout *MonoVertexRollout) GetStatus() *Status {
	return &monoVertexRollout.Status.Status
}
func (monoVertexRollout *MonoVertexRollout) GetChildPluralName() string {
	return "interstepbufferservices"
}

func init() {
	SchemeBuilder.Register(&MonoVertexRollout{}, &MonoVertexRolloutList{})
}

// IsHealthy indicates whether the MonoVertexRollout is healthy.
func (mv *MonoVertexRolloutStatus) IsHealthy() bool {
	return mv.Phase == PhaseDeployed || mv.Phase == PhasePending
}
