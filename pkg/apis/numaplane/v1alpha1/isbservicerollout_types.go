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

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ISBServiceRolloutSpec defines the desired state of ISBServiceRollout
type ISBServiceRolloutSpec struct {
	InterStepBufferService InterStepBufferService `json:"interStepBufferService"`
}

// InterStepBufferService includes the spec of InterStepBufferService in Numaflow
type InterStepBufferService struct {
	Spec runtime.RawExtension `json:"spec"`
}

// ISBServiceRolloutStatus defines the observed state of ISBServiceRollout
type ISBServiceRolloutStatus struct {
	Status `json:",inline"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
//+kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="The current phase"

// ISBServiceRollout is the Schema for the isbservicerollouts API
type ISBServiceRollout struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ISBServiceRolloutSpec   `json:"spec"`
	Status ISBServiceRolloutStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// ISBServiceRolloutList contains a list of ISBServiceRollout
type ISBServiceRolloutList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ISBServiceRollout `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ISBServiceRollout{}, &ISBServiceRolloutList{})
}
