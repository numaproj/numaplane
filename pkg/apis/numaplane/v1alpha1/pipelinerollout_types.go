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

const (
	// ConditionPipelinePaused indicates the Pipeline is either paused or pausing.
	ConditionPipelinePaused ConditionType = "PipelinePaused"

	// TODO: should we also consider including the following conditions from Numaflow Pipeline conditions?
	// PipelinePhaseSucceeded PipelinePhase = "Succeeded"
	// PipelinePhaseDeleting  PipelinePhase = "Deleting"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// PipelineRolloutSpec defines the desired state of PipelineRollout
type PipelineRolloutSpec struct {
	Pipeline runtime.RawExtension `json:"pipeline"`
}

// PipelineRolloutStatus defines the observed state of PipelineRollout
type PipelineRolloutStatus struct {
	Status `json:",inline"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
//+kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="The current phase"

// PipelineRollout is the Schema for the pipelinerollouts API
type PipelineRollout struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PipelineRolloutSpec   `json:"spec"`
	Status PipelineRolloutStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// PipelineRolloutList contains a list of PipelineRollout
type PipelineRolloutList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PipelineRollout `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PipelineRollout{}, &PipelineRolloutList{})
}

// MarkPaused sets ConditionPipelinePaused to True
func (status *Status) MarkPaused() {
	status.MarkTrue(ConditionPipelinePaused)
}
