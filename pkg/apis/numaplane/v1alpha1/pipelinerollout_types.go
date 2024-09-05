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
	// ConditionPipelinePausingOrPaused indicates that the Pipeline is either pausing or paused.
	ConditionPipelinePausingOrPaused ConditionType = "PipelinePausingOrPaused"
)

// PipelineRolloutSpec defines the desired state of PipelineRollout
type PipelineRolloutSpec struct {
	Pipeline Pipeline `json:"pipeline"`
}

// Pipeline includes the spec of Pipeline in Numaflow
type Pipeline struct {
	Spec runtime.RawExtension `json:"spec"`
}

// PipelineRolloutStatus defines the observed state of PipelineRollout
type PipelineRolloutStatus struct {
	Status      `json:",inline"`
	PauseStatus `json:"pauseStatus,omitempty"`

	// UpgradeInProgress indicates the upgrade strategy currently beign used and affecting the resource state or empty if no upgrade is in progress
	UpgradeInProgress string `json:"upgradeInProgress,omitempty"`
}

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="The current phase"
// +kubebuilder:printcolumn:name="Upgrade In Progress",type="string",JSONPath=".status.upgradeInProgress",description="The upgrade strategy currently prosessing the PipelineRollout. No upgrade in progress if empty"
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

func (status *PipelineRolloutStatus) MarkPipelinePausingOrPaused(reason, message string, generation int64) {
	status.MarkTrueWithReason(ConditionPipelinePausingOrPaused, reason, message, generation)
}

func (status *PipelineRolloutStatus) MarkPipelineUnpaused(generation int64) {
	status.MarkFalse(ConditionPipelinePausingOrPaused, "Unpaused", "Pipeline unpaused", generation)
}

func (status *PipelineRolloutStatus) SetUpgradeInProgress(upgradeStrategy string) {
	status.UpgradeInProgress = upgradeStrategy
}

func (status *PipelineRolloutStatus) ClearUpgradeInProgress() {
	status.UpgradeInProgress = ""
}
