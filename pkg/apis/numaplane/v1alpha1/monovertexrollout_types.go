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
	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// ConditionMonoVertexPausingOrPaused indicates that the MonoVertex is either pausing or paused.
	ConditionMonoVertexPausingOrPaused ConditionType = "MonoVertexPausingOrPaused"
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

	// UpgradeInProgress indicates the upgrade strategy currently being used and affecting the resource state or empty if no upgrade is in progress
	UpgradeInProgress UpgradeStrategy `json:"upgradeInProgress,omitempty"`

	// NameCount is used as a suffix for the name of the managed monovertex, to uniquely
	// identify a monovertex.
	NameCount *int32 `json:"nameCount,omitempty"`
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

// the following functions implement the rolloutObject interface:
func (monoVertexRollout *MonoVertexRollout) GetRolloutGVR() metav1.GroupVersionResource {
	return metav1.GroupVersionResource{
		Group:    monoVertexRollout.TypeMeta.GroupVersionKind().Group,
		Version:  monoVertexRollout.TypeMeta.GroupVersionKind().Version,
		Resource: "monovertexrollouts",
	}
}

func (monoVertexRollout *MonoVertexRollout) GetRolloutGVK() schema.GroupVersionKind {
	return monoVertexRollout.TypeMeta.GroupVersionKind()
}

func (monoVertexRollout *MonoVertexRollout) GetChildGVR() metav1.GroupVersionResource {
	return metav1.GroupVersionResource{
		Group:    numaflowv1.MonoVertexGroupVersionKind.Group,
		Version:  numaflowv1.MonoVertexGroupVersionKind.Version,
		Resource: "monovertices",
	}
}

func (monoVertexRollout *MonoVertexRollout) GetChildGVK() schema.GroupVersionKind {
	return numaflowv1.MonoVertexGroupVersionKind
}

func (monoVertexRollout *MonoVertexRollout) GetRolloutObjectMeta() *metav1.ObjectMeta {
	return &monoVertexRollout.ObjectMeta
}

func (monoVertexRollout *MonoVertexRollout) GetRolloutStatus() *Status {
	return &monoVertexRollout.Status.Status
}

func init() {
	SchemeBuilder.Register(&MonoVertexRollout{}, &MonoVertexRolloutList{})
}

func (status *MonoVertexRolloutStatus) SetUpgradeInProgress(upgradeStrategy UpgradeStrategy) {
	status.UpgradeInProgress = upgradeStrategy
}

func (status *MonoVertexRolloutStatus) ClearUpgradeInProgress() {
	status.UpgradeInProgress = ""
}

func (status *MonoVertexRolloutStatus) MarkMonoVertexPaused(reason, message string, generation int64) {
	status.MarkTrueWithReason(ConditionMonoVertexPausingOrPaused, reason, message, generation)
}

func (status *MonoVertexRolloutStatus) MarkMonoVertexUnpaused(generation int64) {
	status.MarkFalse(ConditionMonoVertexPausingOrPaused, "Unpaused", "MonoVertex unpaused", generation)
}

// IsHealthy indicates whether the MonoVertexRollout is healthy.
func (mv *MonoVertexRolloutStatus) IsHealthy() bool {
	return mv.Phase == PhaseDeployed || mv.Phase == PhasePending
}
