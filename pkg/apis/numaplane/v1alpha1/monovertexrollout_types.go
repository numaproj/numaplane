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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

	// NameCount is used as a suffix for the name of the managed monovertex, to uniquely
	// identify a monovertex.
	NameCount *int32 `json:"nameCount,omitempty"`

	// ProgressiveStatus stores fields related to the Progressive strategy
	ProgressiveStatus MonoVertexProgressiveStatus `json:"progressiveStatus,omitempty"`
}

type MonoVertexProgressiveStatus struct {
	// UpgradingMonoVertexStatus represents either the current or otherwise the most recent "upgrading" MonoVertex
	UpgradingMonoVertexStatus *UpgradingMonoVertexStatus `json:"upgradingMonoVertexStatus,omitempty"`
	// PromotedMonoVertexStatus stores information regarding the current "promoted" MonoVertex
	PromotedMonoVertexStatus *PromotedMonoVertexStatus `json:"promotedMonoVertexStatus,omitempty"`
}

// UpgradingMonoVertexStatus describes the status of an upgrading child
type UpgradingMonoVertexStatus struct {
	UpgradingChildStatus `json:",inline"`
}

// PromotedMonoVertexStatus describes the status of the promoted child
type PromotedMonoVertexStatus struct {
	PromotedPipelineTypeStatus `json:",inline"`
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

// GetUpgradingChildStatus is a function of the progressiveRolloutObject
func (monoVertexRollout *MonoVertexRollout) GetUpgradingChildStatus() *UpgradingChildStatus {
	if monoVertexRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus == nil {
		return nil
	}
	return &monoVertexRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus.UpgradingChildStatus
}

// ResetUpgradingChildStatus is a function of the progressiveRolloutObject
// note this resets the entire Upgrading status struct which encapsulates the UpgradingChildStatus struct
func (monoVertexRollout *MonoVertexRollout) ResetUpgradingChildStatus(upgradingMonoVertex *unstructured.Unstructured) error {
	monoVertexRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus = &UpgradingMonoVertexStatus{
		UpgradingChildStatus: UpgradingChildStatus{
			Name: upgradingMonoVertex.GetName(),
		},
	}
	return nil
}

// GetPromotedChildStatus is a function of the progressiveRolloutObject
func (monoVertexRollout *MonoVertexRollout) GetPromotedChildStatus() *PromotedChildStatus {
	if monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus == nil {
		return nil
	}
	return &monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus.PromotedChildStatus
}

// SetUpgradingChildStatus is a function of the progressiveRolloutObject
func (monoVertexRollout *MonoVertexRollout) SetUpgradingChildStatus(status *UpgradingChildStatus) {
	if monoVertexRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus == nil {
		monoVertexRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus = &UpgradingMonoVertexStatus{}
	}
	monoVertexRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus.UpgradingChildStatus = *status.DeepCopy()
}

// ResetPromotedChildStatus is a function of the progressiveRolloutObject
// note this resets the entire Promoted status struct which encapsulates the PromotedChildStatus struct
func (monoVertexRollout *MonoVertexRollout) ResetPromotedChildStatus(promotedMonoVertex *unstructured.Unstructured) error {
	monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus = &PromotedMonoVertexStatus{
		PromotedPipelineTypeStatus: PromotedPipelineTypeStatus{
			PromotedChildStatus: PromotedChildStatus{
				Name: promotedMonoVertex.GetName(),
			},
		},
	}
	return nil
}

// SetPromotedChildStatus is a function of the progressiveRolloutObject
func (monoVertexRollout *MonoVertexRollout) SetPromotedChildStatus(status *PromotedChildStatus) {
	if monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus == nil {
		monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus = &PromotedMonoVertexStatus{}
	}
	monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus.PromotedPipelineTypeStatus.PromotedChildStatus = *status.DeepCopy()
}

func init() {
	SchemeBuilder.Register(&MonoVertexRollout{}, &MonoVertexRolloutList{})
}

func (status *MonoVertexRolloutStatus) MarkMonoVertexPaused(reason, message string, generation int64) {
	status.MarkTrueWithReason(ConditionMonoVertexPausingOrPaused, reason, message, generation)
}

func (status *MonoVertexRolloutStatus) MarkMonoVertexUnpaused(generation int64) {
	status.MarkFalse(ConditionMonoVertexPausingOrPaused, "Unpaused", "MonoVertex unpaused", generation)
}
