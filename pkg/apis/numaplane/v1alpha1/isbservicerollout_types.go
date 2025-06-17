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

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ISBServiceRolloutSpec defines the desired state of ISBServiceRollout
type ISBServiceRolloutSpec struct {
	InterStepBufferService InterStepBufferService     `json:"interStepBufferService"`
	Strategy               *ISBServiceRolloutStrategy `json:"strategy,omitempty"`
	Riders                 []Rider                    `json:"riders,omitempty"`
}

type ISBServiceRolloutStrategy struct {
	Progressive ProgressiveStrategy `json:"progressive,omitempty"`
}

// InterStepBufferService includes the spec of InterStepBufferService in Numaflow
type InterStepBufferService struct {
	Metadata `json:"metadata,omitempty"`
	Spec     runtime.RawExtension `json:"spec"`
}

// ISBServiceRolloutStatus defines the observed state of ISBServiceRollout
type ISBServiceRolloutStatus struct {
	Status `json:",inline"`

	PauseRequestStatus PauseStatus `json:"pauseRequestStatus,omitempty"`

	// NameCount is used as a suffix for the name of the managed isbsvc, to uniquely
	// identify an isbsvc.
	NameCount *int32 `json:"nameCount,omitempty"`

	// ProgressiveStatus stores fields related to the Progressive strategy
	ProgressiveStatus ISBServiceProgressiveStatus `json:"progressiveStatus,omitempty"`

	// Riders stores the list of Riders that have been deployed along with the "promoted" InterstepBufferService
	Riders []RiderStatus `json:"riders,omitempty"`
}

type ISBServiceProgressiveStatus struct {
	// UpgradingISBServiceStatus represents either the current or otherwise the most recent "upgrading" isbservice
	UpgradingISBServiceStatus *UpgradingISBServiceStatus `json:"upgradingISBServiceStatus,omitempty"`
	// PromotedISBServiceStatus stores information regarding the current "promoted" isbservice
	PromotedISBServiceStatus *PromotedISBServiceStatus `json:"promotedISBServiceStatus,omitempty"`
}

// UpgradingISBServiceStatus describes the status of an upgrading child
type UpgradingISBServiceStatus struct {
	UpgradingChildStatus `json:",inline"`
}

// PromotedISBServiceStatus describes the status of a promoted child
type PromotedISBServiceStatus struct {
	PromotedChildStatus `json:",inline"`
}

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="The current phase"
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

func (isbServiceRollout *ISBServiceRollout) GetRolloutGVR() metav1.GroupVersionResource {
	return metav1.GroupVersionResource{
		Group:    isbServiceRollout.TypeMeta.GroupVersionKind().Group,
		Version:  isbServiceRollout.TypeMeta.GroupVersionKind().Version,
		Resource: "isbservicerollouts",
	}
}

func (isbServiceRollout *ISBServiceRollout) GetRolloutGVK() schema.GroupVersionKind {
	return isbServiceRollout.TypeMeta.GroupVersionKind()
}

func (isbServiceRollout *ISBServiceRollout) GetChildGVR() metav1.GroupVersionResource {
	return metav1.GroupVersionResource{
		Group:    numaflowv1.ISBGroupVersionKind.Group,
		Version:  numaflowv1.ISBGroupVersionKind.Version,
		Resource: "interstepbufferservices",
	}
}

func (isbServiceRollout *ISBServiceRollout) GetChildGVK() schema.GroupVersionKind {
	return numaflowv1.ISBGroupVersionKind
}

func (isbServiceRollout *ISBServiceRollout) GetRolloutObjectMeta() *metav1.ObjectMeta {
	return &isbServiceRollout.ObjectMeta
}

func (isbServiceRollout *ISBServiceRollout) GetRolloutStatus() *Status {
	return &isbServiceRollout.Status.Status
}

// GetProgressiveStrategy is a function of the progressiveRolloutObject
func (isbServiceRollout *ISBServiceRollout) GetProgressiveStrategy() ProgressiveStrategy {
	// if the Strategy is not set, return an empty ProgressiveStrategy
	if isbServiceRollout.Spec.Strategy == nil {
		return ProgressiveStrategy{}
	}

	return isbServiceRollout.Spec.Strategy.Progressive
}

// GetUpgradingChildStatus is a function of the progressiveRolloutObject
func (isbServiceRollout *ISBServiceRollout) GetUpgradingChildStatus() *UpgradingChildStatus {
	if isbServiceRollout.Status.ProgressiveStatus.UpgradingISBServiceStatus == nil {
		return nil
	}
	return &isbServiceRollout.Status.ProgressiveStatus.UpgradingISBServiceStatus.UpgradingChildStatus
}

// GetPromotedChildStatus is a function of the progressiveRolloutObject
func (isbServiceRollout *ISBServiceRollout) GetPromotedChildStatus() *PromotedChildStatus {
	if isbServiceRollout.Status.ProgressiveStatus.PromotedISBServiceStatus == nil {
		return nil
	}
	return &isbServiceRollout.Status.ProgressiveStatus.PromotedISBServiceStatus.PromotedChildStatus
}

// ResetUpgradingChildStatus is a function of the progressiveRolloutObject
// note this resets the entire Upgrading status struct which encapsulates the UpgradingChildStatus struct
func (isbServiceRollout *ISBServiceRollout) ResetUpgradingChildStatus(upgradingISBService *unstructured.Unstructured) error {
	isbServiceRollout.Status.ProgressiveStatus.UpgradingISBServiceStatus = &UpgradingISBServiceStatus{
		UpgradingChildStatus: UpgradingChildStatus{
			Name:                   upgradingISBService.GetName(),
			BasicAssessmentEndTime: nil,
			AssessmentResult:       AssessmentResultUnknown,
		},
	}

	return nil
}

// SetUpgradingChildStatus is a function of the progressiveRolloutObject
func (isbServiceRollout *ISBServiceRollout) SetUpgradingChildStatus(status *UpgradingChildStatus) {
	if isbServiceRollout.Status.ProgressiveStatus.UpgradingISBServiceStatus == nil {
		isbServiceRollout.Status.ProgressiveStatus.UpgradingISBServiceStatus = &UpgradingISBServiceStatus{}
	}
	isbServiceRollout.Status.ProgressiveStatus.UpgradingISBServiceStatus.UpgradingChildStatus = *status.DeepCopy()
}

// ResetPromotedChildStatus is a function of the progressiveRolloutObject
// note this resets the entire Promoted status struct which encapsulates the PromotedChildStatus struct
func (isbServiceRollout *ISBServiceRollout) ResetPromotedChildStatus(promotedISBService *unstructured.Unstructured) error {
	isbServiceRollout.Status.ProgressiveStatus.PromotedISBServiceStatus = &PromotedISBServiceStatus{
		PromotedChildStatus: PromotedChildStatus{
			Name: promotedISBService.GetName(),
		},
	}

	return nil
}

// SetPromotedChildStatus is a function of the progressiveRolloutObject
func (isbServiceRollout *ISBServiceRollout) SetPromotedChildStatus(status *PromotedChildStatus) {
	if isbServiceRollout.Status.ProgressiveStatus.PromotedISBServiceStatus == nil {
		isbServiceRollout.Status.ProgressiveStatus.PromotedISBServiceStatus = &PromotedISBServiceStatus{}
	}
	isbServiceRollout.Status.ProgressiveStatus.PromotedISBServiceStatus.PromotedChildStatus = *status.DeepCopy()
}

func init() {
	SchemeBuilder.Register(&ISBServiceRollout{}, &ISBServiceRolloutList{})
}
