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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

type Controller struct {
	Version string `json:"version"`
}

// NumaflowControllerRolloutSpec defines the desired state of NumaflowControllerRollout
type NumaflowControllerRolloutSpec struct {
	Controller Controller                         `json:"controller"`
	Strategy   *NumaflowControllerRolloutStrategy `json:"strategy,omitempty"`
}

type NumaflowControllerRolloutStrategy struct {
	Progressive ProgressiveStrategy `json:"progressive,omitempty"`
}

// NumaflowControllerRolloutStatus defines the observed state of NumaflowControllerRollout
type NumaflowControllerRolloutStatus struct {
	Status             `json:",inline"`
	PauseRequestStatus PauseStatus `json:"pauseRequestStatus,omitempty"`

	// NameCount is used as a suffix for the name of the managed NumaflowController children, to uniquely
	// identify each child.
	NameCount *int32 `json:"nameCount,omitempty"`

	// ProgressiveStatus stores fields related to the Progressive strategy
	ProgressiveStatus NumaflowControllerProgressiveStatus `json:"progressiveStatus,omitempty"`
}

type NumaflowControllerProgressiveStatus struct {
	// UpgradingNumaflowControllerStatus represents either the current or otherwise the most recent "upgrading" NumaflowController
	UpgradingNumaflowControllerStatus *UpgradingNumaflowControllerStatus `json:"upgradingNumaflowControllerStatus,omitempty"`
	// PromotedNumaflowControllerStatus stores information regarding the current "promoted" NumaflowController
	PromotedNumaflowControllerStatus *PromotedNumaflowControllerStatus `json:"promotedNumaflowControllerStatus,omitempty"`
}

// UpgradingNumaflowControllerStatus describes the status of an upgrading child
type UpgradingNumaflowControllerStatus struct {
	UpgradingChildStatus     `json:",inline"`
	ControllerInstanceStatus `json:",inline"`
}

// PromotedNumaflowControllerStatus describes the status of a promoted child
type PromotedNumaflowControllerStatus struct {
	PromotedChildStatus      `json:",inline"`
	ControllerInstanceStatus `json:",inline"`
}

// ControllerInstanceStatus describes the controller instance that a NumaflowController child runs
type ControllerInstanceStatus struct {
	// InstanceID is stamped into the child controller's spec and into every Pipeline or MonoVertex bound to this
	// controller instance. A trial child derives it as "<version>-<nameCount>" (with the version reduced to
	// lowercase letters, digits, and dashes so it stays valid inside Service names), so re-trialing the same
	// version after a failed trial still yields a distinct instance. The controller that existed before
	// Progressive was introduced keeps an empty instance.
	InstanceID string `json:"instanceID,omitempty"`

	// Version is the Numaflow Controller version that the child runs
	Version string `json:"version,omitempty"`
}

// ControllerInstanceStatusOf returns the ControllerInstanceStatus of a NumaflowController child
func ControllerInstanceStatusOf(numaflowController *unstructured.Unstructured) ControllerInstanceStatus {
	instanceID, _, _ := unstructured.NestedString(numaflowController.Object, "spec", "instanceID")
	version, _, _ := unstructured.NestedString(numaflowController.Object, "spec", "version")
	return ControllerInstanceStatus{InstanceID: instanceID, Version: version}
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

func (nfcRollout *NumaflowControllerRollout) GetRolloutGVR() metav1.GroupVersionResource {
	return metav1.GroupVersionResource{
		Group:    NumaflowControllerRolloutGroupVersionResource.Group,
		Version:  NumaflowControllerRolloutGroupVersionResource.Version,
		Resource: NumaflowControllerRolloutGroupVersionResource.Resource,
	}
}

func (nfcRollout *NumaflowControllerRollout) GetRolloutGVK() schema.GroupVersionKind {
	return NumaflowControllerRolloutGroupVersionKind
}

func (nfcRollout *NumaflowControllerRollout) GetChildGVR() metav1.GroupVersionResource {
	return metav1.GroupVersionResource{
		Group:    NumaflowControllerGroupVersionResource.Group,
		Version:  NumaflowControllerGroupVersionResource.Version,
		Resource: NumaflowControllerGroupVersionResource.Resource,
	}
}

func (nfcRollout *NumaflowControllerRollout) GetChildGVK() schema.GroupVersionKind {
	return NumaflowControllerGroupVersionKind
}

func (nfcRollout *NumaflowControllerRollout) GetRolloutObjectMeta() *metav1.ObjectMeta {
	return &nfcRollout.ObjectMeta
}

func (nfcRollout *NumaflowControllerRollout) GetRolloutStatus() *Status {
	return &nfcRollout.Status.Status
}

// GetProgressiveStrategy is a function of the progressiveRolloutObject
func (nfcRollout *NumaflowControllerRollout) GetProgressiveStrategy() ProgressiveStrategy {
	// if the Strategy is not set, return an empty ProgressiveStrategy
	if nfcRollout.Spec.Strategy == nil {
		return ProgressiveStrategy{}
	}
	return nfcRollout.Spec.Strategy.Progressive
}

// GetUpgradingChildStatus is a function of the progressiveRolloutObject
func (nfcRollout *NumaflowControllerRollout) GetUpgradingChildStatus() *UpgradingChildStatus {
	if nfcRollout.Status.ProgressiveStatus.UpgradingNumaflowControllerStatus == nil {
		return nil
	}
	return &nfcRollout.Status.ProgressiveStatus.UpgradingNumaflowControllerStatus.UpgradingChildStatus
}

// GetPromotedChildStatus is a function of the progressiveRolloutObject
func (nfcRollout *NumaflowControllerRollout) GetPromotedChildStatus() *PromotedChildStatus {
	if nfcRollout.Status.ProgressiveStatus.PromotedNumaflowControllerStatus == nil {
		return nil
	}
	return &nfcRollout.Status.ProgressiveStatus.PromotedNumaflowControllerStatus.PromotedChildStatus
}

// ResetUpgradingChildStatus is a function of the progressiveRolloutObject
// note this resets the entire Upgrading status struct which encapsulates the UpgradingChildStatus struct
func (nfcRollout *NumaflowControllerRollout) ResetUpgradingChildStatus(upgradingChild *unstructured.Unstructured) error {
	nfcRollout.Status.ProgressiveStatus.UpgradingNumaflowControllerStatus = &UpgradingNumaflowControllerStatus{
		UpgradingChildStatus: UpgradingChildStatus{
			Name:                   upgradingChild.GetName(),
			BasicAssessmentEndTime: nil,
			AssessmentResult:       AssessmentResultUnknown,
		},
		ControllerInstanceStatus: ControllerInstanceStatusOf(upgradingChild),
	}
	return nil
}

// SetUpgradingChildStatus is a function of the progressiveRolloutObject
func (nfcRollout *NumaflowControllerRollout) SetUpgradingChildStatus(status *UpgradingChildStatus) {
	if nfcRollout.Status.ProgressiveStatus.UpgradingNumaflowControllerStatus == nil {
		nfcRollout.Status.ProgressiveStatus.UpgradingNumaflowControllerStatus = &UpgradingNumaflowControllerStatus{}
	}
	nfcRollout.Status.ProgressiveStatus.UpgradingNumaflowControllerStatus.UpgradingChildStatus = *status.DeepCopy()
}

// ResetPromotedChildStatus is a function of the progressiveRolloutObject
// note this resets the entire Promoted status struct which encapsulates the PromotedChildStatus struct
func (nfcRollout *NumaflowControllerRollout) ResetPromotedChildStatus(promotedChild *unstructured.Unstructured) error {
	nfcRollout.Status.ProgressiveStatus.PromotedNumaflowControllerStatus = &PromotedNumaflowControllerStatus{
		PromotedChildStatus: PromotedChildStatus{
			Name: promotedChild.GetName(),
		},
		ControllerInstanceStatus: ControllerInstanceStatusOf(promotedChild),
	}
	return nil
}

// SetPromotedChildStatus is a function of the progressiveRolloutObject
func (nfcRollout *NumaflowControllerRollout) SetPromotedChildStatus(status *PromotedChildStatus) {
	if nfcRollout.Status.ProgressiveStatus.PromotedNumaflowControllerStatus == nil {
		nfcRollout.Status.ProgressiveStatus.PromotedNumaflowControllerStatus = &PromotedNumaflowControllerStatus{}
	}
	nfcRollout.Status.ProgressiveStatus.PromotedNumaflowControllerStatus.PromotedChildStatus = *status.DeepCopy()
}

// GetChildMetadata is a function of the progressiveRolloutObject. The Rollout carries no user-defined
// metadata for its NumaflowController children.
func (nfcRollout *NumaflowControllerRollout) GetChildMetadata() Metadata {
	return Metadata{}
}

func init() {
	SchemeBuilder.Register(&NumaflowControllerRollout{}, &NumaflowControllerRolloutList{})
}
