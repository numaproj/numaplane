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
	"fmt"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// ConditionPipelinePausingOrPaused indicates that the Pipeline is either pausing or paused.
	ConditionPipelinePausingOrPaused ConditionType = "PipelinePausingOrPaused"
)

// PipelineRolloutSpec defines the desired state of PipelineRollout
type PipelineRolloutSpec struct {
	Pipeline Pipeline                     `json:"pipeline"`
	Strategy *PipelineTypeRolloutStrategy `json:"strategy,omitempty"`
	Riders   []PipelineRider              `json:"riders,omitempty"`
}

// PipelineRider defines a resource that can be deployed along with the primary child of a PipelineRollout
type PipelineRider struct {
	Rider `json:",inline"`

	// if set, rider resource should be created for each vertex of the pipeline
	PerVertex bool `json:"perVertex,omitempty"`
}

// Pipeline includes the spec of Pipeline in Numaflow
type Pipeline struct {
	Metadata `json:"metadata,omitempty"`

	Spec runtime.RawExtension `json:"spec"`
}

// PipelineRolloutStatus defines the observed state of PipelineRollout
type PipelineRolloutStatus struct {
	Status      `json:",inline"`
	PauseStatus PauseStatus `json:"pauseStatus,omitempty"`

	// NameCount is used as a suffix for the name of the managed pipeline, to uniquely
	// identify a pipeline.
	NameCount *int32 `json:"nameCount,omitempty"`

	// ProgressiveStatus stores fields related to the Progressive strategy
	ProgressiveStatus PipelineProgressiveStatus `json:"progressiveStatus,omitempty"`
}

type PipelineProgressiveStatus struct {
	// UpgradingPipelineStatus represents either the current or otherwise the most recent "upgrading" pipeline
	UpgradingPipelineStatus *UpgradingPipelineStatus `json:"upgradingPipelineStatus,omitempty"`
	// PromotedPipelineStatus stores information regarding the current "promoted" pipeline
	PromotedPipelineStatus *PromotedPipelineStatus `json:"promotedPipelineStatus,omitempty"`
}

// UpgradingPipelineStatus describes the status of an upgrading child
type UpgradingPipelineStatus struct {
	// UpgradingPipelineTypeStatus describes the Status for an upgrading child that's particular to "Pipeline types", i.e. Pipeline and MonoVertex
	UpgradingPipelineTypeStatus `json:",inline"`
	// InterStepBufferServiceName is the name of the InterstepBufferService that this Pipeline is using
	InterStepBufferServiceName string `json:"interStepBufferServiceName,omitempty"`

	// OriginalScaleMinMax stores for each vertex, the original scale min and max values as JSON string
	//OriginalScaleMinMax []VertexScale `json:"originalScaleMinMax,omitempty"`
	OriginalScaleMinMax []VertexScaleDefinition `json:"originalScaleMinMax,omitempty"`
}

// ScaleDefinition is a struct to encapsulate scale values (can be used for a Vertex)
type ScaleDefinition struct {
	Min *int64 `json:"min,omitempty"`
	Max *int64 `json:"max,omitempty"`
}

// VertexScaleDefinition is a struct to encapsulate the scale values for a given vertex
type VertexScaleDefinition struct {
	VertexName      string           `json:"vertexName"`
	ScaleDefinition *ScaleDefinition `json:"scaleDefinition,omitempty"`
}

// PromotedPipelineStatus describes the status of the promoted child
type PromotedPipelineStatus struct {
	PromotedPipelineTypeStatus `json:",inline"`
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

func (pipelineRollout *PipelineRollout) GetRolloutGVR() metav1.GroupVersionResource {
	return metav1.GroupVersionResource{
		Group:    pipelineRollout.TypeMeta.GroupVersionKind().Group,
		Version:  pipelineRollout.TypeMeta.GroupVersionKind().Version,
		Resource: "pipelinerollouts",
	}
}

func (pipelineRollout *PipelineRollout) GetRolloutGVK() schema.GroupVersionKind {
	return pipelineRollout.TypeMeta.GroupVersionKind()
}

func (pipelineRollout *PipelineRollout) GetChildGVR() metav1.GroupVersionResource {
	return metav1.GroupVersionResource{
		Group:    numaflowv1.PipelineGroupVersionKind.Group,
		Version:  numaflowv1.PipelineGroupVersionKind.Version,
		Resource: "pipelines",
	}
}

func (pipelineRollout *PipelineRollout) GetChildGVK() schema.GroupVersionKind {
	return numaflowv1.PipelineGroupVersionKind
}

func (pipelineRollout *PipelineRollout) GetRolloutObjectMeta() *metav1.ObjectMeta {
	return &pipelineRollout.ObjectMeta
}

func (pipelineRollout *PipelineRollout) GetRolloutStatus() *Status {
	return &pipelineRollout.Status.Status
}

// GetProgressiveStrategy is a function of the progressiveRolloutObject
func (pipelineRollout *PipelineRollout) GetProgressiveStrategy() ProgressiveStrategy {
	// if the Strategy is not set, return an empty ProgressiveStrategy
	if pipelineRollout.Spec.Strategy == nil {
		return ProgressiveStrategy{}
	}

	return pipelineRollout.Spec.Strategy.Progressive
}

func (pipelineRollout *PipelineRollout) GetAnalysis() Analysis {
	if pipelineRollout.Spec.Strategy == nil {
		return Analysis{}
	}
	return pipelineRollout.Spec.Strategy.Analysis
}

// GetUpgradingChildStatus is a function of the progressiveRolloutObject
func (pipelineRollout *PipelineRollout) GetUpgradingChildStatus() *UpgradingChildStatus {
	if pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus == nil {
		return nil
	}
	return &pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus.UpgradingChildStatus
}

func (pipelineRollout *PipelineRollout) GetAnalysisStatus() *AnalysisStatus {
	if pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus == nil {
		return nil
	}
	return &pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus.Analysis
}

func (pipelineRollout *PipelineRollout) SetAnalysisStatus(status *AnalysisStatus) {
	if pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus == nil {
		pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus = &UpgradingPipelineStatus{}
	}
	pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus.Analysis = *status.DeepCopy()
}

// GetPromotedChildStatus is a function of the progressiveRolloutObject
func (pipelineRollout *PipelineRollout) GetPromotedChildStatus() *PromotedChildStatus {
	if pipelineRollout.Status.ProgressiveStatus.PromotedPipelineStatus == nil {
		return nil
	}
	return &pipelineRollout.Status.ProgressiveStatus.PromotedPipelineStatus.PromotedChildStatus
}

// ResetUpgradingChildStatus is a function of the progressiveRolloutObject
// note this resets the entire Upgrading status struct which encapsulates the UpgradingChildStatus struct
func (pipelineRollout *PipelineRollout) ResetUpgradingChildStatus(upgradingPipeline *unstructured.Unstructured) error {

	isbsvcName, found, err := unstructured.NestedString(upgradingPipeline.Object, "spec", "interStepBufferServiceName")
	if err != nil {
		return fmt.Errorf("unable to reset upgrading child status for pipeline %s/%s: %s", upgradingPipeline.GetNamespace(), upgradingPipeline.GetName(), err)
	}
	if !found {
		isbsvcName = "default" // if not set, the default value is "default"
	}

	upgradingPipelineStatus := &UpgradingPipelineStatus{
		InterStepBufferServiceName: isbsvcName,
		UpgradingPipelineTypeStatus: UpgradingPipelineTypeStatus{
			UpgradingChildStatus: UpgradingChildStatus{
				Name:              upgradingPipeline.GetName(),
				AssessmentEndTime: nil,
				AssessmentResult:  AssessmentResultUnknown,
				FailureReason:     "",
			},
			Analysis: AnalysisStatus{},
		},
	}

	pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus = upgradingPipelineStatus
	return nil
}

// SetUpgradingChildStatus is a function of the progressiveRolloutObject
func (pipelineRollout *PipelineRollout) SetUpgradingChildStatus(status *UpgradingChildStatus) {
	if pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus == nil {
		pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus = &UpgradingPipelineStatus{}
	}
	pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus.UpgradingChildStatus = *status.DeepCopy()
}

// ResetPromotedChildStatus is a function of the progressiveRolloutObject
// note this resets the entire Promoted status struct which encapsulates the PromotedChildStatus struct
func (pipelineRollout *PipelineRollout) ResetPromotedChildStatus(promotedPipeline *unstructured.Unstructured) error {
	pipelineRollout.Status.ProgressiveStatus.PromotedPipelineStatus = &PromotedPipelineStatus{
		PromotedPipelineTypeStatus: PromotedPipelineTypeStatus{
			PromotedChildStatus: PromotedChildStatus{
				Name: promotedPipeline.GetName(),
			},
		},
	}
	return nil
}

// SetPromotedChildStatus is a function of the progressiveRolloutObject
func (pipelineRollout *PipelineRollout) SetPromotedChildStatus(status *PromotedChildStatus) {
	if pipelineRollout.Status.ProgressiveStatus.PromotedPipelineStatus == nil {
		pipelineRollout.Status.ProgressiveStatus.PromotedPipelineStatus = &PromotedPipelineStatus{}
	}
	pipelineRollout.Status.ProgressiveStatus.PromotedPipelineStatus.PromotedPipelineTypeStatus.PromotedChildStatus = *status.DeepCopy()
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
