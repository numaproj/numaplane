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
	Pipeline Pipeline          `json:"pipeline"`
	Strategy *PipelineStrategy `json:"strategy,omitempty"`
	Riders   []PipelineRider   `json:"riders,omitempty"`
}

type PipelineStrategy struct {
	PipelineTypeRolloutStrategy `json:",inline"`

	PPNDStrategy `json:"ppnd,omitempty"`

	RecycleStrategy `json:"recycleStrategy,omitempty"`
}

type PPNDStrategy struct {
	// FastResume indicates if the Pipeline should be resumed with the number of replicas it had before it was paused.
	FastResume bool `json:"fastResume,omitempty"`
}

type RecycleStrategy struct {
	// ScaleFactor is a percentage of Pipeline's original vertex scale that it will scale down by while it's being paused
	// before deleting.
	// Note that the Pipeline's pauseGracePeriodSeconds will be multiplied by the inverse.
	// If not defined, fallback to the one defined in the global ConfigMap
	ScaleFactor *int32 `json:"scaleFactor,omitempty"`
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

	// Riders stores the list of Riders that have been deployed along with the "promoted" Pipeline
	Riders []RiderStatus `json:"riders,omitempty"`
}

type PipelineProgressiveStatus struct {
	// UpgradingPipelineStatus represents either the current or otherwise the most recent "upgrading" pipeline
	UpgradingPipelineStatus *UpgradingPipelineStatus `json:"upgradingPipelineStatus,omitempty"`
	// PromotedPipelineStatus stores information regarding the current "promoted" pipeline
	PromotedPipelineStatus *PromotedPipelineStatus `json:"promotedPipelineStatus,omitempty"`
	// HistoricalPodCount keeps track of per-vertex pod count from the last "promoted" pipeline
	HistoricalPodCount map[string]int `json:"historicalPodCount,omitempty"`
	// RecyclablePipelinesStatus provides status of the current Pipelines marked "recyclable"
	RecyclablePipelinesStatus []RecyclablePipelineStatus `json:"recyclablePipelinesStatus,omitempty"`
}

type RecyclablePipelineStatus struct {
	// Name of the recyclable Pipeline
	Name string `json:"name,omitempty"`

	// DrainAttempts represents the attempts to drain the Pipeline that were done.
	// Entries are stored in chronological order (oldest first, newest last).
	DrainAttempts []DrainAttempt `json:"drainAttempts,omitempty"`
}

// GetRecyclablePipelineStatus returns the RecyclablePipelineStatus for the given pipeline name, or nil if not found.
func (status *PipelineProgressiveStatus) GetRecyclablePipelineStatus(pipelineName string) *RecyclablePipelineStatus {
	for i := range status.RecyclablePipelinesStatus {
		if status.RecyclablePipelinesStatus[i].Name == pipelineName {
			return &status.RecyclablePipelinesStatus[i]
		}
	}
	return nil
}

// GetOrCreateRecyclablePipelineStatus returns the RecyclablePipelineStatus for the given pipeline name,
// appending a new entry if one does not already exist.
func (status *PipelineProgressiveStatus) GetOrCreateRecyclablePipelineStatus(pipelineName string) *RecyclablePipelineStatus {
	if recyclablePipelineStatus := status.GetRecyclablePipelineStatus(pipelineName); recyclablePipelineStatus != nil {
		return recyclablePipelineStatus
	}
	status.RecyclablePipelinesStatus = append(status.RecyclablePipelinesStatus, RecyclablePipelineStatus{
		Name:          pipelineName,
		DrainAttempts: []DrainAttempt{},
	})
	return status.GetRecyclablePipelineStatus(pipelineName)
}

// GetDrainAttempt returns the DrainAttempt that used the given source pipeline's spec, or nil if not found.
func (status *RecyclablePipelineStatus) GetDrainAttempt(sourcePipelineName string) *DrainAttempt {
	for i := range status.DrainAttempts {
		if status.DrainAttempts[i].SourcePipelineSpec == sourcePipelineName {
			return &status.DrainAttempts[i]
		}
	}
	return nil
}

// GetLastDrainAttempt returns the most recent DrainAttempt, or nil if there are none.
// DrainAttempts must be kept in chronological order (oldest first, newest last).
func (status *RecyclablePipelineStatus) GetLastDrainAttempt() *DrainAttempt {
	if len(status.DrainAttempts) == 0 {
		return nil
	}
	return &status.DrainAttempts[len(status.DrainAttempts)-1]
}

// HasDrainAttempt reports whether a drain attempt exists for the given source pipeline spec.
func (status *RecyclablePipelineStatus) HasDrainAttempt(sourcePipelineName string) bool {
	return status.GetDrainAttempt(sourcePipelineName) != nil
}

// IsDrainAttemptComplete reports whether the drain attempt for the given source pipeline spec has ended.
func (status *RecyclablePipelineStatus) IsDrainAttemptComplete(sourcePipelineName string) bool {
	drainAttempt := status.GetDrainAttempt(sourcePipelineName)
	return drainAttempt != nil && drainAttempt.DrainAttemptComplete
}

// HasForceDrainStarted reports whether any force-drain attempt has started for the recyclable pipeline.
func (status *RecyclablePipelineStatus) HasForceDrainStarted(recyclablePipelineName string) bool {
	for _, drainAttempt := range status.DrainAttempts {
		if drainAttempt.SourcePipelineSpec != recyclablePipelineName {
			return true
		}
	}
	return false
}

// StartDrainAttempt appends a new in-progress drain attempt if one does not already exist for the source pipeline spec.
func (status *RecyclablePipelineStatus) StartDrainAttempt(sourcePipelineName string) {
	if status.HasDrainAttempt(sourcePipelineName) {
		return
	}
	// If the most recent drain attempt is still in progress, it is marked Superseded before the new attempt is started.
	if lastDrainAttempt := status.GetLastDrainAttempt(); lastDrainAttempt != nil && !lastDrainAttempt.DrainAttemptComplete {
		status.CompleteDrainAttempt(lastDrainAttempt.SourcePipelineSpec, DrainCompletionReasonSuperseded)
	}
	status.DrainAttempts = append(status.DrainAttempts, DrainAttempt{
		SourcePipelineSpec: sourcePipelineName,
		StartTime:          metav1.Now(),
	})
}

// SetDrainAttemptVertexReplicaCount records the per-vertex replica counts applied when pausing during a drain attempt.
func (status *RecyclablePipelineStatus) SetDrainAttemptVertexReplicaCount(sourcePipelineName string, vertexScaleDefinitions []VertexScaleDefinition) {
	// get a reference to the DrainAttempt and we can modify it
	drainAttempt := status.GetDrainAttempt(sourcePipelineName)
	if drainAttempt == nil {
		return
	}
	vertexReplicaCounts := make([]VertexReplicaCount, len(vertexScaleDefinitions))
	for i, scaleDef := range vertexScaleDefinitions {
		vertexReplicaCounts[i] = VertexReplicaCount{
			Name:     scaleDef.VertexName,
			Replicas: int32(scaleDef.Min()),
		}
	}
	drainAttempt.VertexReplicaCount = vertexReplicaCounts
}

// SetDrainAttemptFailure records that the Pipeline is currently in Failed phase for the given drain attempt.
// If the Pipeline is not already in a failed-phase episode, FailedPhaseStartTime is set.
func (status *RecyclablePipelineStatus) SetDrainAttemptFailure(sourcePipelineName string) {
	drainAttempt := status.GetDrainAttempt(sourcePipelineName)
	if drainAttempt == nil || drainAttempt.DrainAttemptComplete {
		return
	}
	if drainAttempt.FailedPhaseStartTime == nil {
		now := metav1.Now()
		drainAttempt.FailedPhaseStartTime = &now
	}
}

// UnsetDrainAttemptFailure clears an in-progress failed-phase episode so a later failure starts a new timer.
func (status *RecyclablePipelineStatus) UnsetDrainAttemptFailure(sourcePipelineName string) {
	drainAttempt := status.GetDrainAttempt(sourcePipelineName)
	if drainAttempt == nil || drainAttempt.DrainAttemptComplete {
		return
	}
	drainAttempt.FailedPhaseStartTime = nil
}

// CompleteDrainAttempt marks the drain attempt for the given source pipeline spec as ended.
// Idempotent: already-complete attempts are not modified.
func (status *RecyclablePipelineStatus) CompleteDrainAttempt(sourcePipelineName string, reason DrainCompletionReason) {
	drainAttempt := status.GetDrainAttempt(sourcePipelineName)
	if drainAttempt == nil || drainAttempt.DrainAttemptComplete {
		return
	}
	endTime := metav1.Now()
	drainAttempt.DrainAttemptComplete = true
	drainAttempt.EndTime = &endTime
	drainAttempt.DrainCompletionReason = reason
	if reason == DrainCompletionReasonPipelineFailed && drainAttempt.FailedPhaseStartTime != nil {
		drainAttempt.FailedPhaseEndTime = &endTime
	}
}

// DrainCompletionReason describes why a drain attempt ended.
type DrainCompletionReason string

const (
	// DrainCompletionReasonDrainComplete indicates the Pipeline fully drained during the pause.
	DrainCompletionReasonDrainComplete DrainCompletionReason = "DrainComplete"
	// DrainCompletionReasonPipelineFailed indicates the Pipeline entered Failed phase and that caused the drain attempt to stop.
	DrainCompletionReasonPipelineFailed DrainCompletionReason = "PipelineFailed"
	// DrainCompletionReasonMaxPauseTime indicates the pause grace period expired before the Pipeline fully drained.
	DrainCompletionReasonMaxPauseTime DrainCompletionReason = "MaxPauseTime"
	// DrainCompletionReasonSuperseded indicates the drain attempt was abandoned because a newer promoted pipeline spec was used instead.
	DrainCompletionReasonSuperseded DrainCompletionReason = "Superseded"
)

// DrainAttempt describes a single attempt to drain a recyclable Pipeline.
type DrainAttempt struct {
	// SourcePipelineSpec is the name of the Pipeline whose spec was used for this drain attempt.
	// For an original-spec drain, this is the recyclable Pipeline's own name.
	SourcePipelineSpec string `json:"sourcePipelineSpec,omitempty"`

	// StartTime is when this drain attempt began.
	StartTime metav1.Time `json:"startTime,omitempty"`

	// EndTime is when this drain attempt ended. Unset while the attempt is still in progress.
	EndTime *metav1.Time `json:"endTime,omitempty"`

	// DrainAttemptComplete represents if the drain has stopped
	DrainAttemptComplete bool `json:"drainAttemptComplete,omitempty"`

	// DrainCompletionReason indicates why this drain attempt ended. Unset while the attempt is still in progress.
	DrainCompletionReason DrainCompletionReason `json:"drainCompletionReason,omitempty"`

	// FailedPhaseStartTime is when the Pipeline entered Failed phase during the current failed-phase episode of this drain attempt.
	// Cleared when the Pipeline leaves Failed phase so a later failure starts a new timer.
	FailedPhaseStartTime *metav1.Time `json:"failedPhaseStartTime,omitempty"`

	// FailedPhaseEndTime is when a persistent Pipeline failure caused this drain attempt to end.
	FailedPhaseEndTime *metav1.Time `json:"failedPhaseEndTime,omitempty"`

	// VertexReplicaCount captures the replica count per Vertex for this drain attempt.
	VertexReplicaCount []VertexReplicaCount `json:"vertexReplicaCount,omitempty"`
}

// VertexReplicaCount records the replica count for a single Vertex during a drain attempt.
type VertexReplicaCount struct {
	Name     string `json:"name"`
	Replicas int32  `json:"replicas"`
}

// UpgradingPipelineStatus describes the status of an upgrading child
type UpgradingPipelineStatus struct {
	// UpgradingPipelineTypeStatus describes the Status for an upgrading child that's particular to "Pipeline types", i.e. Pipeline and MonoVertex
	UpgradingPipelineTypeStatus `json:",inline"`
	// InterStepBufferServiceName is the name of the InterstepBufferService that this Pipeline is using
	InterStepBufferServiceName string `json:"interStepBufferServiceName,omitempty"`

	// OriginalScaleMinMax stores for each vertex, the original scale min and max values as JSON string
	// Example: {"min":1,"max":10,"disabled":false}
	// Note 'disabled' is a field in numaflow used to represent if scaling is performed by numaflow.
	// If 'disabled==true", then "min" and "max" probably wouldn't be set, and if they are they'll be ignored.
	OriginalScaleMinMax []VertexScaleDefinition `json:"originalScaleMinMax,omitempty"`

	// OriginalScaleDefinitions stores the original scale definitions for each vertex as JSON string
	// The Vertices are in the same order as they're listed in the pipeline spec
	// The difference between this and OriginalScaleMinMax is that this represents the full definition for scale
	// If scale is not defined for a Vertex, it will be represented as "null"
	OriginalScaleDefinitions []string `json:"originalScaleDefinitions,omitempty"`
}

// ScaleDefinition is a struct to encapsulate scale values (can be used for a Vertex)
type ScaleDefinition struct {
	Min      *int64 `json:"min,omitempty"`
	Max      *int64 `json:"max,omitempty"`
	Disabled bool   `json:"disabled,omitempty"`
}

// VertexScaleDefinition is a struct to encapsulate the scale values for a given vertex
type VertexScaleDefinition struct {
	VertexName      string           `json:"vertexName"`
	ScaleDefinition *ScaleDefinition `json:"scaleDefinition,omitempty"`
}

func (scaleDef *VertexScaleDefinition) Min() int64 {
	if scaleDef.ScaleDefinition == nil || scaleDef.ScaleDefinition.Min == nil {
		return int64(1)
	} else {
		return *scaleDef.ScaleDefinition.Min
	}
}

func (scaleDef *VertexScaleDefinition) Max() int64 {
	if scaleDef.ScaleDefinition == nil || scaleDef.ScaleDefinition.Max == nil {
		return int64(1)
	} else {
		return *scaleDef.ScaleDefinition.Max
	}
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
				Name:                   upgradingPipeline.GetName(),
				BasicAssessmentEndTime: nil,
				AssessmentResult:       AssessmentResultUnknown,
				FailureReasons:         []string{},
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

func (pipelineRollout *PipelineRollout) GetChildMetadata() Metadata {
	return pipelineRollout.Spec.Pipeline.Metadata
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
