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

package numaflowtypes

import (
	"context"
	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// PipelineSpec keeps track of minimum number of fields we need to know about
type PipelineSpec struct {
	InterStepBufferServiceName string    `json:"interStepBufferServiceName"`
	Lifecycle                  Lifecycle `json:"lifecycle,omitempty"`
}

func (pipeline PipelineSpec) GetISBSvcName() string {
	if pipeline.InterStepBufferServiceName == "" {
		return "default"
	}
	return pipeline.InterStepBufferServiceName
}

type Lifecycle struct {
	// DesiredPhase used to bring the pipeline from current phase to desired phase
	// +kubebuilder:default=Running
	// +optional
	DesiredPhase string `json:"desiredPhase,omitempty"`
}

type PipelineStatus struct {
	Phase              numaflowv1.PipelinePhase `json:"phase,omitempty"`
	Conditions         []metav1.Condition       `json:"conditions,omitempty"`
	ObservedGeneration int64                    `json:"observedGeneration,omitempty"`
	DrainedOnPause     bool                     `json:"drainedOnPause,omitempty" protobuf:"bytes,12,opt,name=drainedOnPause"`
}

func ParsePipelineStatus(obj *unstructured.Unstructured) (PipelineStatus, error) {
	if obj == nil || len(obj.Object) == 0 {
		return PipelineStatus{}, nil
	}

	var status PipelineStatus
	err := util.StructToStruct(obj.Object["status"], &status)
	if err != nil {
		return PipelineStatus{}, err
	}

	return status, nil
}

func CheckPipelinePhase(ctx context.Context, pipeline *unstructured.Unstructured, phase numaflowv1.PipelinePhase) bool {
	numaLogger := logger.FromContext(ctx)
	pipelineStatus, err := ParsePipelineStatus(pipeline)
	if err != nil {
		numaLogger.Errorf(err, "failed to parse Pipeline Status from pipeline CR: %+v, %v", pipeline, err)
		return false
	}

	return numaflowv1.PipelinePhase(pipelineStatus.Phase) == phase
}

// either pipeline must be:
//   - Paused
//   - Failed (contract with Numaflow is that unpausible Pipelines are "Failed" pipelines)
//   - PipelineRollout parent Annotated to allow data loss
func IsPipelinePausedOrWontPause(ctx context.Context, pipeline *unstructured.Unstructured, pipelineRollout *apiv1.PipelineRollout) bool {
	wontPause := CheckIfPipelineWontPause(ctx, pipeline, pipelineRollout)

	paused := CheckPipelinePhase(ctx, pipeline, numaflowv1.PipelinePhasePaused)
	return paused || wontPause
}

func CheckIfPipelineWontPause(ctx context.Context, pipeline *unstructured.Unstructured, pipelineRollout *apiv1.PipelineRollout) bool {
	numaLogger := logger.FromContext(ctx)

	allowDataLossAnnotation := pipelineRollout.Annotations[common.LabelKeyAllowDataLoss]
	allowDataLoss := allowDataLossAnnotation == "true"
	failed := CheckPipelinePhase(ctx, pipeline, numaflowv1.PipelinePhaseFailed)
	wontPause := allowDataLoss || failed
	numaLogger.Debugf("wontPause=%t, allowDataLoss=%t, failed=%t", wontPause, allowDataLoss, failed)

	return wontPause
}

func WithDesiredPhase(pipeline *unstructured.Unstructured, phase string) error {
	// TODO: I noticed if any of these fields are nil, this function errors out - but can't remember why they'd be nil
	err := unstructured.SetNestedField(pipeline.Object, phase, "spec", "lifecycle", "desiredPhase")
	if err != nil {
		return err
	}
	return nil
}

// TODO: make this and the WithDesiredPhase() signature from above similar to each other
// (this may naturally happen after refactoring)
// remove 'lifecycle.desiredPhase' key/value pair from spec
// also remove 'lifecycle' if it's an empty map
func WithoutDesiredPhase(obj *unstructured.Unstructured) (map[string]interface{}, error) {
	var specAsMap map[string]any
	if err := util.StructToStruct(obj.Object["spec"], &specAsMap); err != nil {
		return nil, err
	}
	// remove "lifecycle.desiredPhase"
	comparisonExcludedPaths := []string{"lifecycle.desiredPhase"}
	util.RemovePaths(specAsMap, comparisonExcludedPaths, ".")
	// if "lifecycle" is there and empty, remove it
	lifecycleMap, found := specAsMap["lifecycle"].(map[string]interface{})
	if found && len(lifecycleMap) == 0 {
		util.RemovePaths(specAsMap, []string{"lifecycle"}, ".")
	}
	return specAsMap, nil
}
