package common

import (
	"context"
	"encoding/json"
	"fmt"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
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

func CheckPipelineStatus(ctx context.Context, pipeline *kubernetes.GenericObject, phase numaflowv1.PipelinePhase) bool {
	numaLogger := logger.FromContext(ctx)
	pipelineStatus, err := kubernetes.ParseStatus(pipeline)
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
func IsPipelinePausedOrWontPause(ctx context.Context, pipeline *kubernetes.GenericObject, pipelineRollout *apiv1.PipelineRollout) bool {
	wontPause := CheckIfPipelineWontPause(ctx, pipeline, pipelineRollout)

	paused := CheckPipelineStatus(ctx, pipeline, numaflowv1.PipelinePhasePaused)
	return paused || wontPause
}

func CheckIfPipelineWontPause(ctx context.Context, pipeline *kubernetes.GenericObject, pipelineRollout *apiv1.PipelineRollout) bool {
	numaLogger := logger.FromContext(ctx)

	allowDataLossAnnotation := pipelineRollout.Annotations[common.LabelKeyAllowDataLoss]
	allowDataLoss := allowDataLossAnnotation == "true"
	failed := CheckPipelineStatus(ctx, pipeline, numaflowv1.PipelinePhaseFailed)
	wontPause := allowDataLoss || failed
	numaLogger.Debugf("wontPause=%t, allowDataLoss=%t, failed=%t", wontPause, allowDataLoss, failed)

	return wontPause
}

func WithDesiredPhase(pipeline *kubernetes.GenericObject, phase string) error {
	unstruc, err := kubernetes.ObjectToUnstructured(pipeline)
	if err != nil {
		return err
	}

	// TODO: I noticed if any of these fields are nil, this function errors out - but can't remember why they'd be nil
	err = unstructured.SetNestedField(unstruc.Object, phase, "spec", "lifecycle", "desiredPhase")
	if err != nil {
		return err
	}

	resultObj, err := kubernetes.UnstructuredToObject(unstruc)
	if err != nil {
		return err
	}
	if pipeline == nil {
		return fmt.Errorf("error converting unstructured %+v to object, result is nil?", unstruc.Object)
	}
	*pipeline = *resultObj
	return nil
}

// remove 'lifecycle.desiredPhase' key/value pair from spec
// also remove 'lifecycle' if it's an empty map
func WithoutDesiredPhase(obj *kubernetes.GenericObject) (map[string]interface{}, error) {
	var specAsMap map[string]any
	if err := json.Unmarshal(obj.Spec.Raw, &specAsMap); err != nil {
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
