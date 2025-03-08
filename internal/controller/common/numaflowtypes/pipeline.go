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
	"fmt"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

func GetRolloutForPipeline(ctx context.Context, c client.Client, pipeline *unstructured.Unstructured) (*apiv1.PipelineRollout, error) {
	rolloutName, err := ctlrcommon.GetRolloutParentName(pipeline.GetName())
	if err != nil {
		return nil, err
	}
	// now find the PipelineRollout of that name
	pipelineRollout := &apiv1.PipelineRollout{}
	err = c.Get(ctx, types.NamespacedName{Namespace: pipeline.GetNamespace(), Name: rolloutName}, pipelineRollout)
	if err != nil {
		return nil, err
	}
	return pipelineRollout, nil
}

func GetPipelinesForRollout(ctx context.Context, c client.Client, pipelineRollout *apiv1.PipelineRollout, live bool) (*unstructured.UnstructuredList, error) {
	if live {
		return kubernetes.ListLiveResource(ctx, common.PipelineGVR.Group, common.PipelineGVR.Version, "pipelines", pipelineRollout.GetNamespace(), fmt.Sprintf("%s=%s", common.LabelKeyParentRollout, pipelineRollout.GetName()), "")
	} else {
		gvk := schema.GroupVersionKind{Group: common.NumaflowAPIGroup, Version: common.NumaflowAPIVersion, Kind: common.NumaflowPipelineKind}

		return kubernetes.ListResources(ctx, c, gvk, pipelineRollout.GetNamespace(),
			client.MatchingLabels{common.LabelKeyParentRollout: pipelineRollout.GetName()})
	}

}

func ParsePipelineStatus(pipeline *unstructured.Unstructured) (PipelineStatus, error) {
	if pipeline == nil || len(pipeline.Object) == 0 {
		return PipelineStatus{}, nil
	}

	var status PipelineStatus
	err := util.StructToStruct(pipeline.Object["status"], &status)
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

func CheckPipelineDrained(ctx context.Context, pipeline *unstructured.Unstructured) (bool, error) {
	pipelineStatus, err := ParsePipelineStatus(pipeline)
	if err != nil {
		return false, fmt.Errorf("failed to parse Pipeline Status from pipeline CR: %+v, %v", pipeline, err)
	}
	pipelinePhase := pipelineStatus.Phase

	return pipelinePhase == numaflowv1.PipelinePhasePaused && pipelineStatus.DrainedOnPause, nil
}

// either pipeline must be:
//   - Paused
//   - Failed (contract with Numaflow is that unpausible Pipelines are "Failed" pipelines)
//   - PipelineRollout parent Annotated to allow data loss
func IsPipelinePausedOrWontPause(ctx context.Context, pipeline *unstructured.Unstructured, pipelineRollout *apiv1.PipelineRollout, requireDrained bool) (bool, error) {
	wontPause := CheckIfPipelineWontPause(ctx, pipeline, pipelineRollout)

	if requireDrained {
		pausedAndDrained, err := CheckPipelineDrained(ctx, pipeline)
		if err != nil {
			return false, err
		}
		return pausedAndDrained || wontPause, nil
	} else {
		paused := CheckPipelinePhase(ctx, pipeline, numaflowv1.PipelinePhasePaused)
		return paused || wontPause, nil
	}
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

func GetPipelineDesiredPhase(pipeline *unstructured.Unstructured) (string, error) {
	desiredPhase, _, err := unstructured.NestedString(pipeline.Object, "spec", "lifecycle", "desiredPhase")
	return desiredPhase, err
}

func GetPipelineISBSVCName(pipeline *unstructured.Unstructured) (string, error) {
	isbsvcName, found, err := unstructured.NestedString(pipeline.Object, "spec", "interStepBufferServiceName")
	if err != nil {
		return isbsvcName, err
	}
	if !found {
		return "default", nil // if not set, the default value is "default"
	}
	return isbsvcName, err
}

func PipelineWithISBServiceName(pipeline *unstructured.Unstructured, isbsvcName string) error {
	err := unstructured.SetNestedField(pipeline.Object, isbsvcName, "spec", "interStepBufferServiceName")
	if err != nil {
		return err
	}
	return nil
}

func PipelineWithDesiredPhase(pipeline *unstructured.Unstructured, phase string) error {
	err := unstructured.SetNestedField(pipeline.Object, phase, "spec", "lifecycle", "desiredPhase")
	if err != nil {
		return err
	}
	return nil
}

// remove 'lifecycle.desiredPhase' key/value pair from spec
// also remove 'lifecycle' if it's an empty map
func PipelineWithoutDesiredPhase(pipeline *unstructured.Unstructured) {
	unstructured.RemoveNestedField(pipeline.Object, "spec", "lifecycle", "desiredPhase")

	// if "lifecycle" is there and empty, remove it
	spec := pipeline.Object["spec"].(map[string]interface{})
	lifecycleMap, found := spec["lifecycle"].(map[string]interface{})
	if found && len(lifecycleMap) == 0 {
		unstructured.RemoveNestedField(pipeline.Object, "spec", "lifecycle")
	}
}

func PipelineWithoutScaleMinMax(pipeline *unstructured.Unstructured) {
	unstructured.RemoveNestedField(pipeline.Object, "spec", "scale", "min")
	unstructured.RemoveNestedField(pipeline.Object, "spec", "scale", "max")

	// if "scale" is there and empty, remove it
	spec := pipeline.Object["spec"].(map[string]interface{})
	scaleMap, found := spec["scale"].(map[string]interface{})
	if found && len(scaleMap) == 0 {
		unstructured.RemoveNestedField(pipeline.Object, "spec", "scale")
	}
}
