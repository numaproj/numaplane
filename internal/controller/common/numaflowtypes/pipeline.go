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
	"errors"
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
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PipelineSpec keeps track of minimum number of fields we need to know about in Numaflow's PipelineSpec, which are presumed not to change from version to version
type PipelineSpec struct {
	InterStepBufferServiceName string           `json:"interStepBufferServiceName"`
	Lifecycle                  Lifecycle        `json:"lifecycle,omitempty"`
	Vertices                   []AbstractVertex `json:"vertices,omitempty"`
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

// PipelineStatus keeps track of minimum number of fields we need to know about in Numaflow's PipelineStatus, which are presumed not to change from version to version
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

func GetPipelinesForRollout(ctx context.Context, c client.Client, pipelineRollout *apiv1.PipelineRollout, live bool) (unstructured.UnstructuredList, error) {
	if live {
		return kubernetes.ListLiveResource(ctx, common.PipelineGVR.Group, common.PipelineGVR.Version, "pipelines", pipelineRollout.GetNamespace(), fmt.Sprintf("%s=%s", common.LabelKeyParentRollout, pipelineRollout.GetName()), "")
	} else {
		gvk := schema.GroupVersionKind{Group: common.NumaflowAPIGroup, Version: common.NumaflowAPIVersion, Kind: common.NumaflowPipelineKind}

		return kubernetes.ListResources(ctx, c, gvk, pipelineRollout.GetNamespace(),
			client.MatchingLabels{common.LabelKeyParentRollout: pipelineRollout.GetName()})
	}

}

func GetPipelineSpecFromRollout(
	pipelineName string,
	pipelineRollout *apiv1.PipelineRollout,
) (map[string]interface{}, error) {
	args := map[string]interface{}{
		common.TemplatePipelineName:      pipelineName,
		common.TemplatePipelineNamespace: pipelineRollout.Namespace,
	}

	return util.ResolveTemplatedSpec(pipelineRollout.Spec.Pipeline.Spec, args)
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

// CheckPipelineSetToRun checks if the pipeline is set to desiredPhase=Running(or unset) plus if all vertices can scale > 0
func CheckPipelineSetToRun(ctx context.Context, pipeline *unstructured.Unstructured) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	vertexScaleDefinitions, err := GetScaleValuesFromPipelineSpec(pipeline)
	if err != nil {
		return false, fmt.Errorf("Failed to check pipeline set to run: %v", err)
	}

	// if any Vertex has max=0, it can't run
	for _, vertexDef := range vertexScaleDefinitions {
		if vertexDef.ScaleDefinition != nil && vertexDef.ScaleDefinition.Max != nil && *vertexDef.ScaleDefinition.Max == 0 {
			numaLogger.WithValues("pipeline", fmt.Sprintf("%s/%s", pipeline.GetNamespace(), pipeline.GetName()), "vertex", vertexDef.VertexName).Debug("pipeline vertex has max=0")
			return false, nil
		}
	}

	desiredPhase, err := GetPipelineDesiredPhase(pipeline)
	return desiredPhase == string(numaflowv1.PipelinePhaseRunning), err
}

// CheckPipelineObservedGeneration verifies that the observedGeneration is not less than the generation, meaning it's been reconciled by Numaflow since being updated
func CheckPipelineObservedGeneration(ctx context.Context, pipeline *unstructured.Unstructured) (bool, int64, int64, error) {
	pipelineStatus, err := ParsePipelineStatus(pipeline)
	if err != nil {
		return false, 0, 0, fmt.Errorf("failed to parse Pipeline Status from pipeline CR: %+v, %v", pipeline, err)
	}
	return pipelineStatus.ObservedGeneration >= pipeline.GetGeneration(), pipeline.GetGeneration(), pipelineStatus.ObservedGeneration, nil
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
	if err != nil {
		return desiredPhase, err
	}

	if desiredPhase == "" {
		desiredPhase = string(numaflowv1.PipelinePhaseRunning)
	}
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

func PipelineWithoutScaleMinMax(pipeline map[string]interface{}) error {
	// for each Vertex, remove the scale min and max:
	vertices, _, _ := unstructured.NestedSlice(pipeline, "spec", "vertices")

	modifiedVertices := make([]interface{}, 0)
	for _, vertex := range vertices {
		if vertexAsMap, ok := vertex.(map[string]any); ok {

			unstructured.RemoveNestedField(vertexAsMap, "scale", "min")
			unstructured.RemoveNestedField(vertexAsMap, "scale", "max")

			// if "scale" is there and empty, remove it
			scaleMap, found := vertexAsMap["scale"].(map[string]interface{})
			if found && len(scaleMap) == 0 {
				unstructured.RemoveNestedField(vertexAsMap, "scale")
			}

			modifiedVertices = append(modifiedVertices, vertexAsMap)
		}
	}

	err := unstructured.SetNestedSlice(pipeline, modifiedVertices, "spec", "vertices")
	if err != nil {
		return err
	}
	return nil
}

func GetVertexFromPipelineSpecMap(
	pipelineSpec map[string]interface{},
	vertexName string,
) (map[string]interface{}, bool, error) {
	// TODO: replace this with GetPipelineVertexDefinitions() call
	vertices, found, err := unstructured.NestedSlice(pipelineSpec, "vertices")
	if err != nil {
		return nil, false, fmt.Errorf("error while getting vertices of pipeline: %v", err)
	}
	if !found {
		return nil, false, fmt.Errorf("no vertices found in pipeline spec?: %+v", pipelineSpec)
	}

	// find the vertex
	for _, vertex := range vertices {

		vertexAsMap := vertex.(map[string]interface{})
		name := vertexAsMap["name"].(string)
		if name == vertexName {
			return vertex.(map[string]interface{}), true, nil
		}
	}

	// Vertex not found
	return nil, false, nil
}

func GetPipelineVertexDefinitions(pipeline *unstructured.Unstructured) ([]interface{}, error) {

	vertexDefinitions, exists, err := unstructured.NestedSlice(pipeline.Object, "spec", "vertices")
	if err != nil {
		return nil, fmt.Errorf("error getting spec.vertices from pipeline %s: %s", pipeline.GetName(), err.Error())
	}
	if !exists {
		return nil, fmt.Errorf("failed to get spec.vertices from pipeline %s: doesn't exist?", pipeline.GetName())
	}

	return vertexDefinitions, nil
}

// get the Scale definitions for each Vertex
func GetScaleValuesFromPipelineSpec(pipelineDef *unstructured.Unstructured) ([]apiv1.VertexScaleDefinition, error) {
	vertices, err := GetPipelineVertexDefinitions(pipelineDef)
	if err != nil {
		return nil, fmt.Errorf("error while getting vertices of pipeline %s/%s: %w", pipelineDef.GetNamespace(), pipelineDef.GetName(), err)
	}

	scaleDefinitions := []apiv1.VertexScaleDefinition{}

	for _, vertex := range vertices {
		if vertexAsMap, ok := vertex.(map[string]any); ok {

			vertexName, foundVertexName, err := unstructured.NestedString(vertexAsMap, "name")
			if err != nil {
				return nil, err
			}
			if !foundVertexName {
				return nil, errors.New("a vertex must have a name")
			}

			vertexScaleDef, err := ExtractScaleMinMax(vertexAsMap, []string{"scale"})
			if err != nil {
				return nil, err
			}
			scaleDefinitions = append(scaleDefinitions, apiv1.VertexScaleDefinition{VertexName: vertexName, ScaleDefinition: vertexScaleDef})
		}
	}
	return scaleDefinitions, nil
}

// find all the Pipeline Vertices in K8S using the Pipeline's definition: return a map of vertex name to resource found
// for any Vertices that can't be found, return an entry mapped to nil
func GetPipelineVertices(ctx context.Context, c client.Client, pipeline *unstructured.Unstructured) (map[string]*unstructured.Unstructured, error) {
	numaLogger := logger.FromContext(ctx).WithValues("pipeline", "pipeline", fmt.Sprintf("%s/%s", pipeline.GetNamespace(), pipeline.GetName()))

	vertexDefinitions, err := GetPipelineVertexDefinitions(pipeline)
	if err != nil {
		return nil, err
	}

	nameToVertex := map[string]*unstructured.Unstructured{}

	// TODO: should we have a Watch on Vertex Kind?
	for _, vertexDef := range vertexDefinitions {
		vertexName, found, err := unstructured.NestedString(vertexDef.(map[string]interface{}), "name")
		if !found {
			return nil, fmt.Errorf("vertex has no name?: %v", vertexDef)
		}
		if err != nil {
			return nil, fmt.Errorf("error getting vertex name: %v", err)
		}

		labels := client.MatchingLabels{
			common.LabelKeyNumaflowPipelineName:       pipeline.GetName(),
			common.LabelKeyNumaflowPipelineVertexName: vertexName,
		}
		vertices, err := kubernetes.ListResources(ctx, c, numaflowv1.VertexGroupVersionKind, pipeline.GetNamespace(), labels)
		if err != nil {
			numaLogger.WithValues("vertex", vertexName, "err", err.Error()).Warnf("can't find Vertex in K8S despite being contained within pipeline spec: labels=%v", labels)
			nameToVertex[vertexName] = nil
			continue
		}
		if len(vertices.Items) == 0 {
			numaLogger.WithValues("vertex", vertexName).Warn("can't find Vertex in K8S despite being contained within pipeline spec")
			nameToVertex[vertexName] = nil
		} else if len(vertices.Items) == 1 {
			nameToVertex[vertexName] = &vertices.Items[0]
		} else {
			return nil, fmt.Errorf("there should not be more than 1 Vertex with labels %v in namespace %s", labels, pipeline.GetNamespace())
		}
	}

	return nameToVertex, nil
}

// MinimizePipelineVertexReplicas clears out the `replicas` field from each Vertex of a Pipeline, which has the effect
// in Numaflow of resetting to "scale.min" value
func MinimizePipelineVertexReplicas(ctx context.Context, c client.Client, pipeline *unstructured.Unstructured) error {
	numaLogger := logger.FromContext(ctx).WithValues("pipeline", "pipeline", fmt.Sprintf("%s/%s", pipeline.GetNamespace(), pipeline.GetName()))

	vertices, err := GetPipelineVertices(ctx, c, pipeline)
	if err != nil {
		return fmt.Errorf("error getting pipeline vertices for pipeline %s/%s: %v", pipeline.GetNamespace(), pipeline.GetName(), err)
	}
	numaLogger.Debug("setting replicas=nil for each vertex")
	for vertexName, vertex := range vertices {
		if vertex == nil {
			numaLogger.WithValues("vertex", vertexName).Warn("can't set replicas=nil since vertex wasn't found")
		} else {
			// patch replicas to null
			patchJson := `{"spec": {"replicas": null}}`
			if err := kubernetes.PatchResource(ctx, c, vertex, patchJson, k8stypes.MergePatchType); err != nil {
				return fmt.Errorf("error patching vertex %s/%s replicas to null: %v", vertex.GetNamespace(), vertex.GetName(), err)
			}
		}
	}

	return nil
}
