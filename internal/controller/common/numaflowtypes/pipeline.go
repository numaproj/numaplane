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

func AllSourceVerticesScaledToZero(ctx context.Context, pipelineSpec map[string]interface{}) (bool, error) {
	vertices, found, _ := unstructured.NestedSlice(pipelineSpec, "vertices")
	if !found {
		return false, fmt.Errorf("no vertices found in pipeline spec?: %+v", pipelineSpec)
	}

	for _, vertex := range vertices {
		if vertexAsMap, ok := vertex.(map[string]interface{}); ok {
			_, isSource, _ := unstructured.NestedFieldNoCopy(vertexAsMap, "source")
			if isSource {
				maxInterface, maxFound, _ := unstructured.NestedFieldNoCopy(vertexAsMap, "scale", "max")
				// If max is not found, it defaults to 1 (not scaled to zero)
				if !maxFound {
					return false, nil
				}
				maxValue, maxValid := util.ToInt64(maxInterface)
				// If max is found and is not 0, it's not scaled to zero
				if maxValid && maxValue != 0 {
					return false, nil
				}
			}
		}
	}
	return true, nil
}

// CanPipelineIngestData checks if the pipeline is set to desiredPhase=Running(or unset) plus if a source vertex has scale.max>0
func CanPipelineIngestData(ctx context.Context, pipeline *unstructured.Unstructured) (bool, error) {
	pipelineSpec, found := pipeline.Object["spec"].(map[string]interface{})
	if !found {
		return false, fmt.Errorf("failed to get spec from pipeline %s", pipeline.GetName())
	}

	allSourcesScaledToZero, err := AllSourceVerticesScaledToZero(ctx, pipelineSpec)
	if err != nil {
		return false, fmt.Errorf("failed to check pipeline source vertices scaled to zero: %v", err)
	}

	desiredPhase, err := GetPipelineDesiredPhase(pipeline)
	return desiredPhase == string(numaflowv1.PipelinePhaseRunning) && !allSourcesScaledToZero, err
}

// CheckPipelineLiveObservedGeneration verifies that the observedGeneration is not less than the generation, meaning it's been reconciled by Numaflow since being updated
func CheckPipelineLiveObservedGeneration(ctx context.Context, pipeline *unstructured.Unstructured) (bool, int64, int64, error) {
	existingPipelineDef, err := kubernetes.GetLiveResource(ctx, pipeline, "pipelines")
	if err != nil {
		return false, 0, 0, fmt.Errorf("failed to check observed generation of live Pipeline: %v", err)
	}
	return CheckPipelineObservedGeneration(ctx, existingPipelineDef)
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

func GetVertexFromPipelineSpecMap(
	pipelineSpec map[string]interface{},
	vertexName string,
) (map[string]interface{}, bool, error) {
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

// CheckForVertex iterates over vertices in a pipeline spec and returns true if any vertex satisfies the check function
func CheckForVertex(pipelineSpec map[string]interface{}, check func(map[string]interface{}) bool) (bool, error) {
	vertices, found, _ := unstructured.NestedSlice(pipelineSpec, "vertices")
	if !found {
		return false, fmt.Errorf("no vertices found in pipeline spec?: %+v", pipelineSpec)
	}

	for _, vertex := range vertices {
		if vertexAsMap, ok := vertex.(map[string]interface{}); ok {
			if check(vertexAsMap) {
				return true, nil
			}
		}
	}
	return false, nil
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

// determine if all Pipeline Vertices have max=0
func CheckPipelineScaledToZero(ctx context.Context, pipeline *unstructured.Unstructured) (bool, error) {
	scaledToZero := true

	scaleDefinitions, err := GetScaleValuesFromPipelineDefinition(ctx, pipeline)
	if err != nil {
		return false, err
	}
	for _, vertexScale := range scaleDefinitions {
		if vertexScale.ScaleDefinition == nil || vertexScale.ScaleDefinition.Max == nil { // by default if max isn't set, it implies 1
			scaledToZero = false
			break
		} else {
			if !(*vertexScale.ScaleDefinition.Max == 0) {
				scaledToZero = false
			}
		}

	}
	return scaledToZero, nil
}

// ensure all Pipeline Vertices have max=0
func EnsurePipelineScaledToZero(ctx context.Context, pipeline *unstructured.Unstructured, c client.Client) error {
	scaledToZero, err := CheckPipelineScaledToZero(ctx, pipeline)
	if err != nil {
		return err
	}

	if !scaledToZero {
		err := ScalePipelineVerticesToZero(ctx, pipeline, c)
		if err != nil {
			return fmt.Errorf("error scaling pipeline %s's vertices to zero: %v", pipeline.GetName(), err)
		}
	}
	return nil
}

// scale any Source Vertices in the Pipeline definition to min=max=0
func ScalePipelineDefSourceVerticesToZero(
	ctx context.Context,
	pipelineDef *unstructured.Unstructured,
) error {
	vertexScaleDefinitions, err := GetScaleValuesFromPipelineDefinition(ctx, pipelineDef)
	if err != nil {
		return err
	}

	zero := int64(0)
	for i, scaleDef := range vertexScaleDefinitions {
		vertexName := scaleDef.VertexName

		// look for this vertex's entire definition so we can determine if it's a source or not
		pipelineSpec, exists, err := unstructured.NestedMap(pipelineDef.Object, "spec")
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("spec not found in pipeline %s/%s", pipelineDef.GetNamespace(), pipelineDef.GetName())
		}
		vertexDef, found, err := GetVertexFromPipelineSpecMap(pipelineSpec, vertexName)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("vertex definition not found in pipeline %s/%s for vertex %s", pipelineDef.GetNamespace(), pipelineDef.GetName(), vertexName)
		}
		_, isSource, err := unstructured.NestedMap(vertexDef, "source")
		if err != nil {
			return err
		}
		if isSource {
			vertexScaleDefinitions[i].ScaleDefinition = &apiv1.ScaleDefinition{Min: &zero, Max: &zero}
		}
	}
	err = ApplyScaleValuesToPipelineDefinition(ctx, pipelineDef, vertexScaleDefinitions)
	if err != nil {
		return err
	}
	return nil
}

func ScalePipelineVerticesToZero(
	ctx context.Context,
	pipelineDef *unstructured.Unstructured,
	c client.Client,
) error {

	numaLogger := logger.FromContext(ctx).WithValues("pipeline", pipelineDef.GetName())

	// scale down every Vertex to 0 Pods
	// for each Vertex: first check to see if it's already scaled down
	vertexScaleDefinitions, err := GetScaleValuesFromPipelineDefinition(ctx, pipelineDef)
	if err != nil {
		return err
	}
	allVerticesScaledDown := true
	for _, vertexScaleDef := range vertexScaleDefinitions {
		scaleDef := vertexScaleDef.ScaleDefinition
		scaledDown := scaleDef != nil && scaleDef.Min != nil && *scaleDef.Min == 0 && scaleDef.Max != nil && *scaleDef.Max == 0

		if !scaledDown {
			allVerticesScaledDown = false
		}
	}
	if !allVerticesScaledDown {
		zero := int64(0)
		for i := range vertexScaleDefinitions {
			vertexScaleDefinitions[i].ScaleDefinition = &apiv1.ScaleDefinition{Min: &zero, Max: &zero, Disabled: false}
		}

		numaLogger.Debug("Scaling down all vertices to 0 Pods")
		if err := ApplyScaleValuesToLivePipeline(ctx, pipelineDef, vertexScaleDefinitions, c); err != nil {
			return fmt.Errorf("error scaling down the pipeline: %w", err)
		}
	}
	return nil
}

// for each Vertex, get the definition of the Scale
// return map of Vertex name to scale definition
func GetScaleValuesFromPipelineDefinition(ctx context.Context, pipelineDef *unstructured.Unstructured) (
	[]apiv1.VertexScaleDefinition, error) {

	numaLogger := logger.FromContext(ctx).WithValues("pipeline", pipelineDef.GetName())

	vertices, _, err := unstructured.NestedSlice(pipelineDef.Object, "spec", "vertices")
	if err != nil {
		return nil, fmt.Errorf("error while getting vertices of pipeline %s/%s: %w", pipelineDef.GetNamespace(), pipelineDef.GetName(), err)
	}

	numaLogger.WithValues("vertices", vertices).Debugf("found vertices for the pipeline: %d", len(vertices))

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

func ApplyScaleValuesToPipelineDefinition(
	ctx context.Context, pipelineDef *unstructured.Unstructured, vertexScaleDefinitions []apiv1.VertexScaleDefinition) error {

	numaLogger := logger.FromContext(ctx).WithValues("pipeline", pipelineDef.GetName())

	// get the current Vertices definition
	vertexDefinitions, exists, err := unstructured.NestedSlice(pipelineDef.Object, "spec", "vertices")
	if err != nil {
		return fmt.Errorf("error getting spec.vertices from pipeline %s: %s", pipelineDef.GetName(), err.Error())
	}
	if !exists {
		return fmt.Errorf("failed to get spec.vertices from pipeline %s: doesn't exist?", pipelineDef.GetName())
	}

	for _, scaleDef := range vertexScaleDefinitions {
		vertexName := scaleDef.VertexName
		// set the scale min/max for the vertex
		// find this Vertex and update it

		foundVertexInExisting := false
		for index, vertex := range vertexDefinitions {
			vertexAsMap := vertex.(map[string]interface{})
			if vertexAsMap["name"] == vertexName {
				foundVertexInExisting = true

				if scaleDef.ScaleDefinition != nil && scaleDef.ScaleDefinition.Min != nil {
					numaLogger.WithValues("vertex", vertexName).Debugf("setting field 'scale.min' to %d", *scaleDef.ScaleDefinition.Min)
					if err = unstructured.SetNestedField(vertexAsMap, *scaleDef.ScaleDefinition.Min, "scale", "min"); err != nil {
						return err
					}
				} else {
					unstructured.RemoveNestedField(vertexAsMap, "scale", "min")
				}
				if scaleDef.ScaleDefinition != nil && scaleDef.ScaleDefinition.Max != nil {
					numaLogger.WithValues("vertex", vertexName).Debugf("setting field 'scale.max' to %d", *scaleDef.ScaleDefinition.Max)
					if err = unstructured.SetNestedField(vertexAsMap, *scaleDef.ScaleDefinition.Max, "scale", "max"); err != nil {
						return err
					}
				} else {
					unstructured.RemoveNestedField(vertexAsMap, "scale", "max")
				}
				if scaleDef.ScaleDefinition != nil {
					numaLogger.WithValues("vertex", vertexName).Debugf("setting field 'scale.disabled' to %t", scaleDef.ScaleDefinition.Disabled)
					if err = unstructured.SetNestedField(vertexAsMap, scaleDef.ScaleDefinition.Disabled, "scale", "disabled"); err != nil {
						return err
					}
				}
				vertexDefinitions[index] = vertexAsMap
			}
		}
		if !foundVertexInExisting {
			numaLogger.WithValues("vertex", vertexName).Warnf("didn't find vertex in pipeline")
		}
	}

	// now add back the vertex slice into the pipeline
	return unstructured.SetNestedSlice(pipelineDef.Object, vertexDefinitions, "spec", "vertices")
}

// apply the scale values to the running pipeline as patches
// note that vertexScaleDefinitions are not required to be in order and also can be a partial set
func ApplyScaleValuesToLivePipeline(
	ctx context.Context, pipelineDef *unstructured.Unstructured, vertexScaleDefinitions []apiv1.VertexScaleDefinition, c client.Client) error {

	numaLogger := logger.FromContext(ctx).WithValues("pipeline", pipelineDef.GetName())

	vertices, found, err := unstructured.NestedSlice(pipelineDef.Object, "spec", "vertices")
	if err != nil {
		return fmt.Errorf("error getting vertices from pipeline %s: %s", pipelineDef.GetName(), err)
	}
	if !found {
		return fmt.Errorf("error getting vertices from pipeline %s: not found", pipelineDef.GetName())
	}

	verticesPatch := "["
	for _, vertexScale := range vertexScaleDefinitions {

		// find the vertex in the existing spec and determine if "scale" is set or unset; if it's not set, we need to set it
		existingIndex := -1
		existingVertex := map[string]interface{}{}
		for index, v := range vertices {
			existingVertex = v.(map[string]interface{})
			if existingVertex["name"] == vertexScale.VertexName {
				existingIndex = index
				break
			}
		}
		if existingIndex == -1 {
			return fmt.Errorf("invalid vertex name %s in vertexScaleDefinitions, pipeline %s has vertices %+v", vertexScale.VertexName, pipelineDef.GetName(), vertices)
		}

		_, found := existingVertex["scale"]
		if !found {
			vertexPatch := fmt.Sprintf(`
			{
				"op": "add",
				"path": "/spec/vertices/%d/scale",
				"value": %s
			},`, existingIndex, `{"min": null, "max": null}`)
			verticesPatch = verticesPatch + vertexPatch
		}

		minStr := "null"
		if vertexScale.ScaleDefinition != nil && vertexScale.ScaleDefinition.Min != nil {
			minStr = fmt.Sprintf("%d", *vertexScale.ScaleDefinition.Min)
		}
		vertexPatch := fmt.Sprintf(`
		{
			"op": "add",
			"path": "/spec/vertices/%d/scale/min",
			"value": %s
		},`, existingIndex, minStr)
		verticesPatch = verticesPatch + vertexPatch

		maxStr := "null"
		if vertexScale.ScaleDefinition != nil && vertexScale.ScaleDefinition.Max != nil {
			maxStr = fmt.Sprintf("%d", *vertexScale.ScaleDefinition.Max)
		}
		vertexPatch = fmt.Sprintf(`
		{
			"op": "add",
			"path": "/spec/vertices/%d/scale/max",
			"value": %s
		},`, existingIndex, maxStr)
		verticesPatch = verticesPatch + vertexPatch

		disabled := false
		if vertexScale.ScaleDefinition != nil && vertexScale.ScaleDefinition.Disabled {
			disabled = true
		}
		vertexPatch = fmt.Sprintf(`
		{
			"op": "add",
			"path": "/spec/vertices/%d/scale/disabled",
			"value": %t
		},`, existingIndex, disabled)
		verticesPatch = verticesPatch + vertexPatch

	}
	// remove terminating comma
	if verticesPatch[len(verticesPatch)-1] == ',' {
		verticesPatch = verticesPatch[0 : len(verticesPatch)-1]
	}
	verticesPatch = verticesPatch + "]"
	numaLogger.WithValues("specPatch patch", verticesPatch).Debug("patching vertices")

	err = kubernetes.PatchResource(ctx, c, pipelineDef, verticesPatch, k8stypes.JSONPatchType)
	return err
}
