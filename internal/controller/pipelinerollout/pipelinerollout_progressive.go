package pipelinerollout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	argorolloutsv1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
)

// CreateUpgradingChildDefinition creates a definition for an "upgrading" pipeline
// This implements a function of the progressiveController interface
func (r *PipelineRolloutReconciler) CreateUpgradingChildDefinition(ctx context.Context, rolloutObject progressive.ProgressiveRolloutObject, name string) (*unstructured.Unstructured, error) {
	numaLogger := logger.FromContext(ctx)

	pipelineRollout := rolloutObject.(*apiv1.PipelineRollout)
	metadata, err := getBasePipelineMetadata(pipelineRollout)
	if err != nil {
		return nil, err
	}

	// which InterstepBufferServiceName should we use?
	// If there is an upgrading isbsvc, use that
	// Otherwise, use the promoted one
	isbsvc, err := r.getISBSvc(ctx, pipelineRollout, common.LabelValueUpgradeInProgress)
	if err != nil {
		return nil, err
	}
	if isbsvc == nil {
		numaLogger.Debugf("no Upgrading isbsvc found for Pipeline, will find promoted one")
		isbsvc, err = r.getISBSvc(ctx, pipelineRollout, common.LabelValueUpgradePromoted)
		if err != nil || isbsvc == nil {
			return nil, fmt.Errorf("failed to find isbsvc that's 'promoted': won't be able to reconcile PipelineRollout, err=%v", err)
		}
	}

	pipeline, err := r.makePipelineDefinition(pipelineRollout, name, isbsvc.GetName(), metadata)
	if err != nil {
		return nil, err
	}

	labels := pipeline.GetLabels()
	labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeInProgress)
	labels[common.LabelKeyISBServiceChildNameForPipeline] = isbsvc.GetName()
	pipeline.SetLabels(labels)

	return pipeline, nil
}

// AssessUpgradingChild makes an assessment of the upgrading child to determine if it was successful, failed, or still not known
// This implements a function of the progressiveController interface
func (r *PipelineRolloutReconciler) AssessUpgradingChild(ctx context.Context, rolloutObject progressive.ProgressiveRolloutObject, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, string, error) {
	verifyReplicasFunc := func(existingUpgradingChildDef *unstructured.Unstructured) (bool, string, error) {
		verticesList, err := kubernetes.ListLiveResource(ctx, common.NumaflowAPIGroup, common.NumaflowAPIVersion,
			numaflowv1.VertexGroupVersionResource.Resource, existingUpgradingChildDef.GetNamespace(),
			fmt.Sprintf("%s=%s", common.LabelKeyNumaflowPodPipelineName, existingUpgradingChildDef.GetName()), "")
		if err != nil {
			return false, "", err
		}

		if verticesList == nil {
			return false, "", errors.New("the pipeline vertices list is nil, this should not occur")
		}

		areAllVerticesReplicasReady := true
		var replicasFailureReason string
		for _, vertex := range verticesList.Items {
			areVertexReplicasReady, failureReason, err := progressive.AreVertexReplicasReady(&vertex)
			if err != nil {
				return false, "", err
			}

			if !areVertexReplicasReady {
				areAllVerticesReplicasReady = false
				replicasFailureReason = failureReason
				break
			}
		}

		return areAllVerticesReplicasReady, replicasFailureReason, nil
	}

	pipelineRollout := rolloutObject.(*apiv1.PipelineRollout)
	analysis := pipelineRollout.GetAnalysis()
	// only check for and create AnalysisRuns if templates are specified
	if len(analysis.Templates) > 0 {
		analysisRun := &argorolloutsv1.AnalysisRun{}
		// check if analysisRun has already been created
		if err := r.client.Get(ctx, client.ObjectKey{Name: existingUpgradingChildDef.GetName(), Namespace: existingUpgradingChildDef.GetNamespace()}, analysisRun); err != nil {
			if apierrors.IsNotFound(err) {
				// analysisRun is created the first time the upgrading child is assessed
				err := progressive.CreateAnalysisRun(ctx, analysis, existingUpgradingChildDef, r.client)
				if err != nil {
					return apiv1.AssessmentResultUnknown, "", err
				}
				analysisStatus := pipelineRollout.GetAnalysisStatus()
				if analysisStatus == nil {
					return apiv1.AssessmentResultUnknown, "", errors.New("analysisStatus not set")
				}
				// analysisStatus is updated with name of AnalysisRun (which is the same name as the upgrading child)
				// and start time for its assessment
				analysisStatus.AnalysisRunName = existingUpgradingChildDef.GetName()
				timeNow := metav1.NewTime(time.Now())
				analysisStatus.StartTime = &timeNow
				pipelineRollout.SetAnalysisStatus(analysisStatus)
			} else {
				return apiv1.AssessmentResultUnknown, "", err
			}
		}

		// assess analysisRun status and set endTime if completed
		analysisStatus := pipelineRollout.GetAnalysisStatus()
		if analysisStatus != nil {
			// assess analysisRun status and set endTime if completed
			if analysisRun.Status.Phase.Completed() && analysisStatus.EndTime == nil {
				analysisStatus.EndTime = analysisRun.Status.CompletedAt
			}
			analysisStatus.AnalysisRunName = existingUpgradingChildDef.GetName()
			analysisStatus.Phase = analysisRun.Status.Phase
			pipelineRollout.SetAnalysisStatus(analysisStatus)
		}

	}

	return progressive.AssessUpgradingPipelineType(ctx, pipelineRollout.GetAnalysisStatus(), existingUpgradingChildDef, verifyReplicasFunc)
}

/*
ProcessPromotedChildPreUpgrade handles the pre-upgrade processing of a promoted pipeline.
It performs the following pre-upgrade operations:
- it ensures that the promoted pipeline vertices are scaled down before proceeding with a progressive upgrade.

Parameters:
  - ctx: the context for managing request-scoped values.
  - pipelineRollout: the pipelineRollout
  - promotedPipelineDef: the definition of the promoted pipeline as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *PipelineRolloutReconciler) ProcessPromotedChildPreUpgrade(
	ctx context.Context,
	pipelineRollout progressive.ProgressiveRolloutObject,
	promotedPipelineDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessPromotedChildPreUpgrade").WithName("PipelineRollout").
		WithValues("promotedPipelineNamespace", promotedPipelineDef.GetNamespace(), "promotedPipelineName", promotedPipelineDef.GetName())

	numaLogger.Debug("started pre-upgrade processing of promoted pipeline")
	pipelineRO, ok := pipelineRollout.(*apiv1.PipelineRollout)
	if !ok {
		return true, fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process promoted pipeline pre-upgrade", pipelineRollout)
	}

	if pipelineRO.Status.ProgressiveStatus.PromotedPipelineStatus == nil {
		return true, errors.New("unable to perform pre-upgrade operations because the rollout does not have promotedChildStatus set")
	}

	// scaleDownPipelineVertices retrieves the currently running pods to update the PromotedPipelineStatus scaleValues,
	// or if already retrieved, updates the promoted pipeline to scale down the vertex pods.
	// This serves to make sure that the vertex pods have been really scaled down before proceeding
	// with the progressive upgrade.
	requeue, err := scaleDownPipelineVertices(ctx, pipelineRO.Status.ProgressiveStatus.PromotedPipelineStatus, promotedPipelineDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debugf("completed pre-upgrade processing of promoted pipeline, requeue=%t", requeue)

	return requeue, nil
}

/*
ProcessPromotedChildPostFailure handles the post-upgrade processing of the promoted pipeline after the "upgrading" pipeline has failed.
It performs the following post-upgrade operations:
- it restores the promoted pipeline vertices scale values to the original values retrieved from the rollout status.

Parameters:
  - ctx: the context for managing request-scoped values.
  - pipelineRollout: the PipelineRollout instance
  - promotedPipelineDef: the definition of the promoted pipeline as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *PipelineRolloutReconciler) ProcessPromotedChildPostFailure(
	ctx context.Context,
	pipelineRollout progressive.ProgressiveRolloutObject,
	promotedPipelineDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessPromotedChildPostFailure").WithName("PipelineRollout").
		WithValues("promotedPipelineNamespace", promotedPipelineDef.GetNamespace(), "promotedPipelineName", promotedPipelineDef.GetName())

	numaLogger.Debug("started post-failure processing of promoted pipeline")

	pipelineRO, ok := pipelineRollout.(*apiv1.PipelineRollout)
	if !ok {
		return true, fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process promoted pipeline post-upgrade", pipelineRollout)
	}

	if pipelineRO.Status.ProgressiveStatus.PromotedPipelineStatus == nil {
		return true, errors.New("unable to perform post-upgrade operations because the rollout does not have promotedChildStatus set")
	}

	requeue, err := scalePipelineVerticesToOriginalValues(ctx, pipelineRO.Status.ProgressiveStatus.PromotedPipelineStatus, promotedPipelineDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed post-failure processing of promoted pipeline")

	return requeue, nil
}

func (r *PipelineRolloutReconciler) ProcessUpgradingChildPostFailure(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingPipelineDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessUpgradingChildPostFailure").WithName("PipelineRollout").
		WithValues("upgradingPipelineNamespace", upgradingPipelineDef.GetNamespace(), "upgradingPipelineName", upgradingPipelineDef.GetName())

	numaLogger.Debug("started post-failure processing of upgrading pipeline")

	// scale down every Vertex to 0 Pods
	err := scalePipelineVerticesToZero(ctx, upgradingPipelineDef, c)

	numaLogger.Debugf("completed post-failure processing of upgrading pipeline, err=%v", err)

	return false, err
}

func (r *PipelineRolloutReconciler) ProcessUpgradingChildPostSuccess(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingPipelineDef *unstructured.Unstructured,
	c client.Client,
) error {

	pipelineRollout, ok := rolloutObject.(*apiv1.PipelineRollout)
	if !ok {
		return fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process upgrading pipeline post-success", rolloutObject)
	}

	// For each Pipeline vertex, patch to the original scale definition
	// Note this is not expected to be executed repeatedly, so we don't need to worry about first verifying it's not already set that way

	upgradingPipelineStatus := pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus
	if upgradingPipelineStatus == nil {
		return fmt.Errorf("can't process upgrading pipeline post-success; missing UpgradingPipelineStatus which should contain scale values")
	}

	return applyScalePatchesToLivePipeline(ctx, upgradingPipelineDef, upgradingPipelineStatus.OriginalScaleMinMax, c)
}

/*
ProcessUpgradingChildPreUpgrade handles the processing of an upgrading pipeline before it's been created
It performs the following pre-upgrade operations:
- it uses the promoted rollout status scale values to calculate the upgrading pipeline scale min and max for each vertex.

Parameters:
  - ctx: the context for managing request-scoped values.
  - rolloutObject: the PipelineRollout instance
  - upgradingPipelineDef: the definition of the upgrading pipeline as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *PipelineRolloutReconciler) ProcessUpgradingChildPreUpgrade(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingPipelineDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessUpgradingChildPreUpgrade").WithName("PipelineRollout").
		WithValues("upgradingPipelineNamespace", upgradingPipelineDef.GetNamespace(), "upgradingPipelineName", upgradingPipelineDef.GetName())

	numaLogger.Debug("started pre-upgrade processing of upgrading pipeline")
	pipelineRollout, ok := rolloutObject.(*apiv1.PipelineRollout)
	if !ok {
		return true, fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process upgrading pipeline pre-upgrade", rolloutObject)
	}

	// save the current scale definitions from the upgrading Pipeline to our Status so we can use them when we scale it back up after success
	scalePatchStrings, err := getScalePatchesFromPipelineSpec(ctx, upgradingPipelineDef)
	if err != nil {
		return true, err
	}

	pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus.OriginalScaleMinMax = scalePatchStrings

	/*if pipelineRollout.Status.ProgressiveStatus.PromotedPipelineStatus == nil {
		return true, errors.New("unable to perform pre-upgrade operations because the rollout does not have promotedChildStatus set")
	}*/

	err = createScaledDownUpgradingPipelineDef(ctx, pipelineRollout, upgradingPipelineDef, c)
	if err != nil {
		return true, err
	}

	// 	err := updatePipelineVertexDefScale(ctx, upgradingPipelineDef, vertexName, &upgradingPipelineVertexScaleTo, &upgradingPipelineVertexScaleTo)
	// 	if err != nil {
	// 		return true, err
	// 	}
	// }

	numaLogger.Debug("completed pre-upgrade processing of upgrading pipeline")

	return false, nil
}

// createScaledDownUpgradingPipelineDef sets the upgrading Pipeline's vertex scale definition to the number of Pods
// that were removed from the same Vertex of the promoted Pipeline (thus the overall number stays the same)
func createScaledDownUpgradingPipelineDef(
	ctx context.Context,
	pipelineRollout *apiv1.PipelineRollout,
	upgradingPipelineDef *unstructured.Unstructured,
	c client.Client,
) error {
	numaLogger := logger.FromContext(ctx).WithValues("pipeline", upgradingPipelineDef.GetName())

	// get the current Vertices definition
	vertexDefinitions, exists, err := unstructured.NestedSlice(upgradingPipelineDef.Object, "spec", "vertices")
	if err != nil {
		return fmt.Errorf("error getting spec.vertices from pipeline %s: %s", upgradingPipelineDef.GetName(), err.Error())
	}
	if !exists {
		return fmt.Errorf("failed to get spec.vertices from pipeline %s: doesn't exist?", upgradingPipelineDef.GetName())
	}

	// map each vertex name to new min/max
	vertexScaleDefinitions := make([]VertexScaleDefinition, len(pipelineRollout.Status.ProgressiveStatus.PromotedPipelineStatus.ScaleValues))
	for index, vertex := range vertexDefinitions {
		vertexAsMap := vertex.(map[string]interface{})
		vertexName := vertexAsMap["name"].(string)
		scaleValue, vertexFound := pipelineRollout.Status.ProgressiveStatus.PromotedPipelineStatus.ScaleValues[vertexName]
		var upgradingVertexScaleTo int64
		if !vertexFound {
			// this must be a new vertex: we still need to set min=max so we will effectively be able to perform resource health check for readyReplicas without
			// autoscaling interfering with the assessment
			// simplest thing is to set min=max=1
			upgradingVertexScaleTo = 1
			numaLogger.WithValues("vertex", vertexName).Debugf("vertex not found previously; scaling upgrading pipeline vertex to min=max=%d", upgradingVertexScaleTo)
		} else {
			// nominal case: found the same vertex from the "promoted" pipeline: set min and max to the number of Pods that were removed from the "promoted" one
			upgradingVertexScaleTo = scaleValue.Initial - scaleValue.ScaleTo
			numaLogger.WithValues("vertex", vertexName).Debugf("scaling upgrading pipeline vertex to min=max=%d", upgradingVertexScaleTo)
		}

		vertexScaleDefinitions[index] = VertexScaleDefinition{
			vertexName: vertexName,
			scaleDefinition: &progressive.ScaleDefinition{
				Min: &upgradingVertexScaleTo,
				Max: &upgradingVertexScaleTo,
			},
		}

	}

	// apply the scale values for each vertex to the new min/max
	return applyScaleValuesToPipelineDefinition(ctx, upgradingPipelineDef, vertexScaleDefinitions)
}

func scalePipelineVerticesToZero(
	ctx context.Context,
	pipelineDef *unstructured.Unstructured,
	c client.Client,
) error {

	numaLogger := logger.FromContext(ctx).WithValues("pipeline", pipelineDef.GetName())

	// scale down every Vertex to 0 Pods
	// for each Vertex: first check to see if it's already scaled down
	vertexScaleDefinitions, err := getScaleValuesFromPipelineSpec(ctx, pipelineDef)
	if err != nil {
		return err
	}
	allVerticesScaledDown := true
	for _, vertexScaleDef := range vertexScaleDefinitions {
		scaleDef := vertexScaleDef.scaleDefinition
		scaledDown := scaleDef != nil && scaleDef.Min != nil && *scaleDef.Min == 0 && scaleDef.Max != nil && *scaleDef.Max == 0

		if !scaledDown {
			allVerticesScaledDown = false
		}
	}
	if !allVerticesScaledDown {

		vertices, _, err := unstructured.NestedSlice(pipelineDef.Object, "spec", "vertices")
		if err != nil {
			return fmt.Errorf("error while getting vertices of promoted pipeline: %w", err)
		}
		for _, vertex := range vertices {
			if vertexAsMap, ok := vertex.(map[string]any); ok {

				if err := unstructured.SetNestedField(vertexAsMap, int64(0), "scale", "min"); err != nil {
					return err
				}

				if err := unstructured.SetNestedField(vertexAsMap, int64(0), "scale", "max"); err != nil {
					return err
				}
			}
		}

		numaLogger.Debug("Scaling down all vertices to 0 Pods")
		if err := patchPipelineVertices(ctx, pipelineDef, vertices, c); err != nil {
			return fmt.Errorf("error scaling down the pipeline: %w", err)
		}
	}
	return nil
}

/*
ProcessUpgradingChildPostUpgrade handles the processing of an upgrading pipeline definition after it's been created

Parameters:
  - ctx: the context for managing request-scoped values.
  - rolloutObject: the MonoVertexRollout instance
  - upgradingMonoVertexDef: the definition of the upgrading monovertex as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *PipelineRolloutReconciler) ProcessUpgradingChildPostUpgrade(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingPipelineDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	return false, nil
}

/*
scaleDownPipelineertices scales down the vertices pods of a pipeline to half of the current count if not already scaled down.
It checks if all vertices are already scaled down and skips the operation if true.

The function updates the scale values in the rollout status and adjusts the scale configuration
of the promoted pipeline definition.

Parameters:
- ctx: the context for managing request-scoped values.
- promotedPipelineStatus: the status of the promoted pipeline in the rollout.
- promotedPipelineDef: the unstructured object representing the promoted pipeline definition.
- c: the Kubernetes client for resource operations.

Returns:
- bool: true if should requeue, false otherwise. Should requeue in case of error or if not all vertices have been scaled down.
- error: an error if any operation fails during the scaling process.
*/
func scaleDownPipelineVertices(
	ctx context.Context,
	promotedPipelineStatus *apiv1.PromotedPipelineStatus,
	promotedPipelineDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("scaleDownPipelineVertices").
		WithValues("promotedPipelineNamespace", promotedPipelineDef.GetNamespace(), "promotedPipelineName", promotedPipelineDef.GetName())

	// If the pipeline vertices have been scaled down already, do not perform scaling down operations
	if promotedPipelineStatus.AreAllVerticesScaledDown(promotedPipelineDef.GetName()) {
		return false, nil
	}

	vertices, _, err := unstructured.NestedSlice(promotedPipelineDef.Object, "spec", "vertices")
	if err != nil {
		return true, fmt.Errorf("error while getting vertices of promoted pipeline: %w", err)
	}

	numaLogger.WithValues("promotedChildName", promotedPipelineDef.GetName(), "vertices", vertices).Debugf("found vertices for the promoted pipeline: %d", len(vertices))

	scaleValuesMap := map[string]apiv1.ScaleValues{}
	if promotedPipelineStatus.ScaleValues != nil {
		scaleValuesMap = promotedPipelineStatus.ScaleValues
	}

	promotedChildNeedsUpdate := false
	for _, vertex := range vertices {
		if vertexAsMap, ok := vertex.(map[string]any); ok {

			vertexName, foundVertexName, err := unstructured.NestedString(vertexAsMap, "name")
			if err != nil {
				return true, err
			}
			if !foundVertexName {
				return true, errors.New("a vertex must have a name")
			}

			podsList, err := kubernetes.ListPodsMetadataOnly(ctx, c, promotedPipelineDef.GetNamespace(), fmt.Sprintf(
				"%s=%s, %s=%s",
				common.LabelKeyNumaflowPodPipelineName, promotedPipelineDef.GetName(),
				common.LabelKeyNumaflowPodPipelineVertexName, vertexName,
			))
			if err != nil {
				return true, err
			}

			currentPodsCount := int64(len(podsList.Items))

			numaLogger.WithValues("vertexName", vertexName, "currentPodsCount", currentPodsCount).Debugf("found pods for the vertex")

			// If for the vertex we already set a ScaleTo value, we only need to update the current pods count
			// to later verify that the pods were actually scaled down.
			// We want to skip scaling down again.
			if vertexScaleValues, exist := scaleValuesMap[vertexName]; exist {
				vertexScaleValues.Current = currentPodsCount
				scaleValuesMap[vertexName] = vertexScaleValues

				numaLogger.WithValues("scaleValuesMap", scaleValuesMap).Debugf("updated scaleValues map for vertex '%s' with running pods count, skipping scaling down for this vertex since it has already been done", vertexName)
				continue
			}

			promotedChildNeedsUpdate = true

			originalScaleMinMax, err := progressive.ExtractScaleMinMaxAsJSONString(vertexAsMap, []string{"scale"})
			if err != nil {
				return true, fmt.Errorf("cannot extract the scale min and max values from the promoted pipeline vertex %s: %w", vertexName, err)
			}

			newMin, newMax, err := progressive.CalculateScaleMinMaxValues(vertexAsMap, int(currentPodsCount), []string{"scale", "min"})
			if err != nil {
				return true, fmt.Errorf("cannot calculate the scale min and max values: %+w", err)
			}

			numaLogger.WithValues(
				"promotedChildName", promotedPipelineDef.GetName(),
				"vertexName", vertexName,
				"currentPodsCount", currentPodsCount,
				"newMin", newMin,
				"newMax", newMax,
				"originalScaleMinMax", originalScaleMinMax,
			).Debugf("found %d pod(s) for the vertex, scaling down to %d", currentPodsCount, newMax)

			if err := unstructured.SetNestedField(vertexAsMap, newMin, "scale", "min"); err != nil {
				return true, err
			}

			if err := unstructured.SetNestedField(vertexAsMap, newMax, "scale", "max"); err != nil {
				return true, err
			}

			scaleValuesMap[vertexName] = apiv1.ScaleValues{
				OriginalScaleMinMax: originalScaleMinMax,
				ScaleTo:             newMax,
				Current:             currentPodsCount,
				Initial:             currentPodsCount,
			}
		}
	}

	if promotedChildNeedsUpdate {
		if err := patchPipelineVertices(ctx, promotedPipelineDef, vertices, c); err != nil {
			return true, fmt.Errorf("error scaling down the existing promoted pipeline: %w", err)
		}

		numaLogger.WithValues("vertices", vertices, "scaleValuesMap", scaleValuesMap).Debug("updated the promoted pipeline with the new scale configuration")
	}

	promotedPipelineStatus.ScaleValues = scaleValuesMap
	promotedPipelineStatus.MarkAllVerticesScaledDown()

	// Set ScaleValuesRestoredToOriginal to false in case previously set to true and now scaling back down to recover from a previous failure
	promotedPipelineStatus.ScaleValuesRestoredToOriginal = false

	return !promotedPipelineStatus.AreAllVerticesScaledDown(promotedPipelineDef.GetName()), nil
}

/*
scalePipelineVerticesToOriginalValues scales the vertices of a pipeline to their original values based on the rollout status.
This function checks if the pipeline vertices have already been scaled to the original values. If not, it restores the scale values
from the rollout's promoted pipeline status and updates the Kubernetes resource accordingly.

Parameters:
- ctx: the context for managing request-scoped values.
- promotedPipelineStatus: the status of the promoted pipeline in the rollout, containing scale values.
- promotedPipelineDef: the unstructured definition of the promoted pipeline resource.
- c: the Kubernetes client for resource operations.

Returns:
- bool: true if should requeue, false otherwise. Should requeue in case of error or if not all vertices have been scaled back to the original values.
- An error if any issues occur during the scaling process.
*/
func scalePipelineVerticesToOriginalValues(
	ctx context.Context,
	promotedPipelineStatus *apiv1.PromotedPipelineStatus,
	promotedPipelineDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("scalePipelineVerticesToOriginalValues").
		WithValues("promotedPipelineNamespace", promotedPipelineDef.GetNamespace(), "promotedPipelineName", promotedPipelineDef.GetName())

	// If all the pipeline vertices have been scaled back to the original values already, do not restore scaling values again
	if promotedPipelineStatus.AreScaleValuesRestoredToOriginal(promotedPipelineDef.GetName()) {
		return false, nil
	}

	if promotedPipelineStatus.ScaleValues == nil {
		return true, errors.New("unable to restore scale values for the promoted pipeline vertices because the rollout does not have promotedChildStatus set")
	}

	vertices, _, err := unstructured.NestedSlice(promotedPipelineDef.Object, "spec", "vertices")
	if err != nil {
		return true, fmt.Errorf("error while getting vertices of promoted pipeline: %w", err)
	}

	for _, vertex := range vertices {
		if vertexAsMap, ok := vertex.(map[string]any); ok {

			vertexName, found, err := unstructured.NestedString(vertexAsMap, "name")
			if err != nil {
				return true, err
			}
			if !found {
				return true, errors.New("a vertex must have a name")
			}

			vertexScaleValues, exists := promotedPipelineStatus.ScaleValues[vertexName]
			if !exists {
				return true, fmt.Errorf("the scale values for vertex '%s' are not present in the rollout promotedChildStatus", vertexName)
			}

			if vertexScaleValues.OriginalScaleMinMax == "null" {
				unstructured.RemoveNestedField(vertexAsMap, "scale")
			} else {
				scaleAsMap := map[string]any{}
				err = json.Unmarshal([]byte(vertexScaleValues.OriginalScaleMinMax), &scaleAsMap)
				if err != nil {
					return true, fmt.Errorf("failed to unmarshal OriginalScaleMinMax: %w", err)
				}

				if err := unstructured.SetNestedField(vertexAsMap, scaleAsMap["min"], "scale", "min"); err != nil {
					return true, err
				}

				if err := unstructured.SetNestedField(vertexAsMap, scaleAsMap["max"], "scale", "max"); err != nil {
					return true, err
				}
			}
		}
	}

	if err := patchPipelineVertices(ctx, promotedPipelineDef, vertices, c); err != nil {
		return true, fmt.Errorf("error scaling the existing promoted pipeline vertices to original values: %w", err)
	}

	numaLogger.WithValues("promotedPipelineDef", promotedPipelineDef).Debug("patched the promoted pipeline vertices with the original scale configuration")

	promotedPipelineStatus.ScaleValuesRestoredToOriginal = true
	promotedPipelineStatus.AllVerticesScaledDown = false
	promotedPipelineStatus.ScaleValues = nil

	return false, nil
}

func patchPipelineVertices(ctx context.Context, pipelineDef *unstructured.Unstructured, vertices []any, c client.Client) error {
	patch := &unstructured.Unstructured{Object: make(map[string]any)}
	err := unstructured.SetNestedSlice(patch.Object, vertices, "spec", "vertices")
	if err != nil {
		return err
	}

	patchAsBytes, err := patch.MarshalJSON()
	if err != nil {
		return err
	}

	if err := kubernetes.PatchResource(ctx, c, pipelineDef, string(patchAsBytes), k8stypes.MergePatchType); err != nil {
		return err
	}

	return nil
}

func applyScaleValuesToPipelineDefinition(
	ctx context.Context, pipelineDef *unstructured.Unstructured, vertexScaleDefinitions []VertexScaleDefinition) error {

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
		vertexName := scaleDef.vertexName
		// set the scale min/max for the vertex
		// find this Vertex and update it

		foundVertexInExisting := false
		for index, vertex := range vertexDefinitions {
			vertexAsMap := vertex.(map[string]interface{})
			if vertexAsMap["name"] == vertexName {
				foundVertexInExisting = true

				if scaleDef.scaleDefinition != nil && scaleDef.scaleDefinition.Min != nil {
					numaLogger.WithValues("vertex", vertexName).Debugf("setting field 'scale.min' to %d", *scaleDef.scaleDefinition.Min)
					if unstructured.SetNestedField(vertexAsMap, *scaleDef.scaleDefinition.Min, "scale", "min"); err != nil {
						return err
					}
				}
				if scaleDef.scaleDefinition != nil && scaleDef.scaleDefinition.Max != nil {
					numaLogger.WithValues("vertex", vertexName).Debugf("setting field 'scale.max' to %d", *scaleDef.scaleDefinition.Max)
					if unstructured.SetNestedField(vertexAsMap, *scaleDef.scaleDefinition.Max, "scale", "max"); err != nil {
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

// TODO: should we be updating our pipeline definition when we do the patching or no?
func applyScaleValuesToLivePipeline(
	ctx context.Context, pipelineDef *unstructured.Unstructured, vertexScaleDefinitions []VertexScaleDefinition, c client.Client) error {
	vertexPatches := make([]apiv1.VertexScale, len(vertexScaleDefinitions))
	for i, scaleDef := range vertexScaleDefinitions {
		vertexPatches[i] = apiv1.VertexScale{
			VertexName:  scaleDef.vertexName,
			ScaleMinMax: progressive.ScaleDefinitionToPatchString(scaleDef.scaleDefinition),
		}
	}

	return applyScalePatchesToLivePipeline(ctx, pipelineDef, vertexPatches, c)
}

func applyScalePatchesToLivePipeline(
	ctx context.Context, pipelineDef *unstructured.Unstructured, vertexScaleDefinitions []apiv1.VertexScale, c client.Client) error {
	numaLogger := logger.FromContext(ctx).WithValues("pipeline", pipelineDef.GetName())

	verticesPatch := "["

	for index, vertexScale := range vertexScaleDefinitions {
		vertexPatch := fmt.Sprintf(`
			{
				"op": "replace",
				"path": "/spec/vertices/%d/scale",
				"value": %s
			},`, index, vertexScale.ScaleMinMax)
		verticesPatch = verticesPatch + vertexPatch
	}

	// remove terminating comma
	if verticesPatch[len(verticesPatch)-1] == ',' {
		verticesPatch = verticesPatch[0 : len(verticesPatch)-1]
	}
	verticesPatch = verticesPatch + "]"
	numaLogger.WithValues("specPatch patch", verticesPatch).Debug("patching vertices")

	return kubernetes.PatchResource(ctx, c, pipelineDef, verticesPatch, k8stypes.JSONPatchType)
}

type VertexScaleDefinition struct {
	vertexName      string
	scaleDefinition *progressive.ScaleDefinition
}

// for each Vertex, get the definition of the Scale
// return map of Vertex name to scale definition
func getScaleValuesFromPipelineSpec(ctx context.Context, pipelineDef *unstructured.Unstructured) (
	[]VertexScaleDefinition, error) {

	numaLogger := logger.FromContext(ctx).WithValues("pipeline", pipelineDef.GetName())

	vertices, _, err := unstructured.NestedSlice(pipelineDef.Object, "spec", "vertices")
	if err != nil {
		return nil, fmt.Errorf("error while getting vertices of pipeline %s/%s: %w", pipelineDef.GetNamespace(), pipelineDef.GetName(), err)
	}

	numaLogger.WithValues("vertices", vertices).Debugf("found vertices for the pipeline: %d", len(vertices))

	scaleDefinitions := []VertexScaleDefinition{}

	for _, vertex := range vertices {
		if vertexAsMap, ok := vertex.(map[string]any); ok {

			vertexName, foundVertexName, err := unstructured.NestedString(vertexAsMap, "name")
			if err != nil {
				return nil, err
			}
			if !foundVertexName {
				return nil, errors.New("a vertex must have a name")
			}

			vertexScaleDef, err := progressive.ExtractScaleMinMax(vertexAsMap, []string{"scale"})
			if err != nil {
				return nil, err
			}
			scaleDefinitions = append(scaleDefinitions, VertexScaleDefinition{vertexName: vertexName, scaleDefinition: vertexScaleDef})
		}
	}
	return scaleDefinitions, nil
}

func getScalePatchesFromPipelineSpec(ctx context.Context, pipelineDef *unstructured.Unstructured) (
	[]apiv1.VertexScale, error) {

	scaleDefinitions, err := getScaleValuesFromPipelineSpec(ctx, pipelineDef)
	if err != nil {
		return nil, err
	}
	scalePatchStrings := []apiv1.VertexScale{}
	for _, vertexScaleDef := range scaleDefinitions {
		scalePatchStrings = append(scalePatchStrings, apiv1.VertexScale{VertexName: vertexScaleDef.vertexName, ScaleMinMax: progressive.ScaleDefinitionToPatchString(vertexScaleDef.scaleDefinition)})
	}
	return scalePatchStrings, nil
}
