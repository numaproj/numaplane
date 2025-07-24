package pipelinerollout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/usde"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
func (r *PipelineRolloutReconciler) AssessUpgradingChild(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	existingUpgradingChildDef *unstructured.Unstructured,
	assessmentSchedule config.AssessmentSchedule) (apiv1.AssessmentResult, string, error) {

	numaLogger := logger.FromContext(ctx)

	pipelineRollout := rolloutObject.(*apiv1.PipelineRollout)

	childStatus := pipelineRollout.GetUpgradingChildStatus()

	// function for checking readiness of Pipeline Vertex replicas
	verifyReplicasFunc := func(existingUpgradingChildDef *unstructured.Unstructured) (bool, string, error) {
		verticesList, err := kubernetes.ListLiveResource(ctx, common.NumaflowAPIGroup, common.NumaflowAPIVersion,
			numaflowv1.VertexGroupVersionResource.Resource, existingUpgradingChildDef.GetNamespace(),
			fmt.Sprintf("%s=%s", common.LabelKeyNumaflowPodPipelineName, existingUpgradingChildDef.GetName()), "")
		if err != nil {
			return false, "", err
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
				replicasFailureReason = fmt.Sprintf("%s (vertex: %s)", failureReason, vertex.GetName())
				break
			}
		}

		return areAllVerticesReplicasReady, replicasFailureReason, nil
	}

	// First perform basic resource health check
	assessment, reasonFailure, err := progressive.PerformResourceHealthCheckForPipelineType(ctx, existingUpgradingChildDef, verifyReplicasFunc)
	if err != nil {
		return assessment, reasonFailure, err
	}
	// if we fail even once, that's considered failure
	if assessment == apiv1.AssessmentResultFailure {
		// set AssessmentEndTime to now and return failure
		_ = progressive.UpdateUpgradingChildStatus(pipelineRollout, func(status *apiv1.UpgradingChildStatus) {
			assessmentEndTime := metav1.NewTime(time.Now())
			status.BasicAssessmentEndTime = &assessmentEndTime
		})

		return assessment, reasonFailure, nil
	}

	// if we succeed, we must continue to succeed for a prescribed period of time in order to consider the resource health
	// check "successful"
	if assessment == apiv1.AssessmentResultSuccess {
		// has AssessmentEndTime been set? if not, set it - now we can start our interval
		if !childStatus.IsAssessmentEndTimeSet() {
			_ = progressive.UpdateUpgradingChildStatus(pipelineRollout, func(status *apiv1.UpgradingChildStatus) {
				assessmentEndTime := metav1.NewTime(time.Now().Add(assessmentSchedule.Period))
				status.BasicAssessmentEndTime = &assessmentEndTime
			})
			numaLogger.WithValues("childStatus", *childStatus).Debug("set upgrading child AssessmentEndTime")
		}

		// if end time has arrived (i.e. we continually determined "Success" for the entire prescribed period of time),
		// if we need to launch an AnalysisRun, we can do it now;
		// otherwise, we can declare success
		if childStatus.BasicAssessmentEndTimeArrived() {
			analysis := pipelineRollout.GetAnalysis()
			// only check for and create AnalysisRun if templates are specified
			if len(analysis.Templates) > 0 {
				// this will create an AnalysisRun if it doesn't exist yet; or otherwise it will check if it's finished running
				analysisStatus, err := progressive.PerformAnalysis(ctx, existingUpgradingChildDef, pipelineRollout, pipelineRollout.GetAnalysis(), pipelineRollout.GetAnalysisStatus(), r.client)
				if err != nil {
					return apiv1.AssessmentResultUnknown, "", err
				}
				return progressive.AssessAnalysisStatus(ctx, existingUpgradingChildDef, analysisStatus)
			} else {
				return apiv1.AssessmentResultSuccess, "", nil
			}
		}

	}

	return apiv1.AssessmentResultUnknown, "", nil

}

// CheckForDifferences() tests for essential equality, with any fields that Numaplane manipulates eliminated from the comparison
// This implements a function of the progressiveController interface, used to determine if a previously Upgrading Pipeline
// should be replaced with a new one.
// What should a user be able to update to cause this?: Ideally, they should be able to change any field if they need to and not just those that are
// configured as "progressive", in the off chance that changing one of those fixes a problem.
// However, we need to exclude any field that Numaplane itself changes or it will confuse things
func (r *PipelineRolloutReconciler) CheckForDifferences(ctx context.Context, from, to *unstructured.Unstructured) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	fromCopy := from.DeepCopy()
	toCopy := to.DeepCopy()

	err := numaflowtypes.PipelineWithoutScaleMinMax(fromCopy)
	if err != nil {
		return false, err
	}
	err = numaflowtypes.PipelineWithoutScaleMinMax(toCopy)
	if err != nil {
		return false, err
	}

	specsEqual := util.CompareStructNumTypeAgnostic(fromCopy.Object["spec"], toCopy.Object["spec"])
	numaLogger.Debugf("specsEqual: %t, metadataRisk=%t, from=%v, to=%v\n",
		specsEqual, fromCopy.Object["spec"], toCopy.Object["spec"])
	metadataRisk := usde.ResourceMetadataHasDataLossRisk(ctx, from, to)

	return !specsEqual || metadataRisk, nil
}

/*
ProcessPromotedChildPreUpgrade handles the pre-upgrade processing of a promoted pipeline.
It performs the following pre-upgrade operations:
- it calculates how to scale down the promoted pipeline vertices before proceeding with a progressive upgrade.

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

	requeue, err := computePipelineVerticesScaleValues(ctx, pipelineRO.Status.ProgressiveStatus.PromotedPipelineStatus, promotedPipelineDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debugf("completed pre-upgrade processing of promoted pipeline, requeue=%t", requeue)

	return requeue, nil
}

func (r *PipelineRolloutReconciler) ProcessPromotedChildPostUpgrade(
	ctx context.Context,
	pipelineRollout progressive.ProgressiveRolloutObject,
	promotedPipelineDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessPromotedChildPostUpgrade").WithName("PipelineRollout").
		WithValues("promotedPipelineNamespace", promotedPipelineDef.GetNamespace(), "promotedPipelineName", promotedPipelineDef.GetName())

	numaLogger.Debug("started post-upgrade processing of promoted pipeline")
	pipelineRO, ok := pipelineRollout.(*apiv1.PipelineRollout)
	if !ok {
		return true, fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process promoted pipeline post-upgrade", pipelineRollout)
	}

	if pipelineRO.Status.ProgressiveStatus.PromotedPipelineStatus == nil {
		return true, errors.New("unable to perform post-upgrade operations because the rollout does not have promotedChildStatus set")
	}

	// Create an array of VertexScaleDefinition objects to use with applyScaleValuesToLivePipeline
	vertexScaleDefinitions := []apiv1.VertexScaleDefinition{}
	for vertexName, vertexScaleValues := range pipelineRO.Status.ProgressiveStatus.PromotedPipelineStatus.ScaleValues {
		vertexScaleDefinitions = append(vertexScaleDefinitions, apiv1.VertexScaleDefinition{
			VertexName: vertexName,
			ScaleDefinition: &apiv1.ScaleDefinition{
				Min: &vertexScaleValues.ScaleTo,
				Max: &vertexScaleValues.ScaleTo,
			},
		})
	}

	if err := applyScaleValuesToLivePipeline(ctx, promotedPipelineDef, vertexScaleDefinitions, c); err != nil {
		return true, fmt.Errorf("error scaling down the existing promoted pipeline: %w", err)
	}

	numaLogger.WithValues("vertexScaleDefinitions", vertexScaleDefinitions).Debug("updated the promoted pipeline with the new scale configuration")

	numaLogger.Debug("completed post-upgrade processing of promoted pipeline")

	return false, nil
}

/*
ProcessPromotedChildPostFailure handles the post-upgrade processing of the promoted pipeline after the "upgrading" pipeline has failed.
It performs the following post-upgrade operations:
- it restores the promoted pipeline vertices' scale values to the original values retrieved from the rollout status.

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

	if err := scalePromotedPipelineToOriginalScale(ctx, pipelineRO.Status.ProgressiveStatus.PromotedPipelineStatus, promotedPipelineDef, c); err != nil {
		return true, err
	}

	numaLogger.Debug("completed post-failure processing of promoted pipeline")

	return false, nil
}

// ProcessPromotedChildPreRecycle processes the Promoted child directly prior to it being recycled
// (due to being replaced by a new Promoted child)
func (r *PipelineRolloutReconciler) ProcessPromotedChildPreRecycle(
	ctx context.Context,
	pipelineRollout progressive.ProgressiveRolloutObject,
	promotedPipelineDef *unstructured.Unstructured,
	c client.Client,
) error {

	numaLogger := logger.FromContext(ctx).WithName("ProcessPromotedChildPostFailure").WithName("PipelineRollout").
		WithValues("promotedPipelineNamespace", promotedPipelineDef.GetNamespace(), "promotedPipelineName", promotedPipelineDef.GetName())

	numaLogger.Debug("started pre-recycle processing of promoted pipeline")

	pipelineRO, ok := pipelineRollout.(*apiv1.PipelineRollout)
	if !ok {
		return fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process promoted pipeline post-upgrade", pipelineRollout)
	}

	// Prior to draining the Pipeline, which we do during recycling, we need to make sure we scale it back up to original scale
	// Original scale can help us to drain it faster.
	// Moreover, if a Vertex was scaled to 0 pods, we need to scale it back up in order for it to drain at all

	if pipelineRO.Status.ProgressiveStatus.PromotedPipelineStatus == nil {
		numaLogger.Error(errors.New("rollout does not have promotedChildStatus set"), "Can't scale Promoted Pipeline back to original scale prior to draining")
	} else {
		if err := scalePromotedPipelineToOriginalScale(ctx, pipelineRO.Status.ProgressiveStatus.PromotedPipelineStatus, promotedPipelineDef, c); err != nil {
			numaLogger.Error(err, "Can't scale Promoted Pipeline back to original scale prior to draining")
		}
	}

	numaLogger.Debug("completed pre-recycle processing of promoted pipeline")
	return nil
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

	return applyScaleValuesToLivePipeline(ctx, upgradingPipelineDef, upgradingPipelineStatus.OriginalScaleMinMax, c)
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
	scaleDefinitions, err := getScaleValuesFromPipelineSpec(ctx, upgradingPipelineDef)
	if err != nil {
		return true, err
	}

	pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus.OriginalScaleMinMax = scaleDefinitions

	err = createScaledDownUpgradingPipelineDef(ctx, pipelineRollout, upgradingPipelineDef)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed pre-upgrade processing of upgrading pipeline")

	return false, nil
}

// createScaledDownUpgradingPipelineDef sets the upgrading Pipeline's vertex scale definition to the number of Pods
// that were removed from the same Vertex of the promoted Pipeline (thus the overall number stays the same)
func createScaledDownUpgradingPipelineDef(
	ctx context.Context,
	pipelineRollout *apiv1.PipelineRollout,
	upgradingPipelineDef *unstructured.Unstructured,
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

	// map each vertex name to new min/max, which is based on the number of Pods that were removed from the corresponding
	// Vertex on the "promomoted" Pipeline, assuming it exists there
	vertexScaleDefinitions := make([]apiv1.VertexScaleDefinition, len(vertexDefinitions))
	for index, vertex := range vertexDefinitions {
		vertexAsMap := vertex.(map[string]interface{})
		vertexName := vertexAsMap["name"].(string)
		originalScaleMinMax, err := progressive.ExtractScaleMinMax(vertexAsMap, []string{"scale"})
		if err != nil {
			return fmt.Errorf("cannot extract the scale min and max values from the upgrading pipeline vertex %s: %w", vertexName, err)
		}
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
			// if for some reason there were no Pods running in the promoted Pipeline's vertex at the time (i.e. maybe some failure) and the Max was not set to 0 explicitly,
			// then we don't want to set our Pods to 0 so set to 1 at least
			upgradingVertexScaleTo = scaleValue.Initial - scaleValue.ScaleTo
			maxZero := originalScaleMinMax != nil && originalScaleMinMax.Max != nil && *originalScaleMinMax.Max == 0
			if upgradingVertexScaleTo <= 0 && !maxZero {
				upgradingVertexScaleTo = 1
			}
			numaLogger.WithValues("vertex", vertexName).Debugf("scaling upgrading pipeline vertex to min=max=%d", upgradingVertexScaleTo)
		}

		vertexScaleDefinitions[index] = apiv1.VertexScaleDefinition{
			VertexName: vertexName,
			ScaleDefinition: &apiv1.ScaleDefinition{
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
		scaleDef := vertexScaleDef.ScaleDefinition
		scaledDown := scaleDef != nil && scaleDef.Min != nil && *scaleDef.Min == 0 && scaleDef.Max != nil && *scaleDef.Max == 0

		if !scaledDown {
			allVerticesScaledDown = false
		}
	}
	if !allVerticesScaledDown {
		zero := int64(0)
		for i := range vertexScaleDefinitions {
			vertexScaleDefinitions[i].ScaleDefinition = &apiv1.ScaleDefinition{Min: &zero, Max: &zero}
		}

		numaLogger.Debug("Scaling down all vertices to 0 Pods")
		if err := applyScaleValuesToLivePipeline(ctx, pipelineDef, vertexScaleDefinitions, c); err != nil {
			return fmt.Errorf("error scaling down the pipeline: %w", err)
		}
	}
	return nil
}

/*
ProcessUpgradingChildPostUpgrade handles the processing of an upgrading pipeline definition after it's been created

Parameters:
  - ctx: the context for managing request-scoped values.
  - rolloutObject: the PipelineRollout instance
  - upgradingPipelineDef: the definition of the upgrading pipeline as an unstructured object.
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

// ProcessUpgradingChildPreRecycle processes the Upgrading child directly prior to it being recycled
// (due to being replaced by a new Upgrading child)
func (r *PipelineRolloutReconciler) ProcessUpgradingChildPreRecycle(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingPipelineDef *unstructured.Unstructured,
	c client.Client,
) error {
	numaLogger := logger.FromContext(ctx)

	numaLogger.Debug("started pre-recycle processing of upgrading pipeline")
	pipelineRollout, ok := rolloutObject.(*apiv1.PipelineRollout)
	if !ok {
		return fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process upgrading pipeline pre-recycle", rolloutObject)
	}

	// For each Pipeline vertex, patch to the original scale definition
	// This enables the Pipeline to drain faster than it otherwise would. (Note that draining will immediately take the Source down to 0)
	upgradingPipelineStatus := pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus
	if upgradingPipelineStatus == nil {
		return fmt.Errorf("can't process upgrading pipeline post-success; missing UpgradingPipelineStatus which should contain scale values")
	}

	err := applyScaleValuesToLivePipeline(ctx, upgradingPipelineDef, upgradingPipelineStatus.OriginalScaleMinMax, c)
	if err != nil {
		numaLogger.Error(err, "failed to perform pre-recycle processing of upgrading pipeline")
	}
	numaLogger.Debug("completed pre-recycle processing of upgrading pipeline")
	return nil
}

/*
computePipelineVerticesScaleValues creates the apiv1.ScaleValues to be stored in the PipelineRollout
for all vertices before performing the actually scaling down of the promoted pipeline.
It checks if the ScaleValues have been already stored and skips the operation if true.

Parameters:
- ctx: the context for managing request-scoped values.
- promotedPipelineStatus: the status of the promoted pipeline in the rollout.
- promotedPipelineDef: the unstructured object representing the promoted pipeline definition.
- c: the Kubernetes client for resource operations.

Returns:
- bool: true if should requeue, false otherwise. Should requeue in case of error or or to store the computed ScaleValues.
- error: an error if any operation fails during the scaling process.
*/
func computePipelineVerticesScaleValues(
	ctx context.Context,
	promotedPipelineStatus *apiv1.PromotedPipelineStatus,
	promotedPipelineDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("computePipelineVerticesScaleValues").
		WithValues("promotedPipelineNamespace", promotedPipelineDef.GetNamespace(), "promotedPipelineName", promotedPipelineDef.GetName())

	vertices, _, err := unstructured.NestedSlice(promotedPipelineDef.Object, "spec", "vertices")
	if err != nil {
		return true, fmt.Errorf("error while getting vertices of promoted pipeline: %w", err)
	}

	numaLogger.WithValues("promotedChildName", promotedPipelineDef.GetName(), "vertices", vertices).Debugf("found vertices for the promoted pipeline: %d", len(vertices))

	scaleValuesMap := map[string]apiv1.ScaleValues{}
	if promotedPipelineStatus.ScaleValues != nil {
		return false, nil
	}

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

			originalScaleMinMax, err := progressive.ExtractScaleMinMaxAsJSONString(vertexAsMap, []string{"scale"})
			if err != nil {
				return true, fmt.Errorf("cannot extract the scale min and max values from the promoted pipeline vertex %s: %w", vertexName, err)
			}

			scaleTo := progressive.CalculateScaleMinMaxValues(int(currentPodsCount))
			newMin := scaleTo
			newMax := scaleTo

			numaLogger.WithValues(
				"promotedChildName", promotedPipelineDef.GetName(),
				"vertexName", vertexName,
				"newMin", newMin,
				"newMax", newMax,
				"originalScaleMinMax", originalScaleMinMax,
			).Debugf("found %d pod(s) for the vertex, scaling down to %d", currentPodsCount, newMax)

			scaleValuesMap[vertexName] = apiv1.ScaleValues{
				OriginalScaleMinMax: originalScaleMinMax,
				ScaleTo:             scaleTo,
				Initial:             currentPodsCount,
			}
		}
	}

	promotedPipelineStatus.ScaleValues = scaleValuesMap

	// Set ScaleValuesRestoredToOriginal to false in case previously set to true and now scaling back down to recover from a previous failure
	promotedPipelineStatus.ScaleValuesRestoredToOriginal = false

	// Requeue if it is the first time that ScaleValues is set for the vertices so that the reconciliation process will store these
	// values in the rollout status in case of failure with the rest of the progressive operations.
	// This will ensure to always calculate the scaleTo values based on the correct number of pods before actually scaling down.
	return true, nil
}

/*
scalePromotedPipelineToOriginalScale scales the vertices of a pipeline to their original values based on the rollout status.
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
func scalePromotedPipelineToOriginalScale(
	ctx context.Context,
	promotedPipelineStatus *apiv1.PromotedPipelineStatus,
	promotedPipelineDef *unstructured.Unstructured,
	c client.Client,
) error {

	numaLogger := logger.FromContext(ctx).WithName("scalePipelineVerticesToOriginalValues").
		WithValues("promotedPipelineNamespace", promotedPipelineDef.GetNamespace(), "promotedPipelineName", promotedPipelineDef.GetName())

	// If all the pipeline vertices have been scaled back to the original values already, do not restore scaling values again
	if promotedPipelineStatus.AreScaleValuesRestoredToOriginal(promotedPipelineDef.GetName()) {
		return nil
	}

	if promotedPipelineStatus.ScaleValues == nil {
		return errors.New("unable to restore scale values for the promoted pipeline vertices because the rollout does not have promotedChildStatus set")
	}

	vertices, _, err := unstructured.NestedSlice(promotedPipelineDef.Object, "spec", "vertices")
	if err != nil {
		return fmt.Errorf("error while getting vertices of promoted pipeline: %w", err)
	}
	vertexScaleDefinitions := make([]apiv1.VertexScaleDefinition, len(vertices))

	for vertexIndex, vertex := range vertices {
		if vertexAsMap, ok := vertex.(map[string]any); ok {

			vertexName, found, err := unstructured.NestedString(vertexAsMap, "name")
			if err != nil {
				return err
			}
			if !found {
				return errors.New("a vertex must have a name")
			}

			vertexScaleValues, exists := promotedPipelineStatus.ScaleValues[vertexName]
			if !exists {
				return fmt.Errorf("the scale values for vertex '%s' are not present in the rollout promotedChildStatus", vertexName)
			}

			if vertexScaleValues.OriginalScaleMinMax == "null" {
				vertexScaleDefinitions[vertexIndex] = apiv1.VertexScaleDefinition{
					VertexName: vertexName,
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: nil,
						Max: nil,
					},
				}
			} else {
				scaleAsMap := map[string]any{}
				err = json.Unmarshal([]byte(vertexScaleValues.OriginalScaleMinMax), &scaleAsMap)
				if err != nil {
					return fmt.Errorf("failed to unmarshal OriginalScaleMinMax: %w", err)
				}

				var min, max *int64
				if scaleAsMap["min"] != nil {
					minInt := int64(scaleAsMap["min"].(float64))
					min = &minInt
				}
				if scaleAsMap["max"] != nil {
					maxInt := int64(scaleAsMap["max"].(float64))
					max = &maxInt
				}

				vertexScaleDefinitions[vertexIndex] = apiv1.VertexScaleDefinition{
					VertexName: vertexName,
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: min,
						Max: max,
					},
				}
			}
		}
	}

	if err := applyScaleValuesToLivePipeline(ctx, promotedPipelineDef, vertexScaleDefinitions, c); err != nil {
		return fmt.Errorf("error scaling the existing promoted pipeline vertices to original values: %w", err)
	}

	numaLogger.WithValues("promotedPipelineDef", promotedPipelineDef).Debug("patched the promoted pipeline vertices with the original scale configuration")

	promotedPipelineStatus.ScaleValuesRestoredToOriginal = true
	promotedPipelineStatus.ScaleValues = nil

	return nil
}

func applyScaleValuesToPipelineDefinition(
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
func applyScaleValuesToLivePipeline(
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

// for each Vertex, get the definition of the Scale
// return map of Vertex name to scale definition
func getScaleValuesFromPipelineSpec(ctx context.Context, pipelineDef *unstructured.Unstructured) (
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

			vertexScaleDef, err := progressive.ExtractScaleMinMax(vertexAsMap, []string{"scale"})
			if err != nil {
				return nil, err
			}
			scaleDefinitions = append(scaleDefinitions, apiv1.VertexScaleDefinition{VertexName: vertexName, ScaleDefinition: vertexScaleDef})
		}
	}
	return scaleDefinitions, nil
}
