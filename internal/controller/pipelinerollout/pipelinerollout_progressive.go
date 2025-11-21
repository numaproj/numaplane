package pipelinerollout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
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
	currentTime := time.Now()

	// If a basic assessment result is not yet set, we need to perform basic assessment first
	// (if it is set, it means we already performed basic assessment and can move on to AnalysisRun if needed)
	if !childStatus.IsBasicAssessmentResultSet() {
		// Check if endTime has arrived, fail immediately
		if currentTime.Sub(childStatus.BasicAssessmentStartTime.Time) > assessmentSchedule.End {
			numaLogger.Debugf("Assessment window ended for upgrading child %s", existingUpgradingChildDef.GetName())
			_ = progressive.UpdateUpgradingChildStatus(pipelineRollout, func(status *apiv1.UpgradingChildStatus) {
				status.AssessmentResult = apiv1.AssessmentResultFailure
				status.BasicAssessmentEndTime = &metav1.Time{Time: currentTime}
				status.BasicAssessmentResult = apiv1.AssessmentResultFailure
			})
			return apiv1.AssessmentResultFailure, "Basic Resource Health Check failed", nil
		}

		// function for checking readiness of Pipeline Vertex replicas
		verifyReplicasFunc := func(existingUpgradingChildDef *unstructured.Unstructured) (bool, string, error) {
			verticesList, err := kubernetes.ListLiveResource(ctx, common.NumaflowAPIGroup, common.NumaflowAPIVersion,
				numaflowv1.VertexGroupVersionResource.Resource, existingUpgradingChildDef.GetNamespace(),
				fmt.Sprintf("%s=%s", common.LabelKeyNumaflowPipelineName, existingUpgradingChildDef.GetName()), "")
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

		// if we fail once, it's okay: we'll check again later
		if assessment == apiv1.AssessmentResultFailure {
			numaLogger.Debugf("Assessment failed for upgrading child %s, checking again...", existingUpgradingChildDef.GetName())
			_ = progressive.UpdateUpgradingChildStatus(pipelineRollout, func(status *apiv1.UpgradingChildStatus) {
				status.TrialWindowStartTime = nil
				status.AssessmentResult = apiv1.AssessmentResultUnknown
			})

			return apiv1.AssessmentResultUnknown, "", nil
		}

		// if we succeed, we must continue to succeed for a prescribed period of time to consider the resource health
		// check "successful".
		if assessment == apiv1.AssessmentResultSuccess {
			if !childStatus.IsTrialWindowStartTimeSet() {
				_ = progressive.UpdateUpgradingChildStatus(pipelineRollout, func(status *apiv1.UpgradingChildStatus) {
					status.TrialWindowStartTime = &metav1.Time{Time: currentTime}
					status.AssessmentResult = apiv1.AssessmentResultUnknown
				})
				numaLogger.Debugf("Assessment succeeded for upgrading child %s, setting TrialWindowStartTime to %s", existingUpgradingChildDef.GetName(), currentTime)
			}

			// Check if the trail window is set and if the success window has passed.
			if childStatus.IsTrialWindowStartTimeSet() && currentTime.Sub(childStatus.TrialWindowStartTime.Time) >= assessmentSchedule.Period {
				// Success window passed, launch AnalysisRun or declared success
				_ = progressive.UpdateUpgradingChildStatus(pipelineRollout, func(status *apiv1.UpgradingChildStatus) {
					status.BasicAssessmentEndTime = &metav1.Time{Time: currentTime}
					status.BasicAssessmentResult = apiv1.AssessmentResultSuccess
				})
				return r.checkAnalysisTemplates(ctx, pipelineRollout, existingUpgradingChildDef)
			}

			numaLogger.Debugf("Assessment succeeded for upgrading child %s, but success window has not passed yet", existingUpgradingChildDef.GetName())
			// Still waiting for a success window to pass
			return apiv1.AssessmentResultUnknown, "", nil
		}
	} else {
		if childStatus.BasicAssessmentResult == apiv1.AssessmentResultSuccess {
			return r.checkAnalysisTemplates(ctx, pipelineRollout, existingUpgradingChildDef)
		}
		return childStatus.BasicAssessmentResult, "Basic assessment failed", nil
	}

	return apiv1.AssessmentResultUnknown, "", nil
}

// checkAnalysisTemplates checks if there are any analysis templates to run and runs them if so.
func (r *PipelineRolloutReconciler) checkAnalysisTemplates(ctx context.Context,
	pipelineRollout *apiv1.PipelineRollout,
	existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, string, error) {

	numaLogger := logger.FromContext(ctx)
	analysis := pipelineRollout.GetAnalysis()

	// only check for and create AnalysisRun if templates are specified
	if len(analysis.Templates) > 0 {
		// this will create an AnalysisRun if it doesn't exist yet; or otherwise it will check if it's finished running
		numaLogger.Debugf("Performing analysis for upgrading child %s", existingUpgradingChildDef.GetName())
		analysisStatus, err := progressive.PerformAnalysis(ctx, existingUpgradingChildDef, pipelineRollout, pipelineRollout.GetAnalysis(), pipelineRollout.GetAnalysisStatus(), r.client)
		if err != nil {
			return apiv1.AssessmentResultUnknown, "", err
		}
		return progressive.AssessAnalysisStatus(ctx, existingUpgradingChildDef, analysisStatus)
	}
	return apiv1.AssessmentResultSuccess, "", nil
}

// CheckForDifferences checks to see if the pipeline definition matches the spec and the required metadata
func (r *PipelineRolloutReconciler) CheckForDifferences(ctx context.Context, pipelineDef *unstructured.Unstructured, requiredSpec map[string]interface{}, requiredMetadata map[string]interface{}) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	pipelineCopy := pipelineDef.DeepCopy()

	var requiredSpecCopy map[string]interface{}
	if err := util.StructToStruct(requiredSpec, &requiredSpecCopy); err != nil {
		return false, fmt.Errorf("failed to deep copy requiredSpec: %w", err)
	}

	err := numaflowtypes.PipelineWithoutScaleFields(pipelineCopy.Object)
	if err != nil {
		return false, err
	}
	err = numaflowtypes.PipelineWithoutScaleFields(requiredSpecCopy)
	if err != nil {
		return false, err
	}

	specsEqual := util.CompareStructNumTypeAgnostic(pipelineCopy.Object["spec"], requiredSpecCopy["spec"])
	// Check required metadata (labels and annotations)
	requiredLabels, requiredAnnotations := kubernetes.ExtractMetadataSubmaps(requiredMetadata)
	actualLabels := pipelineDef.GetLabels()
	actualAnnotations := pipelineDef.GetAnnotations()

	labelsFound := util.IsMapSubset(requiredLabels, actualLabels)
	annotationsFound := util.IsMapSubset(requiredAnnotations, actualAnnotations)
	numaLogger.Debugf("specsEqual: %t, labelsFound=%t, annotationsFound=%v, from=%v, to=%v, requiredLabels=%v, actualLabels=%v, requiredAnnotations=%v, actualAnnotations=%v\n",
		specsEqual, labelsFound, annotationsFound, pipelineCopy.Object["spec"], requiredSpecCopy, requiredLabels, actualLabels, requiredAnnotations, actualAnnotations)

	return !specsEqual || !labelsFound || !annotationsFound, nil
}

// CheckForDifferencesWithRolloutDef tests if there's a meaningful difference between an existing child and the child
// that would be produced by the Rollout definition.
// This implements a function of the progressiveController interface
// In order to do that, it must remove from the check any fields that are manipulated by Numaplane or Numaflow
func (r *PipelineRolloutReconciler) CheckForDifferencesWithRolloutDef(ctx context.Context, existingPipeline *unstructured.Unstructured, rolloutObject ctlrcommon.RolloutObject) (bool, error) {

	pipelineRollout := rolloutObject.(*apiv1.PipelineRollout)

	isbsvcName, err := numaflowtypes.GetPipelineISBSVCName(existingPipeline)
	if err != nil {
		return false, err
	}

	// In order to effectively compare, we need to create a Pipeline Definition from the PipelineRollout which uses the same name and isbsvc name as our current Pipeline
	// (so that won't be interpreted as a difference)
	rolloutBasedPipelineDef, err := r.makePipelineDefinition(pipelineRollout, existingPipeline.GetName(), isbsvcName, pipelineRollout.Spec.Pipeline.Metadata)
	if err != nil {
		return false, err
	}
	rolloutDefinedMetadata, _ := rolloutBasedPipelineDef.Object["metadata"].(map[string]interface{})
	return r.CheckForDifferences(ctx, existingPipeline, rolloutBasedPipelineDef.Object, rolloutDefinedMetadata)
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

	requeue, err := computePromotedPipelineVerticesScaleValues(ctx, &pipelineRO.Status.ProgressiveStatus, promotedPipelineDef, c)
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
				Min:      &vertexScaleValues.ScaleTo,
				Max:      &vertexScaleValues.ScaleTo,
				Disabled: false,
			},
		})
	}

	if err := numaflowtypes.ApplyScaleValuesToLivePipeline(ctx, promotedPipelineDef, vertexScaleDefinitions, c); err != nil {
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
	err := numaflowtypes.ScalePipelineVerticesToZero(ctx, upgradingPipelineDef, c)

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

	return numaflowtypes.ApplyScaleValuesToLivePipeline(ctx, upgradingPipelineDef, upgradingPipelineStatus.OriginalScaleMinMax, c)
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
	scaleDefinitions, err := numaflowtypes.GetScaleValuesFromPipelineDefinition(ctx, upgradingPipelineDef)
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
	vertexDefinitions, err := numaflowtypes.GetPipelineVertexDefinitions(upgradingPipelineDef)
	if err != nil {
		return err
	}

	// map each vertex name to new min/max, which is based on the number of Pods that were removed from the corresponding
	// Vertex on the "promomoted" Pipeline, assuming it exists there
	vertexScaleDefinitions := make([]apiv1.VertexScaleDefinition, len(vertexDefinitions))
	for index, vertex := range vertexDefinitions {
		vertexAsMap := vertex.(map[string]interface{})
		vertexName := vertexAsMap["name"].(string)
		originalScaleMinMax, err := numaflowtypes.ExtractScaleMinMax(vertexAsMap, []string{"scale"})
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
	return numaflowtypes.ApplyScaleValuesToPipelineDefinition(ctx, upgradingPipelineDef, vertexScaleDefinitions)
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

/*
computePromotedPipelineVerticesScaleValues creates the apiv1.ScaleValues to be stored in the PipelineRollout
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
func computePromotedPipelineVerticesScaleValues(
	ctx context.Context,
	pipelineProgressiveStatus *apiv1.PipelineProgressiveStatus,
	promotedPipelineDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("computePipelineVerticesScaleValues").
		WithValues("promotedPipelineNamespace", promotedPipelineDef.GetNamespace(), "promotedPipelineName", promotedPipelineDef.GetName())

	if pipelineProgressiveStatus.PromotedPipelineStatus == nil {
		return true, errors.New("unable to perform pre-upgrade scale down because the rollout does not have promotedChildStatus set")
	}
	promotedPipelineStatus := pipelineProgressiveStatus.PromotedPipelineStatus

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
				common.LabelKeyNumaflowPipelineName, promotedPipelineDef.GetName(),
				common.LabelKeyNumaflowPipelineVertexName, vertexName,
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

	// set the HistoricalPodCount to reflect the Initial values
	// (this duplication is needed since the PromotedPipelineStatus can be cleared)
	pipelineProgressiveStatus.HistoricalPodCount = map[string]int{}
	for vertex, scaleValues := range scaleValuesMap {
		pipelineProgressiveStatus.HistoricalPodCount[vertex] = int(scaleValues.Initial)
	}

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
				disabled := false
				if scaleAsMap["disabled"] != nil && scaleAsMap["disabled"].(bool) {
					disabled = true
				}

				vertexScaleDefinitions[vertexIndex] = apiv1.VertexScaleDefinition{
					VertexName: vertexName,
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min:      min,
						Max:      max,
						Disabled: disabled,
					},
				}
			}
		}
	}

	if err := numaflowtypes.ApplyScaleValuesToLivePipeline(ctx, promotedPipelineDef, vertexScaleDefinitions, c); err != nil {
		return fmt.Errorf("error scaling the existing promoted pipeline vertices to original values: %w", err)
	}

	numaLogger.WithValues("promotedPipelineDef", promotedPipelineDef).Debug("patched the promoted pipeline vertices with the original scale configuration")

	promotedPipelineStatus.ScaleValuesRestoredToOriginal = true
	promotedPipelineStatus.ScaleValues = nil

	return nil
}
func (r *PipelineRolloutReconciler) ProgressiveUnsupported(ctx context.Context, rolloutObject progressive.ProgressiveRolloutObject) bool {
	numaLogger := logger.FromContext(ctx)

	// Temporary: we cannot support Progressive rollout assessment for HPA: See issue https://github.com/numaproj/numaplane/issues/868
	pipelineRollout := rolloutObject.(*apiv1.PipelineRollout)
	for _, rider := range pipelineRollout.Spec.Riders {

		unstruc, err := kubernetes.RawExtensionToUnstructured(rider.Definition)
		if err != nil {
			numaLogger.Errorf(err, "Failed to convert rider definition to map")
			continue
		}
		gvk := unstruc.GroupVersionKind()
		if gvk.Group == "autoscaling" && gvk.Kind == "HorizontalPodAutoscaler" {
			numaLogger.Debug("PipelineRollout %s/%s contains HPA Rider: Full Progressive Rollout is unsupported")
			return true
		}
	}

	return false
}

func (r *PipelineRolloutReconciler) UpdateProgressiveMetrics(rolloutObject progressive.ProgressiveRolloutObject, completed bool) {
	if rolloutObject.GetUpgradingChildStatus() != nil {
		childName := rolloutObject.GetUpgradingChildStatus().Name
		successStatus := metrics.EvaluateSuccessStatusForMetrics(rolloutObject.GetUpgradingChildStatus().AssessmentResult)
		forcedSuccess := rolloutObject.GetUpgradingChildStatus().ForcedSuccess
		basicAssessmentResult := metrics.EvaluateSuccessStatusForMetrics(rolloutObject.GetUpgradingChildStatus().BasicAssessmentResult)

		r.customMetrics.IncPipelineProgressiveResults(rolloutObject.GetRolloutObjectMeta().GetNamespace(), rolloutObject.GetRolloutObjectMeta().GetName(),
			childName, basicAssessmentResult, successStatus, forcedSuccess, completed)
	}
}
