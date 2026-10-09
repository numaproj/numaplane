package isbservicerollout

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
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
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// CreateUpgradingChildDefinition creates an InterstepBufferService in an "upgrading" state with the given name
// This implements a function of the progressiveController interface
func (r *ISBServiceRolloutReconciler) CreateUpgradingChildDefinition(ctx context.Context, rolloutObject progressive.ProgressiveRolloutObject, name string) (*unstructured.Unstructured, error) {
	isbsvcRollout := rolloutObject.(*apiv1.ISBServiceRollout)
	metadata, err := getBaseISBSVCMetadata(isbsvcRollout)
	if err != nil {
		return nil, err
	}
	controllerInstanceID, err := ctlrcommon.GetDesiredControllerInstanceID(ctx, r.client, isbsvcRollout.Namespace)
	if err != nil {
		return nil, err
	}
	isbsvc, err := r.makeISBServiceDefinition(isbsvcRollout, name, metadata, &controllerInstanceID)
	if err != nil {
		return nil, err
	}

	labels := isbsvc.GetLabels()
	labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeTrial)
	isbsvc.SetLabels(labels)

	return isbsvc, nil
}

// AssessUpgradingChild assesses the upgrading ISBService on its own.
// This implements a function of the progressiveController interface.
// Pipelines that use this ISBService, and a trial NumaflowController, are part of AssessResourceChain.
func (r *ISBServiceRolloutReconciler) AssessUpgradingChild(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	existingUpgradingChildDef *unstructured.Unstructured,
	assessmentSchedule config.AssessmentSchedule) (apiv1.AssessmentResult, string, error) {

	isbServiceRollout := rolloutObject.(*apiv1.ISBServiceRollout)

	numaLogger := logger.FromContext(ctx).WithValues("isbservice", existingUpgradingChildDef.GetName())

	childStatus := isbServiceRollout.GetUpgradingChildStatus()
	if childStatus == nil {
		if err := isbServiceRollout.ResetUpgradingChildStatus(existingUpgradingChildDef); err != nil {
			return apiv1.AssessmentResultUnknown, "", err
		}
		childStatus = isbServiceRollout.GetUpgradingChildStatus()
	}

	assessmentResult, failureReasons, err := assessISBServiceHealth(existingUpgradingChildDef)
	if err != nil {
		return apiv1.AssessmentResultUnknown, "", err
	}
	if assessmentResult != apiv1.AssessmentResultUnknown {
		assessmentEndTime := metav1.NewTime(time.Now())
		childStatus.BasicAssessmentEndTime = &assessmentEndTime
	}
	switch assessmentResult {
	case apiv1.AssessmentResultFailure:
		isbServiceChildStatus, err := json.Marshal(existingUpgradingChildDef.Object["status"])
		if err != nil {
			return assessmentResult, "", err
		}
		childStatus.FailureReasons = failureReasons
		childStatus.ChildStatus.Raw = isbServiceChildStatus
	case apiv1.AssessmentResultSuccess:
		childStatus.ChildStatus.Raw = nil
		childStatus.FailureReasons = nil
	}
	numaLogger.WithValues("assessment", assessmentResult).Debug("assessed ISBService resource")
	return assessmentResult, strings.Join(failureReasons, "; "), nil
}

// assessISBServiceHealth assesses the ISBService resource on its own.
// Success: phase is Running and its child resources are healthy.
// Failure: phase is Failed, or child resources are unhealthy for a reason other than still progressing.
// Unknown: otherwise.
// This is not the rolling-window health check used for Pipeline and MonoVertex (see issue 494).
func assessISBServiceHealth(isbsvc *unstructured.Unstructured) (apiv1.AssessmentResult, []string, error) {
	genericStatus, err := kubernetes.ParseStatus(isbsvc)
	if err != nil {
		return apiv1.AssessmentResultUnknown, nil, fmt.Errorf("failed to parse Status from ISBService %s/%s: %w", isbsvc.GetNamespace(), isbsvc.GetName(), err)
	}
	phase := numaflowv1.ISBSvcPhase(genericStatus.Phase)
	if phase == numaflowv1.ISBSvcPhaseFailed {
		return apiv1.AssessmentResultFailure, []string{fmt.Sprintf("ISBService phase is %s", phase)}, nil
	}
	childStatus, childReason := numaflowtypes.GetISBServiceChildResourceHealth(genericStatus.Conditions)
	if childStatus == metav1.ConditionFalse && childReason != apiv1.ProgressingReasonString {
		return apiv1.AssessmentResultFailure, []string{fmt.Sprintf("ISBService child resources are unhealthy (%s)", childReason)}, nil
	}
	if phase == numaflowv1.ISBSvcPhaseRunning && childStatus == metav1.ConditionTrue {
		return apiv1.AssessmentResultSuccess, nil, nil
	}
	return apiv1.AssessmentResultUnknown, nil, nil
}

// Assess the Pipelines of the upgrading ISBService
// return AssessmentResult and if it failed, the name of the pipeline that failed
func (r *ISBServiceRolloutReconciler) assessPipelines(
	ctx context.Context,
	existingUpgradingChildDef *unstructured.Unstructured,
) (apiv1.AssessmentResult, []string, error) {
	numaLogger := logger.FromContext(ctx)

	var failedPipelines, successfulPipelines []string
	// What is the name of the ISBServiceRollout?
	isbsvcRolloutName, found := existingUpgradingChildDef.GetLabels()[common.LabelKeyParentRollout]
	if !found {
		return apiv1.AssessmentResultUnknown, failedPipelines, fmt.Errorf("there is no Label named %q for isbsvc %s/%s; can't make assessment for progressive",
			common.LabelKeyParentRollout, existingUpgradingChildDef.GetNamespace(), existingUpgradingChildDef.GetName())
	}
	// get all PipelineRollouts using this ISBServiceRollout
	pipelineRollouts, err := r.getPipelineRolloutList(ctx, existingUpgradingChildDef.GetNamespace(), isbsvcRolloutName)
	if err != nil {
		return apiv1.AssessmentResultUnknown, failedPipelines, fmt.Errorf("error getting PipelineRollouts: %s", err.Error())
	}
	if len(pipelineRollouts) == 0 {
		numaLogger.Warn("Found no PipelineRollouts using ISBServiceRollout: pipeline portion of the chain is Successful") // not typical but could happen
		return apiv1.AssessmentResultSuccess, failedPipelines, nil
	}

	// for each PipelineRollout, we need to check that its current Upgrading Status is for a Pipeline in fact using this isbsvc
	// otherwise, it may not have yet started the upgrade process for this isbsvc
	for _, pipelineRollout := range pipelineRollouts {
		upgradingPipelineStatus := pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus
		if upgradingPipelineStatus == nil || upgradingPipelineStatus.InterStepBufferServiceName != existingUpgradingChildDef.GetName() {
			// we need to make sure the PipelineRollout has updated to use an Upgrading Pipeline which is on the new isbsvc first
			numaLogger.WithValues("pipelineRollout", pipelineRollout.GetName()).Debug("can't assess ISBService; pipeline is not yet upgrading with this ISBService")
			return apiv1.AssessmentResultUnknown, []string{}, nil
		}
		switch upgradingPipelineStatus.EffectiveResourceAssessment() {
		case apiv1.AssessmentResultFailure:
			numaLogger.WithValues("pipeline", upgradingPipelineStatus.Name).Debug("assessing ISBService; pipeline resource assessment failed")
			failedPipelines = append(failedPipelines, upgradingPipelineStatus.Name)
		case apiv1.AssessmentResultUnknown:
			numaLogger.WithValues("pipeline", upgradingPipelineStatus.Name).Debug("assessing ISBService; pipeline resource assessment is unknown")
		case apiv1.AssessmentResultSuccess:
			numaLogger.WithValues("pipeline", upgradingPipelineStatus.Name).Debug("assessing ISBService; pipeline resource assessment succeeded")
			successfulPipelines = append(successfulPipelines, upgradingPipelineStatus.Name)
		}
	}
	if len(failedPipelines) > 0 {
		return apiv1.AssessmentResultFailure, failedPipelines, nil
	}
	if len(successfulPipelines) == len(pipelineRollouts) {
		return apiv1.AssessmentResultSuccess, []string{}, nil
	}

	return apiv1.AssessmentResultUnknown, []string{}, nil
}

// AssessResourceChain implements progressiveController.
// An ISBService is promoted only when it is healthy itself, every Pipeline rolling onto it has a
// successful resource assessment, and, when a trial NumaflowController exists, that controller's
// resource assessment has succeeded.
func (r *ISBServiceRolloutReconciler) AssessResourceChain(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	existingUpgradingChildDef *unstructured.Unstructured,
) (apiv1.AssessmentResult, []string, error) {
	isbServiceRollout := rolloutObject.(*apiv1.ISBServiceRollout)
	own := apiv1.AssessmentResultUnknown
	if status := isbServiceRollout.GetUpgradingChildStatus(); status != nil {
		own = status.EffectiveResourceAssessment()
	}

	pipelineResult, failedPipelines, err := r.assessPipelines(ctx, existingUpgradingChildDef)
	if err != nil {
		return apiv1.AssessmentResultUnknown, nil, err
	}
	var reasons []string
	for _, failedPipeline := range failedPipelines {
		reasons = append(reasons, fmt.Sprintf("Pipeline %s failed", failedPipeline))
	}

	results := []apiv1.AssessmentResult{own, pipelineResult}
	controllerResult, controllerPresent, controllerReason, err := progressive.TrialControllerResourceAssessment(ctx, r.client, existingUpgradingChildDef.GetNamespace())
	if err != nil {
		return apiv1.AssessmentResultUnknown, nil, err
	}
	results, reasons = progressive.AppendChainMember(results, reasons, controllerResult, controllerPresent, controllerReason)
	return progressive.CombineAssessmentResults(results), reasons, nil
}

// CheckForDifferences checks to see if the isbsvc definition matches the spec and the required metadata
func (r *ISBServiceRolloutReconciler) CheckForDifferences(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	isbsvcDef *unstructured.Unstructured,
	requiredSpec map[string]interface{},
	requiredMetadata map[string]interface{},
	existingChildUpgradeState common.UpgradeState) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	specsEqual := util.CompareStructNumTypeAgnostic(isbsvcDef.Object["spec"], requiredSpec["spec"])
	// Check required metadata (labels and annotations)
	requiredLabels, requiredAnnotations := kubernetes.ExtractMetadataSubmaps(requiredMetadata)
	actualLabels := isbsvcDef.GetLabels()
	actualAnnotations := isbsvcDef.GetAnnotations()

	labelsFound := util.IsMapSubset(requiredLabels, actualLabels)
	annotationsFound := util.IsMapSubset(requiredAnnotations, actualAnnotations)
	numaLogger.Debugf("specsEqual: %t, labelsFound=%t, annotationsFound=%v, from=%v, to=%v, requiredLabels=%v, actualLabels=%v, requiredAnnotations=%v, actualAnnotations=%v\n",
		specsEqual, labelsFound, annotationsFound, isbsvcDef.Object["spec"], requiredSpec, requiredLabels, actualLabels, requiredAnnotations, actualAnnotations)

	return !specsEqual || !labelsFound || !annotationsFound, nil
}

// CheckForDifferencesWithRolloutDef tests if there's a meaningful difference between an existing child and the child
// that would be produced by the Rollout definition.
// This implements a function of the progressiveController interface.
func (r *ISBServiceRolloutReconciler) CheckForDifferencesWithRolloutDef(
	ctx context.Context,
	existingISBSvc *unstructured.Unstructured,
	rolloutObject ctlrcommon.RolloutObject,
	existingChildUpgradeState common.UpgradeState) (bool, error) {
	isbsvcRollout := rolloutObject.(*apiv1.ISBServiceRollout)

	// Don't set the controller instance binding: the instance an ISBService is bound to isn't part of the
	// Rollout definition, and CheckForDifferences only requires the existing child to contain the Rollout's metadata.
	rolloutBasedISBSvcDef, err := r.makeISBServiceDefinition(isbsvcRollout, existingISBSvc.GetName(), isbsvcRollout.Spec.InterStepBufferService.Metadata, nil)
	if err != nil {
		return false, err
	}

	rolloutDefinedMetadata, _ := rolloutBasedISBSvcDef.Object["metadata"].(map[string]interface{})
	return r.CheckForDifferences(ctx, rolloutObject, existingISBSvc, rolloutBasedISBSvcDef.Object, rolloutDefinedMetadata, existingChildUpgradeState)
}

func (r *ISBServiceRolloutReconciler) ProcessPromotedChildPreUpgrade(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	return false, nil
}

func (r *ISBServiceRolloutReconciler) ProcessPromotedChildPostUpgradeStart(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	return false, nil
}

func (r *ISBServiceRolloutReconciler) ProcessPromotedChildPostFailure(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	return false, nil
}

func (r *ISBServiceRolloutReconciler) ProcessUpgradingChildPostFailure(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	return false, nil
}

func (r *ISBServiceRolloutReconciler) ProcessUpgradingChildPreUpgrade(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	return false, nil
}

func (r *ISBServiceRolloutReconciler) ProcessUpgradingChildPostUpgradeStart(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	// need to make sure it has a PodDisruptionBudget associated with it, and owned by it
	if err := r.applyPodDisruptionBudget(ctx, upgradingChildDef); err != nil {
		return false, fmt.Errorf("failed to apply PodDisruptionBudget for ISBService %s, err: %v", upgradingChildDef.GetName(), err)
	}
	return false, nil
}

func (r *ISBServiceRolloutReconciler) UpdateProgressiveMetrics(rolloutObject progressive.ProgressiveRolloutObject) {
	if rolloutObject.GetUpgradingChildStatus() != nil {
		childName := rolloutObject.GetUpgradingChildStatus().Name
		r.customMetrics.IncIBSServiceProgressiveStarted(rolloutObject.GetRolloutObjectMeta().GetNamespace(), rolloutObject.GetRolloutObjectMeta().GetName(), childName)
	}
}

// SkipProgressiveAssessment checks if we should skip the progressive assessment and force promote based on the definition of the ISBServiceRollout
func (r *ISBServiceRolloutReconciler) SkipProgressiveAssessment(ctx context.Context, rolloutObject progressive.ProgressiveRolloutObject) (bool, progressive.SkipProgressiveAssessmentReason, error) {

	isbsvcRollout := rolloutObject.(*apiv1.ISBServiceRollout)
	// check if ForcePromote is set true in the Progressive strategy
	if isbsvcRollout.GetProgressiveStrategy().ForcePromote {
		return true, progressive.SkipProgressiveAssessmentReasonRolloutConfiguration, nil
	}
	return false, progressive.SkipProgressiveAssessmentReasonUndefined, nil
}
