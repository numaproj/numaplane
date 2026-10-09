package isbservicerollout

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
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

// AssessUpgradingChild makes an assessment of the upgrading child to determine if it was successful, failed, or still not known
// This implements a function of the progressiveController interface
func (r *ISBServiceRolloutReconciler) AssessUpgradingChild(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	existingUpgradingChildDef *unstructured.Unstructured,
	assessmentSchedule config.AssessmentSchedule) (apiv1.AssessmentResult, string, error) {

	isbServiceRollout := rolloutObject.(*apiv1.ISBServiceRollout)

	numaLogger := logger.FromContext(ctx).WithValues("isbservice", existingUpgradingChildDef.GetName())
	ctx = logger.WithLogger(ctx, numaLogger)

	childStatus := isbServiceRollout.GetUpgradingChildStatus()
	if childStatus == nil {
		if err := isbServiceRollout.ResetUpgradingChildStatus(existingUpgradingChildDef); err != nil {
			return apiv1.AssessmentResultUnknown, "", err
		}
		childStatus = isbServiceRollout.GetUpgradingChildStatus()
	}
	currentTime := time.Now()

	// A failed Pipeline is enough to fail the InterstepBufferService upgrade, even while its own health window is still open.
	pipelineResult, failedPipelines, err := r.assessPipelines(ctx, existingUpgradingChildDef)
	if err != nil {
		return apiv1.AssessmentResultUnknown, "", err
	}
	if pipelineResult == apiv1.AssessmentResultFailure {
		if err := recordUpgradingChildSnapshot(childStatus, existingUpgradingChildDef, pipelineFailureReasons(failedPipelines)); err != nil {
			return apiv1.AssessmentResultFailure, "", err
		}
		childStatus.AssessmentResult = apiv1.AssessmentResultFailure
		return apiv1.AssessmentResultFailure, "", nil
	}

	// The InterstepBufferService itself must stay healthy for the consecutive-success period before Pipelines are allowed to promote it.
	// Once that basic check has succeeded it is not repeated; Pipeline assessment is the remaining gate.
	if !childStatus.IsBasicAssessmentResultSet() {
		result, message, err := assessISBServiceHealthWindow(ctx, childStatus, existingUpgradingChildDef, assessmentSchedule, currentTime)
		if err != nil || result != apiv1.AssessmentResultSuccess {
			return result, message, err
		}
	} else if childStatus.BasicAssessmentResult != apiv1.AssessmentResultSuccess {
		return childStatus.BasicAssessmentResult, "Basic Resource Health Check failed", nil
	}

	if pipelineResult == apiv1.AssessmentResultSuccess {
		childStatus.ChildStatus.Raw = nil
		childStatus.FailureReasons = nil
		return apiv1.AssessmentResultSuccess, "", nil
	}
	return apiv1.AssessmentResultUnknown, "", nil
}

// assessISBServiceHealthWindow applies the rolling-window health check to the upgrading InterstepBufferService.
// Any observation that is not Success restarts the consecutive-success period. The upgrade fails when
// assessmentSchedule.End passes without that period being completed, including when End is 0.
// Period 0 means the first healthy observation is enough.
func assessISBServiceHealthWindow(
	ctx context.Context,
	childStatus *apiv1.UpgradingChildStatus,
	isbsvc *unstructured.Unstructured,
	assessmentSchedule config.AssessmentSchedule,
	currentTime time.Time,
) (apiv1.AssessmentResult, string, error) {
	numaLogger := logger.FromContext(ctx)

	if childStatus.BasicAssessmentStartTime != nil &&
		currentTime.Sub(childStatus.BasicAssessmentStartTime.Time) > assessmentSchedule.End {
		numaLogger.Debugf("Assessment window ended for upgrading child %s", isbsvc.GetName())
		if len(childStatus.FailureReasons) == 0 {
			childStatus.FailureReasons = []string{"Basic Resource Health Check failed"}
		}
		if err := recordUpgradingChildSnapshot(childStatus, isbsvc, childStatus.FailureReasons); err != nil {
			return apiv1.AssessmentResultUnknown, "", err
		}
		childStatus.AssessmentResult = apiv1.AssessmentResultFailure
		childStatus.BasicAssessmentEndTime = &metav1.Time{Time: currentTime}
		childStatus.BasicAssessmentResult = apiv1.AssessmentResultFailure
		return apiv1.AssessmentResultFailure, "Basic Resource Health Check failed", nil
	}

	assessment, failureReasons, err := assessISBServiceHealth(isbsvc)
	if err != nil {
		return apiv1.AssessmentResultUnknown, "", err
	}

	// One failure is ok: drop the consecutive-success window and check again later.
	if assessment == apiv1.AssessmentResultFailure {
		numaLogger.Debugf("Assessment failed for upgrading child %s, checking again...", isbsvc.GetName())
		if err := recordUpgradingChildSnapshot(childStatus, isbsvc, failureReasons); err != nil {
			return apiv1.AssessmentResultUnknown, "", err
		}
		childStatus.TrialWindowStartTime = nil
		childStatus.AssessmentResult = apiv1.AssessmentResultUnknown
		return apiv1.AssessmentResultUnknown, "", nil
	}

	// Pending, Progressing, and not-yet-reconciled are not Success. They must not count toward the consecutive period.
	if assessment != apiv1.AssessmentResultSuccess {
		childStatus.TrialWindowStartTime = nil
		return apiv1.AssessmentResultUnknown, "", nil
	}

	if !childStatus.IsTrialWindowStartTimeSet() {
		childStatus.TrialWindowStartTime = &metav1.Time{Time: currentTime}
		childStatus.AssessmentResult = apiv1.AssessmentResultUnknown
		numaLogger.Debugf("Assessment succeeded for upgrading child %s, setting TrialWindowStartTime to %s", isbsvc.GetName(), currentTime)
	}
	if childStatus.IsTrialWindowStartTimeSet() && currentTime.Sub(childStatus.TrialWindowStartTime.Time) >= assessmentSchedule.Period {
		childStatus.BasicAssessmentEndTime = &metav1.Time{Time: currentTime}
		childStatus.BasicAssessmentResult = apiv1.AssessmentResultSuccess
		childStatus.ChildStatus.Raw = nil
		childStatus.FailureReasons = nil
		return apiv1.AssessmentResultSuccess, "", nil
	}

	numaLogger.Debugf("Assessment succeeded for upgrading child %s, but success window has not passed yet", isbsvc.GetName())
	return apiv1.AssessmentResultUnknown, "", nil
}

// assessISBServiceHealth assesses the InterstepBufferService resource on its own.
// Success: phase is Running, it has been reconciled at its current generation, and every condition is True.
// Failure: phase is Failed or Deleting, or a condition is False for a reason other than still progressing.
// Unknown: otherwise (Pending, not yet reconciled, or still progressing).
func assessISBServiceHealth(isbsvc *unstructured.Unstructured) (apiv1.AssessmentResult, []string, error) {
	statusRaw, found, err := unstructured.NestedFieldNoCopy(isbsvc.Object, "status")
	if err != nil {
		return apiv1.AssessmentResultUnknown, nil, fmt.Errorf("failed to parse Status from InterstepBufferService %s/%s: %w", isbsvc.GetNamespace(), isbsvc.GetName(), err)
	}
	if !found || statusRaw == nil {
		return apiv1.AssessmentResultUnknown, nil, nil
	}
	var status numaflowv1.InterStepBufferServiceStatus
	if err := util.StructToStruct(statusRaw, &status); err != nil {
		return apiv1.AssessmentResultUnknown, nil, fmt.Errorf("failed to convert InterstepBufferService Status for %s/%s: %w", isbsvc.GetNamespace(), isbsvc.GetName(), err)
	}

	if status.Phase == numaflowv1.ISBSvcPhaseFailed || status.Phase == numaflowv1.ISBSvcPhaseDeleting {
		reason := fmt.Sprintf("InterstepBufferService phase is %s", status.Phase)
		if status.Message != "" {
			reason = fmt.Sprintf("%s: %s", reason, status.Message)
		}
		return apiv1.AssessmentResultFailure, []string{reason}, nil
	}

	var failureReasons []string
	for _, cond := range status.Conditions {
		if cond.Status == metav1.ConditionFalse && cond.Reason != string(apiv1.ProgressingReasonString) {
			failureReasons = append(failureReasons, fmt.Sprintf("condition %s is False for Reason %q: %s", cond.Type, cond.Reason, cond.Message))
		}
	}
	if len(failureReasons) > 0 {
		return apiv1.AssessmentResultFailure, failureReasons, nil
	}

	reconciled := isbsvc.GetGeneration() <= status.ObservedGeneration
	if reconciled && status.Phase == numaflowv1.ISBSvcPhaseRunning && status.IsReady() {
		return apiv1.AssessmentResultSuccess, nil, nil
	}
	return apiv1.AssessmentResultUnknown, nil, nil
}

func pipelineFailureReasons(failedPipelines []string) []string {
	reasons := make([]string, 0, len(failedPipelines))
	for _, failedPipeline := range failedPipelines {
		reasons = append(reasons, fmt.Sprintf("Pipeline %s failed", failedPipeline))
	}
	return reasons
}

func recordUpgradingChildSnapshot(childStatus *apiv1.UpgradingChildStatus, isbsvc *unstructured.Unstructured, reasons []string) error {
	rawStatus, err := json.Marshal(isbsvc.Object["status"])
	if err != nil {
		return err
	}
	childStatus.FailureReasons = reasons
	childStatus.ChildStatus.Raw = rawStatus
	return nil
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
		numaLogger.Warn("Found no PipelineRollouts using ISBServiceRollout: so isbsvc is deemed Successful") // not typical but could happen
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
		switch pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus.AssessmentResult {
		case apiv1.AssessmentResultFailure:
			numaLogger.WithValues("pipeline", upgradingPipelineStatus.Name).Debug("assessing ISBService; pipeline is failed")
			failedPipelines = append(failedPipelines, upgradingPipelineStatus.Name)
		case apiv1.AssessmentResultUnknown:
			numaLogger.WithValues("pipeline", upgradingPipelineStatus.Name).Debug("assessing ISBService; pipeline assessment is unknown")
		case apiv1.AssessmentResultSuccess:
			numaLogger.WithValues("pipeline", upgradingPipelineStatus.Name).Debug("assessing ISBService; pipeline succeeded")
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
