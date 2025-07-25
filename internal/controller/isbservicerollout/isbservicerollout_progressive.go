package isbservicerollout

import (
	"context"
	"fmt"
	"time"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/usde"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CreateUpgradingChildDefinition creates an InterstepBufferService in an "upgrading" state with the given name
// This implements a function of the progressiveController interface
func (r *ISBServiceRolloutReconciler) CreateUpgradingChildDefinition(ctx context.Context, rolloutObject progressive.ProgressiveRolloutObject, name string) (*unstructured.Unstructured, error) {
	isbsvcRollout := rolloutObject.(*apiv1.ISBServiceRollout)
	metadata, err := getBaseISBSVCMetadata(isbsvcRollout)
	if err != nil {
		return nil, err
	}
	isbsvc, err := r.makeISBServiceDefinition(isbsvcRollout, name, metadata)
	if err != nil {
		return nil, err
	}

	labels := isbsvc.GetLabels()
	labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeInProgress)
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

	// TODO: For now, just assessing the health of the underlying Pipelines; need to also assess the health of the isbsvc itself
	// Note: until we have health check for isbsvc, we don't need to worry about resource health check start time or end time
	// If Pipelines are healthy or Pipelines are failed, that's good enough

	assessmentResult, failedPipeline, err := r.assessPipelines(ctx, existingUpgradingChildDef)
	if err != nil {
		return assessmentResult, "", err
	}
	// just set BasicAssessmentEndTime to now
	if assessmentResult != apiv1.AssessmentResultUnknown {
		_ = progressive.UpdateUpgradingChildStatus(isbServiceRollout, func(status *apiv1.UpgradingChildStatus) {
			assessmentEndTime := metav1.NewTime(time.Now())
			status.BasicAssessmentEndTime = &assessmentEndTime
		})
	}
	if assessmentResult == apiv1.AssessmentResultFailure {
		return assessmentResult, fmt.Sprintf("Pipeline %s failed", failedPipeline), nil
	}
	return assessmentResult, "", nil
}

// Assess the Pipelines of the upgrading ISBService
// return AssessmentResult and if it failed, the name of the pipeline that failed
func (r *ISBServiceRolloutReconciler) assessPipelines(
	ctx context.Context,
	existingUpgradingChildDef *unstructured.Unstructured,
) (apiv1.AssessmentResult, string, error) {
	numaLogger := logger.FromContext(ctx)

	// What is the name of the ISBServiceRollout?
	isbsvcRolloutName, found := existingUpgradingChildDef.GetLabels()[common.LabelKeyParentRollout]
	if !found {
		return apiv1.AssessmentResultUnknown, "", fmt.Errorf("there is no Label named %q for isbsvc %s/%s; can't make assessment for progressive",
			common.LabelKeyParentRollout, existingUpgradingChildDef.GetNamespace(), existingUpgradingChildDef.GetName())
	}
	// get all PipelineRollouts using this ISBServiceRollout
	pipelineRollouts, err := r.getPipelineRolloutList(ctx, existingUpgradingChildDef.GetNamespace(), isbsvcRolloutName)
	if err != nil {
		return apiv1.AssessmentResultUnknown, "", fmt.Errorf("error getting PipelineRollouts: %s", err.Error())
	}
	if len(pipelineRollouts) == 0 {
		numaLogger.Warn("Found no PipelineRollouts using ISBServiceRollout: so isbsvc is deemed Successful") // not typical but could happen
		return apiv1.AssessmentResultSuccess, "", nil
	}

	// for each PipelineRollout, we need to check that its current Upgrading Status is for a Pipeline which is in fact using this isbsvc
	// otherwise, it may not have yet started the upgrade process for this isbsvc
	for _, pipelineRollout := range pipelineRollouts {
		upgradingPipelineStatus := pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus
		if upgradingPipelineStatus == nil || upgradingPipelineStatus.InterStepBufferServiceName != existingUpgradingChildDef.GetName() {
			numaLogger.WithValues("pipelinerollout", pipelineRollout.GetName()).Debug("can't assess ISBService; pipeline is not yet upgrading with this ISBService")
			return apiv1.AssessmentResultUnknown, "", nil
		}
		switch pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus.AssessmentResult {
		case apiv1.AssessmentResultFailure:
			numaLogger.WithValues("pipeline", upgradingPipelineStatus.Name).Debug("pipeline is failed")
			return apiv1.AssessmentResultFailure, upgradingPipelineStatus.Name, nil
		case apiv1.AssessmentResultUnknown:
			numaLogger.WithValues("pipeline", upgradingPipelineStatus.Name).Debug("pipeline assessment is unknown")
			return apiv1.AssessmentResultUnknown, "", nil
		case apiv1.AssessmentResultSuccess:
			numaLogger.WithValues("pipeline", upgradingPipelineStatus.Name).Debug("pipeline succeeded")

		}
	}

	return apiv1.AssessmentResultSuccess, "", nil
}

// CheckForDifferences() tests for essential equality.
// This implements a function of the progressiveController interface, used to determine if a previously Upgrading InterstepBufferService
// should be replaced with a new one.
// What should a user be able to update to cause this?: Ideally, they should be able to change any field if they need to and not just those that are
// configured as "progressive", in the off chance that changing one of those fixes a problem.
// However, we need to exclude any field that Numaplane or another platform changes, or it will confuse things.
func (r *ISBServiceRolloutReconciler) CheckForDifferences(ctx context.Context, from, to *unstructured.Unstructured) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	specsEqual := util.CompareStructNumTypeAgnostic(from.Object["spec"], to.Object["spec"])
	// just look specifically for metadata fields that can result in Progressive
	// anything else could be updated by some platform and not by the user, which would cause an issue
	metadataRisk := usde.ResourceMetadataHasDataLossRisk(ctx, from, to)
	numaLogger.Debugf("specsEqual: %t, metadataRisk=%t, from=%v, to=%v\n",
		specsEqual, metadataRisk, from.Object["spec"], to.Object["spec"])

	return !specsEqual || metadataRisk, nil
}

func (r *ISBServiceRolloutReconciler) ProcessPromotedChildPreUpgrade(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	return false, nil
}

func (r *ISBServiceRolloutReconciler) ProcessPromotedChildPostUpgrade(
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

// ProcessPromotedChildPreRecycle process the Promoted child directly prior to it being recycled
// (due to being replaced by a new Promoted child)
func (r *ISBServiceRolloutReconciler) ProcessPromotedChildPreRecycle(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) error {
	return nil
}

func (r *ISBServiceRolloutReconciler) ProcessUpgradingChildPostFailure(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	return false, nil
}

func (r *ISBServiceRolloutReconciler) ProcessUpgradingChildPostSuccess(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingChildDef *unstructured.Unstructured,
	c client.Client,
) error {
	return nil
}

func (r *ISBServiceRolloutReconciler) ProcessUpgradingChildPreUpgrade(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	return false, nil
}

func (r *ISBServiceRolloutReconciler) ProcessUpgradingChildPostUpgrade(
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

// ProcessUpgradingChildPreRecycle process the Upgrading child directly prior to it being recycled
// (due to being replaced by a new Upgrading child)
func (r *ISBServiceRolloutReconciler) ProcessUpgradingChildPreRecycle(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingChildDef *unstructured.Unstructured,
	c client.Client,
) error {
	return nil
}
