package pipelinerollout

import (
	"context"
	"fmt"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/util"
)

// Implemented functions for the progressiveController interface:

func (r *PipelineRolloutReconciler) CreateUpgradingChildDefinition(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, name string) (*unstructured.Unstructured, error) {
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

func (r *PipelineRolloutReconciler) getCurrentChildCount(rolloutObject ctlrcommon.RolloutObject) (int32, bool) {
	pipelineRollout := rolloutObject.(*apiv1.PipelineRollout)
	if pipelineRollout.Status.NameCount == nil {
		return int32(0), false
	} else {
		return *pipelineRollout.Status.NameCount, true
	}
}

func (r *PipelineRolloutReconciler) updateCurrentChildCount(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, nameCount int32) error {
	pipelineRollout := rolloutObject.(*apiv1.PipelineRollout)
	pipelineRollout.Status.NameCount = &nameCount
	return r.updatePipelineRolloutStatus(ctx, pipelineRollout)
}

// increment the child count for the Rollout and return the count to use
func (r *PipelineRolloutReconciler) IncrementChildCount(ctx context.Context, rolloutObject ctlrcommon.RolloutObject) (int32, error) {
	currentNameCount, found := r.getCurrentChildCount(rolloutObject)
	if !found {
		currentNameCount = int32(0)
		err := r.updateCurrentChildCount(ctx, rolloutObject, int32(0))
		if err != nil {
			return int32(0), err
		}
	}

	err := r.updateCurrentChildCount(ctx, rolloutObject, currentNameCount+1)
	if err != nil {
		return int32(0), err
	}
	return currentNameCount, nil
}

// Recycle deletes child; returns true if it was in fact deleted

func (r *PipelineRolloutReconciler) Recycle(ctx context.Context,
	pipeline *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	pipelineRollout, err := numaflowtypes.GetRolloutForPipeline(ctx, c, pipeline)
	if err != nil {
		return false, err
	}
	// if the Pipeline has been paused or if it can't be paused, then delete the pipeline
	pausedOrWontPause, err := numaflowtypes.IsPipelinePausedOrWontPause(ctx, pipeline, pipelineRollout, true)
	if err != nil {
		return false, err
	}
	if pausedOrWontPause {
		err = kubernetes.DeleteResource(ctx, c, pipeline)
		return true, err
	}
	// make sure we request Paused if we haven't yet
	desiredPhaseSetting, err := numaflowtypes.GetPipelineDesiredPhase(pipeline)
	if err != nil {
		return false, err
	}
	if desiredPhaseSetting != string(numaflowv1.PipelinePhasePaused) {
		_ = r.drain(ctx, pipeline)
		return false, nil
	}
	return false, nil

}

// ChildNeedsUpdating() tests for essential equality, with any irrelevant fields eliminated from the comparison
func (r *PipelineRolloutReconciler) ChildNeedsUpdating(ctx context.Context, from, to *unstructured.Unstructured) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	fromCopy := from.DeepCopy()
	toCopy := to.DeepCopy()
	// remove lifecycle.desiredPhase field from comparison to test for equality
	numaflowtypes.PipelineWithoutDesiredPhase(fromCopy)
	numaflowtypes.PipelineWithoutDesiredPhase(toCopy)

	specsEqual := util.CompareStructNumTypeAgnostic(fromCopy.Object["spec"], toCopy.Object["spec"])
	numaLogger.Debugf("specsEqual: %t, from=%v, to=%v\n",
		specsEqual, fromCopy.Object["spec"], toCopy.Object["spec"])
	labelsEqual := util.CompareMaps(from.GetLabels(), to.GetLabels())
	numaLogger.Debugf("labelsEqual: %t, from Labels=%v, to Labels=%v", labelsEqual, from.GetLabels(), to.GetLabels())
	annotationsEqual := util.CompareMaps(from.GetAnnotations(), to.GetAnnotations())
	numaLogger.Debugf("annotationsEqual: %t, from Annotations=%v, to Annotations=%v", annotationsEqual, from.GetAnnotations(), to.GetAnnotations())

	return !specsEqual || !labelsEqual || !annotationsEqual, nil
}

// get the isbsvc child of ISBServiceRollout with the given upgrading state label
func (r *PipelineRolloutReconciler) getISBSvc(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, upgradeState common.UpgradeState) (*unstructured.Unstructured, error) {
	isbsvcRollout, err := r.getISBSvcRollout(ctx, pipelineRollout)
	if err != nil || isbsvcRollout == nil {
		return nil, fmt.Errorf("unable to find ISBServiceRollout, err=%v", err)
	}

	isbsvc, err := progressive.FindMostCurrentChildOfUpgradeState(ctx, isbsvcRollout, upgradeState, false, r.client)
	if err != nil {
		return nil, err
	}
	return isbsvc, nil
}

// make an assessment of the upgrading child to determine if it was successful, failed, or still not known
// TODO: fix this assessment not to return an immediate result as soon as things are healthy or unhealthy
func (r *PipelineRolloutReconciler) AssessUpgradingChild(ctx context.Context, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, error) {

	numaLogger := logger.FromContext(ctx)
	upgradingObjectStatus, err := kubernetes.ParseStatus(existingUpgradingChildDef)
	if err != nil {
		return apiv1.AssessmentResultUnknown, err
	}

	numaLogger.
		WithValues("namespace", existingUpgradingChildDef.GetNamespace(), "name", existingUpgradingChildDef.GetName()).
		Debugf("Upgrading child is in phase %s", upgradingObjectStatus.Phase)

	if upgradingObjectStatus.Phase == "Running" && progressive.IsNumaflowChildReady(&upgradingObjectStatus) {
		return apiv1.AssessmentResultSuccess, nil
	}
	if upgradingObjectStatus.Phase == "Failed" {
		return apiv1.AssessmentResultFailure, nil
	}
	return apiv1.AssessmentResultUnknown, nil
}
