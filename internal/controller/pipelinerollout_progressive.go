package controller

import (
	"context"
	"fmt"
	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func (r *PipelineRolloutReconciler) processExistingPipelineWithProgressive(
	ctx context.Context, pipelineRollout *apiv1.PipelineRollout,
	newPipelineDef *kubernetes.GenericObject, pipelineNeedsToUpdate bool,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	newUpgradingPipelineDef, err := r.makeUpgradingPipelineDefinition(ctx, pipelineRollout)
	if err != nil {
		return false, err
	}

	// Get the object to see if it exists
	_, err = kubernetes.GetCR(ctx, r.restConfig, newUpgradingPipelineDef, "pipelines")
	if err != nil {
		// create object as it doesn't exist
		if apierrors.IsNotFound(err) {

			//pipelineRollout.Status.MarkPending()

			numaLogger.Debugf("Upgrading Pipeline %s/%s doesn't exist so creating", newUpgradingPipelineDef.Namespace, newUpgradingPipelineDef.Name)
			err = kubernetes.CreateCR(ctx, r.restConfig, newUpgradingPipelineDef, "pipelines")
			if err != nil {
				return false, err
			}
			//pipelineRollout.Status.MarkDeployed(pipelineRollout.Generation)
			//r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerPipelineRollout, "create").Observe(time.Since(syncStartTime).Seconds())
			return false, nil
		}

		return false, fmt.Errorf("error getting Pipeline: %v", err)
	}

	err = r.processUpgradingPipelineStatus(ctx, pipelineRollout)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (r *PipelineRolloutReconciler) makeUpgradingPipelineDefinition(
	ctx context.Context,
	pipelineRollout *apiv1.PipelineRollout,
) (*kubernetes.GenericObject, error) {
	pipelineName, err := r.getPipelineName(ctx, pipelineRollout, string(common.LabelValueUpgradeInProgress))
	if err != nil {
		return nil, err
	}

	labels, err := pipelineLabels(pipelineRollout, string(common.LabelValueUpgradeInProgress))
	if err != nil {
		return nil, err
	}

	return r.makePipelineDefinition(pipelineRollout, pipelineName, labels)
}

func (r *PipelineRolloutReconciler) processUpgradingPipelineStatus(
	ctx context.Context,
	pipelineRollout *apiv1.PipelineRollout,
) error {
	numaLogger := logger.FromContext(ctx)

	pipelineDef, err := r.makeUpgradingPipelineDefinition(ctx, pipelineRollout)
	if err != nil {
		return err
	}

	// Get existing upgrading Pipeline
	existingUpgradingPipelineDef, err := kubernetes.GetCR(ctx, r.restConfig, pipelineDef, "pipelines")
	if err != nil {
		if apierrors.IsNotFound(err) {
			numaLogger.WithValues("pipelineDefinition", *pipelineDef).Warn("Pipeline not found. Unable to process status during this reconciliation.")
		} else {
			return fmt.Errorf("error getting Pipeline for status processing: %v", err)
		}
	}

	pipelineStatus, err := kubernetes.ParseStatus(existingUpgradingPipelineDef)
	if err != nil {
		return fmt.Errorf("failed to parse Pipeline Status from pipeline CR: %+v, %v", existingUpgradingPipelineDef, err)
	}

	pipelinePhase := numaflowv1.PipelinePhase(pipelineStatus.Phase)
	if pipelinePhase == numaflowv1.PipelinePhaseFailed {
		//	pipelineRO.status = "PROGRESSIVE failed"
	} else if pipelinePhase == numaflowv1.PipelinePhaseRunning {
		// TODO: label the new pipeline as promoted
		// pause pipeline_old
	} else {
		// TODO: ensure the latest pipeline spec is applied
		// apply pipeline_new
		//continue (re-enqueue)
	}

	return nil
}
