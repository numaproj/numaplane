package controller

import (
	"context"
	"fmt"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// processExistingPipelineWithProgressive should be only called when determined there is
// an update required and should use progressive strategy.
func (r *PipelineRolloutReconciler) processExistingPipelineWithProgressive(
	ctx context.Context, pipelineRollout *apiv1.PipelineRollout,
	existingPipelineDef *kubernetes.GenericObject,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	newUpgradingPipelineDef, err := r.makeUpgradingPipelineDefinition(ctx, pipelineRollout)
	if err != nil {
		return false, err
	}

	// Get the object to see if it exists
	_, err = kubernetes.GetLiveResource(ctx, r.restConfig, newUpgradingPipelineDef, "pipelines")
	if err != nil {
		// create object as it doesn't exist
		if apierrors.IsNotFound(err) {

			numaLogger.Debugf("Upgrading Pipeline %s/%s doesn't exist so creating", newUpgradingPipelineDef.Namespace, newUpgradingPipelineDef.Name)
			err = kubernetes.CreateCR(ctx, r.restConfig, newUpgradingPipelineDef, "pipelines")
			if err != nil {
				return false, err
			}
		} else {
			return false, fmt.Errorf("error getting Pipeline: %v", err)
		}
	}

	done, err := r.processUpgradingPipelineStatus(ctx, pipelineRollout, existingPipelineDef)
	if err != nil {
		return false, err
	}
	return done, nil
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
	existingPipelineDef *kubernetes.GenericObject,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	pipelineDef, err := r.makeUpgradingPipelineDefinition(ctx, pipelineRollout)
	if err != nil {
		return false, err
	}

	// Get existing upgrading Pipeline
	existingUpgradingPipelineDef, err := kubernetes.GetLiveResource(ctx, r.restConfig, pipelineDef, "pipelines")
	if err != nil {
		if apierrors.IsNotFound(err) {
			numaLogger.WithValues("pipelineDefinition", *pipelineDef).
				Warn("Pipeline not found. Unable to process status during this reconciliation.")
		} else {
			return false, fmt.Errorf("error getting Pipeline for status processing: %v", err)
		}
	}

	pipelineStatus, err := parsePipelineStatus(existingUpgradingPipelineDef)
	if err != nil {
		return false, fmt.Errorf("failed to parse Pipeline Status from pipeline CR: %+v, %v", existingUpgradingPipelineDef, err)
	}

	pipelinePhase := pipelineStatus.Phase
	if pipelinePhase == numaflowv1.PipelinePhaseFailed {
		// Mark the failed new pipeline recyclable.
		// TODO: pause the failed new pipeline so it can be drained.
		err = r.updatePipelineLabel(ctx, r.restConfig, existingUpgradingPipelineDef, string(common.LabelValueUpgradeRecyclable))
		if err != nil {
			return false, err
		}
		pipelineRollout.Status.MarkPipelineProgressiveUpgradeFailed("New Pipeline Failed", pipelineRollout.Generation)
		return false, nil
	} else if pipelinePhase == numaflowv1.PipelinePhaseRunning {
		if !isPipelineReady(pipelineStatus.Status) {
			//continue (re-enqueue)
			return false, nil
		}
		// Label the new pipeline as promoted and then remove the label from the old pipeline,
		// since per PipelineRollout is reconciled only once at a time, we do not
		// need to worry about consistency issue.
		err = r.updatePipelineLabel(ctx, r.restConfig, existingUpgradingPipelineDef, string(common.LabelValueUpgradePromoted))
		if err != nil {
			return false, err
		}

		err = r.updatePipelineLabel(ctx, r.restConfig, existingPipelineDef, string(common.LabelValueUpgradeRecyclable))
		if err != nil {
			return false, err
		}

		pipelineRollout.Status.MarkPipelineProgressiveUpgradeSucceeded("New Pipeline Running", pipelineRollout.Generation)
		pipelineRollout.Status.MarkDeployed(pipelineRollout.Generation)

		// Pause old pipeline
		if err := r.setPipelineLifecycle(ctx, true, existingPipelineDef); err != nil {
			return false, err
		}
		return true, nil
	} else {
		// Ensure the latest pipeline spec is applied
		pipelineNeedsToUpdate, err := pipelineSpecNeedsUpdating(ctx, existingUpgradingPipelineDef, pipelineDef)
		if err != nil {
			return false, err
		}
		if pipelineNeedsToUpdate {
			err = kubernetes.UpdateCR(ctx, r.restConfig, pipelineDef, "pipelines")
			if err != nil {
				return false, err
			}
		}
		//continue (re-enqueue)
		return false, nil
	}
}

func (r *PipelineRolloutReconciler) updatePipelineLabel(
	ctx context.Context,
	restConfig *rest.Config,
	pipeline *kubernetes.GenericObject,
	updateState string,
) error {
	labelMapping := pipeline.Labels
	labelMapping[common.LabelKeyUpgradeState] = updateState
	pipeline.Labels = labelMapping

	// TODO: use patch instead
	err := kubernetes.UpdateCR(ctx, restConfig, pipeline, "pipelines")
	if err != nil {
		return err
	}
	return nil
}

func (r *PipelineRolloutReconciler) cleanUpPipelines(
	ctx context.Context,
	pipelineRollout *apiv1.PipelineRollout,
) error {
	recyclablePipelines, err := r.getRecyclablePipelines(ctx, pipelineRollout)
	if err != nil {
		return err
	}

	for _, recyclablePipeline := range recyclablePipelines {
		err = r.processRecyclablePipelineStatus(ctx, recyclablePipeline)
		if err != nil {
			return err
		}
	}

	return nil
}

// getRecyclablePipelines retrieves all the recyclable pipelines managed by the given
// pipelineRollout through the `recyclable` label.
func (r *PipelineRolloutReconciler) getRecyclablePipelines(
	ctx context.Context,
	pipelineRollout *apiv1.PipelineRollout,
) ([]*kubernetes.GenericObject, error) {
	return kubernetes.ListLiveResource(
		ctx, r.restConfig, common.NumaflowAPIGroup, common.NumaflowAPIVersion, "pipelines",
		pipelineRollout.Namespace, fmt.Sprintf(
			"%s=%s,%s=%s", common.LabelKeyPipelineRolloutForPipeline, pipelineRollout.Name,
			common.LabelKeyUpgradeState, common.LabelValueUpgradeRecyclable,
		), "")
}

func (r *PipelineRolloutReconciler) processRecyclablePipelineStatus(
	ctx context.Context,
	pipelineDef *kubernetes.GenericObject,
) error {
	pipelineStatus, err := parsePipelineStatus(pipelineDef)
	if err != nil {
		return fmt.Errorf("failed to parse Pipeline Status from pipeline CR: %+v, %v", pipelineDef, err)
	}
	pipelinePhase := pipelineStatus.Phase

	// Only delete fully drained pipelines
	if pipelinePhase == numaflowv1.PipelinePhasePaused {
		// check if `pipelineStatus.DrainedOnPause` is true
		if pipelineStatus.DrainedOnPause {
			err = kubernetes.DeleteCR(ctx, r.restConfig, pipelineDef, "pipelines")
			if err != nil {
				return err
			}
		}
	}
	return nil
}
