package controller

import (
	"context"
	"fmt"

	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PauseRequester interface manages the safe update of Rollouts by requesting Pipelines to pause
type PauseRequester interface {
	// get the list of Pipelines corresponding to a Rollout
	getPipelineList(ctx context.Context, rolloutNamespace string, rolloutName string) ([]*kubernetes.GenericObject, error)

	// mark this Rollout paused
	markRolloutPaused(ctx context.Context, rollout client.Object, paused bool) error

	// get the unique key corresponding to this Rollout
	getRolloutKey(rolloutNamespace string, rolloutName string) string

	// just a free form string to describe what we're deploying, for logging
	getChildTypeString() string
}

// process a child object, pausing pipelines or resuming pipelines if needed
// return:
// - true if done with PPND
// - error if any (note we'll automatically reuqueue if there's an error anyway)
func processChildObjectWithPPND(ctx context.Context, k8sclient client.Client, rollout client.Object, pauseRequester PauseRequester,
	resourceNeedsUpdating bool, resourceIsUpdating bool, updateFunc func() error) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	rolloutNamespace := rollout.GetNamespace()
	rolloutName := rollout.GetName()

	if resourceNeedsUpdating || resourceIsUpdating {
		numaLogger.Infof("%s either needs to or is in the process of updating", pauseRequester.getChildTypeString())
		// TODO: maybe only pause if the update requires pausing

		// request pause if we haven't already
		pauseRequestUpdated, err := requestPipelinesPause(ctx, pauseRequester, rollout, true)
		if err != nil {
			return false, fmt.Errorf("error requesting Pipelines pause: %w", err)
		}
		// If we need to update the child, pause the pipelines
		// Don't do this yet if we just made a request - it's too soon for anything to have happened
		if !pauseRequestUpdated && resourceNeedsUpdating {

			// check if the pipelines are all paused (or can't be paused)
			allPaused, err := areAllPipelinesPausedOrWontPause(ctx, k8sclient, pauseRequester, rolloutNamespace, rolloutName)
			if err != nil {
				return false, fmt.Errorf("error checking if all Pipelines are paused: %w", err)
			}
			if allPaused {
				numaLogger.Infof("confirmed all Pipelines have paused (or can't pause) so %s can safely update", pauseRequester.getChildTypeString())
				err = updateFunc()
				if err != nil {
					return false, fmt.Errorf("error updating %s: %w", pauseRequester.getChildTypeString(), err)
				}
			} else {
				numaLogger.Debugf("not all Pipelines have paused")
			}

		}
		return false, nil

	} else {
		// remove any pause requirement if necessary
		_, err := requestPipelinesPause(ctx, pauseRequester, rollout, false)
		if err != nil {
			return false, fmt.Errorf("error requesting Pipelines resume: %w", err)
		}
	}

	return true, nil
}

// request that the Pipelines corresponding to this Rollout pause
// return whether an update was made
func requestPipelinesPause(ctx context.Context, pauseRequester PauseRequester, rollout client.Object, pause bool) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	pm := GetPauseModule()

	updated := pm.updatePauseRequest(pauseRequester.getRolloutKey(rollout.GetNamespace(), rollout.GetName()), pause)
	if updated { // if the value is different from what it was then make sure we queue the pipelines to be processed
		numaLogger.Infof("updated pause request = %t", pause)
		pipelines, err := pauseRequester.getPipelineList(ctx, rollout.GetNamespace(), rollout.GetName())
		if err != nil {
			return false, fmt.Errorf("error getting Pipelines: %w", err)
		}
		for _, pipeline := range pipelines {
			pipelineRollout := getPipelineRolloutName(pipeline.Name)
			pipelineROReconciler.enqueuePipeline(k8stypes.NamespacedName{Namespace: pipeline.Namespace, Name: pipelineRollout})
		}
	}

	if err := pauseRequester.markRolloutPaused(ctx, rollout, pause); err != nil {
		return updated, fmt.Errorf("error marking %s paused: %w", pauseRequester.getChildTypeString(), err)
	}
	return updated, nil
}

// check if all Pipelines corresponding to this Rollout have paused or are otherwise not pausible (contract with Numaflow is that this is Pipelines which are "Failed")
// or have an exception for allowing data loss
func areAllPipelinesPausedOrWontPause(ctx context.Context, k8sClient client.Client, pauseRequester PauseRequester, rolloutNamespace string, rolloutName string) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	pipelines, err := pauseRequester.getPipelineList(ctx, rolloutNamespace, rolloutName)
	if err != nil {
		return false, err
	}
	for _, pipeline := range pipelines {

		// Get PipelineRollout CR
		pipelineRolloutName := getPipelineRolloutName(pipeline.Name)
		pipelineRollout := &apiv1.PipelineRollout{}
		if err := k8sClient.Get(ctx, k8stypes.NamespacedName{Namespace: rolloutNamespace, Name: pipelineRolloutName}, pipelineRollout); err != nil {
			return false, err
		}

		if !isPipelinePausedOrWontPause(ctx, pipeline, pipelineRollout) {
			numaLogger.Debugf("pipeline %q not paused or won't pause", pipeline.Name)
			return false, nil
		}
	}
	return true, nil
}
