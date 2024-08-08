package controller

import (
	"context"

	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	k8stypes "k8s.io/apimachinery/pkg/types"
)

type PauseRequester interface {
	getPipelineList(ctx context.Context, rolloutNamespace string, rolloutName string) ([]*kubernetes.GenericObject, error)

	markRolloutPaused(ctx context.Context, rolloutNamespace string, rolloutName string, paused bool) error

	getPauseModuleKey(rolloutNamespace string, rolloutName string) string

	// just a free form string to describe what we're deploying, for logging
	getChildTypeString() string
}

// process a child object, pausing pipelines or resuming pipelines if needed
// return:
// - true if needs a requeue
// - error if any (note we'll automatically reuqueue if there's an error anyway)
func processChildObjectWithoutDataLoss(ctx context.Context, rolloutNamespace string, rolloutName string, pauseRequester PauseRequester,
	resourceNeedsUpdating bool, resourceIsUpdating bool, updateFunc func() error) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	if resourceNeedsUpdating || resourceIsUpdating {
		numaLogger.Infof("%s either needs to or is in the process of updating", pauseRequester.getChildTypeString())
		// TODO: maybe only pause if the update requires pausing

		// request pause if we haven't already
		pauseRequestUpdated, err := requestPipelinesPause(ctx, pauseRequester, rolloutNamespace, rolloutName, true)
		if err != nil {
			return false, err
		}
		// If we need to update the child, pause the pipelines
		// Don't do this yet if we just made a request - it's too soon for anything to have happened
		if !pauseRequestUpdated && resourceNeedsUpdating {

			// check if the pipelines are all paused
			allPaused, err := areAllPipelinesPaused(ctx, pauseRequester, rolloutNamespace, rolloutName)
			if err != nil {
				return false, err
			}
			if allPaused {
				numaLogger.Infof("confirmed all Pipelines have paused so %s can safely update", pauseRequester.getChildTypeString())
				err = updateFunc()
				if err != nil {
					return false, err
				}
			} else {
				numaLogger.Debugf("not all Pipelines have paused")
			}

		}
		return true, nil

	} else {
		// remove any pause requirement if necessary
		_, err := requestPipelinesPause(ctx, pauseRequester, rolloutNamespace, rolloutName, false)
		if err != nil {
			return false, err
		}
	}

	return false, nil
}

// request that Pipelines pause
// return whether an update was made
func requestPipelinesPause(ctx context.Context, pauseRequester PauseRequester, rolloutNamespace string, rolloutName string, pause bool) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	pm := GetPauseModule()

	updated := pm.updatePauseRequest(pauseRequester.getPauseModuleKey(rolloutNamespace, rolloutName), pause)
	if updated { // if the value is different from what it was then make sure we queue the pipelines to be processed
		numaLogger.Infof("updated pause request = %t", pause)
		pipelines, err := pauseRequester.getPipelineList(ctx, rolloutNamespace, rolloutName)
		if err != nil {
			return false, err
		}
		for _, pipeline := range pipelines {
			pipelineROReconciler.enqueuePipeline(k8stypes.NamespacedName{Namespace: pipeline.Namespace, Name: pipeline.Name})
		}
	}

	err := pauseRequester.markRolloutPaused(ctx, rolloutNamespace, rolloutName, pause)
	return updated, err
}

func areAllPipelinesPaused(ctx context.Context, pauseRequester PauseRequester, rolloutNamespace string, rolloutName string) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	pipelines, err := pauseRequester.getPipelineList(ctx, rolloutNamespace, rolloutName)
	if err != nil {
		return false, err
	}
	for _, pipeline := range pipelines {
		status, err := kubernetes.ParseStatus(pipeline)
		if err != nil {
			return false, err
		}
		if status.Phase != "Paused" {
			numaLogger.Debugf("pipeline %q has status.phase=%q", pipeline.Name, status.Phase)
			return false, nil
		}
	}
	return true, nil
}
