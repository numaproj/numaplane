/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ppnd

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// PauseRequester interface manages the safe update of Rollouts by requesting Pipelines to pause
type PauseRequester interface {
	// get the list of Pipelines corresponding to a Rollout
	GetPipelineList(ctx context.Context, rolloutNamespace string, rolloutName string) (*unstructured.UnstructuredList, error)

	// mark this Rollout paused
	MarkRolloutPaused(ctx context.Context, rollout client.Object, paused bool) error

	// get the unique key corresponding to this Rollout
	GetRolloutKey(rolloutNamespace string, rolloutName string) string

	// just a free form string to describe what we're deploying, for logging
	GetChildTypeString() string
}

// process a child object, pausing pipelines or resuming pipelines if needed
// return:
// - true if done with PPND
// - error if any (note we'll automatically reuqueue if there's an error anyway)
func ProcessChildObjectWithPPND(ctx context.Context, k8sclient client.Client, rollout client.Object, pauseRequester PauseRequester,
	resourceNeedsUpdating bool, resourceIsUpdating bool, updateFunc func() error, enqueuePipelineFunc func(k8stypes.NamespacedName)) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	rolloutNamespace := rollout.GetNamespace()
	rolloutName := rollout.GetName()

	if resourceNeedsUpdating || resourceIsUpdating {
		numaLogger.Infof("%s either needs to or is in the process of updating", pauseRequester.GetChildTypeString())
		// TODO: maybe only pause if the update requires pausing

		// request pause if we haven't already
		pauseRequestUpdated, err := requestPipelinesPause(ctx, pauseRequester, rollout, true, enqueuePipelineFunc)
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
				numaLogger.Infof("confirmed all Pipelines have paused (or can't pause) so %s can safely update", pauseRequester.GetChildTypeString())
				err = updateFunc()
				if err != nil {
					return false, fmt.Errorf("error updating %s: %w", pauseRequester.GetChildTypeString(), err)
				}
			} else {
				numaLogger.Debugf("not all Pipelines have paused")
			}

		}
		return false, nil

	} else {
		// remove any pause requirement if necessary
		_, err := requestPipelinesPause(ctx, pauseRequester, rollout, false, enqueuePipelineFunc)
		if err != nil {
			return false, fmt.Errorf("error requesting Pipelines resume: %w", err)
		}
	}

	return true, nil
}

// request that the Pipelines corresponding to this Rollout pause
// return whether an update was made
func requestPipelinesPause(ctx context.Context, pauseRequester PauseRequester, rollout client.Object, pause bool, enqueuePipelineFunc func(k8stypes.NamespacedName)) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	pm := GetPauseModule()

	updated := pm.UpdatePauseRequest(pauseRequester.GetRolloutKey(rollout.GetNamespace(), rollout.GetName()), pause)
	if updated { // if the value is different from what it was then make sure we queue the pipelines to be processed
		numaLogger.Infof("updated pause request = %t", pause)
		pipelines, err := pauseRequester.GetPipelineList(ctx, rollout.GetNamespace(), rollout.GetName())
		if err != nil {
			return false, fmt.Errorf("error getting Pipelines: %w", err)
		}
		for _, pipeline := range pipelines.Items {
			pipelineRollout, err := ctlrcommon.GetRolloutParentName(pipeline.GetName())
			if err != nil {
				return false, fmt.Errorf("error getting PipelineRolloutName: %w", err)
			}
			enqueuePipelineFunc(k8stypes.NamespacedName{Namespace: pipeline.GetNamespace(), Name: pipelineRollout})
		}
	}

	if err := pauseRequester.MarkRolloutPaused(ctx, rollout, pause); err != nil {
		return updated, fmt.Errorf("error marking %s paused: %w", pauseRequester.GetChildTypeString(), err)
	}
	return updated, nil
}

// check if all Pipelines corresponding to this Rollout have paused or are otherwise not pausible (contract with Numaflow is that this is Pipelines which are "Failed")
// or have an exception for allowing data loss
func areAllPipelinesPausedOrWontPause(ctx context.Context, k8sClient client.Client, pauseRequester PauseRequester, rolloutNamespace string, rolloutName string) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	pipelines, err := pauseRequester.GetPipelineList(ctx, rolloutNamespace, rolloutName)
	if err != nil {
		return false, err
	}
	for _, pipeline := range pipelines.Items {

		// Get PipelineRollout CR
		pipelineRolloutName, err := ctlrcommon.GetRolloutParentName(pipeline.GetName())
		if err != nil {
			return false, err
		}
		pipelineRollout := &apiv1.PipelineRollout{}
		if err = k8sClient.Get(ctx, k8stypes.NamespacedName{Namespace: rolloutNamespace, Name: pipelineRolloutName}, pipelineRollout); err != nil {
			return false, err
		}

		if !numaflowtypes.IsPipelinePausedOrWontPause(ctx, &pipeline, pipelineRollout) {
			numaLogger.Debugf("pipeline %q not paused or won't pause", pipeline.GetName())
			return false, nil
		}
	}
	return true, nil
}
