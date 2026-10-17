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

package progressive

import (
	"context"
	"encoding/json"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	// FailedTrialPendingMessage is the Rollout phase message while an update is held for a failed progressive trial.
	FailedTrialPendingMessage = "Waiting for a failed progressive trial to be rolled back or force-promoted"
)

// ShouldHoldNewProgressive reports whether a new progressive upgrade for rollout must stay Pending.
// A failed trial NumaflowController holds Pipeline, MonoVertex, and ISBService updates.
// A failed trial InterstepBufferService holds Pipeline updates that use it.
func ShouldHoldNewProgressive(ctx context.Context, c client.Client, rollout ProgressiveRolloutObject) (bool, error) {
	namespace := rollout.GetRolloutObjectMeta().Namespace
	failed, err := ctlrcommon.TrialControllerAssessmentFailed(ctx, c, namespace)
	if err != nil || failed {
		return failed, err
	}
	pipeline, ok := rollout.(*apiv1.PipelineRollout)
	if !ok {
		return false, nil
	}
	return pipelineISBTrialAssessmentFailed(ctx, c, pipeline)
}

// SuppressDerivedOnlyProgressive drops a Progressive update that is only a controller-instance
// or ISBService-child binding once no upstream trial remains. ResourceNeedsUpdating treats that
// annotation as data loss, so after force-promotion it would start another child forever.
// An existing trial is left in place so checkForUpgradeReplacement can discontinue it.
// A real Rollout spec change, or a live upstream trial, is unchanged.
func SuppressDerivedOnlyProgressive(
	ctx context.Context,
	c client.Client,
	rollout ProgressiveRolloutObject,
	controller progressiveController,
	existingPromoted *unstructured.Unstructured,
	needsUpdate bool,
	strategy apiv1.UpgradeStrategy,
) (bool, apiv1.UpgradeStrategy, error) {
	if !needsUpdate || strategy != apiv1.UpgradeStrategyProgressive || existingPromoted == nil {
		return needsUpdate, strategy, nil
	}
	differs, err := controller.CheckForDifferencesWithRolloutDef(ctx, existingPromoted, rollout, common.LabelValueUpgradePromoted)
	if err != nil || differs {
		return needsUpdate, strategy, err
	}
	hasUpstream, err := upstreamTrialExists(ctx, c, rollout)
	if err != nil || hasUpstream {
		return needsUpdate, strategy, err
	}
	trial, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, rollout, common.LabelValueUpgradeTrial, nil, false, c)
	if err != nil || trial != nil {
		return needsUpdate, strategy, err
	}
	return false, apiv1.UpgradeStrategyNoOp, nil
}

// upstreamTrialExists reports whether rollout still has a live trial dependency. Once a
// force-promoted controller has no trial, an unchanged dependent trial whose only difference
// is its derived binding must be discontinued instead of continuing as a user-requested update.
func upstreamTrialExists(ctx context.Context, c client.Client, rollout ProgressiveRolloutObject) (bool, error) {
	namespace := rollout.GetRolloutObjectMeta().Namespace
	var controllerRollouts apiv1.NumaflowControllerRolloutList
	if err := c.List(ctx, &controllerRollouts, &client.ListOptions{Namespace: namespace}); err != nil {
		return false, fmt.Errorf("listing NumaflowControllerRollouts in namespace %q: %w", namespace, err)
	}
	for i := range controllerRollouts.Items {
		trial, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(
			ctx, &controllerRollouts.Items[i], common.LabelValueUpgradeTrial, nil, false, c)
		if err != nil {
			return false, err
		}
		if trial != nil {
			return true, nil
		}
	}

	pipeline, ok := rollout.(*apiv1.PipelineRollout)
	if !ok {
		return false, nil
	}
	isbRolloutName, err := pipelineISBRolloutName(pipeline)
	if err != nil {
		return false, err
	}
	var isbRollout apiv1.ISBServiceRollout
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: isbRolloutName}, &isbRollout); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("getting ISBServiceRollout %s/%s: %w", namespace, isbRolloutName, err)
	}
	trial, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(
		ctx, &isbRollout, common.LabelValueUpgradeTrial, nil, false, c)
	return trial != nil, err
}

func pipelineISBTrialAssessmentFailed(ctx context.Context, c client.Client, pipeline *apiv1.PipelineRollout) (bool, error) {
	isbRolloutName, err := pipelineISBRolloutName(pipeline)
	if err != nil {
		return false, err
	}
	var isbRollout apiv1.ISBServiceRollout
	if err := c.Get(ctx, types.NamespacedName{Namespace: pipeline.Namespace, Name: isbRolloutName}, &isbRollout); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("getting ISBServiceRollout %s/%s: %w", pipeline.Namespace, isbRolloutName, err)
	}
	trialChild, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, &isbRollout, common.LabelValueUpgradeTrial, nil, false, c)
	if err != nil {
		return false, err
	}
	return ctlrcommon.ChildTrialAssessmentFailed(trialChild, isbRollout.GetUpgradingChildStatus()), nil
}

func pipelineISBRolloutName(pipeline *apiv1.PipelineRollout) (string, error) {
	var spec numaflowtypes.PipelineSpec
	if err := json.Unmarshal(pipeline.Spec.Pipeline.Spec.Raw, &spec); err != nil {
		return "", fmt.Errorf("reading PipelineRollout %s/%s ISBService name: %w", pipeline.Namespace, pipeline.Name, err)
	}
	return spec.GetISBSvcName(), nil
}

// restorePromotedAfterFailedTrial runs the post-failure handlers that put the promoted child back, including scale.
// A trial that never finished initialization has not scaled the promoted child down, so there is nothing to restore.
func restorePromotedAfterFailedTrial(
	ctx context.Context,
	rolloutObject ProgressiveRolloutObject,
	controller progressiveController,
	existingPromotedChildDef, existingUpgradingChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	childStatus := rolloutObject.GetUpgradingChildStatus()
	if childStatus == nil || !childStatus.InitializationComplete || rolloutObject.GetPromotedChildStatus() == nil {
		return false, nil
	}
	requeue, err := controller.ProcessPromotedChildPostFailure(ctx, rolloutObject, existingPromotedChildDef, c)
	if err != nil || requeue {
		return requeue, err
	}
	return controller.ProcessUpgradingChildPostFailure(ctx, rolloutObject, existingUpgradingChildDef, c)
}
