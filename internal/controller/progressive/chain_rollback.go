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

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	// FailedTrialPendingMessage is the Rollout phase message while an update is held for a failed progressive trial.
	FailedTrialPendingMessage = "Waiting for a failed progressive trial to be rolled back or force-promoted"
	// chainRollbackMessage is recorded on every chain member that is rolled back with the failed trial.
	chainRollbackMessage = "Rolled back with a failed progressive trial"
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

// RollbackChain marks the rest of an active progressive chain failed when source's trial assessment has failed.
// A failed NumaflowController trial stays in place so it can keep gating new updates. Every ISBService, Pipeline,
// and MonoVertex trial bound to that controller instance is marked failed with it.
// A failed Pipeline under a trial InterstepBufferService marks that ISBService and the other Pipelines using it.
// The source rollout is left to the caller, which is already processing its own failure.
func RollbackChain(ctx context.Context, c client.Client, source ProgressiveRolloutObject, upgradingChild *unstructured.Unstructured) error {
	namespace := source.GetRolloutObjectMeta().Namespace
	if err := rollbackControllerChain(ctx, c, namespace, source, upgradingChild); err != nil {
		return err
	}
	if err := rollbackISBChain(ctx, c, namespace, source, upgradingChild); err != nil {
		return err
	}
	logger.FromContext(ctx).WithValues("source", source.GetRolloutObjectMeta().Name).
		Debug("rolled back the progressive resource chain after a failed trial assessment")
	return nil
}

func rollbackControllerChain(ctx context.Context, c client.Client, namespace string, source ProgressiveRolloutObject, upgradingChild *unstructured.Unstructured) error {
	nfcRollout, trialController, err := findTrialController(ctx, c, namespace)
	if err != nil || trialController == nil {
		return err
	}
	instanceID, err := unstructuredInstanceID(trialController)
	if err != nil {
		return err
	}
	if instanceID == "" {
		instanceID = numaflowtypes.ControllerInstanceIDFromResource(trialController)
	}
	if instanceID == "" {
		return nil
	}

	sourceKind := rolloutKind(source)
	onTrial := sourceKind == "NumaflowControllerRollout" ||
		(upgradingChild != nil && numaflowtypes.ControllerInstanceIDFromResource(upgradingChild) == instanceID)
	if !onTrial {
		return nil
	}

	if sourceKind != "NumaflowControllerRollout" {
		if err := markChainMemberFailed(ctx, c, nfcRollout, nfcRollout, trialController, chainRollbackMessage); err != nil {
			return err
		}
	}
	return rollbackDependentsBoundToInstance(ctx, c, namespace, instanceID, source)
}

func rollbackDependentsBoundToInstance(ctx context.Context, c client.Client, namespace, instanceID string, source ProgressiveRolloutObject) error {
	var isbRollouts apiv1.ISBServiceRolloutList
	if err := c.List(ctx, &isbRollouts, &client.ListOptions{Namespace: namespace}); err != nil {
		return fmt.Errorf("listing ISBServiceRollouts: %w", err)
	}
	for i := range isbRollouts.Items {
		isbRollout := &isbRollouts.Items[i]
		if sameRollout(source, isbRollout) {
			continue
		}
		if err := rollbackMemberOnInstance(ctx, c, isbRollout, isbRollout, instanceID); err != nil {
			return err
		}
	}

	var pipelineRollouts apiv1.PipelineRolloutList
	if err := c.List(ctx, &pipelineRollouts, &client.ListOptions{Namespace: namespace}); err != nil {
		return fmt.Errorf("listing PipelineRollouts: %w", err)
	}
	for i := range pipelineRollouts.Items {
		pipelineRollout := &pipelineRollouts.Items[i]
		if sameRollout(source, pipelineRollout) {
			continue
		}
		if err := rollbackMemberOnInstance(ctx, c, pipelineRollout, pipelineRollout, instanceID); err != nil {
			return err
		}
	}

	var monoVertexRollouts apiv1.MonoVertexRolloutList
	if err := c.List(ctx, &monoVertexRollouts, &client.ListOptions{Namespace: namespace}); err != nil {
		return fmt.Errorf("listing MonoVertexRollouts: %w", err)
	}
	for i := range monoVertexRollouts.Items {
		monoVertexRollout := &monoVertexRollouts.Items[i]
		if sameRollout(source, monoVertexRollout) {
			continue
		}
		if err := rollbackMemberOnInstance(ctx, c, monoVertexRollout, monoVertexRollout, instanceID); err != nil {
			return err
		}
	}
	return nil
}

func rollbackMemberOnInstance(ctx context.Context, c client.Client, rollout ProgressiveRolloutObject, obj client.Object, instanceID string) error {
	trialChild, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, rollout, common.LabelValueUpgradeTrial, nil, false, c)
	if err != nil || trialChild == nil {
		return err
	}
	if numaflowtypes.ControllerInstanceIDFromResource(trialChild) != instanceID {
		return nil
	}
	return markChainMemberFailed(ctx, c, rollout, obj, trialChild, chainRollbackMessage)
}

func rollbackISBChain(ctx context.Context, c client.Client, namespace string, source ProgressiveRolloutObject, upgradingChild *unstructured.Unstructured) error {
	isbChildName, err := trialISBChildName(source, upgradingChild)
	if err != nil || isbChildName == "" {
		return err
	}

	isbsvc, err := kubernetes.GetResource(ctx, c, numaflowv1.ISBGroupVersionKind, types.NamespacedName{Namespace: namespace, Name: isbChildName})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("getting trial ISBService %s/%s: %w", namespace, isbChildName, err)
	}
	if isbsvc.GetLabels()[common.LabelKeyUpgradeState] != string(common.LabelValueUpgradeTrial) {
		return nil
	}

	parentName := isbsvc.GetLabels()[common.LabelKeyParentRollout]
	sourceIsThisISB := rolloutKind(source) == "ISBServiceRollout" && source.GetRolloutObjectMeta().Name == parentName
	if parentName != "" && !sourceIsThisISB {
		var isbRollout apiv1.ISBServiceRollout
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: parentName}, &isbRollout); err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("getting ISBServiceRollout %s/%s: %w", namespace, parentName, err)
			}
		} else if err := markChainMemberFailed(ctx, c, &isbRollout, &isbRollout, isbsvc, chainRollbackMessage); err != nil {
			return err
		}
	} else if isbsvc.GetLabels()[common.LabelKeyProgressiveResultState] != string(common.LabelValueResultStateFailed) {
		if err := ctlrcommon.UpdateResultState(ctx, c, common.LabelValueResultStateFailed, isbsvc); err != nil {
			return err
		}
	}

	var pipelineRollouts apiv1.PipelineRolloutList
	if err := c.List(ctx, &pipelineRollouts, &client.ListOptions{Namespace: namespace}); err != nil {
		return fmt.Errorf("listing PipelineRollouts: %w", err)
	}
	for i := range pipelineRollouts.Items {
		pipelineRollout := &pipelineRollouts.Items[i]
		if sameRollout(source, pipelineRollout) {
			continue
		}
		upgrading := pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus
		if upgrading == nil || upgrading.InterStepBufferServiceName != isbChildName {
			continue
		}
		trialChild, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, pipelineRollout, common.LabelValueUpgradeTrial, nil, false, c)
		if err != nil {
			return err
		}
		if trialChild == nil {
			continue
		}
		if err := markChainMemberFailed(ctx, c, pipelineRollout, pipelineRollout, trialChild, chainRollbackMessage); err != nil {
			return err
		}
	}
	return nil
}

// trialISBChildName returns the trial InterstepBufferService a Pipeline or ISBService failure rolls back with.
// An empty name means this failure is not part of an ISBService chain.
func trialISBChildName(source ProgressiveRolloutObject, upgradingChild *unstructured.Unstructured) (string, error) {
	switch src := source.(type) {
	case *apiv1.PipelineRollout:
		if src.Status.ProgressiveStatus.UpgradingPipelineStatus != nil && src.Status.ProgressiveStatus.UpgradingPipelineStatus.InterStepBufferServiceName != "" {
			return src.Status.ProgressiveStatus.UpgradingPipelineStatus.InterStepBufferServiceName, nil
		}
		if upgradingChild == nil {
			return "", nil
		}
		isbName, _, err := unstructured.NestedString(upgradingChild.Object, "spec", "interStepBufferServiceName")
		if err != nil {
			return "", fmt.Errorf("reading spec.interStepBufferServiceName: %w", err)
		}
		return isbName, nil
	case *apiv1.ISBServiceRollout:
		if upgradingChild == nil {
			return "", nil
		}
		return upgradingChild.GetName(), nil
	default:
		return "", nil
	}
}

func markChainMemberFailed(ctx context.Context, c client.Client, rollout ProgressiveRolloutObject, obj client.Object, child *unstructured.Unstructured, message string) error {
	status := rollout.GetUpgradingChildStatus()
	if status == nil || status.Name != child.GetName() {
		if err := rollout.ResetUpgradingChildStatus(child); err != nil {
			return err
		}
		status = rollout.GetUpgradingChildStatus()
	}
	if status != nil && (!status.IsFailed() || status.ResourceChainAssessmentResult != apiv1.AssessmentResultFailure) {
		updated := status.DeepCopy()
		if !updated.IsFailed() {
			updated.FailureReasons = append(append([]string{}, updated.FailureReasons...), message)
		}
		// AssessmentResult is what later reconciles treat as the promotion decision. The chain field must
		// fail with it: processUpgradingChild prefers ResourceChainAssessmentResult when it is already set,
		// and an in-progress trial has that field set to Unknown.
		updated.AssessmentResult = apiv1.AssessmentResultFailure
		updated.ResourceChainAssessmentResult = apiv1.AssessmentResultFailure
		rollout.SetUpgradingChildStatus(updated)
		rollout.GetRolloutStatus().MarkProgressiveUpgradeFailed(message, rollout.GetRolloutObjectMeta().Generation)
		if err := c.Status().Update(ctx, obj); err != nil {
			return fmt.Errorf("updating %s %s/%s status after chain rollback: %w", rolloutKind(rollout), rollout.GetRolloutObjectMeta().Namespace, rollout.GetRolloutObjectMeta().Name, err)
		}
	}
	if child.GetLabels()[common.LabelKeyProgressiveResultState] != string(common.LabelValueResultStateFailed) {
		if err := ctlrcommon.UpdateResultState(ctx, c, common.LabelValueResultStateFailed, child); err != nil {
			return err
		}
	}
	return nil
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

func sameRollout(a, b ProgressiveRolloutObject) bool {
	if a == nil || b == nil {
		return false
	}
	return rolloutKind(a) == rolloutKind(b) &&
		a.GetRolloutObjectMeta().Namespace == b.GetRolloutObjectMeta().Namespace &&
		a.GetRolloutObjectMeta().Name == b.GetRolloutObjectMeta().Name
}

// rollBackInProgressTrial returns a dependent's in-progress trial to the promoted child and marks the trial recyclable.
// The promoted child is restored first when the trial had already scaled it down. A NumaflowController trial is not
// recycled here: it stays failed so new updates remain Pending until that trial is rolled back or force-promoted.
func rollBackInProgressTrial(
	ctx context.Context,
	rolloutObject ProgressiveRolloutObject,
	controller progressiveController,
	existingPromotedChildDef, existingUpgradingChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	requeue, err := restorePromotedAfterFailedTrial(ctx, rolloutObject, controller, existingPromotedChildDef, existingUpgradingChildDef, c)
	if err != nil || requeue {
		return requeue, err
	}

	if rolloutObject.GetUpgradingChildStatus() != nil {
		UpdateUpgradingChildStatus(rolloutObject, func(status *apiv1.UpgradingChildStatus) {
			if status == nil {
				return
			}
			if !status.IsFailed() {
				status.FailureReasons = append(status.FailureReasons, chainRollbackMessage)
			}
			status.AssessmentResult = apiv1.AssessmentResultFailure
			status.ResourceChainAssessmentResult = apiv1.AssessmentResultFailure
			status.Discontinued = true
		})
	}
	rolloutObject.GetRolloutStatus().MarkProgressiveUpgradeFailed(chainRollbackMessage, rolloutObject.GetRolloutObjectMeta().Generation)
	rolloutObject.GetRolloutStatus().SetPhase(apiv1.PhasePending, FailedTrialPendingMessage)

	reason := common.LabelValueChainRollback
	if err := ctlrcommon.MarkRecyclable(ctx, c, &reason, existingUpgradingChildDef); err != nil {
		return false, err
	}
	return false, nil
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

func rolloutKind(rollout ProgressiveRolloutObject) string {
	switch rollout.(type) {
	case *apiv1.NumaflowControllerRollout:
		return "NumaflowControllerRollout"
	case *apiv1.ISBServiceRollout:
		return "ISBServiceRollout"
	case *apiv1.PipelineRollout:
		return "PipelineRollout"
	case *apiv1.MonoVertexRollout:
		return "MonoVertexRollout"
	default:
		return ""
	}
}
