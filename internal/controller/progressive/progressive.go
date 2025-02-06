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
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// progressiveController describes a Controller that can progressively roll out a second child alongside the original child,
// taking down the original child once the new one is healthy
type progressiveController interface {
	ctlrcommon.RolloutController

	// CreateUpgradingChildDefinition creates a Kubernetes definition for a child resource of the Rollout with the given name in an "upgrading" state
	CreateUpgradingChildDefinition(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, name string) (*unstructured.Unstructured, error)

	// ChildNeedsUpdating determines if the difference between the current child definition and the desired child definition requires an update
	ChildNeedsUpdating(ctx context.Context, existingChild, newChildDefinition *unstructured.Unstructured) (bool, error)

	// AssessUpgradingChild determines if upgrading child is determined to be healthy, unhealthy, or unknown
	AssessUpgradingChild(ctx context.Context, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, error)

	// ScaleDownPromotedChildSourceVertices scales down by half the promoted child source vertices pods
	ScaleDownPromotedChildSourceVertices(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) (map[string]apiv1.ScaleValues, bool, error)

	// ScalePromotedChildSourceVerticesToDesiredValues scales back up to the desired scale values the promoted child source vertices pods
	ScalePromotedChildSourceVerticesToDesiredValues(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) error
}

// return:
// - whether we're done
// - whether we just created a new child
// - duration indicating the requeue delay for the controller to use for next reconciliation
// - error if any
func ProcessResource(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	liveRolloutObject ctlrcommon.RolloutObject,
	existingPromotedChild *unstructured.Unstructured,
	promotedDifference bool,
	skipPromotedChildSourceVerticesScaling bool,
	controller progressiveController,
	c client.Client,
) (bool, bool, time.Duration, error) {

	numaLogger := logger.FromContext(ctx)

	// is there currently an "upgrading" child?
	currentUpgradingChildDef, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, rolloutObject, common.LabelValueUpgradeInProgress, nil, false, c)
	if err != nil {
		return false, false, 0, err
	}

	// if there's a difference between the desired spec and the current "promoted" child, and there isn't already an "upgrading" definition, then create one and return
	if promotedDifference && currentUpgradingChildDef == nil {
		// Create it, first making sure one doesn't already exist by checking the live K8S API
		currentUpgradingChildDef, err = ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, rolloutObject, common.LabelValueUpgradeInProgress, nil, true, c)
		if err != nil {
			return false, false, 0, fmt.Errorf("error getting %s: %v", currentUpgradingChildDef.GetKind(), err)
		}
		if currentUpgradingChildDef == nil {
			// Scale down the promoted child first
			scaledDown, err := scaleDownPromotedChild(ctx, rolloutObject, liveRolloutObject, existingPromotedChild, skipPromotedChildSourceVerticesScaling, controller, c)
			if err != nil {
				return false, false, 0, err
			}
			if scaledDown {
				return false, false, common.DefaultRequeueDelay, nil
			}

			// create object as it doesn't exist
			newUpgradingChildDef, err := makeUpgradingObjectDefinition(ctx, rolloutObject, controller, c, false)
			if err != nil {
				return false, false, 0, err
			}

			numaLogger.Debugf("Upgrading child of type %s %s/%s doesn't exist so creating", newUpgradingChildDef.GetKind(), newUpgradingChildDef.GetNamespace(), newUpgradingChildDef.GetName())
			err = kubernetes.CreateResource(ctx, c, newUpgradingChildDef)
			return false, true, 0, err
		}
	}
	if currentUpgradingChildDef == nil { // nothing to do (either there's nothing to upgrade, or we just created an "upgrading" child, and it's too early to start reconciling it)
		return true, false, 0, err
	}

	// There's already an Upgrading child, now process it

	// Get the live resource so we don't have issues with an outdated cache
	currentUpgradingChildDef, err = kubernetes.GetLiveResource(ctx, currentUpgradingChildDef, rolloutObject.GetChildGVR().Resource)
	if err != nil {
		return false, false, 0, err
	}

	done, newChild, requeueDelay, err := processUpgradingChild(ctx, rolloutObject, liveRolloutObject, controller, existingPromotedChild, currentUpgradingChildDef, skipPromotedChildSourceVerticesScaling, c)
	if err != nil {
		return false, newChild, 0, err
	}

	return done, newChild, requeueDelay, nil
}

// create the definition for the child of the Rollout which is the one labeled "upgrading"
// if there's already an existing "upgrading" child, create a definition using its name; otherwise, use a new name
func makeUpgradingObjectDefinition(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, controller progressiveController, c client.Client, useExistingChildName bool) (*unstructured.Unstructured, error) {

	numaLogger := logger.FromContext(ctx)

	childName, err := ctlrcommon.GetChildName(ctx, rolloutObject, controller, common.LabelValueUpgradeInProgress, nil, c, useExistingChildName)
	if err != nil {
		return nil, err
	}
	numaLogger.Debugf("Upgrading child: %s", childName)
	upgradingChild, err := controller.CreateUpgradingChildDefinition(ctx, rolloutObject, childName)
	if err != nil {
		return nil, err
	}

	return upgradingChild, nil
}

/*
processUpgradingChild handles the assessment and potential update of a child resource during a progressive upgrade.
It evaluates the current status of the upgrading child, determines if an assessment is needed, and processes the
assessment result.

Parameters:
- ctx: The context for managing request-scoped values, cancellation, and timeouts.
- rolloutObject: The current rollout object (this could be from cache).
- liveRolloutObject: The live rollout object reflecting the current state of the rollout.
- controller: The progressive controller responsible for managing the upgrade process.
- existingPromotedChildDef: The definition of the currently promoted child resource.
- existingUpgradingChildDef: The definition of the child resource currently being upgraded.
- c: The Kubernetes client for interacting with the cluster.

Returns:
- A boolean indicating if the upgrade is done.
- A boolean indicating if a new child was created.
- A duration indicating the requeue delay for the controller to use for next reconciliation.
- An error if any issues occur during the process.
*/
func processUpgradingChild(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	liveRolloutObject ctlrcommon.RolloutObject,
	controller progressiveController,
	existingPromotedChildDef, existingUpgradingChildDef *unstructured.Unstructured,
	skipPromotedChildSourceVerticesScaling bool,
	c client.Client,
) (bool, bool, time.Duration, error) {
	numaLogger := logger.FromContext(ctx)

	globalConfig, err := config.GetConfigManagerInstance().GetConfig()
	if err != nil {
		return false, false, 0, fmt.Errorf("error getting the global config for assessment processing: %w", err)
	}

	assessmentDelay, assessmentPeriod, assessmentInterval, err := globalConfig.GetChildStatusAssessmentSchedule()
	if err != nil {
		return false, false, 0, fmt.Errorf("error getting the child status assessment schedule from global config: %w", err)
	}

	childStatus := liveRolloutObject.GetRolloutStatus().ProgressiveStatus.UpgradingChildStatus
	// Create a new childStatus object if not present in the live rollout object or
	// if it is that of a previous progressive upgrade.
	if childStatus == nil || childStatus.Name != existingUpgradingChildDef.GetName() {
		if childStatus != nil {
			numaLogger.WithValues("name", existingUpgradingChildDef.GetName(), "childStatus", *childStatus).Debug("the live upgrading child status is stale, resetting it")
		} else {
			numaLogger.WithValues("name", existingUpgradingChildDef.GetName()).Debug("the live upgrading child status has not been set yet, initializing it")
		}

		childStatus = &apiv1.UpgradingChildStatus{
			Name:             existingUpgradingChildDef.GetName(),
			AssessmentResult: apiv1.AssessmentResultUnknown,
		}
		childStatus.InitAssessUntil()
	} else {
		numaLogger.WithValues("childStatus", *childStatus).Debug("live upgrading child previously set")
	}

	// If no NextAssessmentTime has been set already, calculate it and set it
	if childStatus.NextAssessmentTime == nil {
		// Add to the current time the assessmentDelay and set the NextAssessmentTime in the Rollout object
		nextAssessmentTime := metav1.NewTime(time.Now().Add(assessmentDelay))
		childStatus.NextAssessmentTime = &nextAssessmentTime
		numaLogger.WithValues("childStatus", *childStatus).Debug("set upgrading child nextAssessmentTime")
	}

	// Assess the upgrading child status only if within the assessment time window and if not previously failed.
	// Otherwise, assess the previous child status.
	assessment := childStatus.AssessmentResult
	if childStatus.CanAssess() {
		assessment, err = controller.AssessUpgradingChild(ctx, existingUpgradingChildDef)
		if err != nil {
			return false, false, 0, err
		}

		numaLogger.WithValues("name", existingUpgradingChildDef.GetName(), "childStatus", *childStatus, "assessment", assessment).
			Debugf("performing upgrading child assessment, assessment returned: %v", assessment)
	} else {
		numaLogger.WithValues("name", existingUpgradingChildDef.GetName(), "childStatus", *childStatus, "assessment", assessment).
			Debug("skipping upgrading child assessment but assessing previous child status")
	}

	// Once a "not unknown" assessment is reached, set the assessment's end time (if not set yet)
	if assessment != apiv1.AssessmentResultUnknown && !childStatus.IsAssessUntilSet() {
		assessUntil := metav1.NewTime(time.Now().Add(assessmentPeriod))
		childStatus.AssessUntil = &assessUntil
		numaLogger.WithValues("childStatus", *childStatus).Debug("set upgrading child assessUntil")
	}

	switch assessment {
	case apiv1.AssessmentResultFailure:

		rolloutObject.GetRolloutStatus().MarkProgressiveUpgradeFailed(fmt.Sprintf("New Child Object %s/%s Failed", existingUpgradingChildDef.GetNamespace(), existingUpgradingChildDef.GetName()), rolloutObject.GetRolloutObjectMeta().Generation)
		childStatus.AssessmentResult = apiv1.AssessmentResultFailure
		rolloutObject.GetRolloutStatus().ProgressiveStatus.UpgradingChildStatus = childStatus

		// check if there are any new incoming changes to the desired spec
		newUpgradingChildDef, err := makeUpgradingObjectDefinition(ctx, rolloutObject, controller, c, true)
		if err != nil {
			return false, false, 0, err
		}
		needsUpdating, err := controller.ChildNeedsUpdating(ctx, existingUpgradingChildDef, newUpgradingChildDef)
		if err != nil {
			return false, false, 0, err
		}

		// if so, mark the existing one for garbage collection and then create a new upgrading one
		if needsUpdating {
			scaledDown, err := scaleDownPromotedChild(ctx, rolloutObject, liveRolloutObject, existingPromotedChildDef, skipPromotedChildSourceVerticesScaling, controller, c)
			if err != nil {
				return false, false, 0, err
			}
			if scaledDown {
				return false, false, common.DefaultRequeueDelay, nil
			}

			// create a definition for the "upgrading" child which has a new name (the definition created above had the previous child's name which was necessary for comparison)
			newUpgradingChildDef, err = makeUpgradingObjectDefinition(ctx, rolloutObject, controller, c, false)
			if err != nil {
				return false, false, 0, err
			}

			numaLogger.WithValues("old child", existingUpgradingChildDef.GetName(), "new child", newUpgradingChildDef.GetName()).Debug("replacing 'upgrading' child")
			reasonFailure := common.LabelValueProgressiveFailure
			err = ctlrcommon.UpdateUpgradeState(ctx, c, common.LabelValueUpgradeRecyclable, &reasonFailure, existingUpgradingChildDef)
			if err != nil {
				return false, false, 0, err
			}

			err = kubernetes.CreateResource(ctx, c, newUpgradingChildDef)
			return false, true, 0, err
		} else {
			scaledUp, err := scaleUpPromotedChild(ctx, rolloutObject, liveRolloutObject, existingPromotedChildDef, skipPromotedChildSourceVerticesScaling, controller, c)
			if err != nil {
				return false, false, 0, err
			}
			if scaledUp {
				return false, false, common.DefaultRequeueDelay, nil
			}
		}

		return false, false, 0, nil

	case apiv1.AssessmentResultSuccess:
		// Label the new child as promoted and then remove the label from the old one
		numaLogger.WithValues("old child", existingPromotedChildDef.GetName(), "new child", existingUpgradingChildDef.GetName(), "replacing 'promoted' child")
		reasonSuccess := common.LabelValueProgressiveSuccess
		err := ctlrcommon.UpdateUpgradeState(ctx, c, common.LabelValueUpgradePromoted, &reasonSuccess, existingUpgradingChildDef)
		if err != nil {
			return false, false, 0, err
		}

		err = ctlrcommon.UpdateUpgradeState(ctx, c, common.LabelValueUpgradeRecyclable, &reasonSuccess, existingPromotedChildDef)
		if err != nil {
			return false, false, 0, err
		}

		rolloutObject.GetRolloutStatus().MarkProgressiveUpgradeSucceeded(fmt.Sprintf("New Child Object %s/%s Running", existingUpgradingChildDef.GetNamespace(), existingUpgradingChildDef.GetName()), rolloutObject.GetRolloutObjectMeta().Generation)
		childStatus.AssessmentResult = apiv1.AssessmentResultSuccess
		rolloutObject.GetRolloutStatus().ProgressiveStatus.UpgradingChildStatus = childStatus
		rolloutObject.GetRolloutStatus().MarkDeployed(rolloutObject.GetRolloutObjectMeta().Generation)

		// if we are still in the assessment window, return we are not done
		return !childStatus.CanAssess(), false, assessmentInterval, nil

	default:
		childStatus.AssessmentResult = apiv1.AssessmentResultUnknown
		rolloutObject.GetRolloutStatus().ProgressiveStatus.UpgradingChildStatus = childStatus

		return false, false, assessmentInterval, nil
	}
}

func IsNumaflowChildReady(upgradingObjectStatus *kubernetes.GenericStatus) bool {
	if len(upgradingObjectStatus.Conditions) == 0 {
		return false
	}
	for _, c := range upgradingObjectStatus.Conditions {
		if c.Status != metav1.ConditionTrue {
			return false
		}
	}
	return true
}

/*
scaleDownPromotedChild attempts to scale down the source vertices of a promoted child.
// It checks if scaling is required based on the rollout
status and performs the scaling operation if necessary. The function updates the rollout
status with the new scale values and returns whether scaling was performed.

Parameters:
  - ctx: The context for managing request-scoped values, cancellation, and timeouts.
  - rolloutObject: The rollout object containing the current rollout status.
  - liveRolloutObject: The live rollout object representing the current state.
  - existingPromotedChild: The unstructured object of the promoted child to be scaled down.
  - skipPromotedChildSourceVerticesScaling: A flag to skip scaling if set to true.
  - controller: The progressive controller handling the scaling logic.
  - c: The Kubernetes client for interacting with the cluster.

Returns:
  - A boolean indicating if scaling was performed.
  - An error if any issues occur during the scaling process.
*/
func scaleDownPromotedChild(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	liveRolloutObject ctlrcommon.RolloutObject,
	existingPromotedChild *unstructured.Unstructured,
	skipPromotedChildSourceVerticesScaling bool,
	controller progressiveController,
	c client.Client,
) (bool, error) {

	if skipPromotedChildSourceVerticesScaling {
		return false, nil
	}

	if liveRolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.AreAllSourceVerticesScaledDown(existingPromotedChild.GetName()) {
		// Return that scaling down was NOT performed
		return false, nil
	}

	// ScaleDownPromotedChildSourceVertices either updates the existingPromotedChild to scale down the source vertices pods or
	// retrieves the currently running pods to update the scaleValuesMap used on the rollout status.
	// This serves to make sure that the pods for each vertex have been really scaled down before proceeding with the progressive update.
	scaleValuesMap, promotedChildNeedsUpdate, err := controller.ScaleDownPromotedChildSourceVertices(ctx, liveRolloutObject, existingPromotedChild, c)
	if err != nil {
		return false, fmt.Errorf("error updating scaling properties to the existing promoted child definition: %w", err)
	}

	if promotedChildNeedsUpdate {
		if err = kubernetes.UpdateResource(ctx, c, existingPromotedChild); err != nil {
			return false, fmt.Errorf("error scaling down the existing promoted child: %w", err)
		}
	}

	if rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus == nil {
		rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus = &apiv1.PromotedChildStatus{}
	}
	rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.Name = existingPromotedChild.GetName()
	rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.ScaleValues = scaleValuesMap
	rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.MarkAllSourceVerticesScaledDown()

	// Set ScaleValuesRestoredToDesired to false in case previously set to true and now scaling back down to recover from a previous failure
	rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.ScaleValuesRestoredToDesired = false

	// Return that scaling down was performed
	return true, nil
}

/*
scaleUpPromotedChild scales up the promoted child of a rollout object to its desired scale values if not already done.

Parameters:
  - ctx: The context for managing request-scoped values, cancellation, and timeouts.
  - rolloutObject: The rollout object containing the desired state and status.
  - liveRolloutObject: The current live state of the rollout object.
  - existingPromotedChild: The promoted child resource to be scaled.
  - skipPromotedChildSourceVerticesScaling: Flag to skip scaling if set to true.
  - controller: The progressive controller responsible for scaling operations.
  - c: The Kubernetes client for interacting with the cluster.

Returns:
  - A boolean indicating whether scaling was performed.
  - An error if scaling fails.
*/
func scaleUpPromotedChild(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	liveRolloutObject ctlrcommon.RolloutObject,
	existingPromotedChild *unstructured.Unstructured,
	skipPromotedChildSourceVerticesScaling bool,
	controller progressiveController,
	c client.Client,
) (bool, error) {

	if skipPromotedChildSourceVerticesScaling {
		return false, nil
	}

	if liveRolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.AreScaleValuesRestoredToDesired(existingPromotedChild.GetName()) {
		// Return that scaling up was NOT performed
		return false, nil
	}

	if err := controller.ScalePromotedChildSourceVerticesToDesiredValues(ctx, liveRolloutObject, existingPromotedChild, c); err != nil {
		return false, err
	}

	if err := kubernetes.UpdateResource(ctx, c, existingPromotedChild); err != nil {
		return false, fmt.Errorf("error scaling back to desired min and max values the existing promoted child: %w", err)
	}

	rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.ScaleValuesRestoredToDesired = true

	rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.AllSourceVerticesScaledDown = false
	rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.ScaleValues = nil

	// Return that scaling up was performed
	return true, nil
}
