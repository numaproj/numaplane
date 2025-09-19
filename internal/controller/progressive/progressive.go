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
	"math"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/controller/common/riders"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/usde"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// progressiveController describes a Controller that can progressively roll out a second child alongside the original child,
// taking down the original child once the new one is healthy
type progressiveController interface {
	ctlrcommon.RolloutController

	// CreateUpgradingChildDefinition creates a Kubernetes definition for a child resource of the Rollout with the given name in an "upgrading" state
	CreateUpgradingChildDefinition(ctx context.Context, rolloutObject ProgressiveRolloutObject, name string) (*unstructured.Unstructured, error)

	// CheckForDifferences determines if the rollout-defined child definition is different from the existing child's definition
	CheckForDifferences(ctx context.Context, existingChild *unstructured.Unstructured, requiredSpec map[string]interface{}, requiredMetadata apiv1.Metadata) (bool, error)

	// CheckForDifferencesWithRolloutDef determines if the rollout-defined child definition is different from the existing child's definition
	CheckForDifferencesWithRolloutDef(ctx context.Context, existingChild *unstructured.Unstructured, rolloutObject ctlrcommon.RolloutObject) (bool, error)

	// AssessUpgradingChild determines if upgrading child is determined to be healthy, unhealthy, or unknown
	AssessUpgradingChild(ctx context.Context, rolloutObject ProgressiveRolloutObject, existingUpgradingChildDef *unstructured.Unstructured, schedule config.AssessmentSchedule) (apiv1.AssessmentResult, string, error)

	// ProcessPromotedChildPreUpgrade performs operations on the promoted child prior to the upgrade (just the operations which are unique to this Kind)
	// return true if requeue is needed (note this is ignored if error != nil)
	ProcessPromotedChildPreUpgrade(ctx context.Context, rolloutObject ProgressiveRolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) (bool, error)

	// ProcessPromotedChildPostUpgrade performs operations on the promoted child after the creation of the Upgrading child in K8S (just the operations which are unique to this Kind)
	// return true if requeue is needed (note this is ignored if error != nil)
	ProcessPromotedChildPostUpgrade(ctx context.Context, rolloutObject ProgressiveRolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) (bool, error)

	// ProcessPromotedChildPostFailure performs operations on the promoted child after the upgrade fails (just the operations which are unique to this Kind)
	// return true if requeue is needed (note this is ignored if error != nil)
	ProcessPromotedChildPostFailure(ctx context.Context, rolloutObject ProgressiveRolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) (bool, error)

	// ProcessUpgradingChildPreUpgrade performs operations on the upgrading child definition prior to its creation in K8S (just the operations which are unique to this Kind)
	// return true if requeue is needed (note this is ignored if error != nil)
	ProcessUpgradingChildPreUpgrade(ctx context.Context, rolloutObject ProgressiveRolloutObject, upgradingChildDef *unstructured.Unstructured, c client.Client) (bool, error)

	// ProcessUpgradingChildPostUpgrade performs operations on the upgrading child after its creation in K8S (just the operations which are unique to this Kind)
	// return true if requeue is needed (note this is ignored if error != nil)
	ProcessUpgradingChildPostUpgrade(ctx context.Context, rolloutObject ProgressiveRolloutObject, upgradingChildDef *unstructured.Unstructured, c client.Client) (bool, error)

	// ProcessUpgradingChildPostFailure performs operations on the upgrading child after the upgrade fails (just the operations which are unique to this Kind)
	// return true if requeue is needed (note this is ignored if error != nil)
	ProcessUpgradingChildPostFailure(ctx context.Context, rolloutObject ProgressiveRolloutObject, upgradingChildDef *unstructured.Unstructured, c client.Client) (bool, error)

	// ProcessUpgradingChildPostSuccess performs operations on the upgrading child after the upgrade succeeds (just the operations which are unique to this Kind)
	ProcessUpgradingChildPostSuccess(ctx context.Context, rolloutObject ProgressiveRolloutObject, upgradingChildDef *unstructured.Unstructured, c client.Client) error

	// ProcessPromotedChildPreRecycle performs operations on the promoted child prior to its being recycled
	//ProcessPromotedChildPreRecycle(ctx context.Context, rolloutObject ProgressiveRolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) error

	// ProcessUpgradingChildPreRecycle performs operations on the upgrading child prior to its being recycled
	//ProcessUpgradingChildPreRecycle(ctx context.Context, rolloutObject ProgressiveRolloutObject, upgradingChildDef *unstructured.Unstructured, c client.Client) error

	// ProgressiveUnsupported checks to see if Full Progressive Rollout (with assessment) is unsupported for this Rollout
	ProgressiveUnsupported(ctx context.Context, rolloutObject ProgressiveRolloutObject) bool
}

// ProgressiveRolloutObject describes a Rollout instance that supports progressive upgrade
type ProgressiveRolloutObject interface {
	ctlrcommon.RolloutObject

	GetProgressiveStrategy() apiv1.ProgressiveStrategy

	GetUpgradingChildStatus() *apiv1.UpgradingChildStatus

	GetPromotedChildStatus() *apiv1.PromotedChildStatus

	SetUpgradingChildStatus(*apiv1.UpgradingChildStatus)

	// note this resets the entire Upgrading status struct which encapsulates the UpgradingChildStatus struct
	ResetUpgradingChildStatus(upgradingChild *unstructured.Unstructured) error

	SetPromotedChildStatus(*apiv1.PromotedChildStatus)

	// note this resets the entire Promoted status struct which encapsulates the PromotedChildStatus struct
	ResetPromotedChildStatus(promotedChild *unstructured.Unstructured) error

	GetChildMetadata() apiv1.Metadata
}

// return:
// - whether we're done
// - duration indicating the requeue delay for the controller to use for next reconciliation
// - error if any
func ProcessResource(
	ctx context.Context,
	rolloutObject ProgressiveRolloutObject,
	existingPromotedChild *unstructured.Unstructured,
	promotedDifference bool,
	controller progressiveController,
	c client.Client,
) (bool, time.Duration, error) {

	// Log progressive objects if the related flag is enabled
	logObjects, err := getConfigLogObjects()
	if err != nil {
		return false, 0, err
	}
	if logObjects {
		// Add the rolloutObject and promotedChild to the logger in context and set the name
		progressiveLogger := logger.FromContext(ctx).WithName("ProgressiveUpgrade").
			WithValues(
				"rolloutObject", rolloutObject,
				"promotedChild", existingPromotedChild,
			)
		ctx = logger.WithLogger(ctx, progressiveLogger)
	}

	// Make sure that our Promoted Child Status reflects the current promoted child
	promotedChildStatus := rolloutObject.GetPromotedChildStatus()
	if promotedChildStatus == nil || promotedChildStatus.Name != existingPromotedChild.GetName() {
		err := rolloutObject.ResetPromotedChildStatus(existingPromotedChild)
		if err != nil {
			return false, 0, err
		}
	}

	// is there currently an "upgrading" child?
	currentUpgradingChildDef, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, rolloutObject, common.LabelValueUpgradeInProgress, nil, true, c)
	if err != nil {
		return false, 0, err
	}

	// if there's a difference between the desired spec and the current "promoted" child, and there isn't already an "upgrading" definition, then create one and return
	if promotedDifference && currentUpgradingChildDef == nil {
		// Create it
		_, needRequeue, err := startUpgradeProcess(ctx, rolloutObject, existingPromotedChild, controller, c)
		if needRequeue {
			return false, common.DefaultRequeueDelay, err
		} else {
			return false, 0, err
		}

	}

	// nothing to do (either there's nothing to upgrade, or we just created an "upgrading" child, and it's too early to start reconciling it)
	if currentUpgradingChildDef == nil {
		return true, 0, err
	}

	// There's already an Upgrading child, now process it

	// Add the upgradingChild to the logger in context (only if the related flag is enabled)
	if logObjects {
		ctx = logger.WithLogger(ctx, logger.FromContext(ctx).WithValues("upgradingChild", currentUpgradingChildDef))
	}

	// get UpgradingChildStatus and reset it if necessary
	childStatus, err := getUpgradingChildStatus(ctx, rolloutObject, currentUpgradingChildDef)
	if err != nil {
		return false, 0, err
	}

	// if the Upgrading child status exists but indicates that we aren't done with upgrade process, then do postupgrade process
	initializationIncomplete := !childStatus.InitializationComplete && childStatus.AssessmentResult == apiv1.AssessmentResultUnknown
	if initializationIncomplete {
		needsRequeue, err := startPostUpgradeProcess(ctx, rolloutObject, existingPromotedChild, currentUpgradingChildDef, controller, c)
		if needsRequeue {
			return false, common.DefaultRequeueDelay, err
		} else {
			return false, 0, err
		}
	}

	// determine if we need to replace the Upgrading child with a newer one
	needsRequeue, done, err := checkForUpgradeReplacement(ctx, rolloutObject, controller, existingPromotedChild, currentUpgradingChildDef, c)
	if needsRequeue {
		return false, common.DefaultRequeueDelay, err
	}
	if done {
		return true, 0, nil
	}

	done, requeueDelay, err := processUpgradingChild(ctx, rolloutObject, controller, existingPromotedChild, currentUpgradingChildDef, c)
	if err != nil {
		return false, 0, err
	}

	return done, requeueDelay, nil
}

// create the definition for the child of the Rollout which is the one labeled "upgrading"
// if there's already an existing "upgrading" child, create a definition using its name; otherwise, use a new name
func makeUpgradingObjectDefinition(ctx context.Context, rolloutObject ProgressiveRolloutObject, controller progressiveController, c client.Client, useExistingChildName bool) (*unstructured.Unstructured, error) {

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

// get the UpgradingChildStatus for the current upgrading child; if it needs to be reset, then reset it
func getUpgradingChildStatus(ctx context.Context, rolloutObject ProgressiveRolloutObject, currentUpgradingChildDef *unstructured.Unstructured) (*apiv1.UpgradingChildStatus, error) {
	numaLogger := logger.FromContext(ctx)

	// if the Upgrading child status is not for current child, reset it
	childStatus := rolloutObject.GetUpgradingChildStatus()
	// Create a new childStatus object if not present in the live rollout object or
	// if it is that of a previous progressive upgrade.
	if childStatus == nil || childStatus.Name != currentUpgradingChildDef.GetName() {
		if childStatus != nil {
			numaLogger.WithValues("name", currentUpgradingChildDef.GetName(), "childStatus", *childStatus).Debug("the live upgrading child status is stale, resetting it")
		} else {
			numaLogger.WithValues("name", currentUpgradingChildDef.GetName()).Debug("the live upgrading child status has not been set yet, initializing it")
		}

		err := rolloutObject.ResetUpgradingChildStatus(currentUpgradingChildDef)
		if err != nil {
			return nil, fmt.Errorf("processing upgrading child, failed to reset the upgrading child status for child %s/%s", currentUpgradingChildDef.GetNamespace(), currentUpgradingChildDef.GetName())
		}
	} else {
		numaLogger.WithValues("childStatus", *childStatus).Debug("live upgrading child previously set")
	}
	return rolloutObject.GetUpgradingChildStatus(), nil
}

func getAnalysisRunTimeout(ctx context.Context) (time.Duration, error) {

	numaLogger := logger.FromContext(ctx)
	globalConfig, err := config.GetConfigManagerInstance().GetConfig()
	if err != nil {
		return 0, fmt.Errorf("error getting the global config for assessment processing: %w", err)
	}
	timeout, err := globalConfig.Progressive.GetAnalysisRunTimeout()
	if err != nil {
		numaLogger.Errorf(err, "error getting AnalysisRun timeout from global config")
		return timeout, nil
	}
	return timeout, nil
}

func getChildStatusAssessmentSchedule(
	ctx context.Context,
	rolloutObject ProgressiveRolloutObject,
) (config.AssessmentSchedule, error) {
	numaLogger := logger.FromContext(ctx)
	globalConfig, err := config.GetConfigManagerInstance().GetConfig()
	if err != nil {
		return config.AssessmentSchedule{}, fmt.Errorf("error getting the global config for assessment processing: %w", err)
	}
	// get the default schedule for this kind
	schedule, err := globalConfig.Progressive.GetChildStatusAssessmentSchedule(rolloutObject.GetChildGVK().Kind)
	if err != nil {
		return config.AssessmentSchedule{}, fmt.Errorf("error getting default child status assessment schedule for type '%s': %w", rolloutObject.GetChildGVK().Kind, err)
	}
	// see if it's specified for this Rollout, and if so use that
	rolloutSpecificSchedule := rolloutObject.GetProgressiveStrategy().AssessmentSchedule
	if rolloutSpecificSchedule != "" {
		rolloutSchedule, err := config.ParseAssessmentSchedule(rolloutSpecificSchedule)
		if err != nil {
			numaLogger.WithValues("schedule", rolloutSpecificSchedule, "error", err.Error()).Warn("failed to parse schedule")
		} else {
			schedule = rolloutSchedule
			numaLogger.Debugf("using assessment schedule specified by rollout: %q", rolloutSpecificSchedule)
		}
	} else {
		numaLogger.Debugf("using default assessment schedule for kind %q", rolloutObject.GetChildGVK().Kind)
	}

	return schedule, nil
}

func getConfigLogObjects() (bool, error) {
	globalConfig, err := config.GetConfigManagerInstance().GetConfig()
	if err != nil {
		return false, fmt.Errorf("error getting the global config to retrieve logObjects: %w", err)
	}

	return globalConfig.Progressive.LogObjects, nil
}

/*
processUpgradingChild handles the assessment and potential update of a child resource during a progressive upgrade.
It evaluates the current status of the upgrading child, determines if an assessment is needed, and processes the
assessment result.

Parameters:
- ctx: The context for managing request-scoped values, cancellation, and timeouts.
- rolloutObject: The current rollout object (this could be from cache).
- controller: The progressive controller responsible for managing the upgrade process.
- existingPromotedChildDef: The definition of the currently promoted child resource.
- existingUpgradingChildDef: The definition of the child resource currently being upgraded.
- c: The Kubernetes client for interacting with the cluster.

Returns:
- A boolean indicating if the upgrade is done.
- A duration indicating the requeue delay for the controller to use for next reconciliation.
- An error if any issues occur during the process.
*/
func processUpgradingChild(
	ctx context.Context,
	rolloutObject ProgressiveRolloutObject,
	controller progressiveController,
	existingPromotedChildDef, existingUpgradingChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, time.Duration, error) {

	numaLogger := logger.FromContext(ctx)

	assessmentSchedule, err := getChildStatusAssessmentSchedule(ctx, rolloutObject)
	if err != nil {
		return false, 0, err
	}

	childStatus := rolloutObject.GetUpgradingChildStatus()

	forcePromote := false
	_, ok := existingUpgradingChildDef.GetLabels()[common.LabelKeyForcePromote]
	if ok {
		forcePromote = true
	}

	// check for Force Promote set in Progressive strategy to force success logic OR if upgrading child has force-promote label
	if rolloutObject.GetProgressiveStrategy().ForcePromote || forcePromote || controller.ProgressiveUnsupported(ctx, rolloutObject) {
		childStatus.ForcedSuccess = true

		done, err := declareSuccess(ctx, rolloutObject, controller, existingPromotedChildDef, existingUpgradingChildDef, childStatus, c)
		if err != nil || done {
			return done, 0, err
		} else {
			return done, assessmentSchedule.Interval, err
		}
	}

	// If no AssessmentStartTime has been set already, calculate it and set it
	if childStatus.BasicAssessmentStartTime == nil {
		// Add to the current time the assessmentSchedule.Delay and set the AssessmentStartTime in the Rollout object
		childStatus = UpdateUpgradingChildStatus(rolloutObject, func(status *apiv1.UpgradingChildStatus) {
			nextAssessmentTime := metav1.NewTime(time.Now().Add(assessmentSchedule.Delay))
			status.BasicAssessmentStartTime = &nextAssessmentTime
		})
		numaLogger.WithValues("childStatus", *childStatus).Debug("set upgrading child AssessmentStartTime")
	}

	// Assess the upgrading child status only if within the assessment time window and if not previously failed.
	// Otherwise, assess the previous child status.
	assessment := childStatus.AssessmentResult
	failureReason := childStatus.FailureReason
	childSts := childStatus.ChildStatus.Raw
	if childStatus.CanAssess() {
		assessment, failureReason, err = controller.AssessUpgradingChild(ctx, rolloutObject, existingUpgradingChildDef, assessmentSchedule)
		if err != nil {
			return false, 0, err
		}
		childSts, err = json.Marshal(existingUpgradingChildDef.Object["status"])
		if err != nil {
			return false, 0, err
		}

		numaLogger.WithValues("name", existingUpgradingChildDef.GetName(), "childStatus", *childStatus, "assessment", assessment).
			Debugf("performing upgrading child assessment, assessment returned: %v", assessment)
	} else {
		numaLogger.WithValues("name", existingUpgradingChildDef.GetName(), "childStatus", *childStatus, "assessment", assessment).
			Debug("skipping upgrading child assessment but assessing previous child status")
	}

	switch assessment {
	case apiv1.AssessmentResultFailure:
		rolloutObject.GetRolloutStatus().MarkProgressiveUpgradeFailed(fmt.Sprintf("New Child Object %s/%s Failed", existingUpgradingChildDef.GetNamespace(), existingUpgradingChildDef.GetName()), rolloutObject.GetRolloutObjectMeta().Generation)

		_ = UpdateUpgradingChildStatus(rolloutObject, func(status *apiv1.UpgradingChildStatus) {
			status.AssessmentResult = apiv1.AssessmentResultFailure
			status.FailureReason = failureReason
			status.ChildStatus.Raw = childSts
		})

		requeue, err := controller.ProcessPromotedChildPostFailure(ctx, rolloutObject, existingPromotedChildDef, c)
		if err != nil {
			return false, 0, err
		}
		if requeue {
			return false, common.DefaultRequeueDelay, nil
		}
		requeue, err = controller.ProcessUpgradingChildPostFailure(ctx, rolloutObject, existingUpgradingChildDef, c)
		if err != nil {
			return false, 0, err
		}
		if requeue {
			return false, common.DefaultRequeueDelay, nil
		}

		return false, 0, nil

	case apiv1.AssessmentResultSuccess:
		done, err := declareSuccess(ctx, rolloutObject, controller, existingPromotedChildDef, existingUpgradingChildDef, childStatus, c)
		if err != nil || done {
			return done, 0, err
		} else {
			return done, assessmentSchedule.Interval, err
		}

	default:
		_ = UpdateUpgradingChildStatus(rolloutObject, func(status *apiv1.UpgradingChildStatus) {
			status.AssessmentResult = apiv1.AssessmentResultUnknown
		})

		return false, assessmentSchedule.Interval, nil
	}
}

// checkForUpgradeReplacement checks to see if we need to replace the current Upgrading child one with a new one because the spec has changed,
// and if so, performs the replacement, thereby recycling the old one and starting upgrade on the new one
// Returns:
// - A boolean indicating we need to requeue
// - A boolean indicating if the upgrade is done
// - Error if any
func checkForUpgradeReplacement(
	ctx context.Context,
	rolloutObject ProgressiveRolloutObject,
	controller progressiveController,
	existingPromotedChildDef, existingUpgradingChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, bool, error) {
	numaLogger := logger.FromContext(ctx)

	childStatus := rolloutObject.GetUpgradingChildStatus()

	// Compare our new spec to:
	// 1. the existing Upgrading child definition
	// 2. the existing Promoted child definition
	// If the new one is different from the existing Upgrading one:
	//  Then if the new one matches the existing Promoted one: remove the Upgrading one
	//  Else replace the Upgrading one with a new one
	newUpgradingChildDef, err := makeUpgradingObjectDefinition(ctx, rolloutObject, controller, c, true)
	if err != nil {
		return false, false, err
	}

	differentFromExistingUpgrading, err := checkForDifferences(ctx, controller, rolloutObject, existingUpgradingChildDef, true, newUpgradingChildDef)
	if err != nil {
		return false, false, err
	}

	differentFromPromoted, err := checkForDifferences(ctx, controller, rolloutObject, existingPromotedChildDef, false, newUpgradingChildDef)
	if err != nil {
		return false, false, err
	}

	if differentFromExistingUpgrading {

		// prepare existing upgrading child for Recycle
		/*err = controller.ProcessUpgradingChildPreRecycle(ctx, rolloutObject, existingUpgradingChildDef, c)
		if err != nil {
			return false, false, err
		}*/

		if differentFromPromoted {

			// recycle the old one
			reason := common.LabelValueProgressiveReplaced
			if childStatus.AssessmentResult == apiv1.AssessmentResultFailure {
				reason = common.LabelValueProgressiveReplacedFailed
			}
			err = ctlrcommon.UpdateUpgradeState(ctx, c, common.LabelValueUpgradeRecyclable, &reason, existingUpgradingChildDef)
			if err != nil {
				return false, false, err
			}

			// Create a new upgrading child to replace it

			needRequeue := false
			newUpgradingChildDef, needRequeue, err = startUpgradeProcess(ctx, rolloutObject, existingPromotedChildDef, controller, c)
			if err != nil {
				return false, false, err
			}
			if newUpgradingChildDef != nil {
				numaLogger.WithValues("old child", existingUpgradingChildDef.GetName(), "new child", newUpgradingChildDef.GetName()).Debug("replacing 'upgrading' child")
			}
			if needRequeue {
				return true, false, nil
			}

			childStatus = rolloutObject.GetUpgradingChildStatus() // update childStatus to reflect new child
		} else {
			numaLogger.WithValues("old child", existingUpgradingChildDef.GetName()).Debug("removing 'upgrading' child as Rollout is back to matching 'promoted' child")

			// Discontinue the Progressive upgrade altogether
			return false, true, Discontinue(ctx, rolloutObject, controller, c)
		}
	}

	// After creating the new Upgrading child, do post-upgrade process (check that AssessmentResult is not set just in case) and return
	if !childStatus.InitializationComplete && childStatus.AssessmentResult == apiv1.AssessmentResultUnknown {

		needsRequeue, err := startPostUpgradeProcess(ctx, rolloutObject, existingPromotedChildDef, newUpgradingChildDef, controller, c)
		if needsRequeue {
			return true, false, err
		}
	}

	// if we just created a new upgrading child, we should requeue
	return differentFromExistingUpgrading, false, nil
}

// does our Rollout need updating?
// compare the latest and greatest spec with either the existing "promoted" child or the existing "upgrading" child
// this could include either the main child definition or a Rider definition
func checkForDifferences(
	ctx context.Context,
	controller progressiveController,
	rolloutObject ProgressiveRolloutObject,
	existingChildDef *unstructured.Unstructured,
	existingIsUpgrading bool, // is the existing child "Upgrading" (vs "Promoted")?
	newUpgradingChildDef *unstructured.Unstructured) (bool, error) {

	needsUpdating := false

	childNeedsUpdating, err := controller.CheckForDifferences(ctx, existingChildDef, newUpgradingChildDef.Object, rolloutObject.GetChildMetadata())
	if err != nil {
		return false, err
	}
	if childNeedsUpdating {
		needsUpdating = childNeedsUpdating
	} else {
		// if child doesn't need updating, let's see if any Riders do
		// (additions, modifications, or deletions)
		needsUpdating, err = checkRidersForDifferences(ctx, controller, rolloutObject, existingChildDef, existingIsUpgrading, newUpgradingChildDef)
		if err != nil {
			return false, err
		}
	}
	return needsUpdating, nil
}

// Do any Riders need updating? (including additions, modifications, or deletions)
// Compare the Riders which would be derived from the latest and greatest spec with those of either the existing "promoted" child or the existing "upgrading" child
func checkRidersForDifferences(
	ctx context.Context,
	controller progressiveController,
	rolloutObject ctlrcommon.RolloutObject,
	existingChildDef *unstructured.Unstructured,
	existingIsUpgrading bool, // is the existing child "Upgrading" (vs "Promoted")?
	newUpgradingChildDef *unstructured.Unstructured) (bool, error) {
	newRiders, err := controller.GetDesiredRiders(rolloutObject, existingChildDef.GetName(), newUpgradingChildDef)
	if err != nil {
		return false, err
	}

	existingRiders, err := controller.GetExistingRiders(ctx, rolloutObject, existingIsUpgrading)
	if err != nil {
		return false, err
	}

	needUpdating, _, _, _, _, err := usde.RidersNeedUpdating(ctx, existingChildDef.GetNamespace(), existingChildDef.GetKind(), existingChildDef.GetName(),
		newRiders, existingRiders)
	if err != nil {
		return false, err
	}
	return needUpdating, nil
}

// PerformResourceHealthCheckForPipelineType makes an assessment of the upgrading child (either Pipeline or MonoVertex) to determine
// if it was successful, failed, or still not known
// Return assessment result along with reason (string) if it failed
//
// Assessment result:
// Success: phase must be "Running" and all conditions must be True
// Failure: phase is "Failed" or any condition is False
// Unknown: neither of the above if met
func PerformResourceHealthCheckForPipelineType(
	ctx context.Context,
	existingUpgradingChildDef *unstructured.Unstructured,
	verifyReplicasFunc func(existingUpgradingChildDef *unstructured.Unstructured) (bool, string, error),
) (apiv1.AssessmentResult, string, error) {

	numaLogger := logger.FromContext(ctx)

	upgradingObjectStatus, err := kubernetes.ParseStatus(existingUpgradingChildDef)
	if err != nil {
		return apiv1.AssessmentResultUnknown, "", err
	}

	healthyConditions, failedCondition := checkChildConditions(&upgradingObjectStatus)

	healthyReplicas, replicasFailureReason, err := verifyReplicasFunc(existingUpgradingChildDef)
	if err != nil {
		return apiv1.AssessmentResultUnknown, replicasFailureReason, err
	}

	numaLogger.
		WithValues("namespace", existingUpgradingChildDef.GetNamespace(), "name", existingUpgradingChildDef.GetName()).
		Debugf("Upgrading child is in phase %s, conditions healthy=%t, ready replicas match desired replicas=%t", upgradingObjectStatus.Phase, healthyConditions, healthyReplicas)

	if upgradingObjectStatus.Phase == "Failed" || !healthyConditions || !healthyReplicas {
		failureReason := CalculateFailureReason(replicasFailureReason, upgradingObjectStatus.Phase, failedCondition)
		return apiv1.AssessmentResultFailure, failureReason, nil
	}

	phaseHealthy, err := IsPipelineTypePhaseHealthy(ctx, existingUpgradingChildDef)
	if err != nil {
		return apiv1.AssessmentResultUnknown, "", err
	}

	// conduct standard health assessment first
	if phaseHealthy && healthyConditions && healthyReplicas {
		return apiv1.AssessmentResultSuccess, "", nil
	}

	return apiv1.AssessmentResultUnknown, "", nil
}

func IsPipelineTypePhaseHealthy(ctx context.Context, existingUpgradingChildDef *unstructured.Unstructured) (bool, error) {

	upgradingObjectStatus, err := kubernetes.ParseStatus(existingUpgradingChildDef)
	if err != nil {
		return false, err
	}

	// if the desired phase is Paused, it's fine for the phase to be Paused or Pausing
	desiredPhaseSetting, err := numaflowtypes.GetPipelineDesiredPhase(existingUpgradingChildDef)
	if err != nil {
		return false, err
	}
	if desiredPhaseSetting == string(numaflowv1.PipelinePhasePaused) &&
		(upgradingObjectStatus.Phase == string(numaflowv1.PipelinePhasePaused) || upgradingObjectStatus.Phase == string(numaflowv1.PipelinePhasePausing)) {
		return true, nil
	}

	return upgradingObjectStatus.Phase == string(numaflowv1.PipelinePhaseRunning), nil

}

// CalculateFailureReason issues a reason for failure; if there are multiple reasons, it returns one of them
func CalculateFailureReason(replicasFailureReason, phase string, failedCondition *metav1.Condition) string {
	if phase == "Failed" {
		return "child phase is in Failed state"
	} else if failedCondition != nil {
		return fmt.Sprintf("condition %s is False for Reason: %s", failedCondition.Type, failedCondition.Reason)
	} else {
		return replicasFailureReason
	}
}

/*
declareSuccess handles the success logic for the update of a child resource during a progressive upgrade.
It patches both the existing and upgrading children, updates the child status and marks the rollout as deployed.

Parameters:
- ctx: The context for managing request-scoped values, cancellation, and timeouts.
- rolloutObject: The current rollout object.
- existingPromotedChildDef: The definition of the currently promoted child resource.
- existingUpgradingChildDef: The definition of the child resource currently being upgraded.
- childStatus: The status of the child resource currently being upgraded (from the Rollout CR)
- c: The Kubernetes client for interacting with the cluster.

Returns:
- A boolean indicating if the upgrade is done.
- An error if any issues occur during the process.
*/
func declareSuccess(
	ctx context.Context,
	rolloutObject ProgressiveRolloutObject,
	controller progressiveController,
	existingPromotedChildDef, existingUpgradingChildDef *unstructured.Unstructured,
	childStatus *apiv1.UpgradingChildStatus,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx)

	err := controller.ProcessUpgradingChildPostSuccess(ctx, rolloutObject, existingUpgradingChildDef, c)
	if err != nil {
		return false, err
	}

	// Label the new child as promoted and then remove the label from the old one
	numaLogger.WithValues("old child", existingPromotedChildDef.GetName(), "new child", existingUpgradingChildDef.GetName()).Debug("replacing 'promoted' child")
	reasonSuccess := common.LabelValueProgressiveSuccess
	err = ctlrcommon.UpdateUpgradeState(ctx, c, common.LabelValueUpgradePromoted, &reasonSuccess, existingUpgradingChildDef)
	if err != nil {
		return false, err
	}

	/*err = controller.ProcessPromotedChildPreRecycle(ctx, rolloutObject, existingPromotedChildDef, c)
	if err != nil {
		return false, err
	}*/
	err = ctlrcommon.UpdateUpgradeState(ctx, c, common.LabelValueUpgradeRecyclable, &reasonSuccess, existingPromotedChildDef)
	if err != nil {
		return false, err
	}

	rolloutObject.GetRolloutStatus().MarkProgressiveUpgradeSucceeded(fmt.Sprintf("New Child Object %s/%s Running", existingUpgradingChildDef.GetNamespace(), existingUpgradingChildDef.GetName()), rolloutObject.GetRolloutObjectMeta().Generation)
	childStatus.AssessmentResult = apiv1.AssessmentResultSuccess
	rolloutObject.SetUpgradingChildStatus(childStatus)
	rolloutObject.GetRolloutStatus().MarkDeployed(rolloutObject.GetRolloutObjectMeta().Generation)

	// we're done
	return true, nil
}

// startUpgradeProcess() is the process required to create a new Upgrading child as well as
// everything that must be done beforehand.
// Critically, the last part of this function is the creation of the child.
// Therefore, it should be called if that child has not yet been created.
// return:
// - the new child we created, if any
// - whether we need to requeue
// - error if any
func startUpgradeProcess(
	ctx context.Context,
	rolloutObject ProgressiveRolloutObject,
	existingPromotedChild *unstructured.Unstructured,
	controller progressiveController,
	c client.Client,
) (*unstructured.Unstructured, bool, error) {
	numaLogger := logger.FromContext(ctx)

	numaLogger.WithValues("promoted child", existingPromotedChild.GetName()).Debug("starting upgrade process")

	requeue, err := controller.ProcessPromotedChildPreUpgrade(ctx, rolloutObject, existingPromotedChild, c)
	if err != nil {
		return nil, false, err
	}
	if requeue {
		return nil, true, nil
	}

	// create object as it doesn't exist
	newUpgradingChildDef, err := makeUpgradingObjectDefinition(ctx, rolloutObject, controller, c, false)
	if err != nil {
		return newUpgradingChildDef, false, err
	}
	// reset the Status to reflect our new Upgrading child
	err = rolloutObject.ResetUpgradingChildStatus(newUpgradingChildDef)
	if err != nil {
		return newUpgradingChildDef, false, fmt.Errorf("processing upgrading child, failed to reset the upgrading child status for child %s/%s", newUpgradingChildDef.GetNamespace(), newUpgradingChildDef.GetName())
	}

	requeue, err = controller.ProcessUpgradingChildPreUpgrade(ctx, rolloutObject, newUpgradingChildDef, c)
	if err != nil {
		return newUpgradingChildDef, false, err
	}
	if requeue {
		return newUpgradingChildDef, true, nil
	}

	// Critically, the last part of this function must be the creation of the child, since this is
	// only guaranteed to be called up until that child has been created.

	numaLogger.Debugf("Upgrading child of type %s %s/%s doesn't exist so creating", newUpgradingChildDef.GetKind(), newUpgradingChildDef.GetNamespace(), newUpgradingChildDef.GetName())
	err = kubernetes.CreateResource(ctx, c, newUpgradingChildDef)

	return newUpgradingChildDef, false, err
}

func startPostUpgradeProcess(
	ctx context.Context,
	rolloutObject ProgressiveRolloutObject,
	existingPromotedChild *unstructured.Unstructured,
	newUpgradingChild *unstructured.Unstructured,
	controller progressiveController,
	c client.Client,
) (bool, error) {
	numaLogger := logger.FromContext(ctx).WithValues(
		"promoted child", existingPromotedChild.GetName(),
		"upgrading child", newUpgradingChild.GetName())

	numaLogger.Debug("starting post upgrade process")

	// Create Riders for the new Upgrading child
	newRiders, err := controller.GetDesiredRiders(rolloutObject, newUpgradingChild.GetName(), newUpgradingChild)
	if err != nil {
		return false, err
	}
	riderAdditions := unstructured.UnstructuredList{}
	riderAdditions.Items = make([]unstructured.Unstructured, len(newRiders))
	for index, rider := range newRiders {
		riderAdditions.Items[index] = rider.Definition
	}
	if err = riders.UpdateRidersInK8S(ctx, newUpgradingChild, riderAdditions, unstructured.UnstructuredList{}, unstructured.UnstructuredList{}, c); err != nil {
		return false, err
	}

	requeue, err := controller.ProcessPromotedChildPostUpgrade(ctx, rolloutObject, existingPromotedChild, c)
	if err != nil {
		return false, err
	}
	if requeue {
		return true, nil
	}
	requeue, err = controller.ProcessUpgradingChildPostUpgrade(ctx, rolloutObject, newUpgradingChild, c)
	if err != nil {
		return false, err
	}
	if requeue {
		return true, nil
	}

	// set Upgrading Child Status
	_ = UpdateUpgradingChildStatus(rolloutObject, func(status *apiv1.UpgradingChildStatus) {
		status.InitializationComplete = true
		status.Riders = make([]apiv1.RiderStatus, len(newRiders))
		for i, rider := range newRiders {
			status.Riders[i] = apiv1.RiderStatus{
				GroupVersionKind: kubernetes.SchemaGVKToMetaGVK(rider.Definition.GroupVersionKind()),
				Name:             rider.Definition.GetName(),
			}
		}
	})

	return false, err

}

// return true if all Conditions are true
func checkChildConditions(upgradingObjectStatus *kubernetes.GenericStatus) (bool, *metav1.Condition) {
	if len(upgradingObjectStatus.Conditions) == 0 {
		return false, nil
	}
	for _, c := range upgradingObjectStatus.Conditions {
		if c.Status != metav1.ConditionTrue {
			return false, &c
		}
	}
	return true, nil
}

func CalculateScaleMinMaxValues(podsCount int) int64 {
	return int64(math.Floor(float64(podsCount) / float64(2)))
}

// ExtractScaleMinMaxAsJSONString returns a JSON string of the scale definition
// including only min and max fields extracted from the given unstructured object.
// It returns "null" if the pathToScale is not found.
func ExtractScaleMinMaxAsJSONString(object map[string]any, pathToScale []string) (string, error) {
	scaleDef, foundScale, err := unstructured.NestedMap(object, pathToScale...)
	if err != nil {
		return "", err
	}

	if !foundScale {
		return "null", nil
	}

	scaleMinMax := map[string]any{
		"min": scaleDef["min"],
		"max": scaleDef["max"],
	}

	jsonBytes, err := json.Marshal(scaleMinMax)
	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}

/*
AreVertexReplicasReady checks if the number of ready replicas of a vertex matches or exceeds the desired replicas.

Parameters:
  - existingUpgradingChildDef: An unstructured object representing the vertex whose replica status is being checked.

Returns:
  - A boolean indicating whether the ready replicas are sufficient.
  - In the case that ready replicas aren't sufficient, a message providing more information
*/
func AreVertexReplicasReady(existingUpgradingChildDef *unstructured.Unstructured) (bool, string, error) {
	desiredReplicas, _, err := unstructured.NestedInt64(existingUpgradingChildDef.Object, "status", "desiredReplicas")
	if err != nil {
		return false, "", err
	}

	readyReplicas, _, err := unstructured.NestedInt64(existingUpgradingChildDef.Object, "status", "readyReplicas")
	if err != nil {
		return false, "", err
	}

	if readyReplicas >= desiredReplicas {
		return true, "", nil
	}

	return false, fmt.Sprintf("readyReplicas=%d is less than desiredReplicas=%d", readyReplicas, desiredReplicas), nil
}

// For a given Rollout, stop the Progressive upgrade
// Essentially, this just means removing any Upgrading children
func Discontinue(ctx context.Context,
	rolloutObject ProgressiveRolloutObject,
	controller progressiveController,
	c client.Client,
) error {

	// mark the upgrading child in the Status as "Discontinued"
	upgradingChildStatus := rolloutObject.GetUpgradingChildStatus()
	if upgradingChildStatus != nil {
		UpdateUpgradingChildStatus(rolloutObject, func(status *apiv1.UpgradingChildStatus) {
			status.Discontinued = true
		})
	}

	// Generally, there should just be one Upgrading child, but just in case there's more than 1, mark all of them recyclable
	upgradingChildren, err := ctlrcommon.FindChildrenOfUpgradeState(ctx, rolloutObject, common.LabelValueUpgradeInProgress, nil, true, c)
	if err != nil {
		return fmt.Errorf("failed to Discontinue progressive upgrade: error looking for Upgrading children of rollout %s: %v", rolloutObject.GetRolloutObjectMeta().Name, err)
	}

	for _, child := range upgradingChildren.Items {
		reason := common.LabelValueDiscontinueProgressive
		err = ctlrcommon.UpdateUpgradeState(ctx, c, common.LabelValueUpgradeRecyclable, &reason, &child)
		if err != nil {
			return fmt.Errorf("failed to Discontinue progressive upgrade: error marking child %s recyclable: %v", child.GetName(), err)
		}
	}

	// We need to set our Progressive Upgrade Condition back to its default state of true since we're no longer
	// doing a Progressive upgrade
	rolloutObject.GetRolloutStatus().MarkProgressiveUpgradeSucceeded("Discontinued", rolloutObject.GetRolloutObjectMeta().Generation)
	rolloutObject.GetRolloutStatus().MarkDeployed(rolloutObject.GetRolloutObjectMeta().Generation)

	return nil
}

func UpdateUpgradingChildStatus(rollout ProgressiveRolloutObject, f func(*apiv1.UpgradingChildStatus)) *apiv1.UpgradingChildStatus {
	upgradingChildStatus := rollout.GetUpgradingChildStatus()
	f(upgradingChildStatus)
	rollout.SetUpgradingChildStatus(upgradingChildStatus)
	return upgradingChildStatus
}
