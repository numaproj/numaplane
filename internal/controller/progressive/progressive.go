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
	"math"
	"strconv"
	"strings"
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
	controller progressiveController,
	c client.Client,
) (bool, bool, time.Duration, error) {

	numaLogger := logger.FromContext(ctx)

	// is there currently an "upgrading" child?
	currentUpgradingChildDef, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, rolloutObject, common.LabelValueUpgradeInProgress, false, c)
	if err != nil {
		return false, false, 0, err
	}

	// if there's a difference between the desired spec and the current "promoted" child, and there isn't already an "upgrading" definition, then create one and return
	if promotedDifference && currentUpgradingChildDef == nil {
		// Create it, first making sure one doesn't already exist by checking the live K8S API
		currentUpgradingChildDef, err = ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, rolloutObject, common.LabelValueUpgradeInProgress, true, c)
		if err != nil {
			return false, false, 0, fmt.Errorf("error getting %s: %v", currentUpgradingChildDef.GetKind(), err)
		}
		if currentUpgradingChildDef == nil {
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

	done, newChild, requeueDelay, err := processUpgradingChild(ctx, rolloutObject, liveRolloutObject, controller, existingPromotedChild, currentUpgradingChildDef, c)
	if err != nil {
		return false, newChild, 0, err
	}

	return done, newChild, requeueDelay, nil
}

// create the definition for the child of the Rollout which is the one labeled "upgrading"
// if there's already an existing "upgrading" child, create a definition using its name; otherwise, use a new name
func makeUpgradingObjectDefinition(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, controller progressiveController, c client.Client, useExistingChildName bool) (*unstructured.Unstructured, error) {

	numaLogger := logger.FromContext(ctx)

	childName, err := ctlrcommon.GetChildName(ctx, rolloutObject, controller, common.LabelValueUpgradeInProgress, c, useExistingChildName)
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

func FindChildrenOfUpgradeState(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, upgradeState common.UpgradeState, checkLive bool, c client.Client) (*unstructured.UnstructuredList, error) {
	childGVR := rolloutObject.GetChildGVR()

	labelSelector := fmt.Sprintf(
		"%s=%s,%s=%s", common.LabelKeyParentRollout, rolloutObject.GetRolloutObjectMeta().Name,
		common.LabelKeyUpgradeState, string(upgradeState))

	var children *unstructured.UnstructuredList
	var err error
	if checkLive {
		children, err = kubernetes.ListLiveResource(
			ctx, childGVR.Group, childGVR.Version, childGVR.Resource,
			rolloutObject.GetRolloutObjectMeta().Namespace, labelSelector, "")
	} else {
		children, err = kubernetes.ListResources(ctx, c, rolloutObject.GetChildGVK(), rolloutObject.GetRolloutObjectMeta().GetNamespace(),
			client.MatchingLabels{
				common.LabelKeyParentRollout: rolloutObject.GetRolloutObjectMeta().Name,
				common.LabelKeyUpgradeState:  string(upgradeState),
			},
		)
	}

	return children, err
}

// find the most current child of a Rollout
// typically we should only find one, but perhaps a previous reconciliation failure could cause us to find multiple
// if we do see older ones, recycle them
func FindMostCurrentChildOfUpgradeState(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, upgradeState common.UpgradeState, checkLive bool, c client.Client) (*unstructured.Unstructured, error) {
	numaLogger := logger.FromContext(ctx)

	children, err := FindChildrenOfUpgradeState(ctx, rolloutObject, upgradeState, checkLive, c)
	if err != nil {
		return nil, err
	}

	numaLogger.Debugf("looking for children of Rollout %s/%s of upgrade state=%v, found: %s",
		rolloutObject.GetRolloutObjectMeta().Namespace, rolloutObject.GetRolloutObjectMeta().Name, upgradeState, kubernetes.ExtractResourceNames(children))

	if len(children.Items) > 1 {
		var mostCurrentChild *unstructured.Unstructured
		recycleList := []*unstructured.Unstructured{}
		mostCurrentIndex := math.MinInt
		for _, child := range children.Items {
			childIndex, err := getChildIndex(rolloutObject.GetRolloutObjectMeta().Name, child.GetName())
			if err != nil {
				// something is improperly named for some reason - don't touch it just in case?
				numaLogger.Warn(err.Error())
				continue
			}
			if mostCurrentChild == nil { // first one in the list
				mostCurrentChild = &child
				mostCurrentIndex = childIndex
			} else if childIndex > mostCurrentIndex { // most current for now
				recycleList = append(recycleList, mostCurrentChild) // recycle the previous one
				mostCurrentChild = &child
				mostCurrentIndex = childIndex
			} else {
				recycleList = append(recycleList, &child)
			}
		}
		// recycle the previous children
		for _, recyclableChild := range recycleList {
			numaLogger.Debugf("found multiple children of Rollout %s/%s of upgrade state=%q, marking recyclable: %s",
				rolloutObject.GetRolloutObjectMeta().Namespace, rolloutObject.GetRolloutObjectMeta().Name, upgradeState, recyclableChild.GetName())
			_ = updateUpgradeState(ctx, c, common.LabelValueUpgradeRecyclable, recyclableChild)
		}
		return mostCurrentChild, nil
	} else if len(children.Items) == 1 {
		return &children.Items[0], nil
	} else {
		return nil, nil
	}
}

// Get the index of the child following the dash in the name
// childName should be the rolloutName + '-<integer>'
// For backward compatibility, support child resources whose names were equivalent to rollout names, returning -1 index
func getChildIndex(rolloutName string, childName string) (int, error) {
	// verify that the initial part of the child name is the rolloutName
	if !strings.HasPrefix(childName, rolloutName) {
		return 0, fmt.Errorf("child name %q should begin with rollout name %q", childName, rolloutName)
	}
	// backward compatibility for older naming convention (before the '-<integer>' suffix was introduced - if it's the same name, consider it to essentially be the smallest index
	if childName == rolloutName {
		return -1, nil
	}

	// next character should be a dash
	dash := childName[len(rolloutName)]
	if dash != '-' {
		return 0, fmt.Errorf("child name %q should begin with rollout name %q, followed by '-<integer>'", childName, rolloutName)
	}

	// remaining characters should be the integer index
	suffix := childName[len(rolloutName)+1:]

	childIndex, err := strconv.Atoi(suffix)
	if err != nil {
		return 0, fmt.Errorf("child name %q has a suffix which is not an integer", childName)
	}
	return childIndex, nil
}

// get the name of the child whose parent is "rolloutObject" and whose upgrade state is "upgradeState"
// if none is found, create a new one
// if one is found, create a new one if "useExistingChild=false", else use existing one
func GetChildName(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, controller progressiveController, upgradeState common.UpgradeState, c client.Client, useExistingChild bool) (string, error) {

	existingChild, err := FindMostCurrentChildOfUpgradeState(ctx, rolloutObject, upgradeState, true, c) // if for some reason there's more than 1
	if err != nil {
		return "", err
	}
	// if existing child doesn't exist or if it does but we don't want to use it, then create a new one
	if existingChild == nil || (existingChild != nil && !useExistingChild) {
		index, err := controller.IncrementChildCount(ctx, rolloutObject)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s-%d", rolloutObject.GetRolloutObjectMeta().Name, index), nil
	} else {
		return existingChild.GetName(), nil
	}
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

		childStatus = &apiv1.ChildStatus{
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
			// create a definition for the "upgrading" child which has a new name (the definition created above had the previous child's name which was necessary for comparison)
			newUpgradingChildDef, err = makeUpgradingObjectDefinition(ctx, rolloutObject, controller, c, false)
			if err != nil {
				return false, false, 0, err
			}

			numaLogger.WithValues("old child", existingUpgradingChildDef.GetName(), "new child", newUpgradingChildDef.GetName()).Debug("replacing 'upgrading' child")
			err = ctlrcommon.UpdateUpgradeState(ctx, c, common.LabelValueUpgradeRecyclable, existingUpgradingChildDef)
			if err != nil {
				return false, false, 0, err
			}

			err = kubernetes.CreateResource(ctx, c, newUpgradingChildDef)
			return false, true, 0, err
		}

		return false, false, 0, nil

	case apiv1.AssessmentResultSuccess:
		// Label the new child as promoted and then remove the label from the old one
		numaLogger.WithValues("old child", existingPromotedChildDef.GetName(), "new child", existingUpgradingChildDef.GetName(), "replacing 'promoted' child")
		err := ctlrcommon.UpdateUpgradeState(ctx, c, common.LabelValueUpgradePromoted, existingUpgradingChildDef)
		if err != nil {
			return false, false, 0, err
		}

		err = ctlrcommon.UpdateUpgradeState(ctx, c, common.LabelValueUpgradeRecyclable, existingPromotedChildDef)
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
