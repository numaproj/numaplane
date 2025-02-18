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
	CreateUpgradingChildDefinition(ctx context.Context, rolloutObject ProgressiveRolloutObject, name string) (*unstructured.Unstructured, error)

	// ChildNeedsUpdating determines if the difference between the current child definition and the desired child definition requires an update
	ChildNeedsUpdating(ctx context.Context, existingChild, newChildDefinition *unstructured.Unstructured) (bool, error)

	// AssessUpgradingChild determines if upgrading child is determined to be healthy, unhealthy, or unknown
	AssessUpgradingChild(ctx context.Context, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, error)

	// ProcessPromotedChildPreUpgrade performs operations on the promoted child prior to the upgrade
	ProcessPromotedChildPreUpgrade(ctx context.Context, rolloutObject ProgressiveRolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) (bool, error)

	// ProcessPromotedChildPostFailure performs operations on the promoted child after the upgrade fails
	ProcessPromotedChildPostFailure(ctx context.Context, rolloutObject ProgressiveRolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) (bool, error)
}

// ProgressiveRolloutObject describes a Rollout instance that supports progressive upgrade
type ProgressiveRolloutObject interface {
	ctlrcommon.RolloutObject

	GetUpgradingChildStatus() *apiv1.UpgradingChildStatus

	GetPromotedChildStatus() *apiv1.PromotedChildStatus

	SetUpgradingChildStatus(*apiv1.UpgradingChildStatus)

	// note this resets the entire Upgrading status struct which encapsulates the UpgradingChildStatus struct
	ResetUpgradingChildStatus(upgradingChild *unstructured.Unstructured) error

	SetPromotedChildStatus(*apiv1.PromotedChildStatus)

	// note this resets the entire Promoted status struct which encapsulates the PromotedChildStatus struct
	ResetPromotedChildStatus(promotedChild *unstructured.Unstructured) error
}

// return:
// - whether we're done
// - whether we just created a new child
// - duration indicating the requeue delay for the controller to use for next reconciliation
// - error if any
func ProcessResource(
	ctx context.Context,
	rolloutObject ProgressiveRolloutObject,
	existingPromotedChild *unstructured.Unstructured,
	promotedDifference bool,
	controller progressiveController,
	c client.Client,
) (bool, bool, time.Duration, error) {

	//numaLogger := logger.FromContext(ctx)

	// Make sure that our Promoted Child Status reflects the current promoted child
	promotedChildStatus := rolloutObject.GetPromotedChildStatus()
	if promotedChildStatus == nil || promotedChildStatus.Name != existingPromotedChild.GetName() {
		err := rolloutObject.ResetPromotedChildStatus(existingPromotedChild)
		if err != nil {
			return false, false, 0, err
		}
	}

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
			newUpgradingChildDef, needRequeue, err := startUpgradeProcess(ctx, rolloutObject, existingPromotedChild, controller, c)
			if needRequeue {
				return false, newUpgradingChildDef != nil, common.DefaultRequeueDelay, err
			} else {
				return false, newUpgradingChildDef != nil, 0, err
			}
			/*
				requeue, err := controller.ProcessPromotedChildPreUpgrade(ctx, rolloutObject, existingPromotedChild, c)
				if err != nil {
					return false, false, 0, err
				}
				if requeue {
					return false, false, common.DefaultRequeueDelay, nil
				}

				// create object as it doesn't exist
				newUpgradingChildDef, err := makeUpgradingObjectDefinition(ctx, rolloutObject, controller, c, false)
				if err != nil {
					return false, false, 0, err
				}

				numaLogger.Debugf("Upgrading child of type %s %s/%s doesn't exist so creating", newUpgradingChildDef.GetKind(), newUpgradingChildDef.GetNamespace(), newUpgradingChildDef.GetName())
				err = kubernetes.CreateResource(ctx, c, newUpgradingChildDef)
				return false, true, 0, err*/
		}
	}

	// nothing to do (either there's nothing to upgrade, or we just created an "upgrading" child, and it's too early to start reconciling it)
	if currentUpgradingChildDef == nil {
		return true, false, 0, err
	}

	// There's already an Upgrading child, now process it

	// Get the live resource so we don't have issues with an outdated cache
	currentUpgradingChildDef, err = kubernetes.GetLiveResource(ctx, currentUpgradingChildDef, rolloutObject.GetChildGVR().Resource)
	if err != nil {
		return false, false, 0, err
	}

	done, newChild, requeueDelay, err := processUpgradingChild(ctx, rolloutObject, controller, existingPromotedChild, currentUpgradingChildDef, c)
	if err != nil {
		return false, newChild, 0, err
	}

	return done, newChild, requeueDelay, nil
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
- A boolean indicating if a new child was created.
- A duration indicating the requeue delay for the controller to use for next reconciliation.
- An error if any issues occur during the process.
*/
func processUpgradingChild(
	ctx context.Context,
	rolloutObject ProgressiveRolloutObject,
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

	childStatus := rolloutObject.GetUpgradingChildStatus()
	// Create a new childStatus object if not present in the live rollout object or
	// if it is that of a previous progressive upgrade.
	if childStatus == nil || childStatus.Name != existingUpgradingChildDef.GetName() {
		if childStatus != nil {
			numaLogger.WithValues("name", existingUpgradingChildDef.GetName(), "childStatus", *childStatus).Debug("the live upgrading child status is stale, resetting it")
		} else {
			numaLogger.WithValues("name", existingUpgradingChildDef.GetName()).Debug("the live upgrading child status has not been set yet, initializing it")
		}

		err = rolloutObject.ResetUpgradingChildStatus(existingUpgradingChildDef)
		if err != nil {
			return false, false, 0, fmt.Errorf("processing upgrading child, failed to reset the upgrading child status for child %s/%s", existingUpgradingChildDef.GetNamespace(), existingUpgradingChildDef.GetName())
		}
	} else {
		numaLogger.WithValues("childStatus", *childStatus).Debug("live upgrading child previously set")
	}
	childStatus = rolloutObject.GetUpgradingChildStatus()

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
		rolloutObject.SetUpgradingChildStatus(childStatus)

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

			newUpgradingChildDef, needRequeue, err := startUpgradeProcess(ctx, rolloutObject, existingPromotedChildDef, controller, c)
			if err != nil {
				return false, false, 0, err
			}
			if needRequeue {
				return false, newUpgradingChildDef != nil, common.DefaultRequeueDelay, nil
			}

			/*requeue, err := controller.ProcessPromotedChildPreUpgrade(ctx, rolloutObject, existingPromotedChildDef, c)
			if err != nil {
				return false, false, 0, err
			}
			if requeue {
				return false, false, common.DefaultRequeueDelay, nil
			}

			// create a definition for the "upgrading" child which has a new name (the definition created above had the previous child's name which was necessary for comparison)
			newUpgradingChildDef, err = makeUpgradingObjectDefinition(ctx, rolloutObject, controller, c, false)
			if err != nil {
				return false, false, 0, err
			}*/

			numaLogger.WithValues("old child", existingUpgradingChildDef.GetName(), "new child", newUpgradingChildDef.GetName()).Debug("replacing 'upgrading' child")
			reasonFailure := common.LabelValueProgressiveFailure
			err = ctlrcommon.UpdateUpgradeState(ctx, c, common.LabelValueUpgradeRecyclable, &reasonFailure, existingUpgradingChildDef)
			if err != nil {
				return false, false, 0, err
			}

			//err = kubernetes.CreateResource(ctx, c, newUpgradingChildDef)
			//return false, true, 0, err
		} else {
			requeue, err := controller.ProcessPromotedChildPostFailure(ctx, rolloutObject, existingPromotedChildDef, c)
			if err != nil {
				return false, false, 0, err
			}
			if requeue {
				return false, false, common.DefaultRequeueDelay, nil
			}
		}

		return false, false, 0, nil

	case apiv1.AssessmentResultSuccess:
		if childStatus.CanDeclareSuccess() {

			// Label the new child as promoted and then remove the label from the old one
			numaLogger.WithValues("old child", existingPromotedChildDef.GetName(), "new child", existingUpgradingChildDef.GetName()).Debug("replacing 'promoted' child")
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
			rolloutObject.SetUpgradingChildStatus(childStatus)
			rolloutObject.GetRolloutStatus().MarkDeployed(rolloutObject.GetRolloutObjectMeta().Generation)

			// we're done
			return true, false, assessmentInterval, nil
		} else {
			return false, false, assessmentInterval, nil
		}

	default:
		childStatus.AssessmentResult = apiv1.AssessmentResultUnknown
		rolloutObject.SetUpgradingChildStatus(childStatus)

		return false, false, assessmentInterval, nil
	}
}

// return:
// - whether we just created a new child
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

	numaLogger.Debugf("Upgrading child of type %s %s/%s doesn't exist so creating", newUpgradingChildDef.GetKind(), newUpgradingChildDef.GetNamespace(), newUpgradingChildDef.GetName())
	err = kubernetes.CreateResource(ctx, c, newUpgradingChildDef)
	return nil, false, err
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
CalculateScaleMinMaxValues computes new minimum and maximum scale values for a given object based on the current
number of pods. It retrieves the existing min and max values from the object using specified paths.
If the min or max values are not found, it returns nil for those values to allow restoration of the
desired values later.

Numaflow notes:
- if max is unset, Numaflow uses DefaultMaxReplicas (50)
- if min is unset or negative, Numaflow uses 0

Parameters:
  - object: A map representing the object from which to retrieve min and max values.
  - podsCount: The current number of pods.
  - pathToMin: A slice of strings representing the path to the min value in the object.
  - pathToMax: A slice of strings representing the path to the max value in the object.

Returns:
  - newMin: The adjusted minimum scale value.
  - newMax: The adjusted maximum scale value.
  - outMin: A pointer to the original min value or nil if not found.
  - outMax: A pointer to the original max value or nil if not found.
  - error: An error if there is an issue retrieving the min or max values.
*/
func CalculateScaleMinMaxValues(object map[string]any, podsCount int, pathToMin, pathToMax []string) (int64, int64, *int64, *int64, error) {
	newMax := int64(math.Floor(float64(podsCount) / float64(2)))

	min, foundMin, err := unstructured.NestedInt64(object, pathToMin...)
	if err != nil {
		return -1, -1, nil, nil, err
	}

	max, foundMax, err := unstructured.NestedInt64(object, pathToMax...)
	if err != nil {
		return -1, -1, nil, nil, err
	}

	// If min exceeds the newMax, reduce also min to newMax
	newMin := min
	if min > newMax {
		newMin = newMax
	}

	// If min was not found, return nil to later restore the appropriate desired value
	outMin := &min
	if !foundMin {
		outMin = nil
	}

	// If max was not found, return nil to later restore the appropriate desired value
	outMax := &max
	if !foundMax {
		outMax = nil
	}

	return newMin, newMax, outMin, outMax, nil
}
