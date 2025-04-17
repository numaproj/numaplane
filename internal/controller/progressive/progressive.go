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

	argorolloutsv1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util"
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
	AssessUpgradingChild(ctx context.Context, rolloutObject ProgressiveRolloutObject, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, string, error)

	// ProcessPromotedChildPreUpgrade performs operations on the promoted child prior to the upgrade (just the operations which are unique to this Kind)
	// return true if requeue is needed
	ProcessPromotedChildPreUpgrade(ctx context.Context, rolloutObject ProgressiveRolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) (bool, error)

	// ProcessPromotedChildPostUpgrade performs operations on the promoted child after the creation of the Upgrading child in K8S (just the operations which are unique to this Kind)
	// return true if requeue is needed
	ProcessPromotedChildPostUpgrade(ctx context.Context, rolloutObject ProgressiveRolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) (bool, error)

	// ProcessPromotedChildPostFailure performs operations on the promoted child after the upgrade fails (just the operations which are unique to this Kind)
	// return true if requeue is needed
	ProcessPromotedChildPostFailure(ctx context.Context, rolloutObject ProgressiveRolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) (bool, error)

	// ProcessUpgradingChildPreUpgrade performs operations on the upgrading child definition prior to its creation in K8S (just the operations which are unique to this Kind)
	// return true if requeue is needed
	ProcessUpgradingChildPreUpgrade(ctx context.Context, rolloutObject ProgressiveRolloutObject, upgradingChildDef *unstructured.Unstructured, c client.Client) (bool, error)

	// ProcessUpgradingChildPostUpgrade performs operations on the upgrading child after its creation in K8S (just the operations which are unique to this Kind)
	// return true if requeue is needed
	ProcessUpgradingChildPostUpgrade(ctx context.Context, rolloutObject ProgressiveRolloutObject, upgradingChildDef *unstructured.Unstructured, c client.Client) (bool, error)

	// ProcessUpgradingChildPostFailure performs operations on the upgrading child after the upgrade fails (just the operations which are unique to this Kind)
	// return true if requeue is needed
	ProcessUpgradingChildPostFailure(ctx context.Context, rolloutObject ProgressiveRolloutObject, upgradingChildDef *unstructured.Unstructured, c client.Client) (bool, error)

	// ProcessUpgradingChildPostSuccess performs operations on the upgrading child after the upgrade succeeds (just the operations which are unique to this Kind)
	ProcessUpgradingChildPostSuccess(ctx context.Context, rolloutObject ProgressiveRolloutObject, upgradingChildDef *unstructured.Unstructured, c client.Client) error
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

	// Make sure that our Promoted Child Status reflects the current promoted child
	promotedChildStatus := rolloutObject.GetPromotedChildStatus()
	if promotedChildStatus == nil || promotedChildStatus.Name != existingPromotedChild.GetName() {
		err := rolloutObject.ResetPromotedChildStatus(existingPromotedChild)
		if err != nil {
			return false, 0, err
		}
	}

	// is there currently an "upgrading" child?
	currentUpgradingChildDef, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, rolloutObject, common.LabelValueUpgradeInProgress, nil, false, c)
	if err != nil {
		return false, 0, err
	}

	// if there's a difference between the desired spec and the current "promoted" child, and there isn't already an "upgrading" definition, then create one and return
	if promotedDifference && currentUpgradingChildDef == nil {
		// Create it, first making sure one doesn't already exist by checking the live K8S API
		currentUpgradingChildDef, err = ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, rolloutObject, common.LabelValueUpgradeInProgress, nil, true, c)
		if err != nil {
			return false, 0, fmt.Errorf("error getting %s: %v", currentUpgradingChildDef.GetKind(), err)
		}
		if currentUpgradingChildDef == nil {
			_, needRequeue, err := startUpgradeProcess(ctx, rolloutObject, existingPromotedChild, controller, c)
			if needRequeue {
				return false, common.DefaultRequeueDelay, err
			} else {
				return false, 0, err
			}
		}
	}

	// nothing to do (either there's nothing to upgrade, or we just created an "upgrading" child, and it's too early to start reconciling it)
	if currentUpgradingChildDef == nil {
		return true, 0, err
	}

	// There's already an Upgrading child, now process it

	// Get the live resource so we don't have issues with an outdated cache
	currentUpgradingChildDef, err = kubernetes.GetLiveResource(ctx, currentUpgradingChildDef, rolloutObject.GetChildGVR().Resource)
	if err != nil {
		return false, 0, err
	}

	// get UpgradingChildStatus and reset it if necessary
	childStatus, err := getUpgradingChildStatus(ctx, rolloutObject, currentUpgradingChildDef)
	if err != nil {
		return false, 0, err
	}

	// if the Upgrading child status exists but indicates that we aren't done with upgrade process, then do postupgrade process
	if !childStatus.InitializationComplete {
		needsRequeue, err := startPostUpgradeProcess(ctx, rolloutObject, existingPromotedChild, currentUpgradingChildDef, controller, c)
		if needsRequeue {
			return false, common.DefaultRequeueDelay, err
		} else {
			return false, 0, err
		}
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

	// check for Force Promote set in Progressive strategy to force success logic
	if rolloutObject.GetProgressiveStrategy().ForcePromote {
		childStatus.ForcedSuccess = true

		done, err := declareSuccess(ctx, rolloutObject, controller, existingPromotedChildDef, existingUpgradingChildDef, childStatus, c)
		if err != nil || done {
			return done, 0, err
		} else {
			return done, assessmentSchedule.Interval, err
		}
	}

	// If no AssessmentStartTime has been set already, calculate it and set it
	if childStatus.AssessmentStartTime == nil {
		// Add to the current time the assessmentSchedule.Delay and set the AssessmentStartTime in the Rollout object
		nextAssessmentTime := metav1.NewTime(time.Now().Add(assessmentSchedule.Delay))
		childStatus.AssessmentStartTime = &nextAssessmentTime
		numaLogger.WithValues("childStatus", *childStatus).Debug("set upgrading child AssessmentStartTime")
	}

	// Assess the upgrading child status only if within the assessment time window and if not previously failed.
	// Otherwise, assess the previous child status.
	assessment := childStatus.AssessmentResult
	failureReason := childStatus.FailureReason
	childSts := childStatus.ChildStatus.Raw
	if childStatus.CanAssess() {
		assessment, failureReason, err = controller.AssessUpgradingChild(ctx, rolloutObject, existingUpgradingChildDef)
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

	// Once a "not unknown" assessment is reached, set the assessment's end time (if not set yet)
	if assessment != apiv1.AssessmentResultUnknown && !childStatus.IsAssessmentEndTimeSet() {
		assessmentEndTime := metav1.NewTime(time.Now().Add(assessmentSchedule.Period))
		childStatus.AssessmentEndTime = &assessmentEndTime
		numaLogger.WithValues("childStatus", *childStatus).Debug("set upgrading child AssessmentEndTime")
	}

	switch assessment {
	case apiv1.AssessmentResultFailure:
		rolloutObject.GetRolloutStatus().MarkProgressiveUpgradeFailed(fmt.Sprintf("New Child Object %s/%s Failed", existingUpgradingChildDef.GetNamespace(), existingUpgradingChildDef.GetName()), rolloutObject.GetRolloutObjectMeta().Generation)
		childStatus.AssessmentResult = apiv1.AssessmentResultFailure
		childStatus.FailureReason = failureReason
		childStatus.ChildStatus.Raw = childSts
		rolloutObject.SetUpgradingChildStatus(childStatus)

		// check if there are any new incoming changes to the desired spec
		newUpgradingChildDef, err := makeUpgradingObjectDefinition(ctx, rolloutObject, controller, c, true)
		if err != nil {
			return false, 0, err
		}
		needsUpdating, err := controller.ChildNeedsUpdating(ctx, existingUpgradingChildDef, newUpgradingChildDef)
		if err != nil {
			return false, 0, err
		}

		// if so, mark the existing one for garbage collection and then create a new upgrading one
		if needsUpdating {
			newUpgradingChildDef, needRequeue, err := startUpgradeProcess(ctx, rolloutObject, existingPromotedChildDef, controller, c)
			if err != nil {
				return false, 0, err
			}
			if needRequeue {
				return false, common.DefaultRequeueDelay, nil
			}
			existingUpgradingChildDef = newUpgradingChildDef

			return false, 0, nil
		}

		// if we now have an upgrading child, make sure we do post-upgrade process if we haven't
		if !childStatus.InitializationComplete {

			numaLogger.WithValues("old child", existingUpgradingChildDef.GetName(), "new child", newUpgradingChildDef.GetName()).Debug("replacing 'upgrading' child")
			reasonFailure := common.LabelValueProgressiveFailure
			err = ctlrcommon.UpdateUpgradeState(ctx, c, common.LabelValueUpgradeRecyclable, &reasonFailure, existingUpgradingChildDef)
			if err != nil {
				return false, 0, err
			}

			needsRequeue, err := startPostUpgradeProcess(ctx, rolloutObject, existingPromotedChildDef, existingUpgradingChildDef, controller, c)
			if needsRequeue {
				return false, common.DefaultRequeueDelay, err
			} else {
				return false, 0, err
			}
		}

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
		if childStatus.CanDeclareSuccess() {
			done, err := declareSuccess(ctx, rolloutObject, controller, existingPromotedChildDef, existingUpgradingChildDef, childStatus, c)
			if err != nil || done {
				return done, 0, err
			} else {
				return done, assessmentSchedule.Interval, err
			}
		} else {
			return false, assessmentSchedule.Interval, nil
		}

	default:
		childStatus.AssessmentResult = apiv1.AssessmentResultUnknown
		rolloutObject.SetUpgradingChildStatus(childStatus)

		return false, assessmentSchedule.Interval, nil
	}
}

// AssessUpgradingPipelineType makes an assessment of the upgrading child (either Pipeline or MonoVertex) to determine
// if it was successful, failed, or still not known
// Assessment:
// Success: phase must be "Running" and all conditions must be True
// Failure: phase is "Failed" or any condition is False
// Unknown: neither of the above if met
func AssessUpgradingPipelineType(
	ctx context.Context,
	analysisStatus *apiv1.AnalysisStatus,
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

	analysisRunTimeout, err := getAnalysisRunTimeout(ctx)
	if err != nil {
		return apiv1.AssessmentResultUnknown, "", err
	}

	// conduct standard health assessment first
	if upgradingObjectStatus.Phase == "Running" && healthyConditions && healthyReplicas {
		// if analysisStatus is set with an AnalysisRun's name, we must also check that it is in a Completed phase to declare success
		if analysisStatus != nil && analysisStatus.AnalysisRunName != "" {
			numaLogger.WithValues("namespace", existingUpgradingChildDef.GetNamespace(), "name", existingUpgradingChildDef.GetName()).
				Debugf("AnalysisRun %s is in phase %s", analysisStatus.AnalysisRunName, analysisStatus.Phase)
			switch analysisStatus.Phase {
			case argorolloutsv1.AnalysisPhaseSuccessful:
				return apiv1.AssessmentResultSuccess, "", nil
			case argorolloutsv1.AnalysisPhaseError, argorolloutsv1.AnalysisPhaseFailed, argorolloutsv1.AnalysisPhaseInconclusive:
				return apiv1.AssessmentResultFailure, fmt.Sprintf("AnalysisRun %s is in phase %s", analysisStatus.AnalysisRunName, analysisStatus.Phase), nil
			default:
				// if analysisRun is not completed yet, we check if it has exceeded the analysisRunTimeout
				if time.Since(analysisStatus.StartTime.Time) >= analysisRunTimeout {
					return apiv1.AssessmentResultFailure, fmt.Sprintf("AnalysisRun %s in phase %s has exceeded the analysisRunTimeout", analysisStatus.AnalysisRunName, analysisStatus.Phase), nil
				}
				return apiv1.AssessmentResultUnknown, "", nil
			}
		}
		return apiv1.AssessmentResultSuccess, "", nil
	}

	return apiv1.AssessmentResultUnknown, "", nil
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
	existingUpgradingChild *unstructured.Unstructured,
	controller progressiveController,
	c client.Client,
) (bool, error) {
	requeue, err := controller.ProcessPromotedChildPostUpgrade(ctx, rolloutObject, existingUpgradingChild, c)
	if err != nil {
		return false, err
	}
	if requeue {
		return true, nil
	}
	requeue, err = controller.ProcessUpgradingChildPostUpgrade(ctx, rolloutObject, existingUpgradingChild, c)
	if err != nil {
		return false, err
	}
	if requeue {
		return true, nil
	}

	childStatus := rolloutObject.GetUpgradingChildStatus()
	childStatus.InitializationComplete = true
	rolloutObject.SetUpgradingChildStatus(childStatus)

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

func ExtractScaleMinMax(object map[string]any, pathToScale []string) (*apiv1.ScaleDefinition, error) {

	scaleDef, foundScale, err := unstructured.NestedMap(object, pathToScale...)
	if err != nil {
		return nil, err
	}

	if !foundScale {
		return nil, nil
	}
	scaleMinMax := apiv1.ScaleDefinition{}
	minInterface := scaleDef["min"]
	maxInterface := scaleDef["max"]
	if minInterface != nil {
		min, valid := util.ToInt64(minInterface)
		if !valid {
			return nil, fmt.Errorf("scale min %+v of unexpected type", minInterface)
		}
		scaleMinMax.Min = &min
	}
	if maxInterface != nil {
		max, valid := util.ToInt64(maxInterface)
		if !valid {
			return nil, fmt.Errorf("scale max %+v of unexpected type", maxInterface)
		}
		scaleMinMax.Max = &max
	}

	return &scaleMinMax, nil
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
