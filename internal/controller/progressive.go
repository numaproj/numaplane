package controller

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
)

// progressiveController describes a Controller that can progressively roll out a second child alongside the original child,
// taking down the original child once the new one is healthy
type progressiveController interface {
	// listChildren lists all children of the Rollout identified by the selectors
	listChildren(ctx context.Context, rolloutObject RolloutObject, labelSelector string, fieldSelector string) ([]*kubernetes.GenericObject, error)

	// createBaseChildDefinition creates a Kubernetes definition for a child resource of the Rollout with the given name
	createBaseChildDefinition(rolloutObject RolloutObject, name string) (*kubernetes.GenericObject, error)

	// incrementChildCount updates the count of children for the Resource in Kubernetes and returns the index that should be used for the next child
	incrementChildCount(ctx context.Context, rolloutObject RolloutObject) (int32, error)

	// childIsDrained checks to see if the child has been fully drained
	childIsDrained(ctx context.Context, child *kubernetes.GenericObject) (bool, error)

	// drain updates the child in Kubernetes to cause it to drain
	drain(ctx context.Context, child *kubernetes.GenericObject) error

	// childNeedsUpdating determines if the difference between the current child definition and the desired child definition requires an update
	childNeedsUpdating(ctx context.Context, existingChild *kubernetes.GenericObject, newChildDefinition *kubernetes.GenericObject) (bool, error)

	// merge is able to take an existing child object and override anything needed from the new one into it to create a revised new object
	merge(existingObj *kubernetes.GenericObject, newObj *kubernetes.GenericObject) *kubernetes.GenericObject
}

// return whether we're done, and error if any
func processResourceWithProgressive(ctx context.Context, rolloutObject RolloutObject,
	existingPromotedChild *kubernetes.GenericObject, controller progressiveController, restConfig *rest.Config) (bool, error) {

	numaLogger := logger.FromContext(ctx)

	newUpgradingChildDef, err := makeUpgradingObjectDefinition(ctx, rolloutObject, controller)
	if err != nil {
		return false, err
	}

	// Get the Upgrading object to see if it exists
	existingUpgradingChildDef, err := kubernetes.GetLiveResource(ctx, restConfig, newUpgradingChildDef, rolloutObject.GetChildPluralName())
	if err != nil {
		// create object as it doesn't exist
		if apierrors.IsNotFound(err) {
			numaLogger.Debugf("Upgrading child of type %s %s/%s doesn't exist so creating", newUpgradingChildDef.Kind, newUpgradingChildDef.Namespace, newUpgradingChildDef.Name)
			err = kubernetes.CreateCR(ctx, restConfig, newUpgradingChildDef, rolloutObject.GetChildPluralName())
			return false, err
		} else {
			return false, fmt.Errorf("error getting %s: %v", newUpgradingChildDef.Kind, err)
		}
	}
	newUpgradingChildDef = controller.merge(existingUpgradingChildDef, newUpgradingChildDef)

	done, err := processUpgradingChild(ctx, rolloutObject, controller, existingPromotedChild, newUpgradingChildDef, existingUpgradingChildDef, restConfig)
	if err != nil {
		return false, err
	}

	return done, nil
}

// create the definition for the child of the Rollout which is the one labeled "upgrading"
func makeUpgradingObjectDefinition(ctx context.Context, rolloutObject RolloutObject, controller progressiveController) (*kubernetes.GenericObject, error) {

	numaLogger := logger.FromContext(ctx)

	childName, err := getChildName(ctx, rolloutObject, controller, string(common.LabelValueUpgradeInProgress))
	if err != nil {
		return nil, err
	}
	numaLogger.Debugf("Upgrading child: %s", childName)
	upgradingChild, err := controller.createBaseChildDefinition(rolloutObject, childName)
	if err != nil {
		return nil, err
	}

	upgradingChild.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeInProgress)

	return upgradingChild, nil
}

func getChildName(ctx context.Context, rolloutObject RolloutObject, controller progressiveController, upgradeState string) (string, error) {
	children, err := controller.listChildren(ctx, rolloutObject, fmt.Sprintf(
		"%s=%s,%s=%s", common.LabelKeyParentRollout, rolloutObject.GetObjectMeta().Name,
		common.LabelKeyUpgradeState, upgradeState,
	), "")

	if err != nil {
		return "", err
	}
	if len(children) > 1 {
		return "", fmt.Errorf("there should only be one promoted or upgrade in progress pipeline")
	} else if len(children) == 0 {
		index, err := controller.incrementChildCount(ctx, rolloutObject)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s-%d", rolloutObject.GetObjectMeta().Name, index), nil
	}
	return children[0].Name, nil
}

// return whether we're done, and error if any
func processUpgradingChild(
	ctx context.Context,
	rolloutObject RolloutObject,
	controller progressiveController,
	existingPromotedChildDef, desiredUpgradingChildDef, existingUpgradingChildDef *kubernetes.GenericObject,
	restConfig *rest.Config,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	upgradingObjectStatus, err := kubernetes.ParseStatus(existingUpgradingChildDef)
	if err != nil {
		return false, err
	}

	numaLogger.Debugf("Upgrading child %s/%s is in phase %s", existingUpgradingChildDef.Namespace, existingUpgradingChildDef.Name, upgradingObjectStatus.Phase)

	switch string(upgradingObjectStatus.Phase) {
	case "Failed":

		rolloutObject.GetStatus().MarkProgressiveUpgradeFailed("New Child Object Failed", rolloutObject.GetObjectMeta().Generation) // TODO: consider that in order for user to be able to update this spec to fix it, we will need to allow that
		return false, nil

	case "Running":
		if !isNumaflowChildReady(&upgradingObjectStatus) {
			//continue (re-enqueue)
			return false, nil
		}
		// Label the new child as promoted and then remove the label from the old one
		err := updateUpgradeState(ctx, restConfig, common.LabelValueUpgradePromoted, existingUpgradingChildDef, rolloutObject)
		if err != nil {
			return false, err
		}

		// TODO: if any of the below items fail, then we return false which keeps us in UpgradeStrategyInProgress=Progressive, but due to "if pipelineNeedsToUpdate" check in pipelinerollout_controller.go, we never come back here
		// to complete the below logic
		// Consider upgrading to "promoted" as the last thing?

		err = updateUpgradeState(ctx, restConfig, common.LabelValueUpgradeRecyclable, existingPromotedChildDef, rolloutObject)
		if err != nil {
			return false, err
		}

		rolloutObject.GetStatus().MarkProgressiveUpgradeSucceeded("New Child Object Running", rolloutObject.GetObjectMeta().Generation)
		rolloutObject.GetStatus().MarkDeployed(rolloutObject.GetObjectMeta().Generation)

		if err := controller.drain(ctx, existingPromotedChildDef); err != nil {
			return false, err
		}
		return true, nil
	default:
		// Ensure the latest spec is applied
		// TODO: this needs revisiting - a race condition means we could deem this "Running" prior to latest version
		// being reconciled

		childNeedsToUpdate, err := controller.childNeedsUpdating(ctx, existingUpgradingChildDef, desiredUpgradingChildDef) // TODO: if we decide not to drain the upgrading one on failure, I think we can change this to DeepEqual() check
		if err != nil {
			return false, err
		}
		if childNeedsToUpdate {
			numaLogger.Debugf("Upgrading child %s/%s has a new update", existingUpgradingChildDef.Namespace, existingUpgradingChildDef.Name)

			err = kubernetes.UpdateCR(ctx, restConfig, desiredUpgradingChildDef, "pipelines")
			if err != nil {
				return false, err
			}
		}
		//continue (re-enqueue)
		return false, nil
	}
}

// update the in-memory object with the new Label and patch the object in K8S
func updateUpgradeState(ctx context.Context, restConfig *rest.Config, upgradeState common.UpgradeState, childObject *kubernetes.GenericObject, rolloutObject RolloutObject) error {
	childObject.Labels[common.LabelKeyUpgradeState] = string(upgradeState)
	patchJson := `{"metadata":{"labels":{"` + common.LabelKeyUpgradeState + `":"` + string(upgradeState) + `"}}}`
	return kubernetes.PatchCR(ctx, restConfig, childObject, rolloutObject.GetChildPluralName(), patchJson, k8stypes.MergePatchType)
}

func isNumaflowChildReady(upgradingObjectStatus *kubernetes.GenericStatus) bool {
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

func garbageCollectChildren(
	ctx context.Context,
	rolloutObject RolloutObject,
	controller progressiveController,
	restConfig *rest.Config,
) error {
	numaLogger := logger.FromContext(ctx)
	recyclableObjects, err := getRecyclableObjects(ctx, rolloutObject, controller)
	if err != nil {
		return err
	}

	numaLogger.WithValues("recylableObjects", recyclableObjects).Debug("recycling")

	for _, recyclableChild := range recyclableObjects {
		err = recycle(ctx, recyclableChild, rolloutObject.GetChildPluralName(), controller, restConfig)
		if err != nil {
			return err
		}
	}
	return nil
}
func getRecyclableObjects(
	ctx context.Context,
	rolloutObject RolloutObject,
	controller progressiveController,
) ([]*kubernetes.GenericObject, error) {
	return controller.listChildren(ctx, rolloutObject, fmt.Sprintf(
		"%s=%s,%s=%s", common.LabelKeyParentRollout, rolloutObject.GetObjectMeta().Name,
		common.LabelKeyUpgradeState, common.LabelValueUpgradeRecyclable,
	), "")
}

func recycle(ctx context.Context,
	childObject *kubernetes.GenericObject,
	childPluralName string,
	controller progressiveController,
	restConfig *rest.Config,
) error {
	isDrained, err := controller.childIsDrained(ctx, childObject)
	if err != nil {
		return err
	}
	if isDrained {
		err = kubernetes.DeleteCR(ctx, restConfig, childObject, childPluralName)
		if err != nil {
			return err
		}
	}
	return nil

}
