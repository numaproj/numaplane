package controller

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
)

type progressiveController interface {
	listChildren(ctx context.Context, rolloutObject RolloutObject, labelSelector string, fieldSelector string) ([]*kubernetes.GenericObject, error)

	createBaseChild(rolloutObject RolloutObject, name string) (*kubernetes.GenericObject, error)

	getNameCount(rolloutObject RolloutObject) (int32, bool)

	// don't only set it in memory but also update K8S
	updateNameCount(ctx context.Context, rolloutObject RolloutObject, nameCount int32) error

	childIsDrained(ctx context.Context, child *kubernetes.GenericObject) (bool, error)

	drain(ctx context.Context, child *kubernetes.GenericObject) error

	childNeedsUpdating(ctx context.Context, existingChild *kubernetes.GenericObject, newChildDefinition *kubernetes.GenericObject) (bool, error)
}

// return whether we're done, and error if any
func processResourceWithProgressive(ctx context.Context, rolloutObject RolloutObject,
	existingChildObject *kubernetes.GenericObject, controller progressiveController, restConfig *rest.Config) (bool, error) {

	numaLogger := logger.FromContext(ctx)

	upgradingChild, err := makeUpgradingObjectDefinition(ctx, rolloutObject, controller)
	if err != nil {
		return false, err
	}

	// Get the object to see if it exists
	_, err = kubernetes.GetLiveResource(ctx, restConfig, upgradingChild, rolloutObject.GetChildPluralName())
	if err != nil {
		// create object as it doesn't exist
		if apierrors.IsNotFound(err) {

			numaLogger.Debugf("Upgrading child of type %s %s/%s doesn't exist so creating", upgradingChild.Kind, upgradingChild.Namespace, upgradingChild.Name)
			err = kubernetes.CreateCR(ctx, restConfig, upgradingChild, rolloutObject.GetChildPluralName())
			if err != nil {
				return false, err
			}
		} else {
			return false, fmt.Errorf("error getting %s: %v", upgradingChild.Kind, err)
		}
	}

	done, err := processUpgradingChild(ctx, rolloutObject, controller, existingChildObject, restConfig)
	if err != nil {
		return false, err
	}

	return done, nil
}

func makeUpgradingObjectDefinition(ctx context.Context, rolloutObject RolloutObject, controller progressiveController) (*kubernetes.GenericObject, error) {

	numaLogger := logger.FromContext(ctx)

	childName, err := getChildName(ctx, rolloutObject, controller, string(common.LabelValueUpgradeInProgress))
	if err != nil {
		return nil, err
	}
	numaLogger.Debugf("Upgrading child: %s", childName)
	upgradingChild, err := controller.createBaseChild(rolloutObject, childName)
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
		suffixName, err := calculateChildNameSuffix(ctx, rolloutObject, controller)
		if err != nil {
			return "", err
		}
		return rolloutObject.GetObjectMeta().Name + suffixName, nil
	}
	return children[0].Name, nil
}

func calculateChildNameSuffix(ctx context.Context, rolloutObject RolloutObject, controller progressiveController) (string, error) {
	currentNameCount, found := controller.getNameCount(rolloutObject)
	if !found {
		currentNameCount = int32(0)
		err := controller.updateNameCount(ctx, rolloutObject, int32(0))
		if err != nil {
			return "", err
		}
	}

	// TODO: why in PipelineRolloutReconciler.calPipelineName() do we only update the Status subresource when it's 0?
	err := controller.updateNameCount(ctx, rolloutObject, currentNameCount+1)
	if err != nil {
		return "", err
	}

	return "-" + fmt.Sprint(currentNameCount), nil
}

// return whether we're done, and error if any
func processUpgradingChild(
	ctx context.Context,
	rolloutObject RolloutObject,
	controller progressiveController,
	currentPromotedChildDef *kubernetes.GenericObject,
	restConfig *rest.Config,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	desiredUpgradingChildDef, err := makeUpgradingObjectDefinition(ctx, rolloutObject, controller)
	if err != nil {
		return false, err
	}

	// Get existing upgrading child
	upgradingObject, err := kubernetes.GetLiveResource(ctx, restConfig, desiredUpgradingChildDef, rolloutObject.GetChildPluralName())
	if err != nil {
		if apierrors.IsNotFound(err) {
			numaLogger.WithValues("childObjectDefinition", *desiredUpgradingChildDef).
				Warn("Child not found. Unable to process status during this reconciliation.")
		} else {
			return false, fmt.Errorf("error getting Child for status processing: %v", err)
		}
	}
	upgradingObjectStatus, err := kubernetes.ParseStatus(upgradingObject)
	if err != nil {
		return false, err
	}

	numaLogger.Debugf("Upgrading child %s/%s is in phase %s", upgradingObject.Namespace, upgradingObject.Name, upgradingObjectStatus.Phase)

	switch string(upgradingObjectStatus.Phase) {
	case "Failed":
		// Mark the failed child recyclable.
		// TODO: pause the failed new pipeline so it can be drained.
		upgradingObject.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeRecyclable)
		// TODO: use patch for all of these calls instead
		err := kubernetes.UpdateCR(ctx, restConfig, upgradingObject, rolloutObject.GetChildPluralName())
		if err != nil {
			return false, err
		}

		rolloutObject.GetStatus().MarkProgressiveUpgradeFailed("New Child Object Failed", rolloutObject.GetObjectMeta().Generation)
		return false, nil

	case "Running":
		if !isNumaflowChildReady(&upgradingObjectStatus) {
			//continue (re-enqueue)
			return false, nil
		}
		// Label the new child as promoted and then remove the label from the old one
		upgradingObject.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradePromoted)
		err := kubernetes.UpdateCR(ctx, restConfig, upgradingObject, rolloutObject.GetChildPluralName())
		if err != nil {
			return false, err
		}

		currentPromotedChildDef.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeRecyclable)
		err = kubernetes.UpdateCR(ctx, restConfig, currentPromotedChildDef, rolloutObject.GetChildPluralName())
		if err != nil {
			return false, err
		}

		rolloutObject.GetStatus().MarkProgressiveUpgradeSucceeded("New Child Object Running", rolloutObject.GetObjectMeta().Generation)
		rolloutObject.GetStatus().MarkDeployed(rolloutObject.GetObjectMeta().Generation)

		if err := controller.drain(ctx, currentPromotedChildDef); err != nil {
			return false, err // TODO: if this errors out, then we return false, but due to "if pipelineNeedsToUpdate" check in pipelinerollout_controller.go, we never come back here - can we move call to "drain()" into the garbage collection logic?
		}
		return true, nil
	default:
		// Ensure the latest spec is applied
		// TODO: this needs revisiting - a race condition means we could deem this "Running" prior to latest version
		// being reconciled

		childNeedsToUpdate, err := controller.childNeedsUpdating(ctx, upgradingObject, desiredUpgradingChildDef)
		if err != nil {
			return false, err
		}
		if childNeedsToUpdate {
			numaLogger.Debugf("Upgrading child %s/%s has a new update", upgradingObject.Namespace, upgradingObject.Name)

			err = kubernetes.UpdateCR(ctx, restConfig, desiredUpgradingChildDef, "pipelines")
			if err != nil {
				return false, err
			}
		}
		//continue (re-enqueue)
		return false, nil
	}
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
	recyclableObjects, err := getRecyclableObjects(ctx, rolloutObject, controller)
	if err != nil {
		return err
	}

	for _, recyclableChild := range recyclableObjects {
		err = recycle(ctx, recyclableChild, controller, restConfig)
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
	controller progressiveController,
	restConfig *rest.Config,
) error {
	isDrained, err := controller.childIsDrained(ctx, childObject)
	if err != nil {
		return err
	}
	if isDrained {
		err = kubernetes.DeleteCR(ctx, restConfig, childObject, "pipelines")
		if err != nil {
			return err
		}
	}
	return nil

}
