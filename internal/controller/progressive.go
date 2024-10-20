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

/*
type controller[T apiv1.MonoVertexRollout | apiv1.PipelineRollout] interface {
	listChildren(ctx context.Context, rolloutObject *T, labelSelector string, fieldSelector string) ([]*kubernetes.GenericObject, error)

	createBaseChild(rolloutObject *T, name string) (*kubernetes.GenericObject, error)

	updateMetadata(f func(*metav1.TypeMeta), rolloutObject *T)

	getNameCount(rolloutObject *T) (int32, bool)

	// don't only set it in memory but also update K8S
	setNameCount(rolloutObject *T, nameCount int32) error

	//addLabel(rolloutObject *T, key string, value string) *T

	//removeLabel(rolloutObject *T, key string) *T
}*/

type controller interface {
	listChildren(ctx context.Context, rolloutObject RolloutObject, labelSelector string, fieldSelector string) ([]*kubernetes.GenericObject, error)

	createBaseChild(rolloutObject RolloutObject, name string) (*kubernetes.GenericObject, error)

	getNameCount(rolloutObject RolloutObject) (int32, bool)

	// don't only set it in memory but also update K8S
	setNameCount(rolloutObject RolloutObject, nameCount int32) error
}

/*
type progressiveRolloutObject interface {
	rolloutObject


}*/

// func processResourceWithProgressive[T apiv1.MonoVertexRollout | apiv1.PipelineRollout](ctx context.Context, rolloutObject *T,
//
//	existingChildObject *kubernetes.GenericObject, controller controller[T]) (bool, error) {
func processResourceWithProgressive(ctx context.Context, rolloutObject RolloutObject,
	existingChildObject *kubernetes.GenericObject, controller controller, restConfig *rest.Config) (bool, error) {

	numaLogger := logger.FromContext(ctx)

	upgradingChild, err := makeUpgradingObjectDefinition(ctx, rolloutObject, controller)
	if err != nil {
		return false, err
	}

	// Get the object to see if it exists
	_, err = kubernetes.GetLiveResource(ctx, restConfig, upgradingChild, rolloutObject.GetPluralName())
	if err != nil {
		// create object as it doesn't exist
		if apierrors.IsNotFound(err) {

			numaLogger.Debugf("Upgrading Pipeline %s/%s doesn't exist so creating", upgradingChild.Namespace, upgradingChild.Name)
			err = kubernetes.CreateCR(ctx, restConfig, upgradingChild, rolloutObject.GetPluralName())
			if err != nil {
				return false, err
			}
		} else {
			return false, fmt.Errorf("error getting Pipeline: %v", err)
		}
	}

	done, err := processUpgradingChild(ctx, rolloutObject, controller, existingChildObject, restConfig)
	if err != nil {
		return false, err
	}

	return done, nil
}

func makeUpgradingObjectDefinition(ctx context.Context, rolloutObject RolloutObject, controller controller) (*kubernetes.GenericObject, error) {
	childName, err := getChildName(ctx, rolloutObject, controller, string(common.LabelValueUpgradeInProgress))
	if err != nil {
		return nil, err
	}
	upgradingChild, err := controller.createBaseChild(rolloutObject, childName)
	if err != nil {
		return nil, err
	}

	upgradingChild.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeInProgress)

	return upgradingChild, nil
}

func getChildName(ctx context.Context, rolloutObject RolloutObject, controller controller, upgradeState string) (string, error) {
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

// func calculateChildNameSuffix[T apiv1.MonoVertexRollout | apiv1.PipelineRollout](ctx context.Context, rolloutObject *T, controller controller[T]) (string, error) {
func calculateChildNameSuffix(ctx context.Context, rolloutObject RolloutObject, controller controller) (string, error) {
	currentNameCount, found := controller.getNameCount(rolloutObject)
	if !found {
		err := controller.setNameCount(rolloutObject, int32(0))
		if err != nil {
			return "", err
		}
	}

	// TODO: why in PipelineRolloutReconciler.calPipelineName() do we only update the Status subresource when it's 0?
	err := controller.setNameCount(rolloutObject, currentNameCount+1)
	if err != nil {
		return "", err
	}

	return "-" + fmt.Sprint(currentNameCount), nil
}

func processUpgradingChild(
	ctx context.Context,
	rolloutObject RolloutObject,
	controller controller,
	currentPromotedChildDef *kubernetes.GenericObject,
	restConfig *rest.Config,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	upgradingChildDef, err := makeUpgradingObjectDefinition(ctx, rolloutObject, controller)
	if err != nil {
		return false, err
	}

	// Get existing upgrading child
	upgradingObject, err := kubernetes.GetLiveResource(ctx, restConfig, upgradingChildDef, rolloutObject.GetPluralName())
	if err != nil {
		if apierrors.IsNotFound(err) {
			numaLogger.WithValues("childObjectDefinition", *upgradingChildDef).
				Warn("Child not found. Unable to process status during this reconciliation.")
		} else {
			return false, fmt.Errorf("error getting Child for status processing: %v", err)
		}
	}
	upgradingObjectStatus, err := kubernetes.ParseStatus(upgradingObject)
	if err != nil {
		return false, err
	}

	switch string(upgradingObjectStatus.Phase) {
	case "Failed":
		// Mark the failed child recyclable.
		// TODO: pause the failed new pipeline so it can be drained.
		upgradingObject.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeRecyclable)
		// TODO: use patch for all of these calls instead
		err := kubernetes.UpdateCR(ctx, restConfig, upgradingObject, rolloutObject.GetPluralName())
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
		err := kubernetes.UpdateCR(ctx, restConfig, upgradingObject, rolloutObject.GetPluralName())
		if err != nil {
			return false, err
		}

		currentPromotedChildDef.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeRecyclable)
		err = kubernetes.UpdateCR(ctx, restConfig, currentPromotedChildDef, rolloutObject.GetPluralName())
		if err != nil {
			return false, err
		}

		rolloutObject.GetStatus().MarkProgressiveUpgradeSucceeded("New Child Object Running", rolloutObject.GetObjectMeta().Generation)
		rolloutObject.GetStatus().MarkDeployed(rolloutObject.GetObjectMeta().Generation)

		if err := controller.Drain(currentPromotedChildDef); err != nil {
			return false, err
		}
		return true, nil
	default:
		// TODO: complete this...
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
