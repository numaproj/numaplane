package progressive

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
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
	getPluralName() string

	listChildren(ctx context.Context, rolloutObject rolloutObject, labelSelector string, fieldSelector string) ([]*kubernetes.GenericObject, error)

	createBaseChild(rolloutObject rolloutObject, name string) (*kubernetes.GenericObject, error)

	updateMetadata(f func(*metav1.ObjectMeta), rolloutObject rolloutObject)

	getNameCount(rolloutObject rolloutObject) (int32, bool)

	// don't only set it in memory but also update K8S
	setNameCount(rolloutObject rolloutObject, nameCount int32) error

	//addLabel(rolloutObject *rolloutObject, key string, value string) *rolloutObject

	//removeLabel(rolloutObject *rolloutObject, key string) *rolloutObject
}

type rolloutObject interface {
	//updateMetadata(f func(*metav1.ObjectMeta))

	getTypeMeta() *metav1.TypeMeta

	getObjectMeta() *metav1.ObjectMeta

	getStatus() *apiv1.Status

	getGeneration() int
}

/*
type progressiveRolloutObject interface {
	rolloutObject


}*/

// func processResourceWithProgressive[T apiv1.MonoVertexRollout | apiv1.PipelineRollout](ctx context.Context, rolloutObject *T,
//
//	existingChildObject *kubernetes.GenericObject, controller controller[T]) (bool, error) {
func processResourceWithProgressive(ctx context.Context, rolloutObject rolloutObject,
	existingChildObject *kubernetes.GenericObject, controller controller, restConfig *rest.Config) (bool, error) {

	numaLogger := logger.FromContext(ctx)

	upgradingChild, err := makeUpgradingObjectDefinition(ctx, rolloutObject, controller)
	if err != nil {
		return false, err
	}

	// Get the object to see if it exists
	_, err = kubernetes.GetLiveResource(ctx, restConfig, upgradingChild, controller.getPluralName())
	if err != nil {
		// create object as it doesn't exist
		if apierrors.IsNotFound(err) {

			numaLogger.Debugf("Upgrading Pipeline %s/%s doesn't exist so creating", upgradingChild.Namespace, upgradingChild.Name)
			err = kubernetes.CreateCR(ctx, restConfig, upgradingChild, controller.getPluralName())
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

func makeUpgradingObjectDefinition(ctx context.Context, rolloutObject rolloutObject, controller controller) (*kubernetes.GenericObject, error) {
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

func getChildName(ctx context.Context, rolloutObject rolloutObject, controller controller, upgradeState string) (string, error) {
	children, err := controller.listChildren(ctx, rolloutObject, fmt.Sprintf(
		"%s=%s,%s=%s", common.LabelKeyParentRollout, rolloutObject.getObjectMeta().Name,
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
		return rolloutObject.getObjectMeta().Name + suffixName, nil
	}
	return children[0].Name, nil
}

// func calculateChildNameSuffix[T apiv1.MonoVertexRollout | apiv1.PipelineRollout](ctx context.Context, rolloutObject *T, controller controller[T]) (string, error) {
func calculateChildNameSuffix(ctx context.Context, rolloutObject rolloutObject, controller controller) (string, error) {
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
	rolloutObject rolloutObject,
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
	upgradingObject, err := kubernetes.GetLiveResource(ctx, restConfig, upgradingChildDef, controller.getPluralName())
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

	switch string(upgradingObjectStatus.Phase){
	case "Failed":
		// Mark the failed child recyclable.
		// TODO: pause the failed new pipeline so it can be drained.
		upgradingObject.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeRecyclable)
		// TODO: use patch for all of these calls instead
		err := kubernetes.UpdateCR(ctx, restConfig, upgradingObject, rolloutObject.getPluralName())
		if err != nil {
			return false, err
		}

		rolloutObject.getStatus().MarkProgressiveUpgradeFailed("New Child Object Failed", rolloutObject.getGeneration())
		return false, nil

	case "Running":
		if !isNumaflowChildReady(upgradingObjectStatus){
			//continue (re-enqueue)
			return false, nil
		}
		// Label the new child as promoted and then remove the label from the old one
		upgradingObject.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradePromoted)
		err := kubernetes.UpdateCR(ctx, restConfig, upgradingObject, rolloutObject.getPluralName())
		if err != nil {
			return false, err
		}

		currentPromotedChildDef.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeRecyclable)
		err := kubernetes.UpdateCR(ctx, restConfig, currentPromotedChildDef, rolloutObject.getPluralName())
		if err != nil {
			return false, err
		}

		rolloutObject.getStatus().MarkProgressiveUpgradeSucceeded("New Child Object Running", rolloutObject.getGeneration())
		rolloutObject.getStatus().MarkDeployed(rolloutObject.getGeneration())

		if err := controller.drain(currentPromotedChildDef); err != nil {
			return false, err
		}
		return true, nil
	default:


	default:
}

func isNumaflowChildReady(upgradingObjectStatus numaflowv1.Status) bool {
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
