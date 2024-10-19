package progressive

import (
	"context"
	"fmt"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	listChildren(ctx context.Context, rolloutObject rolloutObject, labelSelector string, fieldSelector string) ([]*kubernetes.GenericObject, error)

	createBaseChild(rolloutObject *rolloutObject, name string) (*kubernetes.GenericObject, error)

	updateMetadata(f func(*metav1.ObjectMeta), rolloutObject rolloutObject)

	getNameCount(rolloutObject rolloutObject) (int32, bool)

	// don't only set it in memory but also update K8S
	setNameCount(rolloutObject rolloutObject, nameCount int32) error

	//addLabel(rolloutObject *rolloutObject, key string, value string) *rolloutObject

	//removeLabel(rolloutObject *rolloutObject, key string) *rolloutObject
}

type rolloutObject interface {
	updateMetadata(f func(*metav1.ObjectMeta))

	getTypeMeta() *metav1.TypeMeta

	getObjectMeta() *metav1.ObjectMeta
}

/*
type progressiveRolloutObject interface {
	rolloutObject


}*/

// func processResourceWithProgressive[T apiv1.MonoVertexRollout | apiv1.PipelineRollout](ctx context.Context, rolloutObject *T,
//
//	existingChildObject *kubernetes.GenericObject, controller controller[T]) (bool, error) {
func processResourceWithProgressive(ctx context.Context, rolloutObject rolloutObject,
	existingChildObject *kubernetes.GenericObject, controller controller) (bool, error) {

	numaLogger := logger.FromContext(ctx)

	/*upgradingChild, err := controller.createBaseChild(rolloutObject)
	if err != nil {
		return false, err
	}*/

	//controller.addLabel(rolloutObject,

}

func makeUpgradingObjectDefinition[T apiv1.MonoVertexRollout | apiv1.PipelineRollout](ctx context.Context, rolloutObject *T, controller controller[T]) (*kubernetes.GenericObject, error) {

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
