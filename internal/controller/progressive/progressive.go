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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
)

// progressiveController describes a Controller that can progressively roll out a second child alongside the original child,
// taking down the original child once the new one is healthy
type progressiveController interface {
	// listChildren lists all children of the Rollout identified by the selectors
	ListChildren(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, labelSelector string, fieldSelector string) (*unstructured.UnstructuredList, error)

	// createBaseChildDefinition creates a Kubernetes definition for a child resource of the Rollout with the given name
	CreateBaseChildDefinition(rolloutObject ctlrcommon.RolloutObject, name string) (*unstructured.Unstructured, error)

	// incrementChildCount updates the count of children for the Resource in Kubernetes and returns the index that should be used for the next child
	IncrementChildCount(ctx context.Context, rolloutObject ctlrcommon.RolloutObject) (int32, error)

	// childIsDrained checks to see if the child has been fully drained
	ChildIsDrained(ctx context.Context, child *unstructured.Unstructured) (bool, error)

	// drain updates the child in Kubernetes to cause it to drain
	Drain(ctx context.Context, child *unstructured.Unstructured) error

	// childNeedsUpdating determines if the difference between the current child definition and the desired child definition requires an update
	ChildNeedsUpdating(ctx context.Context, existingChild, newChildDefinition *unstructured.Unstructured) (bool, error)

	// merge is able to take an existing child object and override anything needed from the new one into it to create a revised new object
	Merge(existingObj, newObj *unstructured.Unstructured) (*unstructured.Unstructured, error)
}

// return whether we're done, and error if any
func ProcessResourceWithProgressive(ctx context.Context, rolloutObject ctlrcommon.RolloutObject,
	existingPromotedChild *unstructured.Unstructured, controller progressiveController, c client.Client) (bool, error) {

	numaLogger := logger.FromContext(ctx)

	newUpgradingChildDef, err := makeUpgradingObjectDefinition(ctx, rolloutObject, controller)
	if err != nil {
		return false, err
	}

	// Get the Upgrading object to see if it exists
	existingUpgradingChildDef, err := kubernetes.GetLiveResource(ctx, newUpgradingChildDef, rolloutObject.GetChildPluralName())
	if err != nil {
		// create object as it doesn't exist
		if apierrors.IsNotFound(err) {
			numaLogger.Debugf("Upgrading child of type %s %s/%s doesn't exist so creating", newUpgradingChildDef.GetKind(), newUpgradingChildDef.GetNamespace(), newUpgradingChildDef.GetName())
			err = kubernetes.CreateResource(ctx, c, newUpgradingChildDef)
			return false, err
		} else {
			return false, fmt.Errorf("error getting %s: %v", newUpgradingChildDef.GetKind(), err)
		}
	}
	newUpgradingChildDef, err = controller.Merge(existingUpgradingChildDef, newUpgradingChildDef)
	if err != nil {
		return false, err
	}

	done, err := processUpgradingChild(ctx, rolloutObject, controller, existingPromotedChild, newUpgradingChildDef, existingUpgradingChildDef, c)
	if err != nil {
		return false, err
	}

	return done, nil
}

// create the definition for the child of the Rollout which is the one labeled "upgrading"
func makeUpgradingObjectDefinition(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, controller progressiveController) (*unstructured.Unstructured, error) {

	numaLogger := logger.FromContext(ctx)

	childName, err := GetChildName(ctx, rolloutObject, controller, string(common.LabelValueUpgradeInProgress))
	if err != nil {
		return nil, err
	}
	numaLogger.Debugf("Upgrading child: %s", childName)
	upgradingChild, err := controller.CreateBaseChildDefinition(rolloutObject, childName)
	if err != nil {
		return nil, err
	}

	upgradingChild.SetLabels(map[string]string{
		common.LabelKeyUpgradeState: string(common.LabelValueUpgradeInProgress),
	})

	return upgradingChild, nil
}

func GetChildName(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, controller progressiveController, upgradeState string) (string, error) {
	children, err := controller.ListChildren(ctx, rolloutObject, fmt.Sprintf(
		"%s=%s,%s=%s", common.LabelKeyParentRollout, rolloutObject.GetObjectMeta().Name,
		common.LabelKeyUpgradeState, upgradeState,
	), "")

	if err != nil {
		return "", err
	}
	if len(children.Items) > 1 {
		return "", fmt.Errorf("there should only be one promoted or upgrade in progress pipeline")
	} else if len(children.Items) == 0 {
		index, err := controller.IncrementChildCount(ctx, rolloutObject)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s-%d", rolloutObject.GetObjectMeta().Name, index), nil
	}
	return children.Items[0].GetName(), nil
}

// return whether we're done, and error if any
func processUpgradingChild(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	controller progressiveController,
	existingPromotedChildDef, desiredUpgradingChildDef, existingUpgradingChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	upgradingObjectStatus, err := kubernetes.ParseStatus(existingUpgradingChildDef)
	if err != nil {
		return false, err
	}

	numaLogger.Debugf("Upgrading child %s/%s is in phase %s", existingUpgradingChildDef.GetNamespace(), existingUpgradingChildDef.GetName(), upgradingObjectStatus.Phase)

	switch string(upgradingObjectStatus.Phase) {
	case "Failed":

		rolloutObject.GetStatus().MarkProgressiveUpgradeFailed("New Child Object Failed", rolloutObject.GetObjectMeta().Generation)
		return false, nil

	case "Running":
		if !isNumaflowChildReady(&upgradingObjectStatus) {
			//continue (re-enqueue)
			return false, nil
		}
		// Label the new child as promoted and then remove the label from the old one
		err := updateUpgradeState(ctx, c, common.LabelValueUpgradePromoted, existingUpgradingChildDef, rolloutObject)
		if err != nil {
			return false, err
		}

		// TODO: if any of the below items fail, then we return false which keeps us in UpgradeStrategyInProgress=Progressive, but due to "if pipelineNeedsToUpdate" check in pipelinerollout_controller.go, we never come back here
		// to complete the below logic
		// Consider upgrading to "promoted" as the last thing?

		err = updateUpgradeState(ctx, c, common.LabelValueUpgradeRecyclable, existingPromotedChildDef, rolloutObject)
		if err != nil {
			return false, err
		}

		rolloutObject.GetStatus().MarkProgressiveUpgradeSucceeded("New Child Object Running", rolloutObject.GetObjectMeta().Generation)
		rolloutObject.GetStatus().MarkDeployed(rolloutObject.GetObjectMeta().Generation)

		if err := controller.Drain(ctx, existingPromotedChildDef); err != nil {
			return false, err
		}
		return true, nil
	default:
		// Ensure the latest spec is applied
		// TODO: this needs revisiting - a race condition means we could deem this "Running" prior to latest version
		// being reconciled

		childNeedsToUpdate, err := controller.ChildNeedsUpdating(ctx, existingUpgradingChildDef, desiredUpgradingChildDef) // TODO: if we decide not to drain the upgrading one on failure, I think we can change this to DeepEqual() check
		if err != nil {
			return false, err
		}
		if childNeedsToUpdate {
			numaLogger.Debugf("Upgrading child %s/%s has a new update", existingUpgradingChildDef.GetNamespace(), existingUpgradingChildDef.GetName())

			err = kubernetes.UpdateResource(ctx, c, desiredUpgradingChildDef)
			if err != nil {
				return false, err
			}
		}
		//continue (re-enqueue)
		return false, nil
	}
}

// update the in-memory object with the new Label and patch the object in K8S
func updateUpgradeState(ctx context.Context, c client.Client, upgradeState common.UpgradeState, childObject *unstructured.Unstructured, rolloutObject ctlrcommon.RolloutObject) error {
	childObject.SetLabels(map[string]string{common.LabelKeyUpgradeState: string(upgradeState)})
	patchJson := `{"metadata":{"labels":{"` + common.LabelKeyUpgradeState + `":"` + string(upgradeState) + `"}}}`
	return kubernetes.PatchResource(ctx, c, childObject, patchJson, k8stypes.MergePatchType)
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

func GarbageCollectChildren(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	controller progressiveController,
	c client.Client,
) error {
	numaLogger := logger.FromContext(ctx)
	recyclableObjects, err := getRecyclableObjects(ctx, rolloutObject, controller)
	if err != nil {
		return err
	}

	numaLogger.WithValues("recylableObjects", recyclableObjects).Debug("recycling")

	for _, recyclableChild := range recyclableObjects.Items {
		err = recycle(ctx, &recyclableChild, rolloutObject.GetChildPluralName(), controller, c)
		if err != nil {
			return err
		}
	}
	return nil
}
func getRecyclableObjects(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	controller progressiveController,
) (*unstructured.UnstructuredList, error) {
	return controller.ListChildren(ctx, rolloutObject, fmt.Sprintf(
		"%s=%s,%s=%s", common.LabelKeyParentRollout, rolloutObject.GetObjectMeta().Name,
		common.LabelKeyUpgradeState, common.LabelValueUpgradeRecyclable,
	), "")
}

func recycle(ctx context.Context,
	childObject *unstructured.Unstructured,
	childPluralName string,
	controller progressiveController,
	c client.Client,
) error {
	isDrained, err := controller.ChildIsDrained(ctx, childObject)
	if err != nil {
		return err
	}
	if isDrained {
		err = kubernetes.DeleteResource(ctx, c, childObject)
		if err != nil {
			return err
		}
	}
	return nil

}
