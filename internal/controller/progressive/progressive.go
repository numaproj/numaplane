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
	"strconv"
	"strings"

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
	existingPromotedChild *unstructured.Unstructured, promotedDifference bool, controller progressiveController, c client.Client) (bool, error) {

	numaLogger := logger.FromContext(ctx)

	// is there currently an "upgrading" child?
	currentUpgradingChildDef, err := findMostCurrentChildOfUpgradeState(ctx, rolloutObject, common.LabelValueUpgradeInProgress, false, c)
	if err != nil {
		return false, err
	}

	// if there's a difference between the desired spec and the current "promoted" child, and there isn't already an "upgrading" definition, then create one and return
	if promotedDifference && currentUpgradingChildDef == nil {
		// Create it, first making sure one doesn't already exist by checking the live K8S API
		currentUpgradingChildDef, err = findMostCurrentChildOfUpgradeState(ctx, rolloutObject, common.LabelValueUpgradeInProgress, true, c)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// create object as it doesn't exist
				newUpgradingChildDef, err := makeUpgradingObjectDefinition(ctx, rolloutObject, controller, c)
				if err != nil {
					return false, err
				}

				numaLogger.Debugf("Upgrading child of type %s %s/%s doesn't exist so creating", newUpgradingChildDef.GetKind(), newUpgradingChildDef.GetNamespace(), newUpgradingChildDef.GetName())
				err = kubernetes.CreateResource(ctx, c, newUpgradingChildDef)
				return false, err
			} else {
				return false, fmt.Errorf("error getting %s: %v", currentUpgradingChildDef.GetKind(), err)
			}
		}
	}
	if currentUpgradingChildDef == nil { // nothing to do
		return true, err
	}

	// There's already an Upgrading child, now process it

	// Get the live resource so we don't have issues with an outdated cache
	currentUpgradingChildDef, err = kubernetes.GetLiveResource(ctx, currentUpgradingChildDef, rolloutObject.GetChildGVR().Resource)
	if err != nil {
		return false, err
	}

	done, err := processUpgradingChild(ctx, rolloutObject, controller, existingPromotedChild, currentUpgradingChildDef, c)
	if err != nil {
		return false, err
	}

	return done, nil
}

// create the definition for the child of the Rollout which is the one labeled "upgrading"
func makeUpgradingObjectDefinition(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, controller progressiveController, c client.Client) (*unstructured.Unstructured, error) {

	numaLogger := logger.FromContext(ctx)

	childName, err := GetChildName(ctx, rolloutObject, controller, common.LabelValueUpgradeInProgress, c)
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

func findChildrenOfUpgradeState(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, upgradeState common.UpgradeState, checkLive bool, c client.Client) (*unstructured.UnstructuredList, error) {
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

// of the children provided of a Rollout, find the most current one and mark the others recyclable
// typically we should only find one, but perhaps a previous reconciliation failure could cause us to find multiple
func findMostCurrentChildOfUpgradeState(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, upgradeState common.UpgradeState, checkLive bool, c client.Client) (*unstructured.Unstructured, error) {
	numaLogger := logger.FromContext(ctx)

	children, err := findChildrenOfUpgradeState(ctx, rolloutObject, upgradeState, checkLive, c)
	if err != nil {
		return nil, err
	}

	if len(children.Items) > 1 {
		mostCurrentChild := &unstructured.Unstructured{}
		recycleList := []*unstructured.Unstructured{}
		mostCurrentIndex := -1
		for _, child := range children.Items {
			childIndex, err := getChildIndex(child.GetName())
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
			numaLogger.Debugf("found multiple children of upgrade state=%q, marking recyclable: %s", upgradeState, recyclableChild.GetName())
			_ = updateUpgradeState(ctx, c, common.LabelValueUpgradeRecyclable, recyclableChild, rolloutObject)
		}
		return mostCurrentChild, nil
	} else if len(children.Items) == 1 {
		return &children.Items[0], nil
	} else {
		return nil, nil
	}
}

// get the index of the child following the dash in the name
func getChildIndex(childName string) (int, error) {
	dash := strings.LastIndex(childName, "-")
	if dash == -1 {
		return 0, fmt.Errorf("child name %q doesn't contain a dash", childName)
	}
	suffix := childName[dash+1:]
	childIndex, err := strconv.Atoi(suffix)
	if err != nil {
		return 0, fmt.Errorf("child name %q has a suffix which is not an integer", childName)
	}
	return childIndex, nil
}

// TODO: can this function make use of the one above?
func GetChildName(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, controller progressiveController, upgradeState common.UpgradeState, c client.Client) (string, error) {

	existingChild, err := findMostCurrentChildOfUpgradeState(ctx, rolloutObject, upgradeState, true, c) // if for some reason there's more than 1
	if err != nil {
		return "", err
	}
	if existingChild == nil {
		index, err := controller.IncrementChildCount(ctx, rolloutObject)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s-%d", rolloutObject.GetRolloutObjectMeta().Name, index), nil
	} else {
		return existingChild.GetName(), nil
	}
}

// return whether we're done, and error if any
func processUpgradingChild(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	controller progressiveController,
	existingPromotedChildDef, existingUpgradingChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	assessment, err := assessUpgradingChild(ctx, existingUpgradingChildDef)
	if err != nil {
		return false, err
	}
	numaLogger.WithValues("name", existingUpgradingChildDef.GetName(), "asssessment", assessment).Debug("assessment returned")

	switch assessment {
	case AssessmentResultFailure:

		rolloutObject.GetRolloutStatus().MarkProgressiveUpgradeFailed(fmt.Sprintf("New Child Object %s/%s Failed", existingUpgradingChildDef.GetNamespace(), existingUpgradingChildDef.GetName()), rolloutObject.GetRolloutObjectMeta().Generation)

		// check if there are any new incoming changes to the desired spec
		newUpgradingChildDef, err := makeUpgradingObjectDefinition(ctx, rolloutObject, controller, c)
		if err != nil {
			return false, err
		}
		newUpgradingChildDef, err = controller.Merge(existingUpgradingChildDef, newUpgradingChildDef)
		if err != nil {
			return false, err
		}
		needsUpdating, err := controller.ChildNeedsUpdating(ctx, existingUpgradingChildDef, newUpgradingChildDef)

		// if so, mark the existing one for garbage collection and then create a new upgrading one
		if needsUpdating {
			numaLogger.WithValues("old child", existingUpgradingChildDef.GetName(), "new child", newUpgradingChildDef.GetName(), "replacing 'upgrading' child")
			err = updateUpgradeState(ctx, c, common.LabelValueUpgradeRecyclable, existingUpgradingChildDef, rolloutObject)
			if err != nil {
				return false, err
			}

			err = kubernetes.CreateResource(ctx, c, newUpgradingChildDef)
			if err != nil {
				return false, err
			} else {
				return false, fmt.Errorf("error getting %s: %v", newUpgradingChildDef.GetKind(), err)
			}

		}

		return false, nil

	case AssessmentResultSuccess:
		// Label the new child as promoted and then remove the label from the old one
		numaLogger.WithValues("old child", existingPromotedChildDef.GetName(), "new child", existingUpgradingChildDef.GetName(), "replacing 'promoted' child")
		err := updateUpgradeState(ctx, c, common.LabelValueUpgradePromoted, existingUpgradingChildDef, rolloutObject)
		if err != nil {
			return false, err
		}

		// TODO: what if we fail before this line? It seems we will have 2 promoted pipelines in that case - maybe our logic should always assume the highest index "promoted" one is the best and then garbage collect the others?
		err = updateUpgradeState(ctx, c, common.LabelValueUpgradeRecyclable, existingPromotedChildDef, rolloutObject)
		if err != nil {
			return false, err
		}

		rolloutObject.GetRolloutStatus().MarkProgressiveUpgradeSucceeded(fmt.Sprintf("New Child Object %s/%s Running", existingUpgradingChildDef.GetNamespace(), existingUpgradingChildDef.GetName()), rolloutObject.GetRolloutObjectMeta().Generation)
		rolloutObject.GetRolloutStatus().MarkDeployed(rolloutObject.GetRolloutObjectMeta().Generation)

		if err := controller.Drain(ctx, existingPromotedChildDef); err != nil {
			return false, err
		}
		return true, nil
	default:
		// Ensure the latest spec is applied
		// TODO: instead of all of this, under the "Failed" section we can try creating a new one

		/*childNeedsToUpdate, err := controller.ChildNeedsUpdating(ctx, existingUpgradingChildDef, desiredUpgradingChildDef) // TODO: if we decide not to drain the upgrading one on failure, I think we can change this to DeepEqual() check
		if err != nil {
			return false, err
		}
		if childNeedsToUpdate {
			numaLogger.Debugf("Upgrading child %s/%s has a new update", existingUpgradingChildDef.GetNamespace(), existingUpgradingChildDef.GetName())

			err = kubernetes.UpdateResource(ctx, c, desiredUpgradingChildDef)
			if err != nil {
				return false, err
			}
		}*/
		//continue (re-enqueue)
		return false, nil
	}
}

type AssessmentResult int

const (
	AssessmentResultSuccess = iota
	AssessmentResultFailure
	AssessmentResultUnknown
)

func assessUpgradingChild(ctx context.Context, existingUpgradingChildDef *unstructured.Unstructured) (AssessmentResult, error) {
	numaLogger := logger.FromContext(ctx)
	upgradingObjectStatus, err := kubernetes.ParseStatus(existingUpgradingChildDef)
	if err != nil {
		return AssessmentResultUnknown, err
	}

	numaLogger.
		WithValues("namespace", existingUpgradingChildDef.GetNamespace(), "name", existingUpgradingChildDef.GetName()).
		Debugf("Upgrading child is in phase %s", upgradingObjectStatus.Phase)

	if upgradingObjectStatus.Phase == "Running" && isNumaflowChildReady(&upgradingObjectStatus) {
		return AssessmentResultSuccess, nil
	}
	if upgradingObjectStatus.Phase == "Failed" {
		return AssessmentResultFailure, nil
	}
	return AssessmentResultUnknown, nil
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
	recyclableObjects, err := getRecyclableObjects(ctx, rolloutObject, c)
	if err != nil {
		return err
	}

	numaLogger.WithValues("recylableObjects", recyclableObjects).Debug("recycling")

	for _, recyclableChild := range recyclableObjects.Items {
		err = recycle(ctx, &recyclableChild, controller, c)
		if err != nil {
			return err
		}
	}
	return nil
}
func getRecyclableObjects(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	c client.Client,
) (*unstructured.UnstructuredList, error) {
	return kubernetes.ListResources(ctx, c, rolloutObject.GetChildGVK(),
		rolloutObject.GetRolloutObjectMeta().Namespace,
		client.MatchingLabels{
			common.LabelKeyParentRollout: rolloutObject.GetRolloutObjectMeta().Name,
			common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeRecyclable),
		},
	)
}

func recycle(ctx context.Context,
	childObject *unstructured.Unstructured,
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
