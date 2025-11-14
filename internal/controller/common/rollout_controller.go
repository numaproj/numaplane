package common

import (
	"context"
	"fmt"
	"sort"

	k8stypes "k8s.io/apimachinery/pkg/types"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/common/riders"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
)

type RolloutController interface {

	// IncrementChildCount updates the count of children for the Resource in Kubernetes and returns the index that should be used for the next child
	IncrementChildCount(ctx context.Context, rolloutObject RolloutObject) (int32, error)

	// Recycle deletes child; returns true if it was in fact deleted
	Recycle(ctx context.Context, childObject *unstructured.Unstructured, c client.Client) (bool, error)

	// GetDesiredRiders gets the list of Riders as specified in the RolloutObject, templated for the specific child name and
	// based on the child definition.
	// Note the child name can be different from child.GetName()
	// The child name is what's used for templating the Rider definition, while the `child` is really only used by the PipelineRolloutReconciler
	// in the case of "per-vertex" Riders. In this case, it's necessary to use the existing child's name to template in order to effectively compare whether the
	// Rider has changed, but use the latest child definition to derive the current list of Vertices that need Riders.
	GetDesiredRiders(rolloutObject RolloutObject, childName string, child *unstructured.Unstructured) ([]riders.Rider, error)

	// GetExistingRiders gets the list of Riders that already exists, either for the Promoted child or the Upgrading child depending on the value of "upgrading"
	GetExistingRiders(ctx context.Context, rolloutObject RolloutObject, upgrading bool) (unstructured.UnstructuredList, error)

	// SetCurrentRiderList updates the list of Riders
	SetCurrentRiderList(ctx context.Context, rolloutObject RolloutObject, riders []riders.Rider)

	// GetTemplateArguments is the map of Arguments used for templating the child definition
	GetTemplateArguments(child *unstructured.Unstructured) map[string]interface{}
}

// Garbage Collect all recyclable children; return true if we've deleted all that are recyclable
func GarbageCollectChildren(
	ctx context.Context,
	rolloutObject RolloutObject,
	controller RolloutController,
	c client.Client,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	recyclableObjects, err := getRecyclableObjects(ctx, rolloutObject, c)
	if err != nil {
		return false, err
	}

	numaLogger.WithValues("recylableObjects", recyclableObjects).Debug("recycling")

	allDeleted := true
	for _, recyclableChild := range recyclableObjects.Items {
		deleted, err := controller.Recycle(ctx, &recyclableChild, c)
		if err != nil {
			return false, err
		}
		if !deleted {
			allDeleted = false
		}
	}
	return allDeleted, nil
}
func getRecyclableObjects(
	ctx context.Context,
	rolloutObject RolloutObject,
	c client.Client,
) (unstructured.UnstructuredList, error) {
	return kubernetes.ListResources(ctx, c, rolloutObject.GetChildGVK(),
		rolloutObject.GetRolloutObjectMeta().Namespace,
		client.MatchingLabels{
			common.LabelKeyParentRollout: rolloutObject.GetRolloutObjectMeta().Name,
			common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeRecyclable),
		},
	)
}

// Find the children of a given Rollout of specified UpgradeState (plus optional UpgradeStateReason)
func FindChildrenOfUpgradeState(ctx context.Context, rolloutObject RolloutObject, upgradeState common.UpgradeState, upgradeStateReason *common.UpgradeStateReason, checkLive bool, c client.Client) (unstructured.UnstructuredList, error) {
	childGVR := rolloutObject.GetChildGVR()

	var children unstructured.UnstructuredList
	var err error
	if checkLive {
		var labelSelector string
		if upgradeStateReason == nil {
			labelSelector = fmt.Sprintf(
				"%s=%s,%s=%s", common.LabelKeyParentRollout, rolloutObject.GetRolloutObjectMeta().Name,
				common.LabelKeyUpgradeState, string(upgradeState))
		} else {
			labelSelector = fmt.Sprintf(
				"%s=%s,%s=%s,%s=%s", common.LabelKeyParentRollout, rolloutObject.GetRolloutObjectMeta().Name,
				common.LabelKeyUpgradeState, string(upgradeState), common.LabelKeyUpgradeStateReason, string(*upgradeStateReason))
		}
		children, err = kubernetes.ListLiveResource(
			ctx, childGVR.Group, childGVR.Version, childGVR.Resource,
			rolloutObject.GetRolloutObjectMeta().Namespace, labelSelector, "")
	} else {
		var labelMatch client.MatchingLabels
		if upgradeStateReason == nil {
			labelMatch = client.MatchingLabels{
				common.LabelKeyParentRollout: rolloutObject.GetRolloutObjectMeta().Name,
				common.LabelKeyUpgradeState:  string(upgradeState),
			}
		} else {
			labelMatch = client.MatchingLabels{
				common.LabelKeyParentRollout:      rolloutObject.GetRolloutObjectMeta().Name,
				common.LabelKeyUpgradeState:       string(upgradeState),
				common.LabelKeyUpgradeStateReason: string(*upgradeStateReason),
			}

		}
		children, err = kubernetes.ListResources(ctx, c, rolloutObject.GetChildGVK(), rolloutObject.GetRolloutObjectMeta().GetNamespace(), labelMatch)
	}

	return children, err
}

// find the most current child of a Rollout (of specified UpgradeState, plus optional UpgradeStateReason)
// typically we should only find one, but perhaps a previous reconciliation failure could cause us to find multiple
// if we do see older ones, recycle them
func FindMostCurrentChildOfUpgradeState(ctx context.Context, rolloutObject RolloutObject, upgradeState common.UpgradeState, upgradeStateReason *common.UpgradeStateReason, checkLive bool, c client.Client) (*unstructured.Unstructured, error) {
	numaLogger := logger.FromContext(ctx)

	children, err := FindChildrenOfUpgradeState(ctx, rolloutObject, upgradeState, upgradeStateReason, checkLive, c)
	if err != nil {
		return nil, err
	}

	numaLogger.Debugf("looking for children of Rollout %s/%s of upgrade state=%v, upgrade state reason=%v, found: %s",
		rolloutObject.GetRolloutObjectMeta().Namespace, rolloutObject.GetRolloutObjectMeta().Name, upgradeState, util.OptionalString(upgradeStateReason), kubernetes.ExtractResourceNames(&children))

	if len(children.Items) > 1 {
		// Sort children by creation timestamp (newest first)
		sort.Slice(children.Items, func(i, j int) bool {
			return children.Items[i].GetCreationTimestamp().After(children.Items[j].GetCreationTimestamp().Time)
		})

		// The most current child is the first one after sorting (newest)
		mostCurrentChild := &children.Items[0]

		// Mark older children for recycling
		if upgradeState != common.LabelValueUpgradeRecyclable {
			for i := 1; i < len(children.Items); i++ {
				recyclableChild := &children.Items[i]
				numaLogger.Debugf("found multiple children of Rollout %s/%s of upgrade state=%q, marking recyclable: %s",
					rolloutObject.GetRolloutObjectMeta().Namespace, rolloutObject.GetRolloutObjectMeta().Name, upgradeState, recyclableChild.GetName())
				purgeOld := common.LabelValuePurgeOld
				err = UpdateUpgradeState(ctx, c, common.LabelValueUpgradeRecyclable, &purgeOld, recyclableChild)
				if err != nil {
					numaLogger.Error(err, "failed to mark older child objects") // don't return error, as it's a non-essential operation
				}
			}
		}
		return mostCurrentChild, nil
	} else if len(children.Items) == 1 {
		return &children.Items[0], nil
	} else {
		return nil, nil
	}
}

// update the in-memory object with the new Label and patch the object in K8S
func UpdateUpgradeState(ctx context.Context, c client.Client, upgradeState common.UpgradeState, upgradeStateReason *common.UpgradeStateReason, childObject *unstructured.Unstructured) error {
	numaLogger := logger.FromContext(ctx)

	numaLogger.WithValues("upgradeState", upgradeState, "upgradeStateReason", util.OptionalString(upgradeStateReason)).Debugf("patching upgradeState and upgradeStateReason to %s:%s/%s",
		childObject.GetKind(), childObject.GetNamespace(), childObject.GetName())
	var patchJson string
	labels := childObject.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[common.LabelKeyUpgradeState] = string(upgradeState)
	if upgradeStateReason != nil {
		labels[common.LabelKeyUpgradeStateReason] = string(*upgradeStateReason)
		patchJson = `{"metadata":{"labels":{"` + common.LabelKeyUpgradeState + `":"` + string(upgradeState) + `","` + common.LabelKeyUpgradeStateReason + `":"` + string(*upgradeStateReason) + `"}}}`
	} else {
		patchJson = `{"metadata":{"labels":{"` + common.LabelKeyUpgradeState + `":"` + string(upgradeState) + `"}}}`
	}
	childObject.SetLabels(labels)
	return kubernetes.PatchResource(ctx, c, childObject, patchJson, k8stypes.MergePatchType)
}

func GetUpgradeState(ctx context.Context, c client.Client, childObject *unstructured.Unstructured) (*common.UpgradeState, *common.UpgradeStateReason) {
	numaLogger := logger.FromContext(ctx)

	upgradeStateStr, found := childObject.GetLabels()[common.LabelKeyUpgradeState]
	if !found {
		numaLogger.Debug("upgradeState unset for %s:%s/%s", childObject.GetKind(), childObject.GetNamespace(), childObject.GetName())
		return nil, nil
	} else {
		reasonStr, found := childObject.GetLabels()[common.LabelKeyUpgradeStateReason]
		numaLogger.WithValues("upgradeState", upgradeStateStr, "upgradeStateReason", util.OptionalString(reasonStr)).Debugf("reading upgradeState and upgradeStateReason for %s:%s/%s",
			childObject.GetKind(), childObject.GetNamespace(), childObject.GetName())
		if !found {
			upgradeState := common.UpgradeState(upgradeStateStr)
			return &upgradeState, nil
		} else {
			upgradeState := common.UpgradeState(upgradeStateStr)
			reason := common.UpgradeStateReason(reasonStr)
			return &upgradeState, &reason
		}
	}

}

// get the name of the child whose parent is "rolloutObject" and whose upgrade state is "upgradeState" (and if upgradeStateReason is that, check that as well)
// if none is found, create a new one
// if one is found, create a new one if "useExistingChild=false", else use existing one
func GetChildName(ctx context.Context, rolloutObject RolloutObject, controller RolloutController, upgradeState common.UpgradeState, upgradeStateReason *common.UpgradeStateReason, c client.Client, useExistingChild bool) (string, error) {

	existingChild, err := FindMostCurrentChildOfUpgradeState(ctx, rolloutObject, upgradeState, upgradeStateReason, true, c) // if for some reason there's more than 1
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

// Determine the list of Riders which are needed for the child and create them on the cluster
func CreateRidersForNewChild(
	ctx context.Context,
	controller RolloutController,
	rolloutObject RolloutObject,
	child *unstructured.Unstructured,
	c client.Client,
) error {

	// create definitions for riders by templating what's defined in the Rollout definition with the child definition
	newRiders, err := controller.GetDesiredRiders(rolloutObject, child.GetName(), child)
	if err != nil {
		return fmt.Errorf("error getting desired Riders for child %s: %s", child.GetName(), err)
	}
	riderAdditions := unstructured.UnstructuredList{}
	for _, rider := range newRiders {
		riderAdditions.Items = append(riderAdditions.Items, rider.Definition)
	}

	if err = riders.UpdateRidersInK8S(ctx, child, riderAdditions, unstructured.UnstructuredList{}, unstructured.UnstructuredList{}, c); err != nil {
		return err
	}

	// now reflect this in the Status
	controller.SetCurrentRiderList(ctx, rolloutObject, newRiders)
	return nil
}
