package common

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type RolloutController interface {

	// IncrementChildCount updates the count of children for the Resource in Kubernetes and returns the index that should be used for the next child
	IncrementChildCount(ctx context.Context, rolloutObject RolloutObject) (int32, error)

	// Recycle deletes child; returns true if it was in fact deleted
	Recycle(ctx context.Context, childObject *unstructured.Unstructured, c client.Client) (bool, error)
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
) (*unstructured.UnstructuredList, error) {
	return kubernetes.ListResources(ctx, c, rolloutObject.GetChildGVK(),
		rolloutObject.GetRolloutObjectMeta().Namespace,
		client.MatchingLabels{
			common.LabelKeyParentRollout: rolloutObject.GetRolloutObjectMeta().Name,
			common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeRecyclable),
		},
	)
}

func findChildrenOfUpgradeState(ctx context.Context, rolloutObject RolloutObject, upgradeState common.UpgradeState, checkLive bool, c client.Client) (*unstructured.UnstructuredList, error) {
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

// find the most current child of a Rollout
// typically we should only find one, but perhaps a previous reconciliation failure could cause us to find multiple
// if we do see older ones, recycle them
func FindMostCurrentChildOfUpgradeState(ctx context.Context, rolloutObject RolloutObject, upgradeState common.UpgradeState, checkLive bool, c client.Client) (*unstructured.Unstructured, error) {
	numaLogger := logger.FromContext(ctx)

	children, err := findChildrenOfUpgradeState(ctx, rolloutObject, upgradeState, checkLive, c)
	if err != nil {
		return nil, err
	}

	numaLogger.Debugf("looking for children of Rollout %s/%s of upgrade state=%v, found: %s",
		rolloutObject.GetRolloutObjectMeta().Namespace, rolloutObject.GetRolloutObjectMeta().Name, upgradeState, kubernetes.ExtractResourceNames(children))

	if len(children.Items) > 1 {
		var mostCurrentChild *unstructured.Unstructured
		recycleList := []*unstructured.Unstructured{}
		mostCurrentIndex := math.MinInt
		for _, child := range children.Items {
			childIndex, err := getChildIndex(rolloutObject.GetRolloutObjectMeta().Name, child.GetName())
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
			numaLogger.Debugf("found multiple children of Rollout %s/%s of upgrade state=%q, marking recyclable: %s",
				rolloutObject.GetRolloutObjectMeta().Namespace, rolloutObject.GetRolloutObjectMeta().Name, upgradeState, recyclableChild.GetName())
			_ = UpdateUpgradeState(ctx, c, common.LabelValueUpgradeRecyclable, recyclableChild)
		}
		return mostCurrentChild, nil
	} else if len(children.Items) == 1 {
		return &children.Items[0], nil
	} else {
		return nil, nil
	}
}

// update the in-memory object with the new Label and patch the object in K8S
func UpdateUpgradeState(ctx context.Context, c client.Client, upgradeState common.UpgradeState, childObject *unstructured.Unstructured) error {
	labels := childObject.GetLabels()
	labels[common.LabelKeyUpgradeState] = string(upgradeState)
	childObject.SetLabels(labels)
	patchJson := `{"metadata":{"labels":{"` + common.LabelKeyUpgradeState + `":"` + string(upgradeState) + `"}}}`
	return kubernetes.PatchResource(ctx, c, childObject, patchJson, k8stypes.MergePatchType)
}

// Get the index of the child following the dash in the name
// childName should be the rolloutName + '-<integer>'
// For backward compatibility, support child resources whose names were equivalent to rollout names, returning -1 index
func getChildIndex(rolloutName string, childName string) (int, error) {
	// verify that the initial part of the child name is the rolloutName
	if !strings.HasPrefix(childName, rolloutName) {
		return 0, fmt.Errorf("child name %q should begin with rollout name %q", childName, rolloutName)
	}
	// backward compatibility for older naming convention (before the '-<integer>' suffix was introduced - if it's the same name, consider it to essentially be the smallest index
	if childName == rolloutName {
		return -1, nil
	}

	// next character should be a dash
	dash := childName[len(rolloutName)]
	if dash != '-' {
		return 0, fmt.Errorf("child name %q should begin with rollout name %q, followed by '-<integer>'", childName, rolloutName)
	}

	// remaining characters should be the integer index
	suffix := childName[len(rolloutName)+1:]

	childIndex, err := strconv.Atoi(suffix)
	if err != nil {
		return 0, fmt.Errorf("child name %q has a suffix which is not an integer", childName)
	}
	return childIndex, nil
}

// get the name of the child whose parent is "rolloutObject" and whose upgrade state is "upgradeState"
// if none is found, create a new one
// if one is found, create a new one if "useExistingChild=false", else use existing one
func GetChildName(ctx context.Context, rolloutObject RolloutObject, controller RolloutController, upgradeState common.UpgradeState, c client.Client, useExistingChild bool) (string, error) {

	existingChild, err := FindMostCurrentChildOfUpgradeState(ctx, rolloutObject, upgradeState, true, c) // if for some reason there's more than 1
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
