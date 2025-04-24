package riders

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// definition of a Rider to apply
type Rider struct {
	// new definition
	Definition unstructured.Unstructured
	// type of change required
	RequiresProgressive bool
}

// create an annotation to preserve a hash of the metadata and spec, so that we can compare the new value to the existing value
// to know if there was a change
func WithHashAnnotation(ctx context.Context, resource unstructured.Unstructured) (unstructured.Unstructured, string, error) {
	hashVal, err := CalculateHash(ctx, resource)
	if err != nil {
		return unstructured.Unstructured{}, "", err
	}

	unstrucWithAnnotation := resource.DeepCopy()
	annotations := unstrucWithAnnotation.GetAnnotations()

	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[common.AnnotationKeyHash] = hashVal
	unstrucWithAnnotation.SetAnnotations(annotations)
	return *unstrucWithAnnotation, hashVal, nil
}

func CalculateHash(ctx context.Context, resource unstructured.Unstructured) (string, error) {
	numaLogger := logger.FromContext(ctx)

	// json serialize it and then hash that
	asBytes, err := json.Marshal(resource)
	if err != nil {
		return "", err
	}

	h := sha256.New()
	_, err = h.Write(asBytes)
	if err != nil {
		return "", err
	}

	hashVal := hex.EncodeToString(h.Sum(nil))
	numaLogger.WithValues("resource name", resource.GetName(), "hash", hashVal).Debugf("derived hash from resource: %+v", resource)
	return hashVal, nil
}

func GetExistingHashAnnotation(resource unstructured.Unstructured) string {
	return resource.GetAnnotations()[common.AnnotationKeyHash]
}

// update the cluster according to the desired modifications to the resources (additions, mods, deletions)
func UpdateRidersInK8S(
	ctx context.Context,
	child *unstructured.Unstructured,
	riderAdditions unstructured.UnstructuredList,
	riderModifications unstructured.UnstructuredList,
	riderDeletions unstructured.UnstructuredList,
	c client.Client) error {

	numaLogger := logger.FromContext(ctx)

	numaLogger.WithValues(
		"child", child.GetName(),
		"rider additions", riderAdditions,
		"rider modifications", riderModifications,
		"rider deletions", riderDeletions).Debug("updating riders")

	// Create new Resources
	for _, rider := range riderAdditions.Items {
		if err := prepareRiderForDeployment(ctx, &rider, child); err != nil {
			return err
		}

		if err := kubernetes.CreateResource(ctx, c, &rider); err != nil {
			if apierrors.IsAlreadyExists(err) {
				numaLogger.Warnf("rider %s already exists so updating instead of creating", rider.GetName())
				if err := kubernetes.UpdateResource(ctx, c, &rider); err != nil {
					return fmt.Errorf("failed to update resource %s/%s: %s", rider.GetNamespace(), rider.GetName(), err)
				}
			} else {
				return fmt.Errorf("failed to create resource %s/%s: %s", rider.GetNamespace(), rider.GetName(), err)
			}
		}
	}

	// Update existing resources that need updating
	for _, rider := range riderModifications.Items {

		if err := prepareRiderForDeployment(ctx, &rider, child); err != nil {
			return err
		}

		if err := kubernetes.UpdateResource(ctx, c, &rider); err != nil {
			return fmt.Errorf("failed to update resource %s/%s: %s", rider.GetNamespace(), rider.GetName(), err)
		}
	}

	// Delete resources that are no longer needed
	for _, rider := range riderDeletions.Items {
		if err := kubernetes.DeleteResource(ctx, c, &rider); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete resource %s/%s: %s", rider.GetNamespace(), rider.GetName(), err)
		}
	}
	return nil
}

func prepareRiderForDeployment(ctx context.Context, rider *unstructured.Unstructured, owner *unstructured.Unstructured) error {
	var err error
	// note we must hash this before applying any other fields, so that we can correctly compare this hash to a new resource which doesn't have those fields set yet
	*rider, _, err = WithHashAnnotation(ctx, *rider)
	if err != nil {
		return err
	}
	if err := kubernetes.ApplyOwnerReference(rider, owner); err != nil {
		return err
	}
	return nil
}

// Determine the list of Riders which are needed for the child and create them on the cluster
func CreateRidersForNewChild(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	monoVertex *unstructured.Unstructured,
) error {

	// create definitions for riders by templating what's defined in the MonoVertexRollout definition with the monovertex name
	newRiders, err := r.GetDesiredRiders(monoVertexRollout, monoVertex)
	if err != nil {
		return fmt.Errorf("error getting desired Riders for MonoVertex %s: %s", monoVertex.GetName(), err)
	}
	riderAdditions := unstructured.UnstructuredList{}
	for _, rider := range newRiders {
		riderAdditions.Items = append(riderAdditions.Items, rider.Definition)
	}

	if err = riders.UpdateRidersInK8S(ctx, monoVertex, riderAdditions, unstructured.UnstructuredList{}, unstructured.UnstructuredList{}, r.client); err != nil {
		return err
	}

	// now reflect this in the Status
	r.SetCurrentRiderList(monoVertexRollout, newRiders)
	return nil
}
