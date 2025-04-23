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

// have one of these per vertex for pipeline
type Rider struct {
	// new definition
	Definition unstructured.Unstructured
	// type of change required
	RequiresProgressive bool
}

// create an annotation to preserve a hash of the metadata and spec, so that we can compare the new value to the existing value
// to know if there was a change
func WithHashAnnotation(resource unstructured.Unstructured) (unstructured.Unstructured, string, error) {
	// json serialize our resource and then hash that
	asBytes, err := json.Marshal(resource)
	if err != nil {
		return resource, "", err
	}
	//hashVal := hex.EncodeToString(sha256.Sum256([]byte(asBytes)))
	h := sha256.New()
	_, err = h.Write(asBytes)
	if err != nil {
		return resource, "", err
	}

	unstrucWithAnnotation := resource.DeepCopy()
	annotations := unstrucWithAnnotation.GetAnnotations()
	hashVal := hex.EncodeToString(h.Sum(nil))
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[common.AnnotationKeyHash] = hashVal
	unstrucWithAnnotation.SetAnnotations(annotations)
	return *unstrucWithAnnotation, hashVal, nil
}

func GetExistingHashAnnotation(resource unstructured.Unstructured) string {
	return resource.GetAnnotations()[common.AnnotationKeyHash]
}

// update the cluster according to the desired modifications to the resources (additions, mods, deletions)
func UpdateRiders(
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

	for _, rider := range riderAdditions.Items {
		/*if err := prepareRiderForDeployment(&rider, child); err != nil {
			return err
		}*/

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

	for _, rider := range riderModifications.Items {

		/*if err := prepareRiderForDeployment(&rider, child); err != nil {
			return err
		}*/

		if err := kubernetes.UpdateResource(ctx, c, &rider); err != nil {
			return fmt.Errorf("failed to update resource %s/%s: %s", rider.GetNamespace(), rider.GetName(), err)
		}
	}

	for _, rider := range riderDeletions.Items {
		if err := kubernetes.DeleteResource(ctx, c, &rider); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete resource %s/%s: %s", rider.GetNamespace(), rider.GetName(), err)
		}
	}
	return nil
}

/*
func prepareRiderForDeployment(
	rider *unstructured.Unstructured,
	child *unstructured.Unstructured,
) error {
	if err := kubernetes.ApplyOwnerReference(rider, child); err != nil {
		return err
	}
	rider.SetNamespace(child.GetNamespace())
	// rename the Rider to a combination of the Rider name and the child name, for the purpose of creating
	// uniqueness between children of the same Rollout
	riderName := fmt.Sprintf("%s-%s", rider.GetName(), child.GetName())
	rider.SetName(riderName)
	return nil
}
*/
