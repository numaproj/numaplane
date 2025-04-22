package riders

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/numaproj/numaplane/internal/common"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

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
