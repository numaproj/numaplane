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

package numaflowtypes

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/numaproj/numaplane/internal/common"
)

// WithControllerInstanceID keeps Numaflow's controller-selection annotation and Numaplane's lookup label aligned
// on any Numaflow resource that is bound to a controller instance (Pipeline, MonoVertex, ISBService).
// If instanceID is empty, an annotation supplied in the Rollout metadata is preserved and used as the source of truth.
func WithControllerInstanceID(obj *unstructured.Unstructured, instanceID string) error {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	if instanceID != "" {
		annotations[common.AnnotationKeyNumaflowInstanceID] = instanceID
	}
	obj.SetAnnotations(annotations)

	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	effectiveInstanceID := annotations[common.AnnotationKeyNumaflowInstanceID]
	if effectiveInstanceID == "" {
		delete(labels, common.LabelKeyControllerInstanceID)
		obj.SetLabels(labels)
		return nil
	}
	if validationErrors := validation.IsValidLabelValue(effectiveInstanceID); len(validationErrors) > 0 {
		return fmt.Errorf("controller instance ID %q is not a valid Kubernetes label value: %s", effectiveInstanceID, validationErrors[0])
	}
	labels[common.LabelKeyControllerInstanceID] = effectiveInstanceID
	obj.SetLabels(labels)
	return nil
}

// ControllerInstanceIDFromResource returns the Numaflow instance annotation, falling back to Numaplane's lookup label.
func ControllerInstanceIDFromResource(obj *unstructured.Unstructured) string {
	if obj == nil {
		return ""
	}
	if id := obj.GetAnnotations()[common.AnnotationKeyNumaflowInstanceID]; id != "" {
		return id
	}
	return obj.GetLabels()[common.LabelKeyControllerInstanceID]
}

// HasControllerInstanceLabel reports whether Numaplane has bound this resource to a controller instance.
func HasControllerInstanceLabel(obj *unstructured.Unstructured) bool {
	if obj == nil {
		return false
	}
	_, found := obj.GetLabels()[common.LabelKeyControllerInstanceID]
	return found
}
