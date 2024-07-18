package kubernetes

import (
	"encoding/json"
	"fmt"

	"github.com/numaproj/numaplane/internal/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// this file contains utility functions for working with Unstructured types

// GetLabel returns the label identified by "key"
func GetLabel(un *unstructured.Unstructured, key string) (string, error) {
	labels, err := NestedNullableStringMap(un.Object, "metadata", "labels")
	if err != nil {
		return "", fmt.Errorf("failed to get labels from target object %s %s/%s: %w", un.GroupVersionKind().String(), un.GetNamespace(), un.GetName(), err)
	}
	if labels != nil {
		return labels[key], nil
	}
	return "", nil
}

// SetLabel sets the label identified by "key" on an unstructured object
func SetLabel(target *unstructured.Unstructured, key, val string) error {

	labels, err := NestedNullableStringMap(target.Object, "metadata", "labels")
	if err != nil {
		return fmt.Errorf("failed to get labels from target object %s %s/%s: %w", target.GroupVersionKind().String(), target.GetNamespace(), target.GetName(), err)
	}
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[key] = val
	target.SetLabels(labels)

	return nil
}

// NestedNullableStringMap returns a copy of map[string]string value of a nested field.
// Returns an error if not one of map[string]interface{} or nil, or contains non-string values in the map.
func NestedNullableStringMap(obj map[string]interface{}, fields ...string) (map[string]string, error) {
	var m map[string]string
	val, found, err := unstructured.NestedFieldNoCopy(obj, fields...)
	if err != nil {
		return nil, err
	}
	if found && val != nil {
		val, _, err := unstructured.NestedStringMap(obj, fields...)
		return val, err
	}
	return m, err
}

func CompareSpecs(a *GenericObject, b *GenericObject) (bool, error) {
	aAsMap := make(map[string]interface{})
	bAsMap := make(map[string]interface{})
	err := json.Unmarshal(a.Spec.Raw, &aAsMap)
	if err != nil {
		return false, err
	}
	err = json.Unmarshal(b.Spec.Raw, &bAsMap)
	if err != nil {
		return false, err
	}
	return util.CompareMapsIgnoringNulls(aAsMap, bAsMap), nil
}

func (obj *GenericObject) DeepCopy() *GenericObject {
	result := &GenericObject{}
	result.TypeMeta.Kind = obj.TypeMeta.Kind
	result.TypeMeta.APIVersion = obj.TypeMeta.APIVersion
	result.ObjectMeta = *obj.ObjectMeta.DeepCopy()
	result.Spec = *obj.Spec.DeepCopy()
	result.Status = *obj.Status.DeepCopy()
	return result
}
