package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/numaproj/numaplane/internal/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/util/logger"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// this file contains utility functions for working with Unstructured types

type GenericObject struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   runtime.RawExtension `json:"spec"`
	Status runtime.RawExtension `json:"status,omitempty"`
}

func (obj *GenericObject) DeepCopyObject() runtime.Object {
	return obj.DeepCopy()
}

type GenericStatus struct {
	Phase              string             `json:"phase,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
}

func ParseStatus(obj *GenericObject) (GenericStatus, error) {
	if obj == nil || len(obj.Status.Raw) == 0 {
		return GenericStatus{}, nil
	}

	var status GenericStatus
	err := json.Unmarshal(obj.Status.Raw, &status)
	if err != nil {
		return GenericStatus{}, err
	}
	return status, nil
}

func GetLiveUnstructuredResource(
	ctx context.Context,
	object *GenericObject,
	pluralName string,
) (*unstructured.Unstructured, error) {
	numaLogger := logger.FromContext(ctx)
	gvr, err := getGroupVersionResource(object, pluralName)
	if err != nil {
		return nil, err
	}

	unstruc, err := dynamicClient.Resource(gvr).Namespace(object.Namespace).Get(ctx, object.Name, metav1.GetOptions{})
	if unstruc != nil {
		numaLogger.Verbosef("retrieved resource %s/%s of type %+v with value %+v", object.Namespace, object.Name, gvr, unstruc.Object)
	}
	return unstruc, err
}

func ListLiveUnstructuredResource(
	ctx context.Context,
	apiGroup string,
	version string,
	pluralName string,
	namespace string,
	labelSelector string, // set to empty string if none
	fieldSelector string, // set to empty string if none
) (*unstructured.UnstructuredList, error) {
	gvr := schema.GroupVersionResource{
		Group:    apiGroup,
		Version:  version,
		Resource: pluralName,
	}
	return dynamicClient.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: fieldSelector})
}

// GetLiveResource converts the generic object to unstructured object and fetches the resource from the API server
func GetLiveResource(ctx context.Context, object *GenericObject, pluralName string) (*GenericObject, error) {
	uns, err := GetLiveUnstructuredResource(ctx, object, pluralName)
	if uns != nil {
		return UnstructuredToObject(uns)
	} else {
		return nil, err
	}
}

func ListLiveResource(ctx context.Context,
	apiGroup string,
	version string,
	pluralName string,
	namespace string,
	// set to empty string if none
	labelSelector string,
	// set to empty string if none
	fieldSelector string) ([]*GenericObject, error) {
	numaLogger := logger.FromContext(ctx)
	unsList, err := ListLiveUnstructuredResource(ctx, apiGroup, version, pluralName, namespace, labelSelector, fieldSelector)
	if err != nil {
		return nil, err
	}

	if unsList != nil {
		numaLogger.Debugf("found %d %s", len(unsList.Items), pluralName)
		objects := make([]*GenericObject, len(unsList.Items))
		for i, uns := range unsList.Items {
			obj, err := UnstructuredToObject(&uns)
			if err != nil {
				return nil, err
			}
			objects[i] = obj
		}
		return objects, nil
	}

	return nil, err
}

func parseApiVersion(apiVersion string) (string, string, error) {
	// should be separated by slash
	index := strings.Index(apiVersion, "/")
	if index == -1 {
		// if there's no slash, it's just the version, and the group should be "core"
		return "core", apiVersion, nil
	} else if index == len(apiVersion)-1 {
		return "", "", fmt.Errorf("apiVersion incorrectly formatted: unexpected slash at end: %q", apiVersion)
	}
	return apiVersion[0:index], apiVersion[index+1:], nil
}

func ObjectToUnstructured(object *GenericObject) (*unstructured.Unstructured, error) {
	var asMap map[string]any
	err := util.StructToStruct(object, &asMap)
	if err != nil {
		return nil, err
	}

	return &unstructured.Unstructured{Object: asMap}, nil
}

func UnstructuredToObject(u *unstructured.Unstructured) (*GenericObject, error) {
	var genericObject GenericObject
	err := util.StructToStruct(u, &genericObject)
	if err != nil {
		return nil, err
	}

	return &genericObject, err
}

func getGroupVersionResource(object *GenericObject, pluralName string) (schema.GroupVersionResource, error) {
	group, version, err := parseApiVersion(object.APIVersion)
	if err != nil {
		return schema.GroupVersionResource{}, err
	}

	return schema.GroupVersionResource{
		Group:    group,
		Version:  version,
		Resource: pluralName,
	}, nil
}

// GetLabel returns the label identified by "key"
func GetLabel(un *unstructured.Unstructured, key string) (string, error) {
	labels, err := nestedNullableStringMap(un.Object, "metadata", "labels")
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

	labels, err := nestedNullableStringMap(target.Object, "metadata", "labels")
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

// nestedNullableStringMap returns a copy of map[string]string value of a nested field.
// Returns an error if not one of map[string]interface{} or nil, or contains non-string values in the map.
func nestedNullableStringMap(obj map[string]interface{}, fields ...string) (map[string]string, error) {
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

// DeepCopy returns a deep copy of the GenericObject
func (obj *GenericObject) DeepCopy() *GenericObject {
	result := &GenericObject{}
	result.TypeMeta.Kind = obj.TypeMeta.Kind
	result.TypeMeta.APIVersion = obj.TypeMeta.APIVersion
	result.ObjectMeta = *obj.ObjectMeta.DeepCopy()
	result.Spec = *obj.Spec.DeepCopy()
	result.Status = *obj.Status.DeepCopy()
	return result
}

// CreateResource creates the resource in the kubernetes cluster
func CreateResource(ctx context.Context, c client.Client, obj *GenericObject) error {
	unstructuredObj, err := ObjectToUnstructured(obj)
	if err != nil {
		return err
	}

	return c.Create(ctx, unstructuredObj)
}

// GetResource retrieves the resource from the informer cache, if it's not found then it fetches from the API server.
func GetResource(ctx context.Context, c client.Client, gvk schema.GroupVersionKind, namespacedName k8stypes.NamespacedName) (*GenericObject, error) {
	unstructuredObj := &unstructured.Unstructured{}
	unstructuredObj.SetGroupVersionKind(gvk)

	if err := c.Get(ctx, namespacedName, unstructuredObj); err != nil {
		return nil, err
	}

	return UnstructuredToObject(unstructuredObj)
}

// UpdateResource updates the resource in the kubernetes cluster
func UpdateResource(ctx context.Context, c client.Client, obj *GenericObject) error {
	unstructuredObj, err := ObjectToUnstructured(obj)
	if err != nil {
		return err
	}

	if err = c.Update(ctx, unstructuredObj); err != nil {
		return err
	} else {
		result, err := UnstructuredToObject(unstructuredObj)
		if err != nil {
			return err
		}
		*obj = *result
	}

	return nil
}

// ListResources retrieves the list of resources from the informer cache, if it's not found then it fetches from the API server.
func ListResources(ctx context.Context, c client.Client, gvk schema.GroupVersionKind, opts ...client.ListOption) ([]*GenericObject, error) {
	unstructuredList := &unstructured.UnstructuredList{}
	unstructuredList.SetGroupVersionKind(gvk)

	if err := c.List(ctx, unstructuredList, opts...); err != nil {
		return nil, err
	}

	objects := make([]*GenericObject, len(unstructuredList.Items))
	for i, unstructuredObj := range unstructuredList.Items {
		obj, err := UnstructuredToObject(&unstructuredObj)
		if err != nil {
			return nil, err
		}
		objects[i] = obj
	}

	return objects, nil
}

// DeleteResource deletes the resource from the kubernetes cluster
func DeleteResource(ctx context.Context, c client.Client, obj *GenericObject) error {
	unstructuredObj, err := ObjectToUnstructured(obj)
	if err != nil {
		return err
	}

	return c.Delete(ctx, unstructuredObj)
}
