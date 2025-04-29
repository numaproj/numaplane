package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/logger"
)

// this file contains utility functions for working with Unstructured types

type GenericStatus struct {
	Phase              string             `json:"phase,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
}

func ParseStatus(obj *unstructured.Unstructured) (GenericStatus, error) {
	if obj == nil || len(obj.Object) == 0 {
		return GenericStatus{}, nil
	}

	statusRaw, found, err := unstructured.NestedFieldNoCopy(obj.Object, "status")
	if !found || err != nil {
		return GenericStatus{}, nil
	}

	var status GenericStatus
	if err := util.StructToStruct(statusRaw, &status); err != nil {
		return GenericStatus{}, err
	}

	return status, nil
}

// GetLiveResource converts the generic object to unstructured object and fetches the resource from the API server
func GetLiveResource(
	ctx context.Context,
	object *unstructured.Unstructured,
	pluralName string,
) (*unstructured.Unstructured, error) {
	numaLogger := logger.FromContext(ctx)
	gvr, err := getGroupVersionResource(object, pluralName)
	if err != nil {
		return nil, err
	}

	unstruc, err := DynamicClient.Resource(gvr).Namespace(object.GetNamespace()).Get(ctx, object.GetName(), metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	numaLogger.Verbosef("retrieved resource %s/%s of type %+v with value %+v", object.GetNamespace(), object.GetName(), gvr, unstruc.Object)
	return unstruc, nil
}

func ListLiveResource(
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
	return DynamicClient.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: fieldSelector})
}

func PatchResource(
	ctx context.Context,
	c client.Client,
	obj *unstructured.Unstructured,
	patch string,
	patchType k8stypes.PatchType,
) error {
	return c.Patch(ctx, obj, client.RawPatch(patchType, []byte(patch)))
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

func getGroupVersionResource(object *unstructured.Unstructured, pluralName string) (schema.GroupVersionResource, error) {
	group, version, err := parseApiVersion(object.GetAPIVersion())
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

// ExtractResourceNames extracts the names of the resources from an UnstructuredList
func ExtractResourceNames(unstrucList *unstructured.UnstructuredList) []string {
	namespacedNames := []string{}
	for _, u := range unstrucList.Items {
		namespacedNames = append(namespacedNames, fmt.Sprintf("%s/%s", u.GetNamespace(), u.GetName()))
	}
	return namespacedNames
}

// CreateResource creates the resource in the kubernetes cluster
func CreateResource(ctx context.Context, c client.Client, obj *unstructured.Unstructured) error {
	return c.Create(ctx, obj)
}

// GetResource retrieves the resource from the informer cache, if it's not found then it fetches from the API server.
func GetResource(ctx context.Context, c client.Client, gvk schema.GroupVersionKind, namespacedName k8stypes.NamespacedName) (*unstructured.Unstructured, error) {

	numaLogger := logger.FromContext(ctx)
	unstructuredObj := &unstructured.Unstructured{}
	unstructuredObj.SetGroupVersionKind(gvk)

	if err := c.Get(ctx, namespacedName, unstructuredObj); err != nil {
		return nil, err
	}
	numaLogger.Verbosef("retrieved resource %s/%s of type %+v with value %+v", namespacedName.Namespace, namespacedName.Name, gvk, unstructuredObj.Object)
	return unstructuredObj, nil
}

// UpdateResource updates the resource in the kubernetes cluster
func UpdateResource(ctx context.Context, c client.Client, obj *unstructured.Unstructured) error {
	return c.Update(ctx, obj)
}

// ListResources retrieves the list of resources from the informer cache, if it's not found then it fetches from the API server.
func ListResources(ctx context.Context, c client.Client, gvk schema.GroupVersionKind, namespace string, opts ...client.ListOption) (*unstructured.UnstructuredList, error) {
	unstructuredList := &unstructured.UnstructuredList{}
	unstructuredList.SetGroupVersionKind(gvk)

	listOptions := []client.ListOption{client.InNamespace(namespace)}
	listOptions = append(listOptions, opts...)

	if err := c.List(ctx, unstructuredList, listOptions...); err != nil {
		return nil, err
	}

	return unstructuredList, nil
}

// DeleteResource deletes the resource from the kubernetes cluster
func DeleteResource(ctx context.Context, c client.Client, obj *unstructured.Unstructured) error {
	return c.Delete(ctx, obj)
}

func MetaGVKToSchemaGVK(metaGVK metav1.GroupVersionKind) schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   metaGVK.Group,
		Version: metaGVK.Version,
		Kind:    metaGVK.Kind,
	}
}

func SchemaGVKToMetaGVK(schemaGVK schema.GroupVersionKind) metav1.GroupVersionKind {
	return metav1.GroupVersionKind{
		Group:   schemaGVK.Group,
		Version: schemaGVK.Version,
		Kind:    schemaGVK.Kind,
	}
}

func ApplyOwnerReference(child *unstructured.Unstructured, owner *unstructured.Unstructured) error {
	ownerRef := metav1.OwnerReference{
		APIVersion: owner.GetAPIVersion(),
		Kind:       owner.GetKind(),
		Name:       owner.GetName(),
		UID:        owner.GetUID(),
	}
	child.SetOwnerReferences([]metav1.OwnerReference{ownerRef})
	return nil
}

// Calculate the sha256 hash of a Resource's definition
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
