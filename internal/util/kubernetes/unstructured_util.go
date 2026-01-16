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
	"k8s.io/apimachinery/pkg/runtime"
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
) (unstructured.UnstructuredList, error) {
	gvr := schema.GroupVersionResource{
		Group:    apiGroup,
		Version:  version,
		Resource: pluralName,
	}
	ul, err := DynamicClient.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: fieldSelector})
	if err != nil || ul == nil {
		return unstructured.UnstructuredList{}, err
	}
	return *ul, nil
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

// SetLabels sets one or more labels on an unstructured object
func SetLabels(target *unstructured.Unstructured, labelsToSet map[string]string) error {
	if len(labelsToSet) == 0 {
		return nil
	}

	existingLabels, err := nestedNullableStringMap(target.Object, "metadata", "labels")
	if err != nil {
		return fmt.Errorf("failed to get labels from target object %s %s/%s: %w", target.GroupVersionKind().String(), target.GetNamespace(), target.GetName(), err)
	}
	if existingLabels == nil {
		existingLabels = make(map[string]string)
	}

	// Merge new labels into existing labels
	for key, val := range labelsToSet {
		existingLabels[key] = val
	}
	target.SetLabels(existingLabels)

	return nil
}

// PatchLabels patches a Kubernetes resource to add/update one or more labels
func PatchLabels(ctx context.Context, c client.Client, obj *unstructured.Unstructured, labels map[string]string) error {
	if len(labels) == 0 {
		return nil
	}

	// Build JSON for the labels map
	labelsJson, err := json.Marshal(labels)
	if err != nil {
		return fmt.Errorf("failed to marshal labels: %w", err)
	}

	// Create a JSON patch to add/update the labels
	// Using strategic merge patch which handles missing labels map gracefully
	patchJson := fmt.Sprintf(`{"metadata":{"labels":%s}}`, string(labelsJson))
	return PatchResource(ctx, c, obj, patchJson, k8stypes.MergePatchType)
}

// SetAndPatchLabels sets the labels on an unstructured object and then patches them into the live object
func SetAndPatchLabels(ctx context.Context, c client.Client, obj *unstructured.Unstructured, labels map[string]string) error {
	if err := SetLabels(obj, labels); err != nil {
		return err
	}
	return PatchLabels(ctx, c, obj, labels)
}

// GetAnnotation returns the annotation identified by "key"
func GetAnnotation(un *unstructured.Unstructured, key string) (string, error) {
	annotations, err := nestedNullableStringMap(un.Object, "metadata", "annotations")
	if err != nil {
		return "", fmt.Errorf("failed to get annotations from target object %s %s/%s: %w", un.GroupVersionKind().String(), un.GetNamespace(), un.GetName(), err)
	}
	if annotations != nil {
		return annotations[key], nil
	}
	return "", nil
}

// SetAnnotations sets one or more annotations on an unstructured object
func SetAnnotations(target *unstructured.Unstructured, annotationsToSet map[string]string) error {
	if len(annotationsToSet) == 0 {
		return nil
	}

	existingAnnotations, err := nestedNullableStringMap(target.Object, "metadata", "annotations")
	if err != nil {
		return fmt.Errorf("failed to get annotations from target object %s %s/%s: %w", target.GroupVersionKind().String(), target.GetNamespace(), target.GetName(), err)
	}
	if existingAnnotations == nil {
		existingAnnotations = make(map[string]string)
	}

	// Merge new annotations into existing annotations
	for key, val := range annotationsToSet {
		existingAnnotations[key] = val
	}
	target.SetAnnotations(existingAnnotations)

	return nil
}

// PatchAnnotations patches a Kubernetes resource to add/update one or more annotations
func PatchAnnotations(ctx context.Context, c client.Client, obj *unstructured.Unstructured, annotations map[string]string) error {
	if len(annotations) == 0 {
		return nil
	}

	// Build JSON for the annotations map
	annotationsJson, err := json.Marshal(annotations)
	if err != nil {
		return fmt.Errorf("failed to marshal annotations: %w", err)
	}

	// Create a JSON patch to add/update the annotations
	// Using strategic merge patch which handles missing annotations map gracefully
	patchJson := fmt.Sprintf(`{"metadata":{"annotations":%s}}`, string(annotationsJson))
	return PatchResource(ctx, c, obj, patchJson, k8stypes.MergePatchType)
}

// SetAndPatchAnnotations sets the annotations on an unstructured object and then patches them into the live object
func SetAndPatchAnnotations(ctx context.Context, c client.Client, obj *unstructured.Unstructured, annotations map[string]string) error {
	if err := SetAnnotations(obj, annotations); err != nil {
		return err
	}
	return PatchAnnotations(ctx, c, obj, annotations)
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
func ListResources(ctx context.Context, c client.Client, gvk schema.GroupVersionKind, namespace string, opts ...client.ListOption) (unstructured.UnstructuredList, error) {
	unstructuredList := unstructured.UnstructuredList{}
	unstructuredList.SetGroupVersionKind(gvk)

	listOptions := []client.ListOption{client.InNamespace(namespace)}
	listOptions = append(listOptions, opts...)

	if err := c.List(ctx, &unstructuredList, listOptions...); err != nil {
		return unstructured.UnstructuredList{}, err
	}

	return unstructuredList, nil
}

// ListResourcesOwnedBy retrieves the list of resources that are owned by the specified owner.
// It filters the results to only include resources that have an OwnerReference matching the owner's UID.
func ListResourcesOwnedBy(ctx context.Context, c client.Client, gvk schema.GroupVersionKind, namespace string, owner *unstructured.Unstructured, opts ...client.ListOption) (unstructured.UnstructuredList, error) {
	unstructuredList, err := ListResources(ctx, c, gvk, namespace, opts...)
	if err != nil {
		return unstructured.UnstructuredList{}, err
	}

	// Filter to only include resources owned by the specified owner
	ownerUID := owner.GetUID()
	filteredItems := []unstructured.Unstructured{}
	for _, item := range unstructuredList.Items {
		for _, ownerRef := range item.GetOwnerReferences() {
			if ownerRef.UID == ownerUID {
				filteredItems = append(filteredItems, item)
				break
			}
		}
	}

	result := unstructured.UnstructuredList{}
	result.SetGroupVersionKind(gvk)
	result.Items = filteredItems
	return result, nil
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

func RawExtensionToUnstructured(rawExtension runtime.RawExtension) (*unstructured.Unstructured, error) {
	var asMap map[string]interface{}
	if err := util.StructToStruct(rawExtension, &asMap); err != nil {
		return nil, err
	}

	unstruc := &unstructured.Unstructured{}
	unstruc.Object = asMap
	return unstruc, nil
}

// UnstructuredToRawExtension converts an unstructured object to a RawExtension,
// removing status and runtime metadata fields that shouldn't be stored/reapplied.
// This is useful for storing a resource definition that will be re-created later.
func UnstructuredToRawExtension(obj *unstructured.Unstructured) (*runtime.RawExtension, error) {
	// Deep copy to avoid modifying the original
	cleaned := obj.DeepCopy()

	// Remove status - it's runtime-generated and shouldn't be reapplied
	delete(cleaned.Object, "status")

	// Remove runtime metadata fields that will be different when re-created
	if metadata, found, _ := unstructured.NestedMap(cleaned.Object, "metadata"); found {
		delete(metadata, "resourceVersion")
		delete(metadata, "uid")
		delete(metadata, "creationTimestamp")
		delete(metadata, "generation")
		delete(metadata, "managedFields")
		if err := unstructured.SetNestedMap(cleaned.Object, metadata, "metadata"); err != nil {
			return nil, fmt.Errorf("failed to set cleaned metadata: %w", err)
		}
	}

	// Marshal to JSON
	jsonBytes, err := json.Marshal(cleaned.Object)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal unstructured to JSON: %w", err)
	}

	return &runtime.RawExtension{Raw: jsonBytes}, nil
}

// GetLoggableResource returns a map containing only the essential parts of a Kubernetes object
// for logging purposes, excluding noisy metadata like managedFields
func GetLoggableResource(obj *unstructured.Unstructured) map[string]interface{} {
	clean := make(map[string]interface{})

	// Include basic metadata (name, namespace, labels, annotations)
	if _, found, _ := unstructured.NestedMap(obj.Object, "metadata"); found {
		cleanMetadata := make(map[string]interface{})
		if name := obj.GetName(); name != "" {
			cleanMetadata["name"] = name
		}
		if namespace := obj.GetNamespace(); namespace != "" {
			cleanMetadata["namespace"] = namespace
		}
		if labels := obj.GetLabels(); len(labels) > 0 {
			cleanMetadata["labels"] = labels
		}
		if annotations := obj.GetAnnotations(); len(annotations) > 0 {
			cleanMetadata["annotations"] = annotations
		}
		clean["metadata"] = cleanMetadata
	}

	// Include spec and status
	if spec, found, _ := unstructured.NestedMap(obj.Object, "spec"); found {
		clean["spec"] = spec
	}
	if status, found, _ := unstructured.NestedMap(obj.Object, "status"); found {
		clean["status"] = status
	}

	return clean
}

// ExtractMetadataSubmaps extracts labels and annotations from a metadata map and converts them to map[string]string.
// Returns (labels, annotations) - if not found returns empty map
func ExtractMetadataSubmaps(metadata map[string]interface{}) (map[string]string, map[string]string) {
	// Initialize with empty maps instead of nil
	labels := map[string]string{}
	annotations := map[string]string{}

	if metadata == nil {
		return labels, annotations
	}

	// Extract labels
	if labelsInterface, exists := metadata["labels"]; exists && labelsInterface != nil {
		if labelsMap, ok := labelsInterface.(map[string]interface{}); ok {
			labels = util.ConvertInterfaceMapToStringMap(labelsMap)
		}
	}

	// Extract annotations
	if annotationsInterface, exists := metadata["annotations"]; exists && annotationsInterface != nil {
		if annotationsMap, ok := annotationsInterface.(map[string]interface{}); ok {
			annotations = util.ConvertInterfaceMapToStringMap(annotationsMap)
		}
	}

	return labels, annotations
}
