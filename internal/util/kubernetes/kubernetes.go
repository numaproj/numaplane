package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	yamlserializer "k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	k8sClient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// validManifestExtensions contains the supported extension for raw file.
var validManifestExtensions = map[string]struct{}{"yaml": {}, "yml": {}, "json": {}}

type GenericObject struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   runtime.RawExtension `json:"spec"`
	Status runtime.RawExtension `json:"status,omitempty"`
}

type GenericStatus struct {
	Phase      string             `json:"phase,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

func IsValidKubernetesNamespace(name string) bool {
	// All namespace names must be valid RFC 1123 DNS labels.
	errs := validation.IsDNS1123Label(name)
	reservedNamesRegex := regexp.MustCompile(`^(kubernetes-|kube-)`)
	if len(errs) == 0 && !reservedNamesRegex.MatchString(name) {
		return true
	}
	return false
}

// GetNumaplaneInstanceLabel returns the application instance name from label
func GetNumaplaneInstanceLabel(un *unstructured.Unstructured, key string) (string, error) {
	labels, err := nestedNullableStringMap(un.Object, "metadata", "labels")
	if err != nil {
		return "", fmt.Errorf("failed to get labels from target object %s %s/%s: %w", un.GroupVersionKind().String(), un.GetNamespace(), un.GetName(), err)
	}
	if labels != nil {
		return labels[key], nil
	}
	return "", nil
}

// SetNumaplaneInstanceLabel sets the recommended app.kubernetes.io/instance label against an unstructured object
func SetNumaplaneInstanceLabel(target *unstructured.Unstructured, key, val string) error {

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

// GetSecret gets secret using the kubernetes client
func GetSecret(ctx context.Context, client k8sClient.Client, namespace, secretName string) (*corev1.Secret, error) {
	if namespace == "" {
		return nil, fmt.Errorf("namespace cannot be empty")
	}
	if secretName == "" {
		return nil, fmt.Errorf("secretName cannot be empty")
	}
	secret := &corev1.Secret{}
	key := k8sClient.ObjectKey{
		Namespace: namespace,
		Name:      secretName,
	}
	if err := client.Get(ctx, key, secret); err != nil {
		return nil, err
	}
	return secret, nil
}

func IsValidKubernetesManifestFile(fileName string) bool {
	fileExt := strings.Split(fileName, ".")
	if _, ok := validManifestExtensions[fileExt[len(fileExt)-1]]; ok {
		return true
	}
	return false
}

// ownerExists checks if an owner reference already exists in the list of owner references.
func ownerExists(existingRefs []interface{}, ownerRef map[string]interface{}) bool {
	var alreadyExists bool
	for _, ref := range existingRefs {
		if refMap, ok := ref.(map[string]interface{}); ok {
			if refMap["uid"] == ownerRef["uid"] {
				alreadyExists = true
				break
			}
		}
	}
	return alreadyExists
}

func ApplyOwnership(manifest string, controllerRollout *v1alpha1.NumaflowControllerRollout) ([]byte, error) {
	// Decode YAML into an Unstructured object
	decUnstructured := yamlserializer.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	obj := &unstructured.Unstructured{}
	_, _, err := decUnstructured.Decode([]byte(manifest), nil, obj)
	if err != nil {
		return nil, err
	}

	// Construct the new owner reference
	ownerRef := map[string]interface{}{
		"apiVersion":         controllerRollout.APIVersion,
		"kind":               controllerRollout.Kind,
		"name":               controllerRollout.Name,
		"uid":                string(controllerRollout.UID),
		"controller":         true,
		"blockOwnerDeletion": true,
	}

	// Get existing owner references and check if our reference is already there
	existingRefs, found, err := unstructured.NestedSlice(obj.Object, "metadata", "ownerReferences")
	if err != nil {
		return nil, err
	}
	if !found {
		existingRefs = []interface{}{}
	}

	// Check if the owner reference already exists to avoid duplication
	alreadyExists := ownerExists(existingRefs, ownerRef)

	// Add the new owner reference if it does not exist
	if !alreadyExists {
		existingRefs = append(existingRefs, ownerRef)
		err = unstructured.SetNestedSlice(obj.Object, existingRefs, "metadata", "ownerReferences")
		if err != nil {
			return nil, err
		}
	}

	// Marshal the updated object into YAML
	modifiedManifest, err := yaml.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return modifiedManifest, nil
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

func ParseStatus(obj *GenericObject) (GenericStatus, error) {
	if len(obj.Status.Raw) == 0 {
		return GenericStatus{}, nil
	}

	var status GenericStatus
	err := json.Unmarshal(obj.Status.Raw, &status)
	if err != nil {
		return GenericStatus{}, err
	}
	return status, nil
}

func GetResource(
	ctx context.Context,
	restConfig *rest.Config,
	object *GenericObject,
	pluralName string,
) (*unstructured.Unstructured, error) {
	client, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %v", err)
	}

	group, version, err := parseApiVersion(object.APIVersion)
	if err != nil {
		return nil, err
	}

	gvr := schema.GroupVersionResource{
		Group:    group,
		Version:  version,
		Resource: pluralName,
	}

	return client.Resource(gvr).Namespace(object.Namespace).Get(ctx, object.Name, metav1.GetOptions{})
}

// ApplyCRSpec either creates or updates an object identified by the RawExtension, using the new definition,
// first checking to see if there's a difference in Spec before applying
// TODO: use CreateCR and UpdateCR instead
func ApplyCRSpec(ctx context.Context, restConfig *rest.Config, object *GenericObject, pluralName string) error {
	numaLogger := logger.FromContext(ctx)

	client, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %v", err)
	}

	group, version, err := parseApiVersion(object.APIVersion)
	if err != nil {
		return err
	}

	gvr := schema.GroupVersionResource{
		Group:    group,
		Version:  version,
		Resource: pluralName,
	}

	// Get the object to see if it exists
	resource, err := client.Resource(gvr).Namespace(object.Namespace).Get(ctx, object.Name, metav1.GetOptions{})

	if err != nil {
		if apierrors.IsNotFound(err) {
			// create object as it doesn't exist
			numaLogger.Debugf("didn't find resource %s/%s, will create", object.Namespace, object.Name)

			unstruct, err := ObjectToUnstructured(object)
			if err != nil {
				return err
			}

			_, err = client.Resource(gvr).Namespace(object.Namespace).Create(ctx, unstruct, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("failed to create Resource %s/%s, err=%v", object.Namespace, object.Name, err)
			}
			numaLogger.Debugf("successfully created resource %s/%s", object.Namespace, object.Name)
		} else {
			return fmt.Errorf("error attempting to Get resources; GVR=%+v err: %v", gvr, err)
		}

	} else {
		numaLogger.Debugf("found existing Resource definition for %s/%s: %+v", object.Namespace, object.Name, resource)

		currentObj, err := UnstructuredToObject(resource)
		if err != nil {
			return fmt.Errorf("error attempting to convert unstructured resource to generic object: %v", err)
		}

		if reflect.DeepEqual(currentObj.Spec, object.Spec) {
			numaLogger.Debugf("skipping update of resource %s/%s since not necessary", object.Namespace, object.Name)
			return nil
		}

		// replace the Object's Spec
		resource.Object["spec"] = object.Spec

		_, err = client.Resource(gvr).Namespace(object.Namespace).Update(ctx, resource, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update Resource %s/%s, err=%v", object.Namespace, object.Name, err)
		}
		numaLogger.Debugf("successfully updated resource %s/%s", object.Namespace, object.Name)

	}
	return nil
}

// look up a Resource
func GetCR(ctx context.Context, restConfig *rest.Config, object *GenericObject, pluralName string) (*GenericObject, error) {
	unstruc, err := GetResource(ctx, restConfig, object, pluralName)
	if unstruc != nil {
		return UnstructuredToObject(unstruc)
	} else {
		return nil, err
	}
}

func CreateCR(
	ctx context.Context,
	restConfig *rest.Config,
	object *GenericObject,
	pluralName string,
) error {
	numaLogger := logger.FromContext(ctx)

	client, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %v", err)
	}

	group, version, err := parseApiVersion(object.APIVersion)
	if err != nil {
		return err
	}

	gvr := schema.GroupVersionResource{
		Group:    group,
		Version:  version,
		Resource: pluralName,
	}

	numaLogger.Debugf("didn't find resource %s/%s, will create", object.Namespace, object.Name)

	unstruct, err := ObjectToUnstructured(object)
	if err != nil {
		return err
	}

	_, err = client.Resource(gvr).Namespace(object.Namespace).Create(ctx, unstruct, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create Resource %s/%s, err=%v", object.Namespace, object.Name, err)
	}
	numaLogger.Debugf("successfully created resource %s/%s", object.Namespace, object.Name)
	return nil
}

func ObjectToUnstructured(object *GenericObject) (*unstructured.Unstructured, error) {
	asJsonBytes, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	var asMap map[string]interface{}
	err = json.Unmarshal(asJsonBytes, &asMap)
	if err != nil {
		return nil, err
	}

	return &unstructured.Unstructured{Object: asMap}, nil
}

func UnstructuredToObject(u *unstructured.Unstructured) (*GenericObject, error) {
	asJsonBytes, err := json.Marshal(u.Object)
	if err != nil {
		return nil, err
	}
	var genericObject GenericObject
	err = json.Unmarshal(asJsonBytes, &genericObject)

	return &genericObject, err
}

func NewPodDisruptionBudget(name, namespace string, maxUnavailable int32, ownerReference []metav1.OwnerReference) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			OwnerReferences: ownerReference,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &intstr.IntOrString{Type: intstr.Int, IntVal: maxUnavailable},
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/component":      "isbsvc",
					"numaflow.numaproj.io/isbsvc-name": name,
				},
			},
		},
	}
}
