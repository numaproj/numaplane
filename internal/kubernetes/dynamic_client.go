package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/numaproj-labs/numaplane/internal/util/logger"
)

/*
type ResourceInfo struct {
	gvr       schema.GroupVersionResource
	namespace string
	name      string
	spec      *unstructured.Unstructured
	//resourceAsUnstructured *unstructured.Unstructured
}*/

type GenericObject struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              map[string]interface{} `json:"spec"`
}

func ParseRawExtension(ctx context.Context, obj runtime.RawExtension) (*GenericObject, error) {
	numaLogger := logger.FromContext(ctx)
	var genericObject GenericObject
	err := json.Unmarshal(obj.Raw, &genericObject)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling json: %v", err)
	}
	numaLogger.Debugf("successfully unmarshaled into GenericObject: %+v", genericObject)

	return &genericObject, nil
}

/*
// parse the runtime.RawExtension into its respective fields
// note that the plural name for the resource must be passed in, as groupVersionResource seems to require resource=plural name

	func ParseRawExtensionOrig(ctx context.Context, obj runtime.RawExtension, pluralName string) (*ResourceInfo, error) {
		numaLogger := logger.FromContext(ctx)
		var resourceDefAsMap map[string]interface{}
		err := json.Unmarshal(obj.Raw, &resourceDefAsMap)
		if err != nil {
			return nil, fmt.Errorf("error unmarshaling json: %v", err)
		}
		numaLogger.Debugf("resource definition: %+v", resourceDefAsMap)

		metadataAsMap, err := getSubfieldMap(resourceDefAsMap, "metadata")
		if err != nil {
			return nil, err
		}
		name, err := getSubfieldString(metadataAsMap, "name")
		if err != nil {
			return nil, err
		}

		namespace, _ := getSubfieldString(metadataAsMap, "namespace")

		apiVersion, err := getSubfieldString(resourceDefAsMap, "apiVersion")
		if err != nil {
			return nil, err
		}

		group, version, err := parseApiVersion(apiVersion)
		if err != nil {
			return nil, err
		}

		spec, err := getSubfieldMap(resourceDefAsMap, "spec")
		if err != nil {
			return nil, err
		}

		return &ResourceInfo{
			gvr: schema.GroupVersionResource{
				Group:    group,
				Version:  version,
				Resource: pluralName,
			},
			namespace: namespace,
			name:      name,
			spec:      &unstructured.Unstructured{Object: spec},
			//resourceAsUnstructured: &unstructured.Unstructured{Object: resourceDefAsMap},
		}, nil
	}

	func getSubfieldMap(m map[string]interface{}, fieldName string) (map[string]interface{}, error) {
		asInterface, found := m[fieldName]
		if !found {
			return nil, fmt.Errorf("field %q not found in resource definition: %+v", fieldName, m)
		}
		asMap, ok := asInterface.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("field %q has unexpected type in resource definition: %+v", fieldName, m)
		}
		return asMap, nil
	}

	func getSubfieldString(m map[string]interface{}, fieldName string) (string, error) {
		asInterface, found := m[fieldName]
		if !found {
			return "", fmt.Errorf("field %q not found in resource definition: %+v", fieldName, m)
		}
		asString, ok := asInterface.(string)
		if !ok {
			return "", fmt.Errorf("field %q has unexpected type in resource definition: %+v", fieldName, m)
		}
		return asString, nil
	}
*/
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

// updateCRSpec either creates or updates an object identified by the RawExtension, using the new definition,
// first checking to see if there's a difference in Spec before applying
func UpdateCRSpec(ctx context.Context, restConfig *rest.Config, object *GenericObject, pluralName string) error {
	numaLogger := logger.FromContext(ctx)

	// todo: Set the annotation for the hashed spec in the spec

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
	resource, err := client.Resource(gvr).Namespace(object.Namespace).Get(ctx, object.Name, v1.GetOptions{})

	if err != nil {
		if apierrors.IsNotFound(err) {
			// create object as it doesn't exist
			numaLogger.Debugf("didn't find resource %s/%s, will create", object.Namespace, object.Name)

			asJsonBytes, err := json.Marshal(object)
			if err != nil {
				return err
			}
			var asMap map[string]interface{}
			err = json.Unmarshal(asJsonBytes, &asMap)
			if err != nil {
				return err
			}
			fmt.Printf("deletethis: asMap=%+v\n", asMap)

			unstruct := &unstructured.Unstructured{asMap}

			_, err = client.Resource(gvr).Namespace(object.Namespace).Create(ctx, unstruct, v1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("failed to create Resource, err=%v", err)
			}
		} else {
			return fmt.Errorf("error attempting to Get resources; GVR=%+v", gvr)
		}

	} else {
		numaLogger.Debugf("found existing Resource definition: %+v", resource)
		// todo:
		//   If the existing annotation matches the new hash, then nothing to do: log and return
		//   Else update the object - note: can't just use the unstruc object here - need to take the running object and just update spec
	}
	return nil
}
