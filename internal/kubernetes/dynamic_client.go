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

	"github.com/numaproj/numaplane/internal/util/logger"
)

//todo: add unit tests

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

type GenericObject struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec runtime.RawExtension `json:"spec"`
}

// UpdateCRSpec either creates or updates an object identified by the RawExtension, using the new definition,
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

			unstruct, err := objectToUnstructured(object)
			if err != nil {
				return err
			}

			_, err = client.Resource(gvr).Namespace(object.Namespace).Create(ctx, unstruct, v1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("failed to create Resource %s/%s, err=%v", object.Namespace, object.Name, err)
			}
			numaLogger.Debugf("successfully created resource %s/%s", object.Namespace, object.Name)
		} else {
			return fmt.Errorf("error attempting to Get resources; GVR=%+v", gvr)
		}

	} else {
		numaLogger.Debugf("found existing Resource definition for %s/%s: %+v", object.Namespace, object.Name, resource)
		// todo:
		//   If the existing annotation matches the new hash, then nothing to do: log and return

		// replace the Object's Spec
		resource.Object["spec"] = object.Spec

		_, err = client.Resource(gvr).Namespace(object.Namespace).Update(ctx, resource, v1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update Resource %s/%s, err=%v", object.Namespace, object.Name, err)
		}
		numaLogger.Debugf("successfully updated resource %s/%s", object.Namespace, object.Name)

	}
	return nil
}

func objectToUnstructured(object *GenericObject) (*unstructured.Unstructured, error) {
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

// unused - will keep and comment out in case it's needed later
/*
func unstructuredToObject(u *unstructured.Unstructured) (*GenericObject, error) {
	asJsonBytes, err := json.Marshal(u.Object)
	if err != nil {
		return nil, err
	}
	var genericObject GenericObject
	err = json.Unmarshal(asJsonBytes, &genericObject)

	return &genericObject, nil
}*/
