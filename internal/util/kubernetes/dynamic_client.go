package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/numaproj/numaplane/internal/util/logger"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// this file contains generic utility functions for using the dynamic client for Kubernetes
// to create, update, and get CRs as Unstructured types

type GenericObject struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   runtime.RawExtension `json:"spec"`
	Status runtime.RawExtension `json:"status,omitempty"`
}

type GenericStatus struct {
	Phase              string             `json:"phase,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
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
			numaLogger.Infof("successfully created resource %s/%s", object.Namespace, object.Name)
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
		numaLogger.Infof("successfully updated resource %s/%s", object.Namespace, object.Name)

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
	numaLogger.Infof("successfully created resource %s/%s", object.Namespace, object.Name)
	return nil
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
