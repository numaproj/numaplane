package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/logger"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
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

func GetUnstructuredCR(
	ctx context.Context,
	restConfig *rest.Config,
	object *GenericObject,
	pluralName string,
) (*unstructured.Unstructured, error) {
	client, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %v", err)
	}

	gvr, err := getGroupVersionResource(object, pluralName)
	if err != nil {
		return nil, err
	}

	return client.Resource(gvr).Namespace(object.Namespace).Get(ctx, object.Name, metav1.GetOptions{})
}

func ListUnstructuredCR(
	ctx context.Context,
	restConfig *rest.Config,
	apiGroup string,
	version string,
	pluralName string,
	namespace string,
	labelSelector string, // set to empty string if none
	fieldSelector string, // set to empty string if none
) (*unstructured.UnstructuredList, error) {
	client, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %v", err)
	}

	gvr := schema.GroupVersionResource{
		Group:    apiGroup,
		Version:  version,
		Resource: pluralName,
	}
	return client.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: fieldSelector})
}

// look up a Resource
func GetCR(ctx context.Context, restConfig *rest.Config, object *GenericObject, pluralName string) (*GenericObject, error) {
	unstruc, err := GetUnstructuredCR(ctx, restConfig, object, pluralName)
	if unstruc != nil {
		return UnstructuredToObject(unstruc)
	} else {
		return nil, err
	}
}

func ListCR(ctx context.Context,
	restConfig *rest.Config,
	apiGroup string,
	version string,
	pluralName string,
	namespace string,
	// set to empty string if none
	labelSelector string,
	// set to empty string if none
	fieldSelector string) ([]*GenericObject, error) {
	numaLogger := logger.FromContext(ctx)
	unstrucList, err := ListUnstructuredCR(ctx, restConfig, apiGroup, version, pluralName, namespace, labelSelector, fieldSelector)
	if err != nil {
		return nil, err
	}

	if unstrucList != nil {
		numaLogger.Debugf("found %d %s", len(unstrucList.Items), pluralName)
		objects := make([]*GenericObject, len(unstrucList.Items))
		for i, unstruc := range unstrucList.Items {
			obj, err := UnstructuredToObject(&unstruc)
			if err != nil {
				return nil, err
			}
			objects[i] = obj
		}
		return objects, nil
	}

	return nil, err
}

func CreateCR(
	ctx context.Context,
	restConfig *rest.Config,
	object *GenericObject,
	pluralName string,
) error {
	unstruc, err := ObjectToUnstructured(object)
	if err != nil {
		return err
	}

	gvr, err := getGroupVersionResource(object, pluralName)
	if err != nil {
		return err
	}

	return CreateUnstructuredCR(ctx, restConfig, unstruc, gvr, object.Namespace, object.Name)
}

func CreateUnstructuredCR(
	ctx context.Context,
	restConfig *rest.Config,
	unstruc *unstructured.Unstructured,
	gvr schema.GroupVersionResource,
	namespace string,
	name string,
) error {
	numaLogger := logger.FromContext(ctx)
	numaLogger.Debugf("will update resource %s/%s of type %+v", namespace, name, gvr)

	client, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %v", err)
	}

	_, err = client.Resource(gvr).Namespace(namespace).Create(ctx, unstruc, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update resource %s/%s of type %+v, err=%v", namespace, name, gvr, err)
	}

	numaLogger.Infof("successfully updated resource %s/%s of type %+v", namespace, name, gvr)
	return nil
}

func UpdateCR(
	ctx context.Context,
	restConfig *rest.Config,
	object *GenericObject,
	pluralName string,
) error {

	unstruc, err := ObjectToUnstructured(object)
	if err != nil {
		return err
	}

	gvr, err := getGroupVersionResource(object, pluralName)
	if err != nil {
		return err
	}

	return UpdateUnstructuredCR(ctx, restConfig, unstruc, gvr, object.Namespace, object.Name)
}

func UpdateUnstructuredCR(
	ctx context.Context,
	restConfig *rest.Config,
	unstruc *unstructured.Unstructured,
	gvr schema.GroupVersionResource,
	namespace string,
	name string,
) error {
	numaLogger := logger.FromContext(ctx)
	numaLogger.Debugf("will update resource %s/%s of type %+v", namespace, name, gvr)

	client, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %v", err)
	}

	_, err = client.Resource(gvr).Namespace(namespace).Update(ctx, unstruc, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update resource %s/%s of type %+v, err=%v", namespace, name, gvr, err)
	}

	numaLogger.Infof("successfully updated resource %s/%s of type %+v", namespace, name, gvr)
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

func UpdateStatus(ctx context.Context, restConfig *rest.Config, object *GenericObject, pluralName string) error {
	client, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %v", err)
	}

	gvr, err := getGroupVersionResource(object, pluralName)
	if err != nil {
		return err
	}

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource, err := client.Resource(gvr).Namespace(object.Namespace).Get(ctx, object.Name, metav1.GetOptions{})
		if err != nil {
			// NOTE: report the error as-is and do not check for resource existance fo retry purposes
			return err
		}

		resource.Object["status"] = object.Status

		_, err = client.Resource(gvr).Namespace(object.Namespace).UpdateStatus(ctx, resource, metav1.UpdateOptions{})
		if err != nil {
			return err
		}

		return nil
	})

	// TODO: we may want to also implement retry.OnError() to retry in case of errors while updating the status.
	// We should consider to add extra failover logic to avoid this function to not be able to update
	// the status of the resource and return error.

	return err
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
