/*
Copyright 2024.

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

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/numaproj/numaplane/internal/kubernetes"
	"github.com/numaproj/numaplane/internal/util"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

type RolloutChildOperation string

const (
	// The child resource does not exist on the cluster and needs to be created
	RolloutChildNew RolloutChildOperation = "CREATE_CHILD_RESOURCE"
	// The child resource exists on the cluster but needs to be update
	RolloutChildUpdate RolloutChildOperation = "UPDATE_CHILD_RESOURCE"
	// The child resource exists on the cluster and does not need to be update
	RolloutChildNone RolloutChildOperation = "DO_NOTHING"
)

// makeChildResourceFromRolloutAndUpdateSpecHash makes a new kubernetes.GenericObject based on the given rolloutObj.
// It returns the child resource object ready to be created and an operation to be performed with the returned object.
// The operations are defined by the RolloutChildOperation constants.
func makeChildResourceFromRolloutAndUpdateSpecHash(
	ctx context.Context,
	restConfig *rest.Config,
	rolloutObj metav1.Object,
) (*kubernetes.GenericObject, RolloutChildOperation, error) {
	kind := ""
	pluralName := ""
	var groupVersionKind schema.GroupVersionKind
	var childResourceSpec runtime.RawExtension

	// TODO: LOW PRIORITY: alternatively, consider passing kind, pluralName, groupVersionKind, and childResourceSpec as arguments
	switch ro := rolloutObj.(type) {
	case *apiv1.PipelineRollout:
		kind = "Pipeline"
		pluralName = "pipelines"
		groupVersionKind = apiv1.PipelineRolloutGroupVersionKind
		childResourceSpec = ro.Spec.Pipeline
	case *apiv1.ISBServiceRollout:
		kind = "InterStepBufferService"
		pluralName = "interstepbufferservices"
		groupVersionKind = apiv1.ISBServiceRolloutGroupVersionKind
		childResourceSpec = ro.Spec.InterStepBufferService
	default:
		return nil, RolloutChildNone, errors.New("invalid rollout type")
	}

	obj := kubernetes.GenericObject{
		TypeMeta: metav1.TypeMeta{
			Kind:       kind,
			APIVersion: "numaflow.numaproj.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            rolloutObj.GetName(),
			Namespace:       rolloutObj.GetNamespace(),
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(rolloutObj, groupVersionKind)},
		},
	}

	var childResourceSpecAsMap map[string]any
	err := json.Unmarshal(childResourceSpec.Raw, &childResourceSpecAsMap)
	if err != nil {
		return nil, RolloutChildNone, fmt.Errorf("unable to unmarshal %s spec to map: %v", kind, err)
	}
	childResouceSpecHash := util.MustHash(childResourceSpecAsMap)

	rolloutChildOp := RolloutChildNone
	_, err = kubernetes.GetCR(ctx, restConfig, &obj, pluralName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			rolloutChildOp = RolloutChildNew
		} else {
			return nil, RolloutChildNone, fmt.Errorf("unable to get %s %s/%s: %v", kind, obj.Namespace, obj.Name, err)
		}
	}

	if rolloutChildOp == RolloutChildNone {
		annotations := rolloutObj.GetAnnotations()
		if annotation, exists := annotations[apiv1.KeyHash]; exists && annotation != childResouceSpecHash {
			rolloutChildOp = RolloutChildUpdate
		}
	}

	setAnnotation(rolloutObj, apiv1.KeyHash, childResouceSpecHash)
	obj.Spec = childResourceSpec

	return &obj, rolloutChildOp, nil
}

func setAnnotation(obj metav1.Object, key, value string) {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	annotations[key] = value

	obj.SetAnnotations(annotations)
}
