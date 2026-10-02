/*
Copyright 2023.

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

package numaflowcontrollerrollout

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"
)

func TestRecycle(t *testing.T) {
	const (
		childName      = "controller-0"
		instance       = "1-8-3-1"
		otherNamespace = "recycle-other"
	)
	namespace := ctlrcommon.DefaultTestNamespace
	ctx := context.Background()

	restConfig, _, c, k8sClientSet, err := commontest.PrepareK8SEnvironment()
	require.NoError(t, err)
	require.NoError(t, kubernetes.SetClientSets(restConfig))
	_, err = k8sClientSet.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: otherNamespace}}, metav1.CreateOptions{})
	if !apierrors.IsAlreadyExists(err) {
		require.NoError(t, err)
	}
	reconciler := &NumaflowControllerRolloutReconciler{client: c}

	isb := func(name, namespace string, labels, annotations map[string]string) *unstructured.Unstructured {
		return numaflowResource(numaflowv1.ISBGroupVersionKind, namespace, name, labels, annotations)
	}
	monoVertex := func(name, namespace string, labels, annotations map[string]string) *unstructured.Unstructured {
		return numaflowResource(numaflowv1.MonoVertexGroupVersionKind, namespace, name, labels, annotations)
	}
	pipeline := func(name string, labels map[string]string) *unstructured.Unstructured {
		return numaflowResource(numaflowv1.PipelineGroupVersionKind, namespace, name, labels, nil)
	}
	onInstance := map[string]string{common.LabelKeyControllerInstanceID: instance}

	tests := []struct {
		name          string
		instanceID    string
		upgradeState  common.UpgradeState
		upgradeReason common.UpgradeStateReason
		dependents    []client.Object
		wantDeleted   bool
	}{
		{
			name:         "deletes a promoted child with zero references without checking upgrade state",
			instanceID:   instance,
			upgradeState: common.LabelValueUpgradePromoted,
			wantDeleted:  true,
		},
		{
			name:          "deletes a recyclable child left by a successful promotion",
			instanceID:    instance,
			upgradeState:  common.LabelValueUpgradeRecyclable,
			upgradeReason: common.LabelValueProgressiveSuccess,
			wantDeleted:   true,
		},
		{
			name:          "deletes a discontinued trial with zero references",
			instanceID:    instance,
			upgradeState:  common.LabelValueUpgradeRecyclable,
			upgradeReason: common.LabelValueDiscontinueProgressive,
			wantDeleted:   true,
		},
		{
			name:          "deletes a replaced failed trial with zero references",
			instanceID:    instance,
			upgradeState:  common.LabelValueUpgradeRecyclable,
			upgradeReason: common.LabelValueProgressiveReplacedFailed,
			wantDeleted:   true,
		},
		{
			name:         "leaves the child while an ISBService is bound to it",
			instanceID:   instance,
			upgradeState: common.LabelValueUpgradeRecyclable,
			dependents:   []client.Object{isb("isbsvc", namespace, onInstance, nil)},
			wantDeleted:  false,
		},
		{
			name:         "leaves the child while a MonoVertex is bound to it",
			instanceID:   instance,
			upgradeState: common.LabelValueUpgradeRecyclable,
			dependents:   []client.Object{monoVertex("mv", namespace, onInstance, nil)},
			wantDeleted:  false,
		},
		{
			name:         "leaves the child when both an ISBService and a MonoVertex are bound to it",
			instanceID:   instance,
			upgradeState: common.LabelValueUpgradeRecyclable,
			dependents: []client.Object{
				isb("isbsvc", namespace, onInstance, nil),
				monoVertex("mv", namespace, onInstance, nil),
			},
			wantDeleted: false,
		},
		{
			name:         "deletes the child when the only ISBService is bound to a different instance",
			instanceID:   instance,
			upgradeState: common.LabelValueUpgradeRecyclable,
			dependents: []client.Object{
				isb("isbsvc", namespace, map[string]string{common.LabelKeyControllerInstanceID: "other"}, nil),
			},
			wantDeleted: true,
		},
		{
			name:         "deletes the child when the matching ISBService is in another namespace",
			instanceID:   instance,
			upgradeState: common.LabelValueUpgradeRecyclable,
			dependents:   []client.Object{isb("isbsvc", otherNamespace, onInstance, nil)},
			wantDeleted:  true,
		},
		{
			name:         "deletes the child when a Pipeline is bound to it and no ISBService or MonoVertex is",
			instanceID:   instance,
			upgradeState: common.LabelValueUpgradeRecyclable,
			dependents:   []client.Object{pipeline("pipeline", onInstance)},
			wantDeleted:  true,
		},
		{
			name:         "leaves an empty-instance controller while an unannotated ISBService and MonoVertex exist",
			instanceID:   "",
			upgradeState: common.LabelValueUpgradeRecyclable,
			dependents: []client.Object{
				isb("isbsvc", namespace, nil, nil),
				monoVertex("mv", namespace, nil, nil),
			},
			wantDeleted: false,
		},
		{
			name:         "leaves an empty-instance controller while an ISBService has an empty controller-instance-id label",
			instanceID:   "",
			upgradeState: common.LabelValueUpgradeRecyclable,
			dependents: []client.Object{
				isb("isbsvc", namespace, map[string]string{common.LabelKeyControllerInstanceID: ""}, nil),
			},
			wantDeleted: false,
		},
		{
			name:         "deletes an empty-instance controller when remaining dependents are bound to another instance",
			instanceID:   "",
			upgradeState: common.LabelValueUpgradeRecyclable,
			dependents: []client.Object{
				isb("isbsvc", namespace, nil, map[string]string{common.AnnotationKeyNumaflowInstanceID: instance}),
				monoVertex("mv", namespace, onInstance, nil),
			},
			wantDeleted: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, ns := range []string{namespace, otherNamespace} {
				_ = c.DeleteAllOf(ctx, &apiv1.NumaflowController{}, client.InNamespace(ns))
				_ = c.DeleteAllOf(ctx, &numaflowv1.InterStepBufferService{}, client.InNamespace(ns))
				_ = c.DeleteAllOf(ctx, &numaflowv1.MonoVertex{}, client.InNamespace(ns))
				_ = c.DeleteAllOf(ctx, &numaflowv1.Pipeline{}, client.InNamespace(ns))
			}

			child := controllerChild(namespace, childName, tc.instanceID, tc.upgradeState, tc.upgradeReason)
			for _, obj := range append([]client.Object{child}, tc.dependents...) {
				require.NoError(t, c.Create(ctx, obj))
			}

			deleted, err := reconciler.Recycle(ctx, nil, child)

			require.NoError(t, err)
			assert.Equal(t, tc.wantDeleted, deleted)

			got := &unstructured.Unstructured{}
			got.SetGroupVersionKind(apiv1.NumaflowControllerGroupVersionKind)
			err = c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: childName}, got)
			if tc.wantDeleted {
				assert.True(t, apierrors.IsNotFound(err), "child should have been deleted, get err: %v", err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func controllerChild(namespace, name, instanceID string, upgradeState common.UpgradeState, upgradeReason common.UpgradeStateReason) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	obj.SetGroupVersionKind(apiv1.NumaflowControllerGroupVersionKind)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	labels := map[string]string{common.LabelKeyUpgradeState: string(upgradeState)}
	if upgradeReason != "" {
		labels[common.LabelKeyUpgradeStateReason] = string(upgradeReason)
	}
	obj.SetLabels(labels)
	spec := map[string]interface{}{"version": "1.2.3"}
	if instanceID != "" {
		spec["instanceID"] = instanceID
	}
	obj.Object["spec"] = spec
	return obj
}

func numaflowResource(gvk schema.GroupVersionKind, namespace, name string, labels, annotations map[string]string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	obj.SetGroupVersionKind(gvk)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	obj.SetLabels(labels)
	obj.SetAnnotations(annotations)
	obj.Object["spec"] = map[string]interface{}{}
	return obj
}
