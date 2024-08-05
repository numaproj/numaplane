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

package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var _ = Describe("NumaflowControllerRollout Controller", Ordered, func() {
	const namespace = "default"

	Context("When creating a NumaflowControllerRollout resource", func() {
		ctx := context.Background()

		resourceLookupKey := types.NamespacedName{
			Name:      NumaflowControllerDeploymentName,
			Namespace: namespace,
		}

		numaflowControllerResource := apiv1.NumaflowControllerRollout{
			ObjectMeta: metav1.ObjectMeta{
				Name:      NumaflowControllerDeploymentName,
				Namespace: namespace,
			},
			Spec: apiv1.NumaflowControllerRolloutSpec{
				Controller: apiv1.Controller{Version: "1.2.0"},
			},
		}

		It("Should throw a CR validation error", func() {
			By("Creating a NumaflowControllerRollout resource with an invalid name")
			resource := numaflowControllerResource
			resource.Name = "test-numaflow-controller"
			err := k8sClient.Create(ctx, &resource)
			Expect(err).NotTo(Succeed())
			Expect(err.Error()).To(ContainSubstring("The metadata name must be 'numaflow-controller'"))
		})

		It("Should throw duplicate resource error", func() {
			By("Creating duplicate NumaflowControllerRollout resource with the same name")
			resource := numaflowControllerResource
			err := k8sClient.Create(ctx, &resource)
			Expect(err).To(Succeed())

			resource.ResourceVersion = "" // Reset the resource version to create a new resource
			err = k8sClient.Create(ctx, &resource)
			Expect(err).NotTo(Succeed())
			Expect(err.Error()).To(ContainSubstring("numaflowcontrollerrollouts.numaplane.numaproj.io \"numaflow-controller\" already exists"))
		})

		It("Should reconcile the Numaflow Controller Rollout", func() {
			By("Verifying the phase of the NumaflowControllerRollout resource")
			// Loop until the API call returns the desired response or a timeout occurs
			Eventually(func() (apiv1.Phase, error) {
				createdResource := &apiv1.NumaflowControllerRollout{}
				Expect(k8sClient.Get(ctx, resourceLookupKey, createdResource)).To(Succeed())
				return createdResource.Status.Phase, nil
			}, timeout, interval).Should(Equal(apiv1.PhaseDeployed))

			By("Verifying the numaflow controller deployment")
			Eventually(func() bool {
				numaflowDeployment := &appsv1.Deployment{}
				err := k8sClient.Get(ctx, types.NamespacedName{Name: NumaflowControllerDeploymentName, Namespace: namespace}, numaflowDeployment)
				return err == nil
			}, timeout, interval).Should(BeTrue())
		})

		It("Should reconcile the Numaflow Controller Rollout update", func() {
			By("Reconciling the updated resource")
			// Fetch the resource and update the spec
			resource := numaflowControllerResource
			resource.Spec.Controller.Version = "1.2.1"
			Expect(k8sClient.Patch(ctx, &resource, client.Merge)).To(Succeed())

			// Validate the resource status after the update
			By("Verifying the numaflow controller deployment image version")
			Eventually(func() bool {
				numaflowDeployment := &appsv1.Deployment{}
				err := k8sClient.Get(ctx, types.NamespacedName{Name: NumaflowControllerDeploymentName, Namespace: namespace}, numaflowDeployment)
				if err == nil {
					return numaflowDeployment.Spec.Template.Spec.Containers[0].Image == "quay.io/numaproj/numaflow:v1.2.1"
				}
				return false
			}, timeout, interval).Should(BeTrue())
		})

		It("Should have the metrics updated", func() {
			By("Verifying the Numaflow Controller metric")
			Expect(testutil.ToFloat64(customMetrics.NumaflowControllerSynced.WithLabelValues())).Should(Equal(float64(2)))
			Expect(testutil.ToFloat64(customMetrics.NumaflowControllerSyncFailed.WithLabelValues())).Should(Equal(float64(0)))
			Expect(testutil.ToFloat64(customMetrics.NumaflowKubeRequestCounter.WithLabelValues())).Should(BeNumerically(">", 1))
			Expect(testutil.ToFloat64(customMetrics.NumaflowKubectlExecutionCounter.WithLabelValues())).Should(BeNumerically(">", 1))
		})

		It("Should auto heal the Numaflow Controller Deployment with the spec based on the NumaflowControllerRollout version field value when the Deployment spec is changed", func() {
			By("updating the Numaflow Controller Deployment and verifying the changed field is the same as the original and not the modified version")
			verifyAutoHealing(ctx, appsv1.SchemeGroupVersion.WithKind("Deployment"), namespace, "numaflow-controller", "spec.template.spec.serviceAccountName", "someothersaname")
		})

		It("Should auto heal the numaflow-cmd-params-config ConfigMap with the spec based on the NumaflowControllerRollout version field value when the ConfigMap spec is changed", func() {
			By("updating the numaflow-cmd-params-config ConfigMap and verifying the changed field is the same as the original and not the modified version")
			verifyAutoHealing(ctx, corev1.SchemeGroupVersion.WithKind("ConfigMap"), namespace, "numaflow-cmd-params-config", "data.namespaced", "false")
		})

		AfterAll(func() {
			// Cleanup the resource after the tests
			Expect(k8sClient.Delete(ctx, &apiv1.NumaflowControllerRollout{
				ObjectMeta: numaflowControllerResource.ObjectMeta,
			})).Should(Succeed())

			deletedResource := &apiv1.NumaflowControllerRollout{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, deletedResource)
				return errors.IsNotFound(err)
			}, timeout, interval).Should(BeTrue())

			deletingChildResource := &appsv1.Deployment{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, deletingChildResource)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(deletingChildResource.OwnerReferences).Should(HaveLen(1))
			Expect(deletedResource.UID).Should(Equal(deletingChildResource.OwnerReferences[0].UID))

		})
	})
})

var _ = Describe("Apply Ownership Reference", func() {
	Context("ownership reference", func() {
		const resourceName = "test-resource"
		manifests := []string{
			`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: example-deployment
  labels:
    app: example
spec:
  replicas: 2
  selector:
    matchLabels:
      app: example
  template:
    metadata:
      labels:
        app: example
    spec:
      containers:
      - name: example-container
        image: nginx:1.14.2
        ports:
        - containerPort: 80
`,
			`
apiVersion: v1
kind: Service
metadata:
  name: example-service
spec:
  selector:
    app: example
  ports:
    - protocol: TCP
      port: 80
      targetPort: 9376
`}
		emanifests := []string{
			`apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    app: example
  name: example-deployment
  ownerReferences:
  - apiVersion: ""
    blockOwnerDeletion: true
    controller: true
    kind: ""
    name: test-resource
    uid: ""
spec:
  replicas: 2
  selector:
    matchLabels:
      app: example
  template:
    metadata:
      labels:
        app: example
    spec:
      containers:
      - image: nginx:1.14.2
        name: example-container
        ports:
        - containerPort: 80`,
			`apiVersion: v1
kind: Service
metadata:
  name: example-service
  ownerReferences:
  - apiVersion: ""
    blockOwnerDeletion: true
    controller: true
    kind: ""
    name: test-resource
    uid: ""
spec:
  ports:
  - port: 80
    protocol: TCP
    targetPort: 9376
  selector:
    app: example`,
		}

		resource := &apiv1.NumaflowControllerRollout{
			ObjectMeta: metav1.ObjectMeta{
				Name:      resourceName,
				Namespace: "default",
			},
		}
		It("should apply ownership reference correctly", func() {

			manifests, err := applyOwnershipToManifests(manifests, resource)
			Expect(err).To(BeNil())
			Expect(strings.TrimSpace(manifests[0])).To(Equal(strings.TrimSpace(emanifests[0])))
			Expect(strings.TrimSpace(manifests[1])).To(Equal(strings.TrimSpace(emanifests[1])))
		})
		It("should not apply ownership if it already exists", func() {
			manifests, err := applyOwnershipToManifests(emanifests, resource)
			Expect(err).To(BeNil())
			Expect(strings.TrimSpace(manifests[0])).To(Equal(strings.TrimSpace(emanifests[0])))
			Expect(strings.TrimSpace(manifests[1])).To(Equal(strings.TrimSpace(emanifests[1])))

		})
	})
})

func Test_getControllerDeploymentVersion(t *testing.T) {
	testCases := []struct {
		name        string
		containers  []corev1.Container
		expectedTag string
	}{
		{
			name: "standard",
			containers: []corev1.Container{
				{
					Image: "some/path/sidecar:latest",
				},
				{
					Image: "quay.io/numaproj/numaflow:v1.0.2",
				},
			},
			expectedTag: "1.0.2",
		},
		{
			name: "images have no paths",
			containers: []corev1.Container{
				{
					Image: "sidecar",
				},
				{
					Image: "numaflow:v1.0.2", // valid if it's in the default registry
				},
			},
			expectedTag: "1.0.2",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			deployment := appsv1.Deployment{}
			deployment.Spec.Template.Spec.Containers = append(deployment.Spec.Template.Spec.Containers, tc.containers...)
			tag, err := getControllerDeploymentVersion(&deployment)
			assert.Nil(t, err)
			assert.Equal(t, tc.expectedTag, tag)
		})
	}

}
