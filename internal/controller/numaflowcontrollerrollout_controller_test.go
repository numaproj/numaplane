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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var _ = Describe("NumaflowControllerRollout Controller", Ordered, func() {
	const (
		namespace    = "default"
		resourceName = "numaflow-controller"
	)

	ctx := context.Background()

	Context("When reconciling a resource", func() {
		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: namespace,
		}

		numaflowControllerRollout := apiv1.NumaflowControllerRollout{
			ObjectMeta: metav1.ObjectMeta{
				Name:      resourceName,
				Namespace: namespace,
			},
			Spec: apiv1.NumaflowControllerRolloutSpec{
				Controller: apiv1.Controller{Version: "1.2.1"},
			},
		}

		It("Should throw a CR validation error", func() {
			By("Creating a NumaflowControllerRollout resource with an invalid name")
			resource := numaflowControllerRollout
			resource.Name = "test-numaflow-controller"
			err := k8sClient.Create(ctx, &resource)
			Expect(err).NotTo(Succeed())
			Expect(err.Error()).To(ContainSubstring("The metadata name must be 'numaflow-controller'"))
		})

		It("Should throw duplicate resource error", func() {
			By("Creating duplicate NumaflowControllerRollout resource with the same name")
			resource := numaflowControllerRollout
			err := k8sClient.Create(ctx, &resource)
			Expect(err).To(Succeed())

			resource.ResourceVersion = "" // Reset the resource version to create a new resource
			err = k8sClient.Create(ctx, &resource)
			Expect(err).NotTo(Succeed())
			Expect(err.Error()).To(ContainSubstring("numaflowcontrollerrollouts.numaplane.numaproj.io \"numaflow-controller\" already exists"))
		})

		It("Should auto heal the Numaflow Controller Deployment with the spec based on the NumaflowControllerRollout version field value when the Deployment spec is changed", func() {
			By("updating the Numaflow Controller Deployment")
			currentDeployment := &appsv1.Deployment{}
			Eventually(func() (*appsv1.Deployment, error) {
				if err := k8sClient.Get(ctx, typeNamespacedName, currentDeployment); err != nil {
					return nil, err
				}

				return currentDeployment, nil
			}, timeout, interval).ShouldNot(BeNil())

			originalImage := currentDeployment.Spec.Template.Spec.Containers[0].Image
			newImage := "someimage:1.2.3"
			currentDeployment.Spec.Template.Spec.Containers[0].Image = newImage

			Expect(k8sClient.Update(ctx, currentDeployment)).ToNot(HaveOccurred())

			By("Verifying the changed field of the Numaflow Controller Deployment is the same as the original and not the modified version")
			e := Eventually(func() (string, error) {
				updatedDeployment := &appsv1.Deployment{}
				if err := k8sClient.Get(ctx, typeNamespacedName, updatedDeployment); err != nil {
					return "", err
				}

				return updatedDeployment.Spec.Template.Spec.Containers[0].Image, nil
			}, timeout, interval)

			e.Should(Equal(originalImage))
			e.ShouldNot(Equal(newImage))
		})

		AfterAll(func() {
			// Cleanup the resource after each test and ignore the error if it doesn't exist
			resource := &apiv1.NumaflowControllerRollout{}
			_ = k8sClient.Get(ctx, typeNamespacedName, resource)
			_ = k8sClient.Delete(ctx, resource)
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
