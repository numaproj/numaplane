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
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var _ = Describe("NumaflowControllerRollout Controller", func() {
	const (
		namespace = "default"
	)
	Context("When reconciling a resource", func() {
		const resourceName = "numaflow-controller"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: namespace, // TODO(user):Modify as needed
		}
		numaflowcontrollerrollout := &apiv1.NumaflowControllerRollout{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind NumaflowControllerRollout")
			err := k8sClient.Get(ctx, typeNamespacedName, numaflowcontrollerrollout)
			if err != nil && errors.IsNotFound(err) {
				resource := &apiv1.NumaflowControllerRollout{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: namespace,
					},
					// TODO(user): Specify other spec details if needed.
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &apiv1.NumaflowControllerRollout{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance NumaflowControllerRollout")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("Should throw a CR validation error", func() {
			By("Creating a NumaflowControllerRollout resource with an invalid name")
			resource := &apiv1.NumaflowControllerRollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-numaflow-controller", // invalid name: only supported name is "numaflow-controller"
					Namespace: namespace,
				},
				Spec: apiv1.NumaflowControllerRolloutSpec{
					Controller: apiv1.Controller{Version: "1.2.1"},
				},
			}
			err := k8sClient.Create(ctx, resource)
			Expect(err).NotTo(Succeed())
			Expect(err.Error()).To(ContainSubstring("The metadata name must be 'numaflow-controller'"))
		})

		// It("should successfully reconcile the resource", func() {
		// 	By("Reconciling the created resource")
		// 	controllerReconciler := &NumaflowControllerRolloutReconciler{
		// 		client:     k8sClient,
		// 		scheme:     k8sClient.Scheme(),
		// 		restConfig: cfg,
		// 	}

		// 	_, err := controllerReconciler.Reconcile(ctx, sigsReconcile.Request{
		// 		NamespacedName: typeNamespacedName,
		// 	})
		// 	Expect(err).NotTo(HaveOccurred())
		// 	// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
		// 	// Example: If you expect a certain status condition after reconciliation, verify it here.
		// })
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
