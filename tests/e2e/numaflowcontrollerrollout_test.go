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

package e2e

import (
	"context"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("NumaflowControllerRollout e2e", func() {

	const (
		namespace                     = "numaplane-system"
		numaflowControllerRolloutName = "numaflow-controller"
	)
	gvr := getGVRForNumaflowControllerRollout()

	It("Should create the NumaflowControllerRollout if it doesn't exist", func() {

		numaflowControllerRolloutSpec := createNumaflowControllerRolloutSpec(numaflowControllerRolloutName, namespace)

		err := createNumaflowControllerRollout(ctx, numaflowControllerRolloutSpec)
		Expect(err).NotTo(HaveOccurred())

		By("Verifying that the NumaflowControllerRollout was created")
		Eventually(func() error {
			_, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			return err
		}).WithTimeout(timeout).Should(Succeed())

		By("Verifying that the NumaflowController was created")
		Eventually(func() error {
			_, err := dynamicClient.Resource(getGVRForDeployment()).Namespace(namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			return err
		}).WithTimeout(timeout).Should(Succeed())

	})

	It("Should delete the NumaflowControllerRollout and child NumaflowController", func() {

		err := deleteNumaflowControllerRollout(ctx, numaflowControllerRolloutName, namespace)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() bool {
			_, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					Fail("An unexpected error occurred when fetching the NumaflowControllerRollout: " + err.Error())
				}
				return false
			}
			return true
		}, timeout).Should(BeFalse(), "The NumaflowControllerRollout should have been deleted but it was found.")

		Eventually(func() bool {
			_, err := dynamicClient.Resource(getGVRForDeployment()).Namespace(namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					Fail("An unexpected error occurred when fetching the deployment: " + err.Error())
				}
				return false
			}
			return true
		}, timeout).Should(BeFalse(), "The deployment should have been deleted but it was found.")

	})

})

func createNumaflowControllerRolloutSpec(name, namespace string) *unstructured.Unstructured {

	controllerRollout := &apiv1.NumaflowControllerRollout{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "numaplane.numaproj.io/v1alpha1",
			Kind:       "NumaflowControllerRollout",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: apiv1.NumaflowControllerRolloutSpec{
			// TODO: can we also set this to `latest` like ISBService?
			Controller: apiv1.Controller{Version: "1.2.1"},
		},
	}

	unstructuredObj, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(controllerRollout)
	return &unstructured.Unstructured{Object: unstructuredObj}

}

func createNumaflowControllerRollout(ctx context.Context, rollout *unstructured.Unstructured) error {
	_, err := dynamicClient.Resource(getGVRForNumaflowControllerRollout()).Namespace(rollout.GetNamespace()).Create(ctx, rollout, metav1.CreateOptions{})
	return err
}

func deleteNumaflowControllerRollout(ctx context.Context, name, namespace string) error {
	return dynamicClient.Resource(getGVRForNumaflowControllerRollout()).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

func getGVRForNumaflowControllerRollout() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaplane.numaproj.io",
		Version:  "v1alpha1",
		Resource: "numaflowcontrollerrollouts",
	}
}

func getGVRForDeployment() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "apps",
		Version:  "v1",
		Resource: "deployments",
	}
}
