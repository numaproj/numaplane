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
	"encoding/json"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kubernetes/pkg/apis/apps"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("NumaflowControllerRollout e2e", func() {

	const (
		numaflowControllerRolloutName = "numaflow-controller"
	)
	deploygvr := getGVRForDeployment()

	It("Should create the NumaflowControllerRollout if it doesn't exist", func() {

		numaflowControllerRolloutSpec := createNumaflowControllerRolloutSpec(numaflowControllerRolloutName, Namespace)
		_, err := numaflowControllerRolloutClient.Create(ctx, numaflowControllerRolloutSpec, metav1.CreateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		By("Verifying that the NumaflowControllerRollout was created")
		Eventually(func() error {
			_, err := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			return err
		}).WithTimeout(testTimeout).Should(Succeed())

		By("Verifying that the NumaflowController was created")
		Eventually(func() error {
			_, err := dynamicClient.Resource(getGVRForDeployment()).Namespace(Namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			return err
		}).WithTimeout(testTimeout).Should(Succeed())

	})

	It("Should update the child NumaflowController if the NumaflowControllerRollout is updated", func() {

		var err error

		// new NumaflowController spec
		updatedNumaflowControllerSpec := apiv1.NumaflowControllerRolloutSpec{
			Controller: apiv1.Controller{Version: "1.1.7"},
		}

		time.Sleep(3 * time.Second)

		// get current NumaflowControllerRollout
		rollout := &apiv1.NumaflowControllerRollout{}
		Eventually(func() bool {
			rollout, err = numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			return err == nil
		}).WithTimeout(testTimeout).Should(BeTrue())

		// update the NumaflowControllerRollout
		rollout.Spec = updatedNumaflowControllerSpec
		_, err = numaflowControllerRolloutClient.Update(ctx, rollout, metav1.UpdateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		// wait for update to reconcile
		time.Sleep(5 * time.Second)

		createdNumaflowController := &unstructured.Unstructured{}
		Eventually(func() bool {
			unstruct, err := dynamicClient.Resource(deploygvr).Namespace(Namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			if err != nil {
				return false
			}
			createdNumaflowController = unstruct
			return true
		}).WithTimeout(testTimeout).Should(BeTrue())
		createdNumaflowControllerSpec := apps.Deployment{}
		bytes, err := json.Marshal(createdNumaflowController)
		Expect(err).ShouldNot(HaveOccurred())
		err = json.Unmarshal(bytes, &createdNumaflowControllerSpec)
		Expect(err).ShouldNot(HaveOccurred())

		Expect(createdNumaflowControllerSpec.Spec.Template.Spec.Containers[0].Image).Should(Equal("quay.io/numaproj/numaflow:v1.1.7"))

	})

	It("Should delete the NumaflowControllerRollout and child NumaflowController", func() {

		err := numaflowControllerRolloutClient.Delete(ctx, numaflowControllerRolloutName, metav1.DeleteOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		Eventually(func() bool {
			_, err := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					Fail("An unexpected error occurred when fetching the NumaflowControllerRollout: " + err.Error())
				}
				return false
			}
			return true
		}).WithTimeout(testTimeout).Should(BeFalse(), "The NumaflowControllerRollout should have been deleted but it was found.")

		Eventually(func() bool {
			_, err := dynamicClient.Resource(getGVRForDeployment()).Namespace(Namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					Fail("An unexpected error occurred when fetching the deployment: " + err.Error())
				}
				return false
			}
			return true
		}).WithTimeout(testTimeout).Should(BeFalse(), "The deployment should have been deleted but it was found.")

	})

})

func createNumaflowControllerRolloutSpec(name, namespace string) *apiv1.NumaflowControllerRollout {

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
			Controller: apiv1.Controller{Version: "1.2.1"},
		},
	}

	return controllerRollout

}

func getGVRForDeployment() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "apps",
		Version:  "v1",
		Resource: "deployments",
	}
}
