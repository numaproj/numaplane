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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var isbServiceSpec = numaflowv1.InterStepBufferServiceSpec{
	Redis: &numaflowv1.RedisBufferService{},
	JetStream: &numaflowv1.JetStreamBufferService{
		Version: "latest",
		Persistence: &numaflowv1.PersistenceStrategy{
			VolumeSize: &numaflowv1.DefaultVolumeSize,
		},
	},
}

var _ = Describe("ISBServiceRollout e2e", func() {

	const (
		isbServiceRolloutName = "test-isbservice-rollout"
	)
	isbservicegvr := getGVRForISBService()

	It("Should create the ISBServiceRollout if it doesn't exist", func() {

		isbServiceRolloutSpec := createISBServiceRolloutSpec(isbServiceRolloutName, Namespace)
		_, err := isbServiceRolloutClient.Create(ctx, isbServiceRolloutSpec, metav1.CreateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		By("Verifying that the ISBServiceRollout was created")
		Eventually(func() error {
			_, err := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			return err
		}).WithTimeout(testTimeout).Should(Succeed())

		By("Verifying that the ISBService was created")
		Eventually(func() error {
			_, err := dynamicClient.Resource(isbservicegvr).Namespace(Namespace).Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			return err
		}).WithTimeout(testTimeout).Should(Succeed())

	})

	It("Should update the child ISBService if the ISBServiceRollout is updated", func() {

		// new ISBService spec
		updatedISBServiceSpec := isbServiceSpec
		updatedISBServiceSpec.JetStream.Version = "1.0.0"

		rawSpec, err := json.Marshal(updatedISBServiceSpec)
		Expect(err).ShouldNot(HaveOccurred())

		time.Sleep(3 * time.Second)

		// get current ISBServiceRollout
		rollout := &apiv1.ISBServiceRollout{}
		Eventually(func() bool {
			rollout, err = isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			return err == nil
		}).WithTimeout(testTimeout).Should(BeTrue())

		// update the ISBServiceRollout
		rollout.Spec.InterStepBufferService.Spec.Raw = rawSpec
		_, err = isbServiceRolloutClient.Update(ctx, rollout, metav1.UpdateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		// wait for update to reconcile
		time.Sleep(5 * time.Second)

		createdISBService := &unstructured.Unstructured{}
		Eventually(func() bool {
			unstruct, err := dynamicClient.Resource(isbservicegvr).Namespace(Namespace).Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			if err != nil {
				return false
			}
			createdISBService = unstruct
			return true
		}).WithTimeout(testTimeout).Should(BeTrue())
		createdISBServiceSpec := numaflowv1.InterStepBufferServiceSpec{}
		rawISBServiceSpec := createdISBService.Object["spec"].(map[string]interface{})
		rawISBServiceSpecBytes, err := json.Marshal(rawISBServiceSpec)
		Expect(err).ShouldNot(HaveOccurred())
		err = json.Unmarshal(rawISBServiceSpecBytes, &createdISBServiceSpec)
		Expect(err).ShouldNot(HaveOccurred())

		By("Verifying isbService spec is equal to updated spec")
		Expect(createdISBServiceSpec).Should(Equal(updatedISBServiceSpec))

	})

	It("Should delete the ISBServiceRollout and child ISBService", func() {

		err := isbServiceRolloutClient.Delete(ctx, isbServiceRolloutName, metav1.DeleteOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		Eventually(func() bool {
			_, err := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					Fail("An unexpected error occurred when fetching the ISBServiceRollout: " + err.Error())
				}
				return false
			}
			return true
		}).WithTimeout(testTimeout).Should(BeFalse(), "The ISBServiceRollout should have been deleted but it was found.")

		Eventually(func() bool {
			_, err := dynamicClient.Resource(isbservicegvr).Namespace(Namespace).Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					Fail("An unexpected error occurred when fetching the ISBService: " + err.Error())
				}
				return false
			}
			return true
		}).WithTimeout(testTimeout).Should(BeFalse(), "The ISBService should have been deleted but it was found.")

	})

})

func createISBServiceRolloutSpec(name, namespace string) *apiv1.ISBServiceRollout {

	rawSpec, err := json.Marshal(isbServiceSpec)
	Expect(err).ShouldNot(HaveOccurred())

	isbServiceRollout := &apiv1.ISBServiceRollout{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "numaplane.numaproj.io/v1alpha1",
			Kind:       "ISBServiceRollout",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: apiv1.ISBServiceRolloutSpec{
			InterStepBufferService: apiv1.InterStepBufferService{
				Spec: runtime.RawExtension{
					Raw: rawSpec,
				},
			},
		},
	}

	return isbServiceRollout

}

func getGVRForISBService() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "interstepbufferservices",
	}
}
