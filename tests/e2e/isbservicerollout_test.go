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
		namespace             = "numaplane-system"
		isbServiceRolloutName = "test-isbservice-rollout"
	)
	rolloutgvr := getGVRForISBServiceRollout()
	isbservicegvr := getGVRForISBService()

	It("Should create the ISBServiceRollout if it doesn't exist", func() {

		isbServiceRolloutSpec := createISBServiceRolloutSpec(isbServiceRolloutName, namespace)

		err := createISBServiceRollout(ctx, isbServiceRolloutSpec)
		Expect(err).NotTo(HaveOccurred())

		By("Verifying that the ISBServiceRollout was created")
		Eventually(func() error {
			_, err := dynamicClient.Resource(rolloutgvr).Namespace(namespace).Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			return err
		}).WithTimeout(timeout).Should(Succeed())

		By("Verifying that the ISBService was created")
		Eventually(func() error {
			_, err := dynamicClient.Resource(isbservicegvr).Namespace(namespace).Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			return err
		}).WithTimeout(timeout).Should(Succeed())

	})

	It("Should update the child ISBService if the ISBServiceRollout is updated", func() {

		// new ISBService spec
		updatedISBServiceSpec := isbServiceSpec
		updatedISBServiceSpec.JetStream.Version = "1.0.0"

		time.Sleep(3 * time.Second)

		// get current ISBServiceRollout
		createdResource := &unstructured.Unstructured{}
		Eventually(func() bool {
			unstruct, err := dynamicClient.Resource(rolloutgvr).Namespace(namespace).Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			if err != nil {
				return false
			}
			createdResource = unstruct
			return true
		}).WithTimeout(timeout).Should(BeTrue())

		// update spec.interstepbufferservice.spec of returned ISBServiceRollout object
		createdResource.Object["spec"].(map[string]interface{})["interStepBufferService"].(map[string]interface{})["spec"] = updatedISBServiceSpec

		// update the ISBServiceRollout
		_, err := dynamicClient.Resource(rolloutgvr).Namespace(namespace).Update(ctx, createdResource, metav1.UpdateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		// wait for update to reconcile
		time.Sleep(5 * time.Second)

		createdISBService := &unstructured.Unstructured{}
		Eventually(func() bool {
			unstruct, err := dynamicClient.Resource(isbservicegvr).Namespace(namespace).Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			if err != nil {
				return false
			}
			createdISBService = unstruct
			return true
		}).WithTimeout(timeout).Should(BeTrue())
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

		err := deleteISBServiceRollout(ctx, isbServiceRolloutName, namespace)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() bool {
			_, err := dynamicClient.Resource(rolloutgvr).Namespace(namespace).Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					Fail("An unexpected error occurred when fetching the ISBServiceRollout: " + err.Error())
				}
				return false
			}
			return true
		}, timeout).Should(BeFalse(), "The ISBServiceRollout should have been deleted but it was found.")

		Eventually(func() bool {
			_, err := dynamicClient.Resource(isbservicegvr).Namespace(namespace).Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					Fail("An unexpected error occurred when fetching the ISBService: " + err.Error())
				}
				return false
			}
			return true
		}, timeout).Should(BeFalse(), "The ISBService should have been deleted but it was found.")

	})

})

func createISBServiceRolloutSpec(name, namespace string) *unstructured.Unstructured {

	// spec := numaflowv1.InterStepBufferServiceSpec{
	// 	Redis: &numaflowv1.RedisBufferService{},
	// 	JetStream: &numaflowv1.JetStreamBufferService{
	// 		Version: "latest",
	// 		Persistence: &numaflowv1.PersistenceStrategy{
	// 			VolumeSize: &numaflowv1.DefaultVolumeSize,
	// 		},
	// 	},
	// }

	rawSpec, err := json.Marshal(isbServiceSpec)
	Expect(err).ToNot(HaveOccurred())

	ISBServiceRollout := &apiv1.ISBServiceRollout{
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

	unstructuredObj, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(ISBServiceRollout)
	return &unstructured.Unstructured{Object: unstructuredObj}

}

func createISBServiceRollout(ctx context.Context, rollout *unstructured.Unstructured) error {
	_, err := dynamicClient.Resource(getGVRForISBServiceRollout()).Namespace(rollout.GetNamespace()).Create(ctx, rollout, metav1.CreateOptions{})
	return err
}

func deleteISBServiceRollout(ctx context.Context, name, namespace string) error {
	return dynamicClient.Resource(getGVRForISBServiceRollout()).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

func getGVRForISBServiceRollout() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaplane.numaproj.io",
		Version:  "v1alpha1",
		Resource: "isbservicerollouts",
	}
}
func getGVRForISBService() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "interstepbufferservices",
	}
}
