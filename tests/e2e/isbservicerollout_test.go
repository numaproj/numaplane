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

	spec := numaflowv1.InterStepBufferServiceSpec{
		Redis: &numaflowv1.RedisBufferService{},
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: "latest",
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &numaflowv1.DefaultVolumeSize,
			},
		},
	}

	rawSpec, err := json.Marshal(spec)
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
