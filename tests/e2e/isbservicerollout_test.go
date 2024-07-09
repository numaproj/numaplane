package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var _ = Describe("ISBService E2E", func() {
	const (
		namespace             = "numaplane-system"
		isbServiceRolloutName = "test-isb-service-rollout"
		timeout               = 30 * time.Second
		duration              = time.Second
		interval              = 250 * time.Millisecond
	)
	gvr := getGVRForISBServiceRollout()

	BeforeEach(func() {
		err := dynamicClient.Resource(gvr).Namespace(namespace).Delete(ctx, isbServiceRolloutName, metav1.DeleteOptions{})
		if err != nil && !errors.IsNotFound(err) {
			Expect(err).NotTo(HaveOccurred())
		}
	})

	It("Should create, update, and delete a ISBService", func() {
		// Creating ISB Service Rollout
		By("Creating a new ISBServiceRollout")
		isbServiceRollout := createISBServiceRolloutSpec(isbServiceRolloutName, namespace)
		// fmt.Printf(isbServiceRollout)
		err := createISBServiceRollout(ctx, isbServiceRollout)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() error {
			_, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			return err
		}, time.Second*30, time.Second).Should(Succeed())

		By("Updating the existing ISBServiceRollout")
		updateLabelValue := "update-1"

		// Get the latest ISBServiceRollout again before updating
		gotISBServiceRollout, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Update the ISBServiceRollout.
		unstructured.SetNestedField(gotISBServiceRollout.Object, updateLabelValue, "metadata", "labels", "customLabelKey")
		_, err = dynamicClient.Resource(gvr).Namespace(namespace).Update(ctx, gotISBServiceRollout, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Verify the update.
		updatedISBServiceRollout, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		val, _, _ := unstructured.NestedString(updatedISBServiceRollout.Object, "metadata", "labels", "customLabelKey")
		Expect(val).To(Equal(updateLabelValue))

		By("Deleting the ISBServiceRollout")
		err = deleteISBServiceRollout(ctx, namespace, isbServiceRolloutName)
		Expect(err).NotTo(HaveOccurred())

		By("Checking the ISBServiceRollout has been deleted")
		Eventually(func() bool {
			_, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			return errors.IsNotFound(err)
		}, timeout, duration).Should(BeTrue())
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

	// Convert spec to rawExtension
	rawSpec, err := json.Marshal(spec)
	if err != nil {
		println(fmt.Sprintf("error marshaling spec: %v", err))
	}

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
			InterStepBufferService: runtime.RawExtension{Raw: rawSpec},
		},
	}

	unstructuredObj, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(ISBServiceRollout)
	return &unstructured.Unstructured{Object: unstructuredObj}
}

func createISBServiceRollout(ctx context.Context, isbServiceRollout *unstructured.Unstructured) error {
	gvr := getGVRForISBServiceRollout()
	_, err := dynamicClient.Resource(gvr).Namespace(isbServiceRollout.GetNamespace()).Create(ctx, isbServiceRollout, metav1.CreateOptions{})
	return err
}

func deleteISBServiceRollout(ctx context.Context, namespace, name string) error {
	gvr := getGVRForISBServiceRollout()
	return dynamicClient.Resource(gvr).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

func getGVRForISBServiceRollout() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaplane.numaproj.io",
		Version:  "v1alpha1",
		Resource: "isbservicerollouts",
	}
}
