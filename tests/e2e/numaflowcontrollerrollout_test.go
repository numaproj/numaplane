package e2e

import (
	"context"
	"time"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var _ = Describe("NumaflowControllerRollout E2E", func() {
	const (
		namespace                     = "numaplane-system"
		numaflowControllerRolloutName = "numaflow-controller"
		timeout                       = 30 * time.Second
		duration                      = time.Second
	)
	gvr := getGVRForNumaflowControllerRollout()

	BeforeEach(func() {
		err := dynamicClient.Resource(gvr).Namespace(namespace).Delete(ctx, numaflowControllerRolloutName, metav1.DeleteOptions{})
		if err != nil && !errors.IsNotFound(err) {
			Expect(err).NotTo(HaveOccurred())
		}
	})

	It("Should create, update, and delete a NumaflowControllerRollout", func() {
		By("Creating a new NumaflowControllerRollout")
		numaflowControllerRollout := createNumaflowControllerRolloutSpec(numaflowControllerRolloutName, namespace)

		err := createNumaflowControllerRollout(ctx, numaflowControllerRollout)
		Expect(err).NotTo(HaveOccurred())

		By("Checking if the NumaflowControllerRollout was created")

		Eventually(func() error {
			_, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			return err
		}, timeout, duration).Should(Succeed())

		By("Updating the existing NumaflowControllerRollout")
		updateLabelValue := "update-1"

		gotNumaflowControllerRollout, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		err = unstructured.SetNestedField(gotNumaflowControllerRollout.Object, updateLabelValue, "metadata", "labels", "customLabelKey")
		Expect(err).NotTo(HaveOccurred())
		_, err = dynamicClient.Resource(gvr).Namespace(namespace).Update(ctx, gotNumaflowControllerRollout, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())

		updatedNumaflowControllerRollout, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		val, _, _ := unstructured.NestedString(updatedNumaflowControllerRollout.Object, "metadata", "labels", "customLabelKey")
		Expect(val).To(Equal(updateLabelValue))

		By("Deleting the NumaflowControllerRollout")

		err = deleteNumaflowControllerRollout(ctx, namespace, numaflowControllerRolloutName)
		Expect(err).NotTo(HaveOccurred())

		By("Checking the NumaflowControllerRollout has been deleted")
		Eventually(func() bool {
			_, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			return errors.IsNotFound(err)
		}, timeout, duration).Should(BeTrue())
	})
})

func createNumaflowControllerRolloutSpec(name, namespace string) *unstructured.Unstructured {
	ControllerRollout := &apiv1.NumaflowControllerRollout{
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

	unstructuredObj, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(ControllerRollout)
	return &unstructured.Unstructured{Object: unstructuredObj}
}

func createNumaflowControllerRollout(ctx context.Context, numaflowControllerRollout *unstructured.Unstructured) error {
	gvr := getGVRForNumaflowControllerRollout()
	_, err := dynamicClient.Resource(gvr).Namespace(numaflowControllerRollout.GetNamespace()).Create(ctx, numaflowControllerRollout, metav1.CreateOptions{})
	return err
}

func deleteNumaflowControllerRollout(ctx context.Context, namespace, name string) error {
	gvr := getGVRForNumaflowControllerRollout()
	return dynamicClient.Resource(gvr).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

func getGVRForNumaflowControllerRollout() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaplane.numaproj.io",
		Version:  "v1alpha1",
		Resource: "numaflowcontrollerrollouts",
	}
}
