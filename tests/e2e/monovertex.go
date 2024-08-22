package e2e

import (
	"fmt"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func getGVRForMonoVertex() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "monovertices",
	}
}

// Get MonoVertexSpec from Unstructured type
func getMonoVertexSpec(u *unstructured.Unstructured) (numaflowv1.MonoVertexSpec, error) {
	specMap := u.Object["spec"]
	var monoVertexSpec numaflowv1.MonoVertexSpec
	err := util.StructToStruct(&specMap, &monoVertexSpec)
	return monoVertexSpec, err
}

func verifyMonoVertexSpec(namespace, name string, f func(numaflowv1.MonoVertexSpec) bool) {

	document("verifying MonoVertex Spec")
	var retrievedMonoVertexSpec numaflowv1.MonoVertexSpec
	Eventually(func() bool {
		unstruct, err := dynamicClient.Resource(getGVRForMonoVertex()).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		if retrievedMonoVertexSpec, err = getMonoVertexSpec(unstruct); err != nil {
			return false
		}
		return f(retrievedMonoVertexSpec)
	}).WithTimeout(testTimeout).Should(BeTrue())

}

func verifyMonoVertexReady(namespace, monoVertexName string) {

	document("Verifying that the MonoVertex is running")
	verifyMonoVertexStatus(namespace, monoVertexName,
		func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec, retrievedMonoVertexStatus kubernetes.GenericStatus) bool {
			return retrievedMonoVertexStatus.Phase == string(numaflowv1.MonoVertexPhaseRunning)
		})

	vertexLabelSelector := fmt.Sprintf("%s=%s,%s=%s", numaflowv1.KeyMonoVertexName, monoVertexName, numaflowv1.KeyComponent, "mono-vertex")
	daemonLabelSelector := fmt.Sprintf("%s=%s,%s=%s", numaflowv1.KeyMonoVertexName, monoVertexName, numaflowv1.KeyComponent, "mono-vertex-daemon")

	document("Verifying that the MonoVertex is ready")
	verifyPodsRunning(namespace, 1, vertexLabelSelector)
	verifyPodsRunning(namespace, 1, daemonLabelSelector)

}

func verifyMonoVertexStatus(namespace, monoVertexName string, f func(numaflowv1.MonoVertexSpec, kubernetes.GenericStatus) bool) {

	document("verifying MonoVertexStatus")
	var retrievedMonoVertexSpec numaflowv1.MonoVertexSpec
	var retrievedMonoVertexStatus kubernetes.GenericStatus
	Eventually(func() bool {
		unstruct, err := dynamicClient.Resource(getGVRForMonoVertex()).Namespace(namespace).Get(ctx, monoVertexName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		if retrievedMonoVertexSpec, err = getMonoVertexSpec(unstruct); err != nil {
			return false
		}
		if retrievedMonoVertexStatus, err = getNumaflowResourceStatus(unstruct); err != nil {
			return false
		}
		return f(retrievedMonoVertexSpec, retrievedMonoVertexStatus)
	}).WithTimeout(testTimeout).Should(BeTrue())

}

func updateMonoVertexRolloutInK8S(name string, f func(apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error)) {

	document("updating MonoVertexRollout")
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rollout, err := monoVertexRolloutClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		*rollout, err = f(*rollout)
		if err != nil {
			return err
		}

		_, err = monoVertexRolloutClient.Update(ctx, rollout, metav1.UpdateOptions{})
		return err
	})
	Expect(err).ShouldNot(HaveOccurred())
}
