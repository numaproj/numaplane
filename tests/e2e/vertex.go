package e2e

import (
	"context"
	"fmt"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/retry"

	"github.com/numaproj/numaplane/internal/util"
)

func GetGVRForVertex() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "vertices",
	}
}

func GetVertexByName(namespace, name string) (*unstructured.Unstructured, error) {
	return dynamicClient.Resource(GetGVRForVertex()).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
}

func watchVertices() {

	watchResourceType(func() (watch.Interface, error) {
		watcher, err := dynamicClient.Resource(GetGVRForVertex()).Namespace(Namespace).Watch(context.Background(), metav1.ListOptions{})
		return watcher, err
	}, func(o runtime.Object) Output {
		if obj, ok := o.(*unstructured.Unstructured); ok {
			vtx := numaflowv1.Vertex{}
			err := util.StructToStruct(&obj, &vtx)
			if err != nil {
				fmt.Printf("Failed to convert unstruct: %v\n", err)
				return Output{}
			}
			vtx.ManagedFields = nil
			return Output{
				APIVersion: NumaflowAPIVersion,
				Kind:       "Vertex",
				Metadata:   vtx.ObjectMeta,
				Spec:       vtx.Spec,
				Status:     vtx.Status,
			}
		}
		return Output{}
	})

}

func UpdateVertexInK8S(name string, f func(*unstructured.Unstructured) (*unstructured.Unstructured, error)) {
	By(fmt.Sprintf("updating Vertex %q", name))

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		vertex, err := dynamicClient.Resource(GetGVRForVertex()).Namespace(Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil || vertex == nil {
			return err
		}

		updatedVertex, err := f(vertex)
		if err != nil {
			return err
		}

		_, err = dynamicClient.Resource(GetGVRForVertex()).Namespace(Namespace).Update(ctx, updatedVertex, metav1.UpdateOptions{})
		return err
	})
	Expect(err).ShouldNot(HaveOccurred())
}

func VerifyVertexSpecStatus(namespace string, vertexName string, f func(numaflowv1.VertexSpec, numaflowv1.VertexStatus) bool) {
	var retrievedVertexSpec numaflowv1.VertexSpec
	var retrievedVertexStatus numaflowv1.VertexStatus
	CheckEventually("verifying Vertex Spec/Status", func() bool {
		unstruct, err := GetVertex(namespace, vertexName)
		if err != nil {
			return false
		}
		if retrievedVertexSpec, err = GetVertexSpec(unstruct); err != nil {
			return false
		}
		if retrievedVertexStatus, err = GetVertexStatus(unstruct); err != nil {
			return false
		}

		return f(retrievedVertexSpec, retrievedVertexStatus)
	}).Should(BeTrue())
}

func GetVertex(namespace string, vertexName string) (*unstructured.Unstructured, error) {
	return dynamicClient.Resource(GetGVRForVertex()).Namespace(namespace).Get(ctx, vertexName, metav1.GetOptions{})
}

func GetVertexSpec(u *unstructured.Unstructured) (numaflowv1.VertexSpec, error) {
	specMap := u.Object["spec"]
	var vertexSpec numaflowv1.VertexSpec
	err := util.StructToStruct(&specMap, &vertexSpec)
	return vertexSpec, err
}

func GetVertexStatus(u *unstructured.Unstructured) (numaflowv1.VertexStatus, error) {
	statusMap := u.Object["status"]
	var vertexStatus numaflowv1.VertexStatus
	err := util.StructToStruct(&statusMap, &vertexStatus)
	return vertexStatus, err
}
