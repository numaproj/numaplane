package e2e

import (
	"context"
	"time"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

type ExpectHelper struct {
	client dynamic.Interface
}

func NewExpectHelper(client dynamic.Interface) *ExpectHelper {
	return &ExpectHelper{client: client}
}

func (e *ExpectHelper) PipelineRolloutToExist(ctx context.Context, namespace, name string) {
	gvr := schema.GroupVersionResource{
		Group:    "numaplane.numaproj.io",
		Version:  "v1alpha1",
		Resource: "pipelinerollouts",
	}

	gomega.Eventually(func() bool {
		_, err := e.client.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		return err == nil
	}, time.Second*10, time.Millisecond*250).Should(gomega.BeTrue())
}

func (e *ExpectHelper) PipelineRolloutToBeDeleted(ctx context.Context, namespace, name string) {
	gvr := schema.GroupVersionResource{
		Group:    "numaplane.numaproj.io",
		Version:  "v1alpha1",
		Resource: "pipelinerollouts",
	}

	gomega.Eventually(func() bool {
		_, err := e.client.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		return err != nil
	}, time.Second*10, time.Millisecond*250).Should(gomega.BeTrue())
}

func (e *ExpectHelper) PipelineRolloutToBeUpdated(ctx context.Context, namespace, name string, expectedSpec []byte) {
	gvr := schema.GroupVersionResource{
		Group:    "numaplane.numaproj.io",
		Version:  "v1alpha1",
		Resource: "pipelinerollouts",
	}

	gomega.Eventually(func() bool {
		obj, err := e.client.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		spec, found, err := unstructured.NestedFieldCopy(obj.Object, "spec", "pipeline", "raw")
		if !found || err != nil {
			return false
		}
		return string(spec.([]byte)) == string(expectedSpec)
	}, time.Second*10, time.Millisecond*250).Should(gomega.BeTrue())
}
