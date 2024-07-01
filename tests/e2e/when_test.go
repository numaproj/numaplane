package e2e

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

type WhenHelper struct {
	client dynamic.Interface
}

func NewWhenHelper(client dynamic.Interface) *WhenHelper {
	return &WhenHelper{client: client}
}

func (w *WhenHelper) CreatePipelineRollout(ctx context.Context, pipelineRollout *unstructured.Unstructured) error {
	gvr := schema.GroupVersionResource{
		Group:    "numaplane.numaproj.io",
		Version:  "v1alpha1",
		Resource: "pipelinerollouts",
	}

	// unstructuredObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pipelineRollout)
	// if err != nil {
	// 	return err
	// }

	_, err := w.client.Resource(gvr).Namespace(pipelineRollout.GetNamespace()).Create(ctx, pipelineRollout, metav1.CreateOptions{})
	return err
}

func (w *WhenHelper) UpdatePipelineRollout(ctx context.Context, pipelineRollout *unstructured.Unstructured) error {
	gvr := schema.GroupVersionResource{
		Group:    "numaplane.numaproj.io",
		Version:  "v1alpha1",
		Resource: "pipelinerollouts",
	}

	_, err := w.client.Resource(gvr).Namespace(pipelineRollout.GetNamespace()).Update(ctx, pipelineRollout, metav1.UpdateOptions{})
	return err
}

func (w *WhenHelper) DeletePipelineRollout(ctx context.Context, namespace, name string) error {
	gvr := schema.GroupVersionResource{
		Group:    "numaplane.numaproj.io",
		Version:  "v1alpha1",
		Resource: "pipelinerollouts",
	}

	return w.client.Resource(gvr).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})

}
