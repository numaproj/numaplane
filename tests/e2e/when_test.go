package e2e

import (
	"context"
	"testing"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type When struct {
	t         *testing.T
	k8sClient client.Client
}

func NewWhen(t *testing.T, k8sClient client.Client) *When {
	return &When{t: t, k8sClient: k8sClient}
}

func (w *When) PipelineRolloutIsCreated() *When {
	pipelineRollout := &apiv1.PipelineRollout{
		// add required spec status and metadata for creating a PipelineRollout
		// Assuming Namespace and Name are set here
	}

	err := w.k8sClient.Create(context.TODO(), pipelineRollout)
	if err != nil {
		w.t.Fatal(err)
	}

	return w
}

func (w *When) NumaflowControllerRolloutIsCreated() *When {
	numaflowControllerRollout := &apiv1.NumaflowControllerRollout{
		// add the required spec status and metadata for creating a NumaflowControllerRollout
		// Assuming Namespace and Name are set here
	}

	err := w.k8sClient.Create(context.TODO(), numaflowControllerRollout)
	if err != nil {
		w.t.Fatal(err)
	}

	return w
}

func (w *When) ISBServiceRolloutIsCreated() *When {
	iSBServiceRollout := &apiv1.ISBServiceRollout{
		//add the required spec status and metadata for creating an ISBServiceRollout
		// Assuming Namespace and Name are set here
	}

	err := w.k8sClient.Create(context.TODO(), iSBServiceRollout)
	if err != nil {
		w.t.Fatal(err)
	}

	return w
}
