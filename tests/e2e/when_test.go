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

func (w *When) PipelineRolloutIsCreated(pipelineRollout *apiv1.PipelineRollout) {
	err := w.k8sClient.Create(context.TODO(), pipelineRollout)
	if err != nil {
		w.t.Fatal(err)
	}
}

func (w *When) PipelineRolloutIsUpdated(pipelineRollout *apiv1.PipelineRollout) {
	err := w.k8sClient.Update(context.TODO(), pipelineRollout)
	if err != nil {
		w.t.Fatal(err)
	}
}

func (w *When) PipelineRolloutIsDeleted(pipelineRollout *apiv1.PipelineRollout) {
	err := w.k8sClient.Delete(context.TODO(), pipelineRollout)
	if err != nil {
		w.t.Fatal(err)
	}
}
