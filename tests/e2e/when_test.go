package e2e

import (
	"context"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type WhenHelper struct {
	client client.Client
}

func NewWhenHelper(client client.Client) *WhenHelper {
	return &WhenHelper{client: client}
}

func (w *WhenHelper) CreatePipelineRollout(ctx context.Context, pipelineRollout *apiv1.PipelineRollout) error {
	return w.client.Create(ctx, pipelineRollout)
}

func (w *WhenHelper) UpdatePipelineRollout(ctx context.Context, pipelineRollout *apiv1.PipelineRollout) error {
	return w.client.Update(ctx, pipelineRollout)
}

func (w *WhenHelper) DeletePipelineRollout(ctx context.Context, pipelineRollout *apiv1.PipelineRollout) error {
	return w.client.Delete(ctx, pipelineRollout)
}
