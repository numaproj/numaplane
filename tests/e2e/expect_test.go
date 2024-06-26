package e2e

import (
	"context"
	"time"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ExpectHelper struct {
	client client.Client
}

func NewExpectHelper(client client.Client) *ExpectHelper {
	return &ExpectHelper{client: client}
}

func (e *ExpectHelper) PipelineRolloutToExist(ctx context.Context, namespace, name string) {
	gomega.Eventually(func() bool {
		pipelineRollout := &apiv1.PipelineRollout{}
		err := e.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, pipelineRollout)
		return err == nil
	}, time.Second*10, time.Millisecond*250).Should(gomega.BeTrue())
}

func (e *ExpectHelper) PipelineRolloutToBeDeleted(ctx context.Context, namespace, name string) {
	gomega.Eventually(func() bool {
		pipelineRollout := &apiv1.PipelineRollout{}
		err := e.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, pipelineRollout)
		return err != nil
	}, time.Second*10, time.Millisecond*250).Should(gomega.BeTrue())
}

func (e *ExpectHelper) PipelineRolloutToBeUpdated(ctx context.Context, namespace, name string, expectedSpec []byte) {
	gomega.Eventually(func() bool {
		pipelineRollout := &apiv1.PipelineRollout{}
		err := e.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, pipelineRollout)
		if err != nil {
			return false
		}
		return string(pipelineRollout.Spec.Pipeline.Raw) == string(expectedSpec)
	}, time.Second*10, time.Millisecond*250).Should(gomega.BeTrue())
}
