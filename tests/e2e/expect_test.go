package e2e

import (
	"context"
	"testing"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Expect struct {
	t         *testing.T
	k8sClient client.Client
}

func NewExpect(t *testing.T, k8sClient client.Client) *Expect {
	return &Expect{t: t, k8sClient: k8sClient}
}

func (e *Expect) AssertPipelineRolloutIsPresent(namespace, name string) {
	e.t.Helper()
	require.Eventually(e.t, func() bool {
		pipelineRollout := &apiv1.PipelineRollout{}
		err := e.k8sClient.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, pipelineRollout)
		return err == nil
	}, timeout, interval, "PipelineRollout should be present")
}

func (e *Expect) AssertPipelineRolloutIsUpdated(namespace, name string, expected *apiv1.PipelineRollout) {
	e.t.Helper()
	require.Eventually(e.t, func() bool {
		actual := &apiv1.PipelineRollout{}
		err := e.k8sClient.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, actual)
		if err != nil {
			return false
		}
		return string(actual.Spec.Pipeline.Raw) == string(expected.Spec.Pipeline.Raw)
	}, timeout, interval, "PipelineRollout should be updated")
}

func (e *Expect) AssertPipelineRolloutIsAbsent(namespace, name string) {
	e.t.Helper()
	require.Eventually(e.t, func() bool {
		pipelineRollout := &apiv1.PipelineRollout{}
		err := e.k8sClient.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, pipelineRollout)
		return apierrors.IsNotFound(err)
	}, timeout, interval, "PipelineRollout should be absent")
}
