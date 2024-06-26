package e2e

import (
	"testing"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Given struct {
	t         *testing.T
	k8sClient client.Client
}

func NewGiven(t *testing.T, k8sClient client.Client) *Given {
	return &Given{t: t, k8sClient: k8sClient}
}

func (g *Given) APipelineRollout(name, namespace string) *apiv1.PipelineRollout {
	return &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: runtime.RawExtension{Raw: []byte(`{"interStepBufferServiceName":"test-isbsvc"}`)},
		},
	}
}

func (g *Given) AnUpdatedPipelineRollout(original *apiv1.PipelineRollout) *apiv1.PipelineRollout {
	updated := original.DeepCopy()
	updated.Spec.Pipeline.Raw = []byte(`{"interStepBufferServiceName":"updated-isbsvc"}`)
	return updated
}
