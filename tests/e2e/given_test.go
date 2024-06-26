package e2e

import (
	"encoding/json"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type GivenHelper struct {
	client client.Client
}

func NewGivenHelper(client client.Client) *GivenHelper {
	return &GivenHelper{client: client}
}

func (g *GivenHelper) APipelineRollout(name, namespace string) *apiv1.PipelineRollout {
	pipelineSpec := numaflowv1.PipelineSpec{
		InterStepBufferServiceName: "test-isbsvc",
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: "in",
				Source: &numaflowv1.Source{
					Generator: &numaflowv1.GeneratorSource{
						RPU:      pointer.Int64(5),
						Duration: &metav1.Duration{Duration: time.Second},
					},
				},
			},
			{
				Name: "out",
				Sink: &numaflowv1.Sink{
					AbstractSink: numaflowv1.AbstractSink{
						Log: &numaflowv1.Log{},
					},
				},
			},
		},
		Edges: []numaflowv1.Edge{
			{
				From: "in",
				To:   "out",
			},
		},
	}

	pipelineSpecRaw, _ := json.Marshal(pipelineSpec)

	return &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: runtime.RawExtension{
				Raw: pipelineSpecRaw,
			},
		},
	}
}
