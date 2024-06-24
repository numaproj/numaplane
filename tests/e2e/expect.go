package e2e

import (
	"context"
	"testing"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Expected struct{ t *testing.T }

func NewExpected(t *testing.T) *Expected {
	return &Expected{t: t}
}

func (t *Expected) AssertPipelineRolloutIsPresent(ns, name string) *Expected {
	pipelineRollout := &apiv1.PipelineRollout{}
	err := k8sClient.Get(context.TODO(), client.ObjectKey{
		Namespace: ns,
		Name:      name,
	}, pipelineRollout)

	if err != nil {
		t.t.Fatal(err)
	}
	return t
}

func (t *Expected) AssertNumaflowControllerRolloutIsPresent(ns, name string) *Expected {
	numaflowControllerRollout := &apiv1.NumaflowControllerRollout{}
	err := k8sClient.Get(context.TODO(), client.ObjectKey{
		Namespace: ns,
		Name:      name,
	}, numaflowControllerRollout)

	if err != nil {
		t.t.Fatal(err)
	}
	return t
}

func (t *Expected) AssertISBServiceRolloutIsPresent(ns, name string) *Expected {
	iSBServiceRollout := &apiv1.ISBServiceRollout{}
	err := k8sClient.Get(context.TODO(), client.ObjectKey{
		Namespace: ns,
		Name:      name,
	}, iSBServiceRollout)

	if err != nil {
		t.t.Fatal(err)
	}
	return t
}
