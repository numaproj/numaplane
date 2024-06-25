package e2e

import (
	"context"
	"testing"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Expect struct{ t *testing.T }

func NewExpect(t *testing.T) *Expect {
	return &Expect{t: t}
}

func (t *Expect) AssertPipelineRolloutIsPresent(ns, name string) *Expect {
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

func (t *Expect) AssertNumaflowControllerRolloutIsPresent(ns, name string) *Expect {
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

func (t *Expect) AssertISBServiceRolloutIsPresent(ns, name string) *Expect {
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
