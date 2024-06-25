package e2e

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Expect struct {
	t         *testing.T
	k8sClient client.Client
}

func NewExpect(t *testing.T, k8sClient client.Client) *Expect {
	return &Expect{t: t, k8sClient: k8sClient}
}

func (t *Expect) AssertPipelineRolloutIsPresent(ns, name string) *Expect {
	pipelineRollout := &apiv1.PipelineRollout{}
	err := t.k8sClient.Get(context.TODO(), client.ObjectKey{
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
	err := t.k8sClient.Get(context.TODO(), client.ObjectKey{
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
	err := t.k8sClient.Get(context.TODO(), client.ObjectKey{
		Namespace: ns,
		Name:      name,
	}, iSBServiceRollout)

	if err != nil {
		t.t.Fatal(err)
	}
	return t
}

// AssertPipelineRolloutIsUpdated fetches the PipelineRollout and asserts it's updated correctly.
func (e *Expect) AssertPipelineRolloutIsUpdated(namespace, name string, pipelineRollout *apiv1.PipelineRollout) {
	e.t.Helper()

	// Fetch the PipelineRollout
	fetched := &apiv1.PipelineRollout{}
	err := e.k8sClient.Get(context.TODO(), client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, fetched)
	if err != nil {
		e.t.Fatal(err)
	}

	// Assert that interStepBufferServiceName has been changed to "my-isbsvc-updated"
	expectedPipelineSpecJSON := `{"interStepBufferServiceName":"my-isbsvc-updated","vertices":[{"name":"in","source":{"generator":{"Duration":"1s","RPU":5}}}]}`

	if !bytes.Equal(fetched.Spec.Pipeline.Raw, []byte(expectedPipelineSpecJSON)) {
		e.t.Fatal(fmt.Errorf("fetched PipelineRollout spec was %v, expected %v", string(fetched.Spec.Pipeline.Raw), expectedPipelineSpecJSON))
	}
}
