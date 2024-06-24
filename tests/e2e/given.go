package e2e

import (
	"context"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/runtime"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var k8sClient client.Client // global variable declaration

var (
	kubeClient client.Client
	scheme     = runtime.NewScheme()
)

type Given struct {
	t *testing.T
}

func NewGiven(t *testing.T) *Given {
	// Setup kubeClient if not set
	if kubeClient == nil {
		kubeConfig := ctrl.GetConfigOrDie()
		var err error
		kubeClient, err = client.New(kubeConfig, client.Options{Scheme: scheme})
		if err != nil {
			t.Fatal(err, "Unable to create kubeClient")
		}
	}

	return &Given{t: t}
}

func (g *Given) PipelineRollout(text string) *Given {
	g.t.Helper()
	pipelineRollout := &apiv1.PipelineRollout{}
	g.readResource(text, pipelineRollout)
	if err := kubeClient.Create(context.Background(), pipelineRollout); err != nil {
		g.t.Fatal(err, "Unable to create test PipelineRollout")
	}
	return g
}

func (g *Given) NumaflowControllerRollout(text string) *Given {
	g.t.Helper()
	numaflowControllerRollout := &apiv1.NumaflowControllerRollout{}
	g.readResource(text, numaflowControllerRollout)
	if err := kubeClient.Create(context.Background(), numaflowControllerRollout); err != nil {
		g.t.Fatal(err, "Unable to create test NumaflowControllerRollout")
	}
	return g
}

func (g *Given) ISBServiceRollout(text string) *Given {
	g.t.Helper()
	iSBServiceRollout := &apiv1.ISBServiceRollout{}
	g.readResource(text, iSBServiceRollout)
	if err := kubeClient.Create(context.Background(), iSBServiceRollout); err != nil {
		g.t.Fatal(err, "Unable to create test ISBServiceRollout")
	}
	return g
}

// helper func to read and unmarshal GitSync YAML into object
func (g *Given) readResource(text string, v metav1.Object) {
	g.t.Helper()
	var data string
	if !strings.HasPrefix(text, "@") {
		data = text
	} else {
		file := strings.TrimPrefix(text, "@")
		f, err := os.ReadFile(file)
		if err != nil {
			g.t.Fatal(err)
		}
		data = string(f)
	}
	err := yaml.Unmarshal([]byte(data), v)
	if err != nil {
		g.t.Fatal(err)
	}
}
