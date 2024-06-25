package e2e

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	/* resource names */
	E2ELabel       = "numaplane-e2e"
	E2ELabelValue  = "true"
	defaultTimeout = 60 * time.Second
)

type E2ESuite struct {
	suite.Suite
	restConfig *rest.Config
	kubeClient kubernetes.Interface
	stopch     chan struct{}
}

func (s *E2ESuite) SetupSuite() {
	var err error
	s.stopch = make(chan struct{})

	if os.Getenv("KUBERNETES_SERVICE_HOST") == "" && os.Getenv("KUBERNETES_SERVICE_PORT") == "" {
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			kubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "config")
		}
		s.restConfig, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		s.restConfig, err = rest.InClusterConfig()
	}
	if err != nil {
		s.T().Fatal(err)
	}

	s.kubeClient = kubernetes.NewForConfigOrDie(s.restConfig)
	c, _ := client.New(s.restConfig, client.Options{})
	kubeClient = c
}

// Assuming you might need the following functions for your e2e tests,
// If they're not needed, feel free to remove these.

func (s *E2ESuite) TearDownSuite() {
	// Implement your teardown logic
}

func (s *E2ESuite) BeforeTest(suiteName, testName string) {
	// Implement any logic that needs to be done before every test
}

func (s *E2ESuite) AfterTest(suiteName, testName string) {
	// Implement cleanup logic that needs to be done after every test
}

func (s *E2ESuite) CheckError(err error) {
	s.T().Helper()
	if err != nil {
		s.T().Fatal(err)
	}
}

func (s *E2ESuite) Given() *Given {
	return NewGiven(s.T())
}

func (s *E2ESuite) TestPipelineRollout() {
	Given := s.Given()
	Expect := NewExpect(s.T())

	samplePipelineRollout := "@../../config/samples/numaplane.numaproj.io_v1alpha1_pipelinerollout.yaml"

	Given.PipelineRollout(samplePipelineRollout)

	Expect.AssertPipelineRolloutIsPresent("<numaplane-system>", "my-pipeline") // Replace <ns> and <name> with actual values
}

func TestE2E(t *testing.T) {
	suite.Run(t, new(E2ESuite))
}
