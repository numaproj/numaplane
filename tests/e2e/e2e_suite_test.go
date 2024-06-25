package e2e

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const (
	/* resource names */
	E2ELabel       = "numaplane-e2e"
	E2ELabelValue  = "true"
	defaultTimeout = 60 * time.Second
)

type E2ESuite struct {
	suite.Suite
	env       *envtest.Environment
	k8sClient client.Client
}

func (s *E2ESuite) SetupSuite() {
	var err error

	scheme := runtime.NewScheme()
	err = clientgoscheme.AddToScheme(scheme)
	if err != nil {
		s.T().Fatal(err)
	}

	err = apiv1.AddToScheme(scheme) // Register PipelineRollout to scheme
	if err != nil {
		s.T().Fatal(err)
	}

	s.env = &envtest.Environment{
		// These paths contain the YAML manifests of the CRDs
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "config", "crd", "bases"), // assuming this is where the CRDs for  types reside
			// externalCRDsDir,  // if there are more external CRDs needed
		},
		ErrorIfCRDPathMissing: true, // cause test to error if CRDs are not found
	}

	cfg, err := s.env.Start()
	if err != nil {
		s.T().Fatal(err)
	}

	s.k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		s.T().Fatal(err)
	}
}

func (s *E2ESuite) TearDownSuite() {
	if err := s.env.Stop(); err != nil {
		s.T().Fatal(err)
	}
}

func (s *E2ESuite) BeforeTest(suiteName, testName string) {
	// Create a new namespace with a unique name for each test
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "numaplane-system",
		},
	}
	if err := s.k8sClient.Create(context.Background(), ns); err != nil {
		s.T().Fatal(err, "Unable to create namespace", ns.Name)
	}
}

func (s *E2ESuite) AfterTest(suiteName, testName string) {
	nsSpec := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "numaplane-system",
		},
	}
	err := s.k8sClient.Delete(context.Background(), nsSpec)
	if err != nil {
		s.T().Fatal(err, "Unable to delete namespace", "numaplane-system")
	}
}

func (s *E2ESuite) CheckError(err error) {
	s.T().Helper()
	if err != nil {
		s.T().Fatal(err)
	}
}

func (s *E2ESuite) Given() *Given {
	return NewGiven(s.T(), s.k8sClient)
}

func (s *E2ESuite) TestCreatePipelineRollout() {
	pipelineSpec := runtime.RawExtension{
		Raw: []byte(`{
            "interStepBufferServiceName": "my-isbsvc",
            "vertices": [
                {
                    "name": "in",
                    "source": {
                        "generator": {
                            "RPU": 5,
                            "Duration": "1s"
                        }
                    }
                },
                {
                    "name": "cat",
                    "UDF": {
                        "builtin": {
                            "name": "cat"
                        }
                    }
                },
                {
                    "name": "out",
                    "sink": {
                        "log": {}
                    }
                }
            ],
            "edges": [
                {
                    "from": "in",
                    "to": "cat"
                },
                {
                    "from": "cat",
                    "to": "out"
                }
            ]
        }`),
	}

	pipelineRollout := &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-pipeline",
			Namespace: "numaplane-system",
		},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: pipelineSpec,
		},
	}

	if err := s.k8sClient.Create(context.Background(), pipelineRollout); err != nil {
		s.T().Fatal(err)
	}

	Expect := NewExpect(s.T(), s.k8sClient)
	Expect.AssertPipelineRolloutIsPresent("numaplane-system", "my-pipeline")
}

func TestE2E(t *testing.T) {
	suite.Run(t, new(E2ESuite))
}
