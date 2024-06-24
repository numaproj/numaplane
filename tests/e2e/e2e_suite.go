/*
...
*/

package e2e

import (
	"time"

	"github.com/stretchr/testify/suite"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
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
	s.CheckError(err)

	// Set up the client so it can interact with the API
	kubeConfig := ctrl.GetConfigOrDie()
	// Since client here is a global variable, it's set up only once and reused across tests
	kubeClient, err = client.New(kubeConfig, client.Options{Scheme: scheme})
	s.CheckError(err)
}

// Implement TearDownSuite, BeforeTest, and other related methods assuming
// the setup and cleanup required is different from the previous version

func (s *E2ESuite) CheckError(err error) {
	s.T().Helper()
	if err != nil {
		s.T().Fatal(err)
	}
}

func (s *E2ESuite) Given() *Given {
	return NewGiven(s.T())
}
