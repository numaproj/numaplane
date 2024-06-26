package e2e

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/numaproj/numaplane/internal/controller"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const (
	timeout  = time.Second * 10
	interval = time.Millisecond * 250
)

type E2ESuite struct {
	suite.Suite
	env       *envtest.Environment
	k8sClient client.Client
	ctx       context.Context
	cancel    context.CancelFunc
}

func (s *E2ESuite) SetupSuite() {
	fmt.Println("Starting SetupSuite")
	s.ctx, s.cancel = context.WithCancel(context.Background())

	scheme := runtime.NewScheme()
	s.Require().NoError(clientgoscheme.AddToScheme(scheme))
	s.Require().NoError(apiv1.AddToScheme(scheme))

	s.env = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}

	fmt.Println("Starting envtest env")
	cfg, err := s.env.Start()
	s.Require().NoError(err)

	s.k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	s.Require().NoError(err)

	// Start the manager
	fmt.Println("Creating manager")
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme})
	s.Require().NoError(err)

	// Add your reconcilers here
	fmt.Println("Setting up reconciler")
	reconciler := &controller.PipelineRolloutReconciler{}
	err = reconciler.SetupWithManager(mgr)
	s.Require().NoError(err)

	fmt.Println("Starting manager")
	go func() {
		fmt.Println("Manager goroutine started")
		s.Require().NoError(mgr.Start(s.ctx))
		fmt.Println("Manager goroutine ended")
	}()

	fmt.Println("Waiting for manager cache")
	s.Require().Eventually(func() bool {
		synced := mgr.GetCache().WaitForCacheSync(s.ctx)
		fmt.Printf("Cache sync status: %v\n", synced)
		return synced
	}, time.Minute, time.Second)
	fmt.Println("SetupSuite completed")
}

func (s *E2ESuite) TearDownSuite() {
	s.cancel()
	s.Require().NoError(s.env.Stop())
}

func (s *E2ESuite) SetupTest() {
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "numaplane-system"},
	}
	s.Require().NoError(s.k8sClient.Create(s.ctx, ns))
}

func (s *E2ESuite) TearDownTest() {
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "numaplane-system"},
	}
	s.Require().NoError(s.k8sClient.Delete(s.ctx, ns))
}

func (s *E2ESuite) TestCreatePipelineRollout() {
	given := NewGiven(s.T(), s.k8sClient)
	when := NewWhen(s.T(), s.k8sClient)
	expect := NewExpect(s.T(), s.k8sClient)

	pipelineRollout := given.APipelineRollout("test-pipeline", "numaplane-system")
	when.PipelineRolloutIsCreated(pipelineRollout)
	expect.AssertPipelineRolloutIsPresent("numaplane-system", "test-pipeline")
}

func (s *E2ESuite) TestUpdatePipelineRollout() {
	given := NewGiven(s.T(), s.k8sClient)
	when := NewWhen(s.T(), s.k8sClient)
	expect := NewExpect(s.T(), s.k8sClient)

	pipelineRollout := given.APipelineRollout("test-pipeline", "numaplane-system")
	when.PipelineRolloutIsCreated(pipelineRollout)
	expect.AssertPipelineRolloutIsPresent("numaplane-system", "test-pipeline")

	updatedPipelineRollout := given.AnUpdatedPipelineRollout(pipelineRollout)
	when.PipelineRolloutIsUpdated(updatedPipelineRollout)
	expect.AssertPipelineRolloutIsUpdated("numaplane-system", "test-pipeline", updatedPipelineRollout)
}

func (s *E2ESuite) TestDeletePipelineRollout() {
	given := NewGiven(s.T(), s.k8sClient)
	when := NewWhen(s.T(), s.k8sClient)
	expect := NewExpect(s.T(), s.k8sClient)

	pipelineRollout := given.APipelineRollout("test-pipeline", "numaplane-system")
	when.PipelineRolloutIsCreated(pipelineRollout)
	expect.AssertPipelineRolloutIsPresent("numaplane-system", "test-pipeline")

	when.PipelineRolloutIsDeleted(pipelineRollout)
	expect.AssertPipelineRolloutIsAbsent("numaplane-system", "test-pipeline")
}

func TestE2E(t *testing.T) {
	suite.Run(t, new(E2ESuite))
}
