package e2e

import (
	"context"
	"testing"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"

	// clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	dynamicClient dynamic.Interface
	testEnv       *envtest.Environment
	ctx           context.Context
	cancel        context.CancelFunc
	// externalCRDsDir string
)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	RunSpecs(t, "E2E Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("bootstrapping test environment")
	ctx, cancel = context.WithTimeout(context.Background(), time.Minute*5) //WithCancel(context.TODO())

	var err error
	scheme := runtime.NewScheme()
	// err = clientgoscheme.AddToScheme(scheme)
	// Expect(err).NotTo(HaveOccurred())
	err = apiv1.AddToScheme(scheme)
	Expect(err).NotTo(HaveOccurred())
	err = numaflowv1.AddToScheme(scheme)
	Expect(err).NotTo(HaveOccurred())
	useExistingCluster := true

	testEnv = &envtest.Environment{
		UseExistingCluster: &useExistingCluster,
	}

	cfg, err := testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	// k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	dynamicClient, err = dynamic.NewForConfig(cfg)

	Expect(err).NotTo(HaveOccurred())
	Expect(dynamicClient).NotTo(BeNil())

})
