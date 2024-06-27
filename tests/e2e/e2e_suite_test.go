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
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	k8sClient client.Client
	testEnv   *envtest.Environment
	ctx       context.Context
	cancel    context.CancelFunc
	// externalCRDsDir string
)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Suite")
}

var _ = BeforeEach(func() {
	err := k8sClient.DeleteAllOf(ctx, &apiv1.PipelineRollout{}, client.InNamespace("numaplane-system"))
	Expect(err).NotTo(HaveOccurred())
})

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("bootstrapping test environment")
	ctx, cancel = context.WithTimeout(context.Background(), time.Minute*5) //WithCancel(context.TODO())

	var err error
	scheme := runtime.NewScheme()
	err = clientgoscheme.AddToScheme(scheme)
	Expect(err).NotTo(HaveOccurred())
	err = apiv1.AddToScheme(scheme)
	Expect(err).NotTo(HaveOccurred())
	err = numaflowv1.AddToScheme(scheme)
	Expect(err).NotTo(HaveOccurred())
	// // Download Numaflow CRDs
	// crdsURLs := []string{
	// 	"https://raw.githubusercontent.com/numaproj/numaflow/main/config/base/crds/minimal/numaflow.numaproj.io_interstepbufferservices.yaml",
	// 	"https://raw.githubusercontent.com/numaproj/numaflow/main/config/base/crds/minimal/numaflow.numaproj.io_pipelines.yaml",
	// 	"https://raw.githubusercontent.com/numaproj/numaflow/main/config/base/crds/minimal/numaflow.numaproj.io_vertices.yaml",
	// }
	// externalCRDsDir = filepath.Join("..", "..", "config", "crd", "external")
	// for _, crdURL := range crdsURLs {
	// 	downloadCRD(crdURL, externalCRDsDir)
	// }
	useExistingCluster := true

	testEnv = &envtest.Environment{
		// CRDDirectoryPaths: []string{
		// 	filepath.Join("..", "..", "config", "crd", "bases"),
		// 	// externalCRDsDir,
		// },
		// ErrorIfCRDPathMissing: true,
		UseExistingCluster: &useExistingCluster,
	}

	cfg, err := testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	// // Find a free port
	// listener, err := net.Listen("tcp", ":0")
	// Expect(err).NotTo(HaveOccurred())
	// port := listener.Addr().(*net.TCPAddr).Port
	// Expect(listener.Close()).To(Succeed())

	// mgr, err := ctrl.NewManager(cfg, ctrl.Options{
	// 	Scheme: scheme,
	// 	Metrics: server.Options{
	// 		BindAddress: fmt.Sprintf(":%d", port),
	// 	},
	// })
	// Expect(err).NotTo(HaveOccurred())

	// reconciler := controller.NewPipelineRolloutReconciler(
	// 	mgr.GetClient(),
	// 	mgr.GetScheme(),
	// 	cfg,
	// )
	// err = reconciler.SetupWithManager(mgr)
	// Expect(err).NotTo(HaveOccurred())

	// go func() {
	// 	err = mgr.Start(ctx)
	// 	Expect(err).NotTo(HaveOccurred())
	// }()

	// // Wait for the cache to be synced
	// Expect(mgr.GetCache().WaitForCacheSync(ctx)).To(BeTrue())
})

// var _ = AfterSuite(func() {
// 	cancel()
// 	By("tearing down the test environment")
// 	err := testEnv.Stop()
// 	Expect(err).NotTo(HaveOccurred())

// 	err = os.RemoveAll(externalCRDsDir)
// 	Expect(err).NotTo(HaveOccurred())
// })

// func downloadCRD(url string, downloadDir string) {
// 	// Create the download directory
// 	err := os.MkdirAll(downloadDir, os.ModePerm)
// 	Expect(err).ToNot(HaveOccurred())

// 	// Create the file
// 	fileName := filepath.Base(url)
// 	filePath := filepath.Join(downloadDir, fileName)
// 	out, err := os.Create(filePath)
// 	Expect(err).ToNot(HaveOccurred())
// 	defer out.Close()

// 	// Download the file
// 	resp, err := http.Get(url)
// 	Expect(err).ToNot(HaveOccurred())
// 	defer resp.Body.Close()

// 	// Write the response body to file
// 	_, err = io.Copy(out, resp.Body)
// 	Expect(err).ToNot(HaveOccurred())
// }
