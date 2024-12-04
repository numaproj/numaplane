/*
Copyright 2023 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/controller/isbservicerollout"
	"github.com/numaproj/numaplane/internal/controller/monovertexrollout"
	"github.com/numaproj/numaplane/internal/controller/numaflowcontroller"
	"github.com/numaproj/numaplane/internal/controller/numaflowcontrollerrollout"
	"github.com/numaproj/numaplane/internal/controller/pipelinerollout"
	"github.com/numaproj/numaplane/internal/sync"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

var (
	cfg             *rest.Config
	testEnv         *envtest.Environment
	externalCRDsDir string
)

func TestControllers(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Controllers Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	// Download Numaflow CRDs
	crdsURLs := []string{
		"https://raw.githubusercontent.com/numaproj/numaflow/main/config/base/crds/minimal/numaflow.numaproj.io_interstepbufferservices.yaml",
		"https://raw.githubusercontent.com/numaproj/numaflow/main/config/base/crds/minimal/numaflow.numaproj.io_pipelines.yaml",
		"https://raw.githubusercontent.com/numaproj/numaflow/main/config/base/crds/minimal/numaflow.numaproj.io_vertices.yaml",
		"https://raw.githubusercontent.com/numaproj/numaflow/main/config/base/crds/minimal/numaflow.numaproj.io_monovertices.yaml",
	}
	externalCRDsDir = filepath.Join("..", "..", "config", "crd", "external")
	for _, crdURL := range crdsURLs {
		downloadCRD(crdURL, externalCRDsDir)
	}

	By("bootstrapping test environment")
	// TODO: IDEA: could set useExistingCluster via an env var to be able to reuse some tests cases in e2e tests
	// by setting this variable in CI for unit tests and e2e tests appropriately.
	useExistingCluster := false
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases"), externalCRDsDir},
		ErrorIfCRDPathMissing: true,

		// The BinaryAssetsDirectory is only required if you want to run the tests directly
		// without call the makefile target test. If not informed it will look for the
		// default path defined in controller-runtime which is /usr/local/kubebuilder/.
		// Note that you must have the required binaries setup under the bin directory to perform
		// the tests directly. When we run make test it will be setup and used automatically.
		BinaryAssetsDirectory: filepath.Join("..", "..", "bin", "k8s",
			fmt.Sprintf("1.28.0-%s-%s", runtime.GOOS, runtime.GOARCH)),

		// NOTE: it's necessary to run on existing cluster to allow for deletion of child resources.
		// See https://book.kubebuilder.io/reference/envtest#testing-considerations for more details.
		UseExistingCluster: &useExistingCluster,
	}

	numaLogger := logger.New().WithName("controller-manager")

	numaLogger.SetLevel(4) // change to 3 for "info" level
	logger.SetBaseLogger(numaLogger)

	var err error
	// cfg is defined in this file globally.
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	err = numaflowv1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	err = apiv1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	//+kubebuilder:scaffold:scheme

	k8sManager, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme.Scheme})
	Expect(err).ToNot(HaveOccurred())

	ctlrcommon.TestK8sClient = k8sManager.GetClient()
	Expect(ctlrcommon.TestK8sClient).ToNot(BeNil())

	// other tests may call this, but it fails if called more than once
	if ctlrcommon.TestCustomMetrics == nil {
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics()
	}

	Expect(kubernetes.SetDynamicClient(k8sManager.GetConfig())).To(Succeed())

	err = pipelinerollout.NewPipelineRolloutReconciler(k8sManager.GetClient(), k8sManager.GetScheme(), ctlrcommon.TestCustomMetrics,
		k8sManager.GetEventRecorderFor(apiv1.RolloutPipelineName)).SetupWithManager(k8sManager)
	Expect(err).ToNot(HaveOccurred())

	err = isbservicerollout.NewISBServiceRolloutReconciler(k8sManager.GetClient(), k8sManager.GetScheme(), ctlrcommon.TestCustomMetrics,
		k8sManager.GetEventRecorderFor(apiv1.RolloutISBSvcName)).SetupWithManager(k8sManager)
	Expect(err).ToNot(HaveOccurred())

	err = monovertexrollout.NewMonoVertexRolloutReconciler(k8sManager.GetClient(), k8sManager.GetScheme(), ctlrcommon.TestCustomMetrics,
		k8sManager.GetEventRecorderFor(apiv1.RolloutMonoVertexName)).SetupWithManager(k8sManager)
	Expect(err).ToNot(HaveOccurred())

	err = numaflowcontroller.NewNumaflowControllerReconciler(k8sManager.GetClient(), k8sManager.GetScheme(), ctlrcommon.TestCustomMetrics,
		k8sManager.GetEventRecorderFor(apiv1.NumaflowControllerName)).SetupWithManager(k8sManager)
	Expect(err).ToNot(HaveOccurred())

	stateCache := sync.NewLiveStateCache(cfg, ctlrcommon.TestCustomMetrics)
	err = stateCache.Init(nil)
	Expect(err).ToNot(HaveOccurred())

	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) { Expect(err).ToNot(HaveOccurred()) })

	Expect(err).ToNot(HaveOccurred())
	definitions, err := ctlrcommon.GetNumaflowControllerDefinitions("../../tests/config/controller-definitions-config.yaml")
	Expect(err).ToNot(HaveOccurred())
	config.GetConfigManagerInstance().GetControllerDefinitionsMgr().UpdateNumaflowControllerDefinitionConfig(*definitions)

	numaflowControllerReconciler, err := numaflowcontrollerrollout.NewNumaflowControllerRolloutReconciler(k8sManager.GetClient(), k8sManager.GetScheme(),
		cfg, kubernetes.NewKubectl(), ctlrcommon.TestCustomMetrics, k8sManager.GetEventRecorderFor(apiv1.RolloutNumaflowControllerName))
	Expect(err).ToNot(HaveOccurred())
	err = numaflowControllerReconciler.SetupWithManager(k8sManager)
	Expect(err).ToNot(HaveOccurred())

	go func() {
		defer GinkgoRecover()
		err = k8sManager.Start(ctrl.SetupSignalHandler())
		Expect(err).ToNot(HaveOccurred(), "failed to run manager")
	}()
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")

	err := os.RemoveAll(externalCRDsDir)
	Expect(err).ToNot(HaveOccurred())

	// This fails the test. See https://github.com/kubernetes-sigs/controller-runtime/issues/1571
	// err = testEnv.Stop()
	// Expect(err).NotTo(HaveOccurred())
})

func downloadCRD(url string, downloadDir string) {
	// Create the download directory
	err := os.MkdirAll(downloadDir, os.ModePerm)
	Expect(err).ToNot(HaveOccurred())

	// Create the file
	fileName := filepath.Base(url)                   // Extract the file name from the URL
	filePath := filepath.Join(downloadDir, fileName) // Construct the local file path
	out, err := os.Create(filePath)                  // Create a new file under filePath
	Expect(err).ToNot(HaveOccurred())
	defer out.Close()

	// Download the file
	resp, err := http.Get(url)
	Expect(err).ToNot(HaveOccurred())
	defer resp.Body.Close()

	// Write the response body to file
	_, err = io.Copy(out, resp.Body)
	Expect(err).ToNot(HaveOccurred())
}
