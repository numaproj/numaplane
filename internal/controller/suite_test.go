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
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/sync"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

const (
	timeout  = 15 * time.Second
	duration = 10 * time.Second
	interval = 250 * time.Millisecond
)

var (
	cfg             *rest.Config
	k8sClient       client.Client
	testEnv         *envtest.Environment
	externalCRDsDir string
	customMetrics   *metrics.CustomMetrics
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

	k8sClient = k8sManager.GetClient()
	Expect(k8sClient).ToNot(BeNil())

	customMetrics = metrics.RegisterCustomMetrics()

	err = NewPipelineRolloutReconciler(
		k8sManager.GetClient(),
		k8sManager.GetScheme(),
		cfg, customMetrics).SetupWithManager(k8sManager)
	Expect(err).ToNot(HaveOccurred())

	err = NewISBServiceRolloutReconciler(
		k8sManager.GetClient(),
		k8sManager.GetScheme(),
		cfg, customMetrics).SetupWithManager(k8sManager)
	Expect(err).ToNot(HaveOccurred())

	stateCache := sync.NewLiveStateCache(cfg)
	err = stateCache.Init(nil)
	Expect(err).ToNot(HaveOccurred())

	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {
		Expect(err).ToNot(HaveOccurred())
	})
	Expect(err).ToNot(HaveOccurred())
	config.GetConfigManagerInstance().GetControllerDefinitionsMgr().UpdateControllerDefinitionConfig(getNumaflowControllerDefinitions())

	err = (&NumaflowControllerRolloutReconciler{
		client:        k8sManager.GetClient(),
		scheme:        k8sManager.GetScheme(),
		restConfig:    cfg,
		rawConfig:     cfg,
		kubectl:       kubernetes.NewKubectl(),
		stateCache:    stateCache,
		customMetrics: customMetrics,
	}).SetupWithManager(k8sManager)
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

func getNumaflowControllerDefinitions() config.NumaflowControllerDefinitionConfig {
	// Read definitions config file
	configData, err := os.ReadFile("../../tests/config/controller-definitions-config.yaml")
	Expect(err).ToNot(HaveOccurred())
	var controllerConfig config.NumaflowControllerDefinitionConfig
	err = yaml.Unmarshal(configData, &controllerConfig)
	Expect(err).ToNot(HaveOccurred())

	return controllerConfig
}

// verifyAutoHealing tests the auto healing feature
func verifyAutoHealing(ctx context.Context, gvk schema.GroupVersionKind, namespace string, resourceName string, pathToValue string, newValue any) {
	lookupKey := types.NamespacedName{Name: resourceName, Namespace: namespace}

	// Get current resource
	currentResource := unstructured.Unstructured{}
	currentResource.SetGroupVersionKind(gvk)
	Eventually(func() error {
		return k8sClient.Get(ctx, lookupKey, &currentResource)
	}, timeout, interval).Should(Succeed())
	Expect(currentResource.Object).ToNot(BeEmpty())

	// Get the original value at the specified path (pathToValue)
	pathSlice := strings.Split(pathToValue, ".")
	originalValue, found, err := unstructured.NestedFieldNoCopy(currentResource.Object, pathSlice...)
	Expect(err).ToNot(HaveOccurred())
	Expect(found).To(BeTrue())

	// Set new value and update resource
	err = unstructured.SetNestedField(currentResource.Object, newValue, pathSlice...)
	Expect(err).ToNot(HaveOccurred())
	Expect(k8sClient.Update(ctx, &currentResource)).ToNot(HaveOccurred())

	// Get updated resource and the value at the specified path (pathToValue)
	e := Eventually(func() (any, error) {
		updatedResource := unstructured.Unstructured{}
		updatedResource.SetGroupVersionKind(gvk)
		if err := k8sClient.Get(ctx, lookupKey, &updatedResource); err != nil {
			return nil, err
		}

		currentValue, found, err := unstructured.NestedFieldNoCopy(updatedResource.Object, pathSlice...)
		Expect(err).ToNot(HaveOccurred())
		Expect(found).To(BeTrue())

		return currentValue, nil
	}, timeout, interval)

	// Verify that the value matches the original value and not the new value
	e.Should(Equal(originalValue))
	e.ShouldNot(Equal(newValue))
}

func verifyStatusPhase(ctx context.Context, gvk schema.GroupVersionKind, namespace string, resourceName string, desiredPhase apiv1.Phase) {
	lookupKey := types.NamespacedName{Name: resourceName, Namespace: namespace}

	currentResource := unstructured.Unstructured{}
	currentResource.SetGroupVersionKind(gvk)
	Consistently(func() (bool, error) {
		err := k8sClient.Get(ctx, lookupKey, &currentResource)
		if err != nil {
			return false, err
		}

		phase, found, err := unstructured.NestedString(currentResource.Object, "status", "phase")
		if err != nil {
			return false, err
		}
		if !found {
			return false, nil
		}

		observedGeneration, found, err := unstructured.NestedInt64(currentResource.Object, "status", "observedGeneration")
		if err != nil {
			return false, err
		}
		if !found {
			return false, nil
		}

		generation, found, err := unstructured.NestedInt64(currentResource.Object, "metadata", "generation")
		if err != nil {
			return false, err
		}
		if !found {
			return false, nil
		}

		return apiv1.Phase(phase) == desiredPhase && observedGeneration == generation, nil
	}, duration, interval).Should(BeTrue())
}
