package controller

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	k8sclientgo "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	numaflowversioned "github.com/numaproj/numaflow/pkg/client/clientset/versioned"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

var (
	defaultNamespace = "default"
)

func prepareK8SEnvironment() (restConfig *rest.Config, numaflowClientSet *numaflowversioned.Clientset, numaplaneClient client.Client, k8sClientSet *k8sclientgo.Clientset, err error) {
	// download Numaflow CRDs to a temporary location

	path, _ := os.Getwd()
	// TODO: make this work even if your current directory has multiple directories in the path named 'numaplane'
	numaplaneRootDirectoryIndex := strings.Index(path, "/numaplane/")
	if numaplaneRootDirectoryIndex < 0 {
		panic("current directory %q doesn't include /numaplane/")
	}
	pathUpToRoot := path[0:numaplaneRootDirectoryIndex]
	crdDirectory := pathUpToRoot + "/numaplane/config/crd"

	crdsURLs := []string{
		"https://raw.githubusercontent.com/numaproj/numaflow/main/config/base/crds/minimal/numaflow.numaproj.io_interstepbufferservices.yaml",
		"https://raw.githubusercontent.com/numaproj/numaflow/main/config/base/crds/minimal/numaflow.numaproj.io_pipelines.yaml",
		"https://raw.githubusercontent.com/numaproj/numaflow/main/config/base/crds/minimal/numaflow.numaproj.io_vertices.yaml",
		"https://raw.githubusercontent.com/numaproj/numaflow/main/config/base/crds/full/numaflow.numaproj.io_monovertices.yaml",
	}
	externalCRDsDir := crdDirectory + "external"
	for _, crdURL := range crdsURLs {
		downloadCRDToPath(crdURL, externalCRDsDir)
	}

	useExistingCluster := false
	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{crdDirectory + "/bases", externalCRDsDir},
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

	// create REST Config
	restConfig, err = testEnv.Start()
	if err != nil {
		return
	}

	// create clients for Numaflow, Numaplane, Kubernetes

	err = numaflowv1.AddToScheme(scheme.Scheme)
	if err != nil {
		return
	}
	err = apiv1.AddToScheme(scheme.Scheme)
	if err != nil {
		return
	}

	numaflowClientSet = numaflowversioned.NewForConfigOrDie(restConfig)
	//numaplaneClientSet := numaplaneversioned.NewForConfigOrDie(restConfig)
	numaplaneClient, err = client.New(restConfig, client.Options{})
	if err != nil {
		return
	}
	k8sClientSet, err = k8sclientgo.NewForConfig(restConfig)
	return
}

func downloadCRDToPath(url string, downloadDir string) error {
	// Create the download directory
	err := os.MkdirAll(downloadDir, os.ModePerm)
	if err != nil {
		return err
	}

	// Create the file
	fileName := filepath.Base(url)                   // Extract the file name from the URL
	filePath := filepath.Join(downloadDir, fileName) // Construct the local file path
	out, err := os.Create(filePath)                  // Create a new file under filePath
	if err != nil {
		return err
	}
	defer out.Close()

	// Download the file
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Write the response body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}
	return nil
}
