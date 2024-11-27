package controller

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"

	k8sclientgo "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctlrruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	numaflowversioned "github.com/numaproj/numaflow/pkg/client/clientset/versioned"
	"github.com/numaproj/numaplane/internal/controller/config"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func PrepareK8SEnvironment() (restConfig *rest.Config, numaflowClientSet *numaflowversioned.Clientset, client ctlrruntimeclient.Client, k8sClientSet *k8sclientgo.Clientset, err error) {

	// Set up a test Kubernetes environment which includes both our Numaplane and Numaflow CRDs

	// Numaplane CRDs can be found in our repository
	// Numaflow CRDs must be downloaded

	// find Numaplane CRD directory
	rootDirectory, err := findRootDirectory()
	if err != nil {
		return
	}
	crdDirectory := rootDirectory + "/config/crd"

	crdsURLs := []string{
		"https://raw.githubusercontent.com/numaproj/numaflow/main/config/base/crds/minimal/numaflow.numaproj.io_interstepbufferservices.yaml",
		"https://raw.githubusercontent.com/numaproj/numaflow/main/config/base/crds/minimal/numaflow.numaproj.io_pipelines.yaml",
		"https://raw.githubusercontent.com/numaproj/numaflow/main/config/base/crds/minimal/numaflow.numaproj.io_vertices.yaml",
		"https://raw.githubusercontent.com/numaproj/numaflow/main/config/base/crds/minimal/numaflow.numaproj.io_monovertices.yaml",
	}
	externalCRDsDir := crdDirectory + "/external"
	for _, crdURL := range crdsURLs {
		err = downloadCRDToPath(crdURL, externalCRDsDir)
		if err != nil {
			return
		}
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
		BinaryAssetsDirectory: filepath.Join(rootDirectory, "bin", "k8s",
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
	client, err = ctlrruntimeclient.New(restConfig, ctlrruntimeclient.Options{})
	if err != nil {
		return
	}
	k8sClientSet, err = k8sclientgo.NewForConfig(restConfig)
	return
}

func findRootDirectory() (string, error) {
	// we know we should see "/numaplane/config/crd" - look for that in order to find the "numaplane" root
	crdSubdirectory := "/config/crd"
	path, _ := os.Getwd()

	r := regexp.MustCompile(`/numaplane`)
	matches := r.FindAllStringIndex(path, -1) // this returns a set of slices, where each slice represents the first and last index of the "/numaplane/" string
	if matches == nil {
		return "", fmt.Errorf("no occurrences of '/numaplane' found in path %q", path)
	}
	for _, occurrence := range matches {
		endIndex := occurrence[1]
		possibleCRDDirectory := path[0:endIndex] + crdSubdirectory
		_, err := os.Stat(possibleCRDDirectory)
		if err == nil {
			return path[0:endIndex], nil
		}
	}
	return "", fmt.Errorf("no occurrence of %q found in any higher level directory from current working directory %q", crdSubdirectory, path)
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

func LoadGlobalConfig(fileDirectory string, fileName string) error {
	configManager := config.GetConfigManagerInstance()
	return configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(fileDirectory), config.WithConfigFileName(fileName))
}
