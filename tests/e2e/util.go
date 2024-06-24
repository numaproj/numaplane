package e2e

import (
	"os"
	"path/filepath"

	// appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/yaml"
)

var CRDPath = filepath.Join("..", "..", "config", "crd", "bases") // Path to your CRDs

// Other utility functions

// Function to read YAML file and return a runtime.Object
func readYAML(filePath string) (runtime.Object, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := yaml.NewYAMLOrJSONDecoder(file, 1024)
	var obj runtime.Object
	err = decoder.Decode(&obj)
	return obj, err
}
