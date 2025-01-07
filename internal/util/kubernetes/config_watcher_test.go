package kubernetes

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
)

func Test_watchConfigMaps(t *testing.T) {
	ctx := context.TODO()
	scheme := runtime.NewScheme()
	err := corev1.AddToScheme(scheme)
	assert.NoError(t, err)

	clientSet := fake.NewSimpleClientset()
	go watchConfigMaps(ctx, clientSet, "default")
	time.Sleep(10 * time.Second)

	data, err := os.ReadFile("../../../tests/config/controller-definitions-config.yaml")
	assert.NoError(t, err)

	// Create a new ConfigMap object
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "numaflow-controller-definitions-config",
			Namespace: "default",
			Labels: map[string]string{
				common.LabelKeyNumaplaneControllerConfig: common.LabelValueNumaflowControllerDefinitions,
			},
		},
		Data: map[string]string{
			"controller_definitions.yaml": string(data),
		},
	}

	// Create the ConfigMap object in the fake clientset
	_, err = clientSet.CoreV1().ConfigMaps("default").Create(ctx, configMap, metav1.CreateOptions{})
	assert.NoError(t, err)

	// Wait for the controller to process the ConfigMap
	time.Sleep(5 * time.Second)

	// Validate the controller definition config is set correctly
	definition := config.GetConfigManagerInstance().GetControllerDefinitionsMgr().GetNumaflowControllerDefinitionsConfig()
	assert.Len(t, definition, 2)

	// Delete the ConfigMap object from the fake clientset
	err = clientSet.CoreV1().ConfigMaps("default").Delete(ctx, configMap.Name, metav1.DeleteOptions{})
	assert.NoError(t, err)

	// Wait for the controller to process the ConfigMap
	time.Sleep(5 * time.Second)

	// Validate the controller definition config has been removed
	definition = config.GetConfigManagerInstance().GetControllerDefinitionsMgr().GetNumaflowControllerDefinitionsConfig()
	assert.Len(t, definition, 0)

	// === USDE Config Test =====================================

	configMap = &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "numaplane-controller-usde-config",
			Namespace: "default",
			Labels: map[string]string{
				common.LabelKeyNumaplaneControllerConfig: "usde-config",
			},
		},
		Data: map[string]string{
			"pipeline": `
        dataLoss:
          - path: "abc"
          - path: "abcde/xyz"
          - path: "path/array/sample"
        recreate:
        - path: "recreate/path"
      `,
			"isbService": `
        dataLoss:
          - path: "invalid"
        progressive:
          - path: "progressive/path/a"
          - path: "progressive/path/b"
          - path: "progressive/path/c"
      `,
		},
	}

	_, err = clientSet.CoreV1().ConfigMaps("default").Create(ctx, configMap, metav1.CreateOptions{})
	assert.NoError(t, err)

	time.Sleep(5 * time.Second)

	expectedUSDEConfig := config.USDEConfig{
		Pipeline: config.USDEResourceConfig{
			DataLoss: []config.SpecField{{Path: "abc"}, {Path: "abcde/xyz"}, {Path: "path/array/sample"}},
			Recreate: []config.SpecField{{Path: "recreate/path"}},
		},
		ISBService: config.USDEResourceConfig{
			DataLoss:    []config.SpecField{{Path: "invalid"}},
			Progressive: []config.SpecField{{Path: "progressive/path/a"}, {Path: "progressive/path/b"}, {Path: "progressive/path/c"}},
		},
	}

	actualUSDEConfig := config.GetConfigManagerInstance().GetUSDEConfig()
	assert.Equal(t, expectedUSDEConfig, actualUSDEConfig)

	err = clientSet.CoreV1().ConfigMaps("default").Delete(ctx, configMap.Name, metav1.DeleteOptions{})
	assert.NoError(t, err)

	time.Sleep(5 * time.Second)

	actualUSDEConfig = config.GetConfigManagerInstance().GetUSDEConfig()
	assert.Equal(t, config.USDEConfig{}, actualUSDEConfig)
}
