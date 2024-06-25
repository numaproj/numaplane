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
				common.LabelKeyNumaplaneControllerConfig: common.LabelValueNumaplaneControllerConfig,
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
	definition := config.GetConfigManagerInstance().GetControllerDefinitionsConfig()
	assert.Len(t, definition, 2)

	// Create the ConfigMap object in the fake clientset
	err = clientSet.CoreV1().ConfigMaps("default").Delete(ctx, configMap.Name, metav1.DeleteOptions{})
	assert.NoError(t, err)

	// Wait for the controller to process the ConfigMap
	time.Sleep(5 * time.Second)
	// Validate the controller definition config is updated correctly
	definition = config.GetConfigManagerInstance().GetControllerDefinitionsConfig()
	assert.Len(t, definition, 0)
}
