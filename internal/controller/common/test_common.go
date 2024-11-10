/*
Copyright 2023.

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

package common

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	k8sclientgo "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	numaflowversioned "github.com/numaproj/numaflow/pkg/client/clientset/versioned"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var (
	DefaultTestNamespace = "default"

	DefaultTestISBSvcRolloutName     = "isbservicerollout-test"
	DefaultTestISBSvcName            = "isbservicerollout-test" // TODO: change to add "-0" suffix after Progressive
	DefaultTestPipelineRolloutName   = "pipelinerollout-test"
	DefaultTestPipelineName          = DefaultTestPipelineRolloutName + "-0"
	DefaultTestNewPipelineName       = DefaultTestPipelineRolloutName + "-1"
	DefaultTestMonoVertexRolloutName = "monovertexrollout-test"
	DefaultTestMonoVertexName        = DefaultTestMonoVertexRolloutName + "-0"
)

const (
	TestDefaultTimeout  = 15 * time.Second
	TestDefaultDuration = 10 * time.Second
	TestDefaultInterval = 250 * time.Millisecond
)

var (
	TestRESTConfig    *rest.Config
	TestK8sClient     client.Client
	TestCustomMetrics *metrics.CustomMetrics
)

func CreatePipelineRolloutInK8S(ctx context.Context, t *testing.T, numaplaneClient client.Client, pipelineRollout *apiv1.PipelineRollout) {
	err := numaplaneClient.Create(ctx, pipelineRollout)
	assert.NoError(t, err)
	err = numaplaneClient.Status().Update(ctx, pipelineRollout)
	assert.NoError(t, err)
}

func CreatePipelineInK8S(ctx context.Context, t *testing.T, numaflowClientSet *numaflowversioned.Clientset, pipeline *numaflowv1.Pipeline) {
	resultPipeline, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(DefaultTestNamespace).Create(ctx, pipeline, metav1.CreateOptions{})
	assert.NoError(t, err)
	resultPipeline.Status = pipeline.Status

	// updating the Status subresource is a separate operation
	_, err = numaflowClientSet.NumaflowV1alpha1().Pipelines(DefaultTestNamespace).UpdateStatus(ctx, resultPipeline, metav1.UpdateOptions{})
	assert.NoError(t, err)
}

func CreateISBSvcInK8S(ctx context.Context, t *testing.T, numaflowClientSet *numaflowversioned.Clientset, isbsvc *numaflowv1.InterStepBufferService) {
	resultISBSvc, err := numaflowClientSet.NumaflowV1alpha1().InterStepBufferServices(DefaultTestNamespace).Create(ctx, isbsvc, metav1.CreateOptions{})
	assert.NoError(t, err)
	// update Status subresource
	resultISBSvc.Status = isbsvc.Status
	_, err = numaflowClientSet.NumaflowV1alpha1().InterStepBufferServices(DefaultTestNamespace).UpdateStatus(ctx, resultISBSvc, metav1.UpdateOptions{})
	assert.NoError(t, err)
}

func CreateStatefulSetInK8S(ctx context.Context, t *testing.T, k8sClientSet *k8sclientgo.Clientset, statefulSet *appsv1.StatefulSet) {
	ss, err := k8sClientSet.AppsV1().StatefulSets(DefaultTestNamespace).Create(ctx, statefulSet, metav1.CreateOptions{})
	assert.NoError(t, err)
	// update Status subresource
	ss.Status = statefulSet.Status
	_, err = k8sClientSet.AppsV1().StatefulSets(DefaultTestNamespace).UpdateStatus(ctx, ss, metav1.UpdateOptions{})
	assert.NoError(t, err)
}

func CreateDeploymentInK8S(ctx context.Context, t *testing.T, k8sClientSet *k8sclientgo.Clientset, deployment *appsv1.Deployment) {
	resultDeployment, err := k8sClientSet.AppsV1().Deployments(DefaultTestNamespace).Create(ctx, deployment, metav1.CreateOptions{})
	assert.NoError(t, err)
	resultDeployment.Status = deployment.Status
	_, err = k8sClientSet.AppsV1().Deployments(DefaultTestNamespace).UpdateStatus(ctx, resultDeployment, metav1.UpdateOptions{})
	assert.NoError(t, err)
}

func CreateDefaultTestPipelineOfPhase(phase numaflowv1.PipelinePhase) *numaflowv1.Pipeline {
	return &numaflowv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:              DefaultTestPipelineName,
			Namespace:         DefaultTestNamespace,
			UID:               "some-uid",
			CreationTimestamp: metav1.NewTime(time.Now()),
			Generation:        1,
			Labels: map[string]string{
				common.LabelKeyISBServiceNameForPipeline: DefaultTestISBSvcRolloutName,
				common.LabelKeyParentRollout:             DefaultTestPipelineRolloutName},
		},
		Spec: numaflowv1.PipelineSpec{
			InterStepBufferServiceName: DefaultTestISBSvcRolloutName,
		},
		Status: numaflowv1.PipelineStatus{
			Phase: phase,
		},
	}
}

func CreateTestPipelineRollout(isbsvcSpec numaflowv1.PipelineSpec, annotations map[string]string, labels map[string]string) *apiv1.PipelineRollout {
	pipelineRaw, _ := json.Marshal(isbsvcSpec)
	return &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         DefaultTestNamespace,
			Name:              DefaultTestPipelineRolloutName,
			UID:               "uid",
			CreationTimestamp: metav1.NewTime(time.Now()),
			Generation:        1,
			Annotations:       annotations,
			Labels:            labels,
		},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: apiv1.Pipeline{
				Spec: runtime.RawExtension{
					Raw: pipelineRaw,
				},
			},
		},
	}
}

func CreateTestPipelineOfSpec(
	spec numaflowv1.PipelineSpec,
	name string,
	phase numaflowv1.PipelinePhase,
	innerStatus numaflowv1.Status,
	drainedOnPause bool,
	labels map[string]string,
) *numaflowv1.Pipeline {
	status := numaflowv1.PipelineStatus{
		Phase:          phase,
		Status:         innerStatus,
		DrainedOnPause: drainedOnPause,
	}
	return &numaflowv1.Pipeline{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pipeline",
			APIVersion: "numaflow.numaproj.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: DefaultTestNamespace,
			Labels:    labels,
		},
		Spec:   spec,
		Status: status,
	}

}

func PipelineWithDesiredPhase(spec numaflowv1.PipelineSpec, phase numaflowv1.PipelinePhase) numaflowv1.PipelineSpec {
	spec.Lifecycle.DesiredPhase = phase
	return spec
}

func GetNumaflowControllerDefinitions(definitionsFile string) (*config.NumaflowControllerDefinitionConfig, error) {
	// Read definitions config file
	configData, err := os.ReadFile(definitionsFile)
	if err != nil {
		return nil, err
	}
	var controllerConfig config.NumaflowControllerDefinitionConfig
	err = yaml.Unmarshal(configData, &controllerConfig)
	if err != nil {
		return nil, err
	}

	return &controllerConfig, nil
}

// VerifyAutoHealing tests the auto healing feature
func VerifyAutoHealing(ctx context.Context, gvk schema.GroupVersionKind, namespace string, resourceName string, pathToValue string, newValue any) {
	lookupKey := types.NamespacedName{Name: resourceName, Namespace: namespace}

	// Get current resource
	currentResource := unstructured.Unstructured{}
	currentResource.SetGroupVersionKind(gvk)
	Eventually(func() error {
		return TestK8sClient.Get(ctx, lookupKey, &currentResource)
	}, TestDefaultTimeout, TestDefaultInterval).Should(Succeed())
	Expect(currentResource.Object).ToNot(BeEmpty())

	// Get the original value at the specified path (pathToValue)
	pathSlice := strings.Split(pathToValue, ".")
	originalValue, found, err := unstructured.NestedFieldNoCopy(currentResource.Object, pathSlice...)
	Expect(err).ToNot(HaveOccurred())
	Expect(found).To(BeTrue())

	// Set new value and update resource
	err = unstructured.SetNestedField(currentResource.Object, newValue, pathSlice...)
	Expect(err).ToNot(HaveOccurred())
	Expect(TestK8sClient.Update(ctx, &currentResource)).ToNot(HaveOccurred())

	// Get updated resource and the value at the specified path (pathToValue)
	e := Eventually(func() (any, error) {
		updatedResource := unstructured.Unstructured{}
		updatedResource.SetGroupVersionKind(gvk)
		if err := TestK8sClient.Get(ctx, lookupKey, &updatedResource); err != nil {
			return nil, err
		}

		currentValue, found, err := unstructured.NestedFieldNoCopy(updatedResource.Object, pathSlice...)
		Expect(err).ToNot(HaveOccurred())
		Expect(found).To(BeTrue())

		return currentValue, nil
	}, TestDefaultTimeout, TestDefaultInterval)

	// Verify that the value matches the original value and not the new value
	e.Should(Equal(originalValue))
	e.ShouldNot(Equal(newValue))
}

func VerifyStatusPhase(ctx context.Context, gvk schema.GroupVersionKind, namespace string, resourceName string, desiredPhase apiv1.Phase) {
	lookupKey := types.NamespacedName{Name: resourceName, Namespace: namespace}

	currentResource := unstructured.Unstructured{}
	currentResource.SetGroupVersionKind(gvk)
	Eventually(func() (bool, error) {
		err := TestK8sClient.Get(ctx, lookupKey, &currentResource)
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
	}, TestDefaultTimeout, TestDefaultInterval).Should(BeTrue())
}
