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
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
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
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var DefaultScaleJSONString string = `{"disabled":null,"min":2,"max":2}`
var DefaultScaleTo int64 = 1

var (
	DefaultTestNamespace = "default"

	DefaultTestISBSvcRolloutName                = "isbservicerollout-test"
	DefaultTestISBSvcName                       = DefaultTestISBSvcRolloutName + "-0"
	DefaultTestPipelineRolloutName              = "pipelinerollout-test"
	DefaultTestPipelineName                     = DefaultTestPipelineRolloutName + "-0"
	DefaultTestMonoVertexRolloutName            = "monovertexrollout-test"
	DefaultTestMonoVertexName                   = DefaultTestMonoVertexRolloutName + "-0"
	DefaultTestNumaflowControllerRolloutName    = "numaflow-controller"
	DefaultTestNumaflowControllerName           = "numaflow-controller" // TODO: change to add "-0" suffix after Progressive
	DefaultTestNumaflowControllerDeploymentName = "numaflow-controller"
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
	pipelineRolloutCopy := *pipelineRollout
	err := numaplaneClient.Create(ctx, pipelineRollout)
	assert.NoError(t, err)
	pipelineRollout.Status = pipelineRolloutCopy.Status
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

func CreatePipelineInK8SFromUnstructured(ctx context.Context, t *testing.T, numaflowClientSet *numaflowversioned.Clientset, pipeline *unstructured.Unstructured) {
	// Convert unstructured to typed Pipeline for creation in K8S
	var typedPipeline numaflowv1.Pipeline
	err := util.StructToStruct(pipeline.Object, &typedPipeline)
	assert.NoError(t, err)
	CreatePipelineInK8S(ctx, t, numaflowClientSet, &typedPipeline)
}

func CreateVertexInK8S(ctx context.Context, t *testing.T, numaflowClientSet *numaflowversioned.Clientset, vertex *numaflowv1.Vertex) {
	resultVertex, err := numaflowClientSet.NumaflowV1alpha1().Vertices(DefaultTestNamespace).Create(ctx, vertex, metav1.CreateOptions{})
	assert.NoError(t, err)
	resultVertex.Status = vertex.Status

	// updating the Status subresource is a separate operation
	_, err = numaflowClientSet.NumaflowV1alpha1().Vertices(DefaultTestNamespace).UpdateStatus(ctx, resultVertex, metav1.UpdateOptions{})
	assert.NoError(t, err)
}

func CreateMVRolloutInK8S(ctx context.Context, t *testing.T, numaplaneClient client.Client, monoVertexRollout *apiv1.MonoVertexRollout) {
	rolloutCopy := *monoVertexRollout
	err := numaplaneClient.Create(ctx, monoVertexRollout)
	assert.NoError(t, err)
	monoVertexRollout.Status = rolloutCopy.Status
	err = numaplaneClient.Status().Update(ctx, monoVertexRollout)
	assert.NoError(t, err)
}

func CreateMonoVertexInK8S(ctx context.Context, t *testing.T, numaflowClientSet *numaflowversioned.Clientset, monoVertex *numaflowv1.MonoVertex) {
	resultMV, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(DefaultTestNamespace).Create(ctx, monoVertex, metav1.CreateOptions{})
	assert.NoError(t, err)
	resultMV.Status = monoVertex.Status

	// updating the Status subresource is a separate operation
	_, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(DefaultTestNamespace).UpdateStatus(ctx, resultMV, metav1.UpdateOptions{})
	assert.NoError(t, err)
}

func CreateISBServiceRolloutInK8S(ctx context.Context, t *testing.T, numaplaneClient client.Client, isbsvcRollout *apiv1.ISBServiceRollout) {
	rolloutCopy := *isbsvcRollout
	err := numaplaneClient.Create(ctx, isbsvcRollout)
	assert.NoError(t, err)
	isbsvcRollout.Status = rolloutCopy.Status
	err = numaplaneClient.Status().Update(ctx, isbsvcRollout)
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

func CreateNumaflowControllerInK8S(ctx context.Context, t *testing.T, numaplaneClient client.Client, numaflowController *apiv1.NumaflowController) {
	err := numaplaneClient.Create(ctx, numaflowController)
	assert.NoError(t, err)
	err = numaplaneClient.Status().Update(ctx, numaflowController)
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
				common.LabelKeyISBServiceRONameForPipeline:    DefaultTestISBSvcRolloutName,
				common.LabelKeyISBServiceChildNameForPipeline: DefaultTestISBSvcName,
				common.LabelKeyParentRollout:                  DefaultTestPipelineRolloutName},
		},
		Spec: numaflowv1.PipelineSpec{
			InterStepBufferServiceName: DefaultTestISBSvcName,
		},
		Status: numaflowv1.PipelineStatus{
			Phase: phase,
		},
	}
}

func CreateTestPipelineRollout(pipelineSpec numaflowv1.PipelineSpec, rolloutAnnotations map[string]string, rolloutLabels map[string]string, pipelineAnnotations map[string]string, pipelineLabels map[string]string, optionalStatus *apiv1.PipelineRolloutStatus) *apiv1.PipelineRollout {
	pipelineRaw, _ := json.Marshal(pipelineSpec)

	pipelineRolloutStatus := apiv1.PipelineRolloutStatus{}
	if optionalStatus != nil {
		pipelineRolloutStatus = *optionalStatus
	}

	return &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         DefaultTestNamespace,
			Name:              DefaultTestPipelineRolloutName,
			UID:               "uid",
			CreationTimestamp: metav1.NewTime(time.Now()),
			Generation:        1,
			Annotations:       rolloutAnnotations,
			Labels:            rolloutLabels,
		},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: apiv1.Pipeline{
				Metadata: apiv1.Metadata{
					Labels:      pipelineLabels,
					Annotations: pipelineAnnotations,
				},
				Spec: runtime.RawExtension{
					Raw: pipelineRaw,
				},
			},
		},
		Status: pipelineRolloutStatus,
	}
}

func CreateTestPipelineOfSpec(
	spec numaflowv1.PipelineSpec,
	name string,
	phase numaflowv1.PipelinePhase,
	status numaflowv1.Status,
	drainedOnPause bool,
	labels map[string]string,
	annotations map[string]string,
) *numaflowv1.Pipeline {
	pipelineStatus := numaflowv1.PipelineStatus{
		Phase:          phase,
		Status:         status,
		DrainedOnPause: drainedOnPause,
	}
	return &numaflowv1.Pipeline{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pipeline",
			APIVersion: "numaflow.numaproj.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   DefaultTestNamespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec:   spec,
		Status: pipelineStatus,
	}

}

func CreateTestPipelineUnstructured(name string, spec string) (*unstructured.Unstructured, error) {
	pipelineDef := &unstructured.Unstructured{Object: make(map[string]interface{})}
	pipelineDef.SetGroupVersionKind(numaflowv1.PipelineGroupVersionKind)
	pipelineDef.SetName(name)
	pipelineDef.SetNamespace(DefaultTestNamespace)
	var pipelineSpec map[string]interface{}
	if err := json.Unmarshal([]byte(spec), &pipelineSpec); err != nil {
		return nil, err
	}
	pipelineDef.Object["spec"] = pipelineSpec

	return pipelineDef, nil
}

func CreateDefaultTestMVOfPhase(phase numaflowv1.MonoVertexPhase) *numaflowv1.MonoVertex {
	return &numaflowv1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:              DefaultTestMonoVertexName,
			Namespace:         DefaultTestNamespace,
			UID:               "some-uid",
			CreationTimestamp: metav1.NewTime(time.Now()),
			Generation:        1,
			Labels: map[string]string{
				common.LabelKeyParentRollout: DefaultTestMonoVertexRolloutName},
		},
		Spec: numaflowv1.MonoVertexSpec{},
		Status: numaflowv1.MonoVertexStatus{
			Phase: phase,
		},
	}
}

func CreateTestMVRollout(mvSpec numaflowv1.MonoVertexSpec, rolloutAnnotations map[string]string, rolloutLabels map[string]string, mvAnnotations map[string]string, mvLabels map[string]string, optionalStatus *apiv1.MonoVertexRolloutStatus) *apiv1.MonoVertexRollout {
	mvRaw, _ := json.Marshal(mvSpec)

	monoVertexRolloutStatus := apiv1.MonoVertexRolloutStatus{}
	if optionalStatus != nil {
		monoVertexRolloutStatus = *optionalStatus
	}

	return &apiv1.MonoVertexRollout{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         DefaultTestNamespace,
			Name:              DefaultTestMonoVertexRolloutName,
			UID:               "uid",
			CreationTimestamp: metav1.NewTime(time.Now()),
			Generation:        1,
			Annotations:       rolloutAnnotations,
			Labels:            rolloutLabels,
		},
		Spec: apiv1.MonoVertexRolloutSpec{
			MonoVertex: apiv1.MonoVertex{
				Metadata: apiv1.Metadata{
					Labels:      mvLabels,
					Annotations: mvAnnotations,
				},
				Spec: runtime.RawExtension{
					Raw: mvRaw,
				},
			},
		},
		Status: monoVertexRolloutStatus,
	}
}

func CreateTestMonoVertexOfSpec(
	spec numaflowv1.MonoVertexSpec,
	name string,
	phase numaflowv1.MonoVertexPhase,
	status numaflowv1.Status,
	labels map[string]string,
	annotations map[string]string,
) *numaflowv1.MonoVertex {
	mvStatus := numaflowv1.MonoVertexStatus{
		Phase:  phase,
		Status: status,
	}
	return &numaflowv1.MonoVertex{
		TypeMeta: metav1.TypeMeta{
			Kind:       "MonoVertex",
			APIVersion: "numaflow.numaproj.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   DefaultTestNamespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec:   spec,
		Status: mvStatus,
	}

}

func CreateDefaultISBServiceSpec(jetstreamVersion string) numaflowv1.InterStepBufferServiceSpec {
	return numaflowv1.InterStepBufferServiceSpec{
		Redis: &numaflowv1.RedisBufferService{},
		JetStream: &numaflowv1.JetStreamBufferService{
			Version:     jetstreamVersion,
			Persistence: nil,
		},
	}
}

func CreateDefaultISBService(jetstreamVersion string, phase numaflowv1.ISBSvcPhase, fullyReconciled bool) *numaflowv1.InterStepBufferService {
	status := numaflowv1.InterStepBufferServiceStatus{
		Phase: phase,
	}
	if fullyReconciled {
		status.ObservedGeneration = 1
	} else {
		status.ObservedGeneration = 0
	}
	return &numaflowv1.InterStepBufferService{
		TypeMeta: metav1.TypeMeta{
			Kind:       common.NumaflowISBServiceKind,
			APIVersion: common.NumaflowAPIGroup + "/" + common.NumaflowAPIVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultTestISBSvcName,
			Namespace: DefaultTestNamespace,
			Labels: map[string]string{
				common.LabelKeyParentRollout: DefaultTestISBSvcRolloutName,
				common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
			},
		},
		Spec:   CreateDefaultISBServiceSpec(jetstreamVersion),
		Status: status,
	}
}

func CreateTestISBService(
	jetstreamVersion string,
	name string,
	phase numaflowv1.ISBSvcPhase,
	fullyReconciled bool,
	labels map[string]string,
	annotations map[string]string,
) *numaflowv1.InterStepBufferService {
	isbsvcStatus := numaflowv1.InterStepBufferServiceStatus{
		Phase: phase,
	}
	if fullyReconciled {
		isbsvcStatus.ObservedGeneration = 1
	} else {
		isbsvcStatus.ObservedGeneration = 0
	}
	return &numaflowv1.InterStepBufferService{
		TypeMeta: metav1.TypeMeta{
			Kind:       common.NumaflowISBServiceKind,
			APIVersion: common.NumaflowAPIGroup + "/" + common.NumaflowAPIVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   DefaultTestNamespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec:   CreateDefaultISBServiceSpec(jetstreamVersion),
		Status: isbsvcStatus,
	}

}

func CreateISBServiceRollout(isbsvcSpec numaflowv1.InterStepBufferServiceSpec, optionalStatus *apiv1.ISBServiceRolloutStatus) *apiv1.ISBServiceRollout {
	isbsSpecRaw, _ := json.Marshal(isbsvcSpec)

	isbSvcRolloutStatus := apiv1.ISBServiceRolloutStatus{}
	if optionalStatus != nil {
		isbSvcRolloutStatus = *optionalStatus
	}

	return &apiv1.ISBServiceRollout{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         DefaultTestNamespace,
			Name:              DefaultTestISBSvcRolloutName,
			CreationTimestamp: metav1.NewTime(time.Now()),
			Generation:        1,
		},
		Spec: apiv1.ISBServiceRolloutSpec{
			InterStepBufferService: apiv1.InterStepBufferService{
				Spec: k8sruntime.RawExtension{
					Raw: isbsSpecRaw,
				},
			},
		},
		Status: isbSvcRolloutStatus,
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
