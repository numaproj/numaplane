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

package numaflowcontrollerrollout

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/controller/pipelinerollout"
	"github.com/numaproj/numaplane/internal/controller/ppnd"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"
)

func createNumaflowControllerRolloutDef(namespace string, version string, phase apiv1.Phase, conditions []metav1.Condition) *apiv1.NumaflowControllerRollout {
	return &apiv1.NumaflowControllerRollout{
		TypeMeta: metav1.TypeMeta{
			Kind:       "numaflowcontrollerrollout",
			APIVersion: "numaplane.numaproj.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              NumaflowControllerDeploymentName,
			Namespace:         namespace,
			UID:               "uid",
			CreationTimestamp: metav1.NewTime(time.Now()),
			Generation:        1,
		},
		Spec: apiv1.NumaflowControllerRolloutSpec{
			Controller: apiv1.Controller{Version: version},
		},
		Status: apiv1.NumaflowControllerRolloutStatus{
			Status: apiv1.Status{
				Phase:      phase,
				Conditions: conditions,
			},
		},
	}
}

func Test_getControllerDeploymentVersion(t *testing.T) {
	testCases := []struct {
		name        string
		containers  []corev1.Container
		expectedTag string
	}{
		{
			name: "standard",
			containers: []corev1.Container{
				{
					Image: "some/path/sidecar:latest",
				},
				{
					Image: "quay.io/numaproj/numaflow:v1.0.2",
				},
			},
			expectedTag: "1.0.2",
		},
		{
			name: "images have no paths",
			containers: []corev1.Container{
				{
					Image: "sidecar",
				},
				{
					Image: "numaflow:v1.0.2", // valid if it's in the default registry
				},
			},
			expectedTag: "1.0.2",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			deployment := appsv1.Deployment{}
			deployment.Spec.Template.Spec.Containers = append(deployment.Spec.Template.Spec.Containers, tc.containers...)
			tag, err := getControllerDeploymentVersion(&deployment)
			assert.Nil(t, err)
			assert.Equal(t, tc.expectedTag, tag)
		})
	}

}

func Test_resolveManifestTemplate(t *testing.T) {
	defaultInstanceID := "123"

	defaultRollout := &apiv1.NumaflowControllerRollout{
		Spec: apiv1.NumaflowControllerRolloutSpec{
			Controller: apiv1.Controller{
				InstanceID: defaultInstanceID,
			},
		},
	}

	testCases := []struct {
		name             string
		manifest         string
		rollout          *apiv1.NumaflowControllerRollout
		expectedManifest string
		expectedError    error
	}{
		{
			name:             "nil rollout",
			manifest:         "",
			rollout:          nil,
			expectedManifest: "",
			expectedError:    nil,
		}, {
			name:             "empty manifest",
			manifest:         "",
			rollout:          defaultRollout,
			expectedManifest: "",
			expectedError:    nil,
		}, {
			name:             "manifest with invalid template field",
			manifest:         "this is {{.Invalid}} invalid",
			rollout:          defaultRollout,
			expectedManifest: "",
			expectedError:    fmt.Errorf("unable to apply information to manifest: template: manifest:1:10: executing \"manifest\" at <.Invalid>: can't evaluate field Invalid in type struct { InstanceSuffix string; InstanceID string }"),
		}, {
			name:     "manifest with valid template and rollout without instanceID",
			manifest: "valid-template-no-id{{.InstanceSuffix}}",
			rollout: &apiv1.NumaflowControllerRollout{
				Spec: apiv1.NumaflowControllerRolloutSpec{
					Controller: apiv1.Controller{},
				},
			},
			expectedManifest: "valid-template-no-id",
			expectedError:    nil,
		}, {
			name:             "manifest with valid template and rollout with instanceID",
			manifest:         "valid-template-no-id{{.InstanceSuffix}}",
			rollout:          defaultRollout,
			expectedManifest: fmt.Sprintf("valid-template-no-id-%s", defaultInstanceID),
			expectedError:    nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manifestBytes, err := resolveManifestTemplate(tc.manifest, tc.rollout)

			if tc.expectedError != nil {
				assert.Error(t, err)
				assert.EqualError(t, err, tc.expectedError.Error())
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tc.expectedManifest, string(manifestBytes))
		})
	}
}

func Test_reconcile_numaflowcontrollerrollout_PPND(t *testing.T) {

	restConfig, numaflowClientSet, numaplaneClient, k8sClientSet, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetDynamicClient(restConfig))

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig"))
	assert.NoError(t, err)

	controllerDefinitions, err := ctlrcommon.GetNumaflowControllerDefinitions("../../../tests/config/controller-definitions-config.yaml")
	assert.Nil(t, err)
	config.GetConfigManagerInstance().GetControllerDefinitionsMgr().UpdateNumaflowControllerDefinitionConfig(*controllerDefinitions)

	ctx := context.Background()

	// other tests may call this, but it fails if called more than once
	if ctlrcommon.TestCustomMetrics == nil {
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics()
	}

	recorder := record.NewFakeRecorder(64)

	r, err := NewNumaflowControllerRolloutReconciler(
		numaplaneClient,
		scheme.Scheme,
		restConfig,
		kubernetes.NewKubectl(),
		ctlrcommon.TestCustomMetrics,
		recorder,
	)

	trueValue := true
	falseValue := false

	pipelinerollout.PipelineROReconciler = &pipelinerollout.PipelineRolloutReconciler{Queue: util.NewWorkQueue("fake_queue")}

	testCases := []struct {
		name                    string
		newControllerVersion    string             // new version specified in NumaflowControllerRollout
		existingController      *appsv1.Deployment // if nil, then there isn't a Controller installed
		existingPipelineRollout *apiv1.PipelineRollout
		existingPipeline        *numaflowv1.Pipeline
		existingPauseRequest    *bool // was Numaflow Controller previously requesting pause?
		expectedPauseRequest    *bool // after reconcile(), should it be requesting pause?
		expectedRolloutPhase    apiv1.Phase
		// require these Conditions to be set (note that in real life, previous reconciliations may have set other Conditions from before which are still present)
		expectedConditionsSet           map[apiv1.ConditionType]metav1.ConditionStatus
		expectedResultControllerVersion string // the one that's been deployed
		expectedReconcileError          bool
	}{
		{
			name:                    "no existing Controller",
			newControllerVersion:    "1.2.0",
			existingController:      nil,
			existingPipelineRollout: nil,
			existingPipeline:        nil,
			existingPauseRequest:    nil,
			expectedPauseRequest:    nil,
			expectedRolloutPhase:    apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
			},
			expectedResultControllerVersion: "1.2.0",
		},
		{
			name:                    "new Controller version, pipelines not yet paused",
			newControllerVersion:    "1.2.1",
			existingController:      createDeploymentDefinition("quay.io/numaproj/numaflow:v1.2.0", false),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName}, map[string]string{}, map[string]string{}),
			existingPipeline:        ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhasePausing), // could be pausing for another reason such as Pipeline updating
			existingPauseRequest:    &falseValue,
			expectedPauseRequest:    &trueValue,
			expectedRolloutPhase:    apiv1.PhasePending,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionPausingPipelines: metav1.ConditionTrue,
			},
			expectedResultControllerVersion: "1.2.0",
		},
		{
			name:                    "new Controller version, pipelines paused",
			newControllerVersion:    "1.2.1",
			existingController:      createDeploymentDefinition("quay.io/numaproj/numaflow:v1.2.0", false),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName}, map[string]string{}, map[string]string{}),
			existingPipeline:        ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhasePaused),
			existingPauseRequest:    &trueValue,
			expectedPauseRequest:    &trueValue,
			expectedRolloutPhase:    apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
				apiv1.ConditionPausingPipelines:      metav1.ConditionTrue,
			},
			expectedResultControllerVersion: "1.2.1",
		},
		{
			name:                    "new Controller version done reconciling",
			newControllerVersion:    "1.2.1",
			existingController:      createDeploymentDefinition("quay.io/numaproj/numaflow:v1.2.1", false),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName}, map[string]string{}, map[string]string{}),
			existingPipeline:        ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhasePaused),
			existingPauseRequest:    &trueValue,
			expectedPauseRequest:    &falseValue,
			expectedRolloutPhase:    apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionPausingPipelines: metav1.ConditionFalse},
			expectedResultControllerVersion: "1.2.1",
		},
		{
			name:                    "new Controller version, pipelines not paused but failed",
			newControllerVersion:    "1.2.1",
			existingController:      createDeploymentDefinition("quay.io/numaproj/numaflow:v1.2.0", false),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName}, map[string]string{}, map[string]string{}),
			existingPipeline:        ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhaseFailed),
			existingPauseRequest:    &trueValue,
			expectedPauseRequest:    &trueValue,
			expectedRolloutPhase:    apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
				apiv1.ConditionPausingPipelines:      metav1.ConditionTrue,
			},
			expectedResultControllerVersion: "1.2.1",
		},
		{
			name:                    "new Controller version, pipelines not paused but set to allow data loss",
			newControllerVersion:    "1.2.1",
			existingController:      createDeploymentDefinition("quay.io/numaproj/numaflow:v1.2.0", false),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName}, map[string]string{common.LabelKeyAllowDataLoss: "true"}, map[string]string{}),
			existingPipeline:        ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhasePausing),
			existingPauseRequest:    &trueValue,
			expectedPauseRequest:    &trueValue,
			expectedRolloutPhase:    apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
				apiv1.ConditionPausingPipelines:      metav1.ConditionTrue,
			},
			expectedResultControllerVersion: "1.2.1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			// first delete previous resources in case they already exist, in Kubernetes
			_ = k8sClientSet.AppsV1().Deployments(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = k8sClientSet.CoreV1().ConfigMaps(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = k8sClientSet.RbacV1().Roles(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = k8sClientSet.RbacV1().RoleBindings(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).Delete(ctx, fmt.Sprintf("%s-0", ctlrcommon.DefaultTestPipelineRolloutName), metav1.DeleteOptions{})
			_ = numaplaneClient.Delete(ctx, &apiv1.PipelineRollout{ObjectMeta: metav1.ObjectMeta{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestPipelineRolloutName}})
			_ = numaplaneClient.Delete(ctx, &apiv1.NumaflowControllerRollout{ObjectMeta: metav1.ObjectMeta{Namespace: ctlrcommon.DefaultTestNamespace, Name: NumaflowControllerDeploymentName}})

			// create NumaflowControllerRollout definition
			rollout := createNumaflowControllerRolloutDef(ctlrcommon.DefaultTestNamespace, tc.newControllerVersion, "", []metav1.Condition{})

			// the Reconcile() function does this, so we need to do it before calling reconcile() as well
			rollout.Status.Init(rollout.Generation)

			if tc.existingController != nil {
				// create the already-existing Deployment in Kubernetes
				ctlrcommon.CreateDeploymentInK8S(ctx, t, k8sClientSet, tc.existingController)

			}

			if tc.existingPipelineRollout != nil {
				ctlrcommon.CreatePipelineRolloutInK8S(ctx, t, numaplaneClient, tc.existingPipelineRollout)

			}
			if tc.existingPipeline != nil {
				// create the Pipeline beforehand in Kubernetes
				ctlrcommon.CreatePipelineInK8S(ctx, t, numaflowClientSet, tc.existingPipeline)
			}

			pm := ppnd.GetPauseModule()
			pm.PauseRequests[pm.GetNumaflowControllerKey(ctlrcommon.DefaultTestNamespace)] = tc.existingPauseRequest

			// call reconcile()
			_, err = r.reconcile(ctx, rollout, ctlrcommon.DefaultTestNamespace, time.Now())
			if tc.expectedReconcileError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			////// check results:

			// Check in-memory pause request:
			assert.Equal(t, tc.expectedPauseRequest, (pm.PauseRequests[pm.GetNumaflowControllerKey(ctlrcommon.DefaultTestNamespace)]))

			// Check Phase of Rollout:
			assert.Equal(t, tc.expectedRolloutPhase, rollout.Status.Phase)
			if tc.expectedResultControllerVersion != "" {
				// Check Deployment
				deploymentRetrieved, err := k8sClientSet.AppsV1().Deployments(ctlrcommon.DefaultTestNamespace).Get(ctx, "numaflow-controller", metav1.GetOptions{})
				assert.NoError(t, err)
				assert.Equal(t, fmt.Sprintf("quay.io/numaproj/numaflow:v%s", tc.expectedResultControllerVersion), deploymentRetrieved.Spec.Template.Spec.Containers[0].Image)
			}

			// Check Conditions:
			for conditionType, conditionStatus := range tc.expectedConditionsSet {
				found := false
				for _, condition := range rollout.Status.Conditions {
					if condition.Type == string(conditionType) && condition.Status == conditionStatus {
						found = true
						break
					}
				}
				assert.True(t, found, "condition type %s failed, conditions=%+v", conditionType, rollout.Status.Conditions)
			}
		})
	}

}

func createDeploymentDefinition(imagePath string, stillReconciling bool) *appsv1.Deployment {
	generation := 1
	observedGeneration := generation
	if stillReconciling {
		observedGeneration = generation - 1
	}
	replicas := int32(1)
	labels := map[string]string{
		"app.kubernetes.io/component": "controller-manager",
		"app.kubernetes.io/name":      "controller-manager",
		"app.kubernetes.io/part-of":   "numaflow",
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  ctlrcommon.DefaultTestNamespace,
			Name:       NumaflowControllerDeploymentName,
			Generation: int64(generation),
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "controller-manager",
							Image: imagePath,
						},
					},
				},
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
			},
			Replicas: &replicas,
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: int64(observedGeneration),
			Replicas:           1,
			UpdatedReplicas:    replicas,
			AvailableReplicas:  1,
			ReadyReplicas:      1,
		},
	}
}
