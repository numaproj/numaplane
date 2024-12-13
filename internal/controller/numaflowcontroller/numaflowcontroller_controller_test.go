/*
Copyright 2024.

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

package numaflowcontroller

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

func Test_resolveManifestTemplate(t *testing.T) {
	defaultInstanceID := "123"

	defaultController := &apiv1.NumaflowController{
		Spec: apiv1.NumaflowControllerSpec{
			InstanceID: defaultInstanceID,
		},
	}

	testCases := []struct {
		name             string
		manifest         string
		controller       *apiv1.NumaflowController
		expectedManifest string
		expectedError    error
	}{
		{
			name:             "nil controller",
			manifest:         "",
			controller:       nil,
			expectedManifest: "",
			expectedError:    nil,
		}, {
			name:             "empty manifest",
			manifest:         "",
			controller:       defaultController,
			expectedManifest: "",
			expectedError:    nil,
		}, {
			name:             "manifest with invalid template field",
			manifest:         "this is {{.Invalid}} invalid",
			controller:       defaultController,
			expectedManifest: "",
			expectedError:    fmt.Errorf("unable to apply information to manifest: template: manifest:1:10: executing \"manifest\" at <.Invalid>: can't evaluate field Invalid in type struct { InstanceSuffix string; InstanceID string }"),
		}, {
			name:     "manifest with valid template and controller without instanceID",
			manifest: "valid-template-no-id{{.InstanceSuffix}}",
			controller: &apiv1.NumaflowController{
				Spec: apiv1.NumaflowControllerSpec{},
			},
			expectedManifest: "valid-template-no-id",
			expectedError:    nil,
		}, {
			name:             "manifest with valid template and controller with instanceID",
			manifest:         "valid-template-no-id{{.InstanceSuffix}}",
			controller:       defaultController,
			expectedManifest: fmt.Sprintf("valid-template-no-id-%s", defaultInstanceID),
			expectedError:    nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manifestBytes, err := resolveManifestTemplate(tc.manifest, tc.controller)

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

func createNumaflowControllerDef(namespace string, version string, phase apiv1.Phase, conditions []metav1.Condition) *apiv1.NumaflowController {
	return &apiv1.NumaflowController{
		TypeMeta: metav1.TypeMeta{
			Kind:       "NumaflowController",
			APIVersion: "numaplane.numaproj.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              NumaflowControllerDeploymentName,
			Namespace:         namespace,
			UID:               "uid",
			CreationTimestamp: metav1.NewTime(time.Now()),
			Generation:        1,
		},
		Spec: apiv1.NumaflowControllerSpec{
			Version: version,
		},
		Status: apiv1.NumaflowControllerStatus{
			Status: apiv1.Status{
				Phase:      phase,
				Conditions: conditions,
			},
		},
	}
}

func Test_reconcile_numaflowcontroller_PPND(t *testing.T) {

	restConfig, numaflowClientSet, client, k8sClientSet, err := commontest.PrepareK8SEnvironment()
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

	r, err := NewNumaflowControllerReconciler(
		client,
		scheme.Scheme,
		restConfig,
		kubernetes.NewKubectl(),
		ctlrcommon.TestCustomMetrics,
		recorder,
	)
	assert.NoError(t, err)

	trueValue := true

	pipelinerollout.PipelineROReconciler = &pipelinerollout.PipelineRolloutReconciler{Queue: util.NewWorkQueue("fake_queue")}

	testCases := []struct {
		name                    string
		newControllerVersion    string             // new version specified in NumaflowController
		existingController      *appsv1.Deployment // if nil, then there isn't a Controller installed
		existingPipelineRollout *apiv1.PipelineRollout
		existingPipeline        *numaflowv1.Pipeline
		existingPauseRequest    *bool // was Numaflow Controller previously requesting pause?
		expectedPauseRequest    *bool // after reconcile(), should it be requesting pause?
		expectedPhase           apiv1.Phase
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
			expectedPhase:           apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
			},
			expectedResultControllerVersion: "1.2.0",
		},
		{
			name:                 "new Controller version done reconciling",
			newControllerVersion: "1.2.1",
			existingController:   createDeploymentDefinition("quay.io/numaproj/numaflow:v1.2.1", false),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}),
			existingPipeline:                ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhasePaused),
			existingPauseRequest:            &trueValue,
			expectedPauseRequest:            &trueValue,
			expectedPhase:                   apiv1.PhaseDeployed,
			expectedConditionsSet:           map[apiv1.ConditionType]metav1.ConditionStatus{},
			expectedResultControllerVersion: "1.2.1",
		},
		{
			name:                 "new Controller version, pipelines not paused but failed",
			newControllerVersion: "1.2.1",
			existingController:   createDeploymentDefinition("quay.io/numaproj/numaflow:v1.2.0", false),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}),
			existingPipeline:     ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhaseFailed),
			existingPauseRequest: &trueValue,
			expectedPauseRequest: &trueValue,
			expectedPhase:        apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
			},
			expectedResultControllerVersion: "1.2.1",
		},
		{
			name:                 "new Controller version, pipelines not paused but set to allow data loss",
			newControllerVersion: "1.2.1",
			existingController:   createDeploymentDefinition("quay.io/numaproj/numaflow:v1.2.0", false),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{common.LabelKeyAllowDataLoss: "true"}, map[string]string{}, map[string]string{}, map[string]string{}),
			existingPipeline:     ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhasePausing),
			existingPauseRequest: &trueValue,
			expectedPauseRequest: &trueValue,
			expectedPhase:        apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
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
			_ = client.Delete(ctx, &apiv1.PipelineRollout{ObjectMeta: metav1.ObjectMeta{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestPipelineRolloutName}})
			_ = client.Delete(ctx, &apiv1.NumaflowController{ObjectMeta: metav1.ObjectMeta{Namespace: ctlrcommon.DefaultTestNamespace, Name: NumaflowControllerDeploymentName}})

			// create NumaflowController definition
			nfc := createNumaflowControllerDef(ctlrcommon.DefaultTestNamespace, tc.newControllerVersion, "", []metav1.Condition{})

			// the Reconcile() function does this, so we need to do it before calling reconcile() as well
			nfc.Status.Init(nfc.Generation)

			if tc.existingController != nil {
				// create the already-existing Deployment in Kubernetes
				ctlrcommon.CreateDeploymentInK8S(ctx, t, k8sClientSet, tc.existingController)

			}

			if tc.existingPipelineRollout != nil {
				ctlrcommon.CreatePipelineRolloutInK8S(ctx, t, client, tc.existingPipelineRollout)

			}
			if tc.existingPipeline != nil {
				// create the Pipeline beforehand in Kubernetes
				ctlrcommon.CreatePipelineInK8S(ctx, t, numaflowClientSet, tc.existingPipeline)
			}

			pm := ppnd.GetPauseModule()
			pm.PauseRequests[pm.GetNumaflowControllerKey(ctlrcommon.DefaultTestNamespace)] = tc.existingPauseRequest

			// call reconcile()
			_, err = r.reconcile(ctx, nfc, ctlrcommon.DefaultTestNamespace, time.Now())
			if tc.expectedReconcileError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			////// check results:

			// Check in-memory pause request:
			pauseRequestVal := pm.PauseRequests[pm.GetNumaflowControllerKey(ctlrcommon.DefaultTestNamespace)]
			if pauseRequestVal != nil && tc.expectedPauseRequest != nil {
				assert.Equal(t, *tc.expectedPauseRequest, *pauseRequestVal)
			} else {
				assert.Equal(t, tc.expectedPauseRequest, pauseRequestVal)
			}

			// Check Phase of NumaflowController:
			assert.Equal(t, tc.expectedPhase, nfc.Status.Phase)
			if tc.expectedResultControllerVersion != "" {
				// Check Deployment
				deploymentRetrieved, err := k8sClientSet.AppsV1().Deployments(ctlrcommon.DefaultTestNamespace).Get(ctx, NumaflowControllerDeploymentName, metav1.GetOptions{})
				assert.NoError(t, err)
				assert.Equal(t, fmt.Sprintf("quay.io/numaproj/numaflow:v%s", tc.expectedResultControllerVersion), deploymentRetrieved.Spec.Template.Spec.Containers[0].Image)
			}

			// Check Conditions:
			for conditionType, conditionStatus := range tc.expectedConditionsSet {
				found := false
				for _, condition := range nfc.Status.Conditions {
					if condition.Type == string(conditionType) && condition.Status == conditionStatus {
						found = true
						break
					}
				}
				assert.True(t, found, "condition type %s failed, conditions=%+v", conditionType, nfc.Status.Conditions)
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
