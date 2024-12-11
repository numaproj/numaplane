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
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
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
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"

	crCli "sigs.k8s.io/controller-runtime/pkg/client"
)

func Test_reconcile_NumaflowControllerRollout_PPND(t *testing.T) {
	ctx := context.Background()

	numaLogger := logger.New()
	numaLogger.SetLevel(3)
	logger.SetBaseLogger(numaLogger)
	ctx = logger.WithLogger(ctx, numaLogger)

	restConfig, numaflowClientSet, client, k8sClientSet, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetDynamicClient(restConfig))

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig"))
	assert.NoError(t, err)

	// other tests may call this, but it fails if called more than once
	if ctlrcommon.TestCustomMetrics == nil {
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics()
	}

	recorder := record.NewFakeRecorder(64)

	r := NewNumaflowControllerRolloutReconciler(client, scheme.Scheme, ctlrcommon.TestCustomMetrics, recorder)

	trueValue := true
	falseValue := false

	pipelinerollout.PipelineROReconciler = &pipelinerollout.PipelineRolloutReconciler{Queue: util.NewWorkQueue("fake_queue")}

	testCases := []struct {
		name                          string
		newNumaflowControllerVersion  string
		existingNumaflowControllerDef *apiv1.NumaflowController
		existingDeploymentDef         *appsv1.Deployment
		existingPipelineRollout       *apiv1.PipelineRollout
		existingPipeline              *numaflowv1.Pipeline
		existingPauseRequest          *bool // was NumaflowControllerRollout previously requesting pause?
		initialInProgressStrategy     apiv1.UpgradeStrategy
		expectedPauseRequest          *bool // after reconcile(), should it be requesting pause?
		expectedRolloutPhase          apiv1.Phase
		// require these Conditions to be set (note that in real life, previous reconciliations may have set other Conditions from before which are still present)
		expectedConditionsSet             map[apiv1.ConditionType]metav1.ConditionStatus
		expectedNumaflowControllerVersion string
		expectedInProgressStrategy        apiv1.UpgradeStrategy
	}{
		{
			name:                          "new NumaflowController",
			newNumaflowControllerVersion:  "1.2.3",
			existingNumaflowControllerDef: nil,
			existingDeploymentDef:         nil,
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}),
			existingPipeline:          ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhaseRunning),
			existingPauseRequest:      nil,
			initialInProgressStrategy: apiv1.UpgradeStrategyNoOp,
			expectedPauseRequest:      nil,
			expectedRolloutPhase:      apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
			},
			expectedNumaflowControllerVersion: "1.2.3",
			expectedInProgressStrategy:        apiv1.UpgradeStrategyNoOp,
		},
		{
			name:                          "existing NumaflowController - no change",
			newNumaflowControllerVersion:  "1.2.3",
			existingNumaflowControllerDef: createDefaultNumaflowController("1.2.3", apiv1.PhaseDeployed, true),
			existingDeploymentDef:         createDefaultNumaflowControllerDeployment("1.2.3", true),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}),
			existingPipeline:                  ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhaseRunning),
			existingPauseRequest:              &falseValue,
			initialInProgressStrategy:         apiv1.UpgradeStrategyNoOp,
			expectedPauseRequest:              &falseValue,
			expectedRolloutPhase:              apiv1.PhaseDeployed,
			expectedConditionsSet:             map[apiv1.ConditionType]metav1.ConditionStatus{}, // some Conditions may be set from before, but in any case nothing new to verify
			expectedNumaflowControllerVersion: "1.2.3",
		},
		{
			name:                          "existing NumaflowController - new spec - pipelines not paused",
			newNumaflowControllerVersion:  "3.2.1",
			existingNumaflowControllerDef: createDefaultNumaflowController("1.2.3", apiv1.PhaseDeployed, true),
			existingDeploymentDef:         createDefaultNumaflowControllerDeployment("1.2.3", true),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}),
			existingPipeline:                  ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhaseRunning),
			existingPauseRequest:              &falseValue,
			initialInProgressStrategy:         apiv1.UpgradeStrategyNoOp,
			expectedPauseRequest:              &trueValue,
			expectedRolloutPhase:              apiv1.PhasePending,
			expectedConditionsSet:             map[apiv1.ConditionType]metav1.ConditionStatus{},
			expectedNumaflowControllerVersion: "1.2.3",
			expectedInProgressStrategy:        apiv1.UpgradeStrategyPPND,
		},
		{
			name:                          "existing NumaflowController - new spec - pipelines paused",
			newNumaflowControllerVersion:  "3.2.1",
			existingNumaflowControllerDef: createDefaultNumaflowController("1.2.3", apiv1.PhaseDeployed, true),
			existingDeploymentDef:         createDefaultNumaflowControllerDeployment("1.2.3", true),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}),
			existingPipeline:          ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhasePaused),
			existingPauseRequest:      &trueValue,
			initialInProgressStrategy: apiv1.UpgradeStrategyPPND,
			expectedPauseRequest:      &trueValue,
			expectedRolloutPhase:      apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
			},
			expectedNumaflowControllerVersion: "3.2.1",
			expectedInProgressStrategy:        apiv1.UpgradeStrategyPPND,
		},
		{
			name:                          "existing NumaflowController - new spec - pipelines failed",
			newNumaflowControllerVersion:  "3.2.1",
			existingNumaflowControllerDef: createDefaultNumaflowController("1.2.3", apiv1.PhaseDeployed, true),
			existingDeploymentDef:         createDefaultNumaflowControllerDeployment("1.2.3", true),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}),
			existingPipeline:          ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhaseFailed),
			existingPauseRequest:      &trueValue,
			initialInProgressStrategy: apiv1.UpgradeStrategyPPND,
			expectedPauseRequest:      &trueValue,
			expectedRolloutPhase:      apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
			},
			expectedNumaflowControllerVersion: "3.2.1",
			expectedInProgressStrategy:        apiv1.UpgradeStrategyPPND,
		},
		{
			name:                          "existing NumaflowController - new spec - pipelines set to allow data loss",
			newNumaflowControllerVersion:  "3.2.1",
			existingNumaflowControllerDef: createDefaultNumaflowController("1.2.3", apiv1.PhasePending, true),
			existingDeploymentDef:         createDefaultNumaflowControllerDeployment("1.2.3", true),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{common.LabelKeyAllowDataLoss: "true"}, map[string]string{}, map[string]string{}, map[string]string{}),
			existingPipeline:          ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhasePausing),
			existingPauseRequest:      &trueValue,
			initialInProgressStrategy: apiv1.UpgradeStrategyPPND,
			expectedPauseRequest:      &trueValue,
			expectedRolloutPhase:      apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
			},
			expectedNumaflowControllerVersion: "3.2.1",
			expectedInProgressStrategy:        apiv1.UpgradeStrategyPPND,
		},
		{
			name:                          "existing NumaflowController - spec already updated - NumaflowController done reconciling",
			newNumaflowControllerVersion:  "1.2.3",
			existingNumaflowControllerDef: createDefaultNumaflowController("1.2.3", apiv1.PhaseDeployed, true),
			existingDeploymentDef:         createDefaultNumaflowControllerDeployment("1.2.3", true),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}),
			existingPipeline:          ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhasePaused),
			existingPauseRequest:      &trueValue,
			initialInProgressStrategy: apiv1.UpgradeStrategyPPND,
			expectedPauseRequest:      &falseValue,
			expectedRolloutPhase:      apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
			},
			expectedNumaflowControllerVersion: "1.2.3",
			expectedInProgressStrategy:        apiv1.UpgradeStrategyNoOp,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			// first delete any previous resources if they already exist, in Kubernetes
			_ = client.Delete(ctx, &apiv1.NumaflowController{ObjectMeta: metav1.ObjectMeta{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestNumaflowControllerName}})
			_ = k8sClientSet.AppsV1().Deployments(ctlrcommon.DefaultTestNamespace).Delete(ctx, ctlrcommon.DefaultTestNumaflowControllerDeploymentName, metav1.DeleteOptions{})
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).Delete(ctx, ctlrcommon.DefaultTestPipelineName, metav1.DeleteOptions{})
			_ = client.Delete(ctx, &apiv1.PipelineRollout{ObjectMeta: metav1.ObjectMeta{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestPipelineRolloutName}})

			nfcList := apiv1.NumaflowControllerList{}
			err := client.List(ctx, &nfcList, &crCli.ListOptions{
				Namespace: ctlrcommon.DefaultTestNamespace,
			})
			assert.NoError(t, err)
			assert.Len(t, nfcList.Items, 0)
			deplList, err := k8sClientSet.AppsV1().Deployments(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Len(t, deplList.Items, 0)
			pipelineList, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Len(t, pipelineList.Items, 0)

			// create NumaflowControllerRollout definition
			nfcRollout := createNumaflowControllerRollout(tc.newNumaflowControllerVersion)

			// the Reconcile() function does this, so we need to do it before calling reconcile() as well
			nfcRollout.Status.Init(nfcRollout.Generation)

			// create the already-existing NumaflowController in Kubernetes
			if tc.existingNumaflowControllerDef != nil {
				ctlrcommon.CreateNumaflowControllerInK8S(ctx, t, client, tc.existingNumaflowControllerDef)
			}

			// create the already-existing Deployment in Kubernetes
			if tc.existingDeploymentDef != nil {
				ctlrcommon.CreateDeploymentInK8S(ctx, t, k8sClientSet, tc.existingDeploymentDef)
			}

			ctlrcommon.CreatePipelineRolloutInK8S(ctx, t, client, tc.existingPipelineRollout)

			ctlrcommon.CreatePipelineInK8S(ctx, t, numaflowClientSet, tc.existingPipeline)

			pm := ppnd.GetPauseModule()
			pm.PauseRequests[pm.GetNumaflowControllerKey(ctlrcommon.DefaultTestNamespace)] = tc.existingPauseRequest

			nfcRollout.Status.UpgradeInProgress = tc.initialInProgressStrategy
			r.inProgressStrategyMgr.Store.SetStrategy(k8stypes.NamespacedName{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestPipelineRolloutName}, tc.initialInProgressStrategy)

			// call reconcile()
			_, err = r.reconcile(ctx, nfcRollout, ctlrcommon.DefaultTestNamespace, time.Now())
			assert.NoError(t, err)

			////// check results:

			// Check in-memory pause request:
			pauseRequestVal := pm.PauseRequests[pm.GetNumaflowControllerKey(ctlrcommon.DefaultTestNamespace)]
			if pauseRequestVal != nil && tc.expectedPauseRequest != nil {
				assert.Equal(t, *tc.expectedPauseRequest, *pauseRequestVal)
			} else {
				assert.Equal(t, tc.expectedPauseRequest, pauseRequestVal)
			}

			// Check Phase of NumaflowControllerRollout:
			assert.Equal(t, tc.expectedRolloutPhase, nfcRollout.Status.Phase)
			// Check In-Progress Strategy
			assert.Equal(t, tc.expectedInProgressStrategy, nfcRollout.Status.UpgradeInProgress)
			// Check NumaflowController
			numaflowController := &apiv1.NumaflowController{}
			err = client.Get(ctx, k8stypes.NamespacedName{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestNumaflowControllerName}, numaflowController)
			assert.NoError(t, err)
			assert.NotNil(t, numaflowController)
			assert.Equal(t, tc.expectedNumaflowControllerVersion, numaflowController.Spec.Version)

			// Check Conditions:
			for conditionType, conditionStatus := range tc.expectedConditionsSet {
				found := false
				for _, condition := range nfcRollout.Status.Conditions {
					if condition.Type == string(conditionType) && condition.Status == conditionStatus {
						found = true
						break
					}
				}
				assert.True(t, found, "condition type %s failed, conditions=%+v", conditionType, nfcRollout.Status.Conditions)
			}

		})
	}
}

func createDefaultNumaflowController(version string, phase apiv1.Phase, fullyReconciled bool) *apiv1.NumaflowController {
	status := apiv1.NumaflowControllerStatus{
		Status: apiv1.Status{
			Phase: phase,
		},
	}

	if fullyReconciled {
		status.ObservedGeneration = 1
	} else {
		status.ObservedGeneration = 0
	}

	return &apiv1.NumaflowController{
		TypeMeta: metav1.TypeMeta{
			Kind:       apiv1.NumaflowControllerGroupVersionKind.Kind,
			APIVersion: apiv1.NumaflowControllerGroupVersionKind.GroupVersion().String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ctlrcommon.DefaultTestNumaflowControllerRolloutName,
			Namespace: ctlrcommon.DefaultTestNamespace,
			Labels: map[string]string{
				common.LabelKeyParentRollout: ctlrcommon.DefaultTestNumaflowControllerRolloutName,
			},
		},
		Spec: apiv1.NumaflowControllerSpec{
			Version: version,
		},
		Status: status,
	}
}

func createDefaultNumaflowControllerDeployment(version string, fullyReconciled bool) *appsv1.Deployment {
	defaultImagePath := "quay.io/numaproj/numaflow:v"
	replicas := int32(3)

	var status appsv1.DeploymentStatus
	if fullyReconciled {
		status.ObservedGeneration = 1
		status.Replicas = replicas
		status.UpdatedReplicas = replicas
		// status.AvailableReplicas = 1
		// status.ReadyReplicas = 1
	} else {
		status.ObservedGeneration = 0
		status.Replicas = replicas
	}

	labels := map[string]string{
		"app.kubernetes.io/component": "controller-manager",
		"numaflow.numaproj.io/name":   "controller-manager",
		"app.kubernetes.io/part-of":   "numaflow",
	}

	selector := metav1.LabelSelector{MatchLabels: labels}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ctlrcommon.DefaultTestNamespace,
			Name:      ctlrcommon.DefaultTestNumaflowControllerDeploymentName,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &selector,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: v1.PodSpec{Containers: []v1.Container{{
					Name:  "controller-manager",
					Image: fmt.Sprintf("%s%s", defaultImagePath, version),
				}}},
			},
		},
		Status: status,
	}
}

func createNumaflowControllerRollout(version string) *apiv1.NumaflowControllerRollout {
	return &apiv1.NumaflowControllerRollout{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         ctlrcommon.DefaultTestNamespace,
			Name:              ctlrcommon.DefaultTestNumaflowControllerRolloutName,
			UID:               "some-uid",
			CreationTimestamp: metav1.NewTime(time.Now()),
			Generation:        1,
		},
		Spec: apiv1.NumaflowControllerRolloutSpec{
			Controller: apiv1.Controller{
				Version: version,
			},
		},
	}
}
