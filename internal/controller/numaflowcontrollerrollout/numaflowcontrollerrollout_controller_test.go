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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	k8sclientgo "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/controller/pipelinerollout"
	"github.com/numaproj/numaplane/internal/controller/ppnd"
	"github.com/numaproj/numaplane/internal/controller/progressive"
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
	assert.Nil(t, kubernetes.SetClientSets(restConfig))

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig"))
	assert.NoError(t, err)

	usdeConfig := config.USDEConfig{
		"numaflowcontroller": config.USDEResourceConfig{
			Recreate: []config.SpecField{{Path: "spec", IncludeSubfields: true}},
		},
	}

	config.GetConfigManagerInstance().UpdateUSDEConfig(usdeConfig)

	// other tests may call this, but it fails if called more than once
	if ctlrcommon.TestCustomMetrics == nil {
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics(numaLogger)
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
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}, nil),
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
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}, nil),
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
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}, nil),
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
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}, nil),
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
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}, nil),
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
				map[string]string{common.LabelKeyAllowDataLoss: "true"}, map[string]string{}, map[string]string{}, map[string]string{}, nil),
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			// first delete any previous resources if they already exist, in Kubernetes
			deleteNumaflowControllerTestResources(ctx, t, client, k8sClientSet)
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).Delete(ctx, ctlrcommon.DefaultTestPipelineName, metav1.DeleteOptions{})
			_ = client.Delete(ctx, &apiv1.PipelineRollout{ObjectMeta: metav1.ObjectMeta{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestPipelineRolloutName}})

			pipelineList, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Len(t, pipelineList.Items, 0)

			// create NumaflowControllerRollout definition, in Kubernetes as well since the reconciler updates its Status while naming children
			nfcRollout := createNumaflowControllerRollout(tc.newNumaflowControllerVersion)
			ctlrcommon.CreateNumaflowControllerRolloutInK8S(ctx, t, client, nfcRollout)

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

// deleteNumaflowControllerTestResources deletes the NumaflowControllerRollout, all NumaflowControllers and Deployments in the test namespace,
// and waits for them to be gone
func deleteNumaflowControllerTestResources(ctx context.Context, t *testing.T, client crCli.Client, k8sClientSet *k8sclientgo.Clientset) {
	_ = client.Delete(ctx, &apiv1.NumaflowControllerRollout{ObjectMeta: metav1.ObjectMeta{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestNumaflowControllerRolloutName}})
	_ = client.DeleteAllOf(ctx, &apiv1.NumaflowController{}, crCli.InNamespace(ctlrcommon.DefaultTestNamespace))
	_ = k8sClientSet.AppsV1().Deployments(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})

	assert.Eventually(t, func() bool {
		nfcRolloutList := apiv1.NumaflowControllerRolloutList{}
		if err := client.List(ctx, &nfcRolloutList, &crCli.ListOptions{Namespace: ctlrcommon.DefaultTestNamespace}); err != nil || len(nfcRolloutList.Items) > 0 {
			return false
		}
		nfcList := apiv1.NumaflowControllerList{}
		if err := client.List(ctx, &nfcList, &crCli.ListOptions{Namespace: ctlrcommon.DefaultTestNamespace}); err != nil || len(nfcList.Items) > 0 {
			return false
		}
		deplList, err := k8sClientSet.AppsV1().Deployments(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
		return err == nil && len(deplList.Items) == 0
	}, 10*time.Second, 100*time.Millisecond, "test resources were not deleted")
}

// createDefaultNumaflowController creates the "promoted" NumaflowController child as this reconciler names it
func createDefaultNumaflowController(version string, phase apiv1.Phase, fullyReconciled bool) *apiv1.NumaflowController {
	return createNumaflowController(ctlrcommon.DefaultTestNumaflowControllerName, version, "", common.LabelValueUpgradePromoted, phase, fullyReconciled, nil)
}

// createLegacyNumaflowController creates a NumaflowController as it was created before Progressive support:
// named after the Rollout, with no upgrade-state label and no instance
func createLegacyNumaflowController(version string, phase apiv1.Phase, fullyReconciled bool) *apiv1.NumaflowController {
	nfc := createNumaflowController(ctlrcommon.DefaultTestNumaflowControllerRolloutName, version, "", "", phase, fullyReconciled, nil)
	delete(nfc.Labels, common.LabelKeyUpgradeState)
	return nfc
}

func createNumaflowController(name string, version string, instanceID string, upgradeState common.UpgradeState, phase apiv1.Phase, fullyReconciled bool, conditions []metav1.Condition) *apiv1.NumaflowController {
	status := apiv1.NumaflowControllerStatus{
		Status: apiv1.Status{
			Phase:      phase,
			Conditions: conditions,
		},
	}

	if fullyReconciled {
		status.ObservedGeneration = 1
	} else {
		status.ObservedGeneration = 0
	}

	labels := map[string]string{
		common.LabelKeyParentRollout: ctlrcommon.DefaultTestNumaflowControllerRolloutName,
		common.LabelKeyUpgradeState:  string(upgradeState),
	}
	if instanceID != "" {
		labels[common.LabelKeyControllerInstanceID] = instanceID
	}

	return &apiv1.NumaflowController{
		TypeMeta: metav1.TypeMeta{
			Kind:       apiv1.NumaflowControllerGroupVersionKind.Kind,
			APIVersion: apiv1.NumaflowControllerGroupVersionKind.GroupVersion().String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ctlrcommon.DefaultTestNamespace,
			Labels:    labels,
		},
		Spec: apiv1.NumaflowControllerSpec{
			Version:    version,
			InstanceID: instanceID,
		},
		Status: status,
	}
}

func healthyChildResourcesCondition() []metav1.Condition {
	return []metav1.Condition{{
		Type:               string(apiv1.ConditionChildResourceHealthy),
		Status:             metav1.ConditionTrue,
		Reason:             "Successful",
		LastTransitionTime: metav1.NewTime(time.Now()),
	}}
}

func Test_reconcile_NumaflowControllerRollout_Progressive(t *testing.T) {
	ctx := context.Background()

	numaLogger := logger.New()
	numaLogger.SetLevel(3)
	logger.SetBaseLogger(numaLogger)
	ctx = logger.WithLogger(ctx, numaLogger)

	restConfig, numaflowClientSet, client, k8sClientSet, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetClientSets(restConfig))

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	// testconfig2 uses "progressive" as the default upgrade strategy
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig2"))
	assert.NoError(t, err)

	usdeConfig := config.USDEConfig{
		"numaflowcontroller": config.USDEResourceConfig{
			DataLoss: []config.SpecField{{Path: "spec.version", IncludeSubfields: false}},
		},
	}
	config.GetConfigManagerInstance().UpdateUSDEConfig(usdeConfig)

	// other tests may call this, but it fails if called more than once
	if ctlrcommon.TestCustomMetrics == nil {
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics(numaLogger)
	}

	recorder := record.NewFakeRecorder(64)
	r := NewNumaflowControllerRolloutReconciler(client, scheme.Scheme, ctlrcommon.TestCustomMetrics, recorder)
	pipelinerollout.PipelineROReconciler = &pipelinerollout.PipelineRolloutReconciler{Queue: util.NewWorkQueue("fake_queue")}

	promotedName := ctlrcommon.DefaultTestNumaflowControllerName            // numaflow-controller-0
	trialName := ctlrcommon.DefaultTestNumaflowControllerRolloutName + "-1" // numaflow-controller-1
	trialInstanceID := "3-2-1-1"                                            // derived from version 3.2.1 and name count 1
	pastTime := metav1.NewTime(time.Now().Add(-time.Minute))
	originalInstance := apiv1.ControllerInstanceStatus{InstanceID: "", Version: "1.2.3"}
	trialInstance := apiv1.ControllerInstanceStatus{InstanceID: trialInstanceID, Version: "3.2.1"}

	// the Rollout's Status when the trial child has already been created and initialized, so the next reconciliation assesses it
	assessingTrialStatus := func() apiv1.NumaflowControllerRolloutStatus {
		nameCount := int32(2)
		return apiv1.NumaflowControllerRolloutStatus{
			Status:    apiv1.Status{UpgradeInProgress: apiv1.UpgradeStrategyProgressive},
			NameCount: &nameCount,
			ProgressiveStatus: apiv1.NumaflowControllerProgressiveStatus{
				UpgradingNumaflowControllerStatus: &apiv1.UpgradingNumaflowControllerStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                     trialName,
						InitializationComplete:   true,
						AssessmentResult:         apiv1.AssessmentResultUnknown,
						BasicAssessmentStartTime: &pastTime,
					},
					ControllerInstanceStatus: trialInstance,
				},
				PromotedNumaflowControllerStatus: &apiv1.PromotedNumaflowControllerStatus{
					PromotedChildStatus:      apiv1.PromotedChildStatus{Name: promotedName},
					ControllerInstanceStatus: originalInstance,
				},
			},
		}
	}

	// a MonoVertexRollout whose trial MonoVertex is bound to the given controller instance and has the given assessment
	monoVertexRolloutOnInstance := func(instanceID string, assessment apiv1.AssessmentResult) (*apiv1.MonoVertexRollout, *numaflowv1.MonoVertex) {
		mvName := ctlrcommon.DefaultTestMonoVertexRolloutName + "-1"
		mvRollout := ctlrcommon.CreateTestMVRollout(monoVertexSpec, nil, nil, nil, nil, &apiv1.MonoVertexRolloutStatus{
			ProgressiveStatus: apiv1.MonoVertexProgressiveStatus{
				UpgradingMonoVertexStatus: &apiv1.UpgradingMonoVertexStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{Name: mvName, AssessmentResult: assessment},
					},
				},
			},
		})
		mv := ctlrcommon.CreateTestMonoVertexOfSpec(monoVertexSpec, mvName, numaflowv1.MonoVertexPhaseRunning, numaflowv1.Status{},
			map[string]string{common.LabelKeyParentRollout: ctlrcommon.DefaultTestMonoVertexRolloutName, common.LabelKeyUpgradeState: string(common.LabelValueUpgradeTrial)},
			map[string]string{common.AnnotationKeyNumaflowInstanceID: instanceID})
		return mvRollout, mv
	}

	testCases := []struct {
		name                          string
		newNumaflowControllerVersion  string
		initialRolloutStatus          apiv1.NumaflowControllerRolloutStatus
		existingNumaflowControllers   []*apiv1.NumaflowController
		existingMonoVertexRollout     *apiv1.MonoVertexRollout
		existingMonoVertex            *numaflowv1.MonoVertex
		expectedRolloutPhase          apiv1.Phase
		expectedInProgressStrategy    apiv1.UpgradeStrategy
		expectedUpgradingAssessment   *apiv1.AssessmentResult // nil means no UpgradingChildStatus expected
		expectedControllers           map[string]expectedController
		expectedPromotedName          string
		expectedPromotedInstance      apiv1.ControllerInstanceStatus
		expectedUpgradingInstance     apiv1.ControllerInstanceStatus // only checked if expectedUpgradingAssessment is set
		expectedControllerInstanceIDs [2]string                      // promoted, trial as resolved by dependents
	}{
		{
			name:                         "new NumaflowController is named and labeled",
			newNumaflowControllerVersion: "1.2.3",
			expectedRolloutPhase:         apiv1.PhaseDeployed,
			expectedInProgressStrategy:   apiv1.UpgradeStrategyNoOp,
			expectedControllers: map[string]expectedController{
				promotedName: {version: "1.2.3", instanceID: "", upgradeState: common.LabelValueUpgradePromoted},
			},
			expectedPromotedName:     promotedName,
			expectedPromotedInstance: originalInstance,
		},
		{
			name:                         "pre-existing NumaflowController is adopted as promoted",
			newNumaflowControllerVersion: "1.2.3",
			existingNumaflowControllers:  []*apiv1.NumaflowController{createLegacyNumaflowController("1.2.3", apiv1.PhaseDeployed, true)},
			expectedRolloutPhase:         apiv1.PhaseDeployed,
			expectedInProgressStrategy:   apiv1.UpgradeStrategyNoOp,
			expectedControllers: map[string]expectedController{
				ctlrcommon.DefaultTestNumaflowControllerRolloutName: {version: "1.2.3", instanceID: "", upgradeState: common.LabelValueUpgradePromoted},
			},
			expectedPromotedName:     ctlrcommon.DefaultTestNumaflowControllerRolloutName,
			expectedPromotedInstance: originalInstance,
		},
		{
			name:                         "new version creates trial child on its own instance while promoted keeps serving",
			newNumaflowControllerVersion: "3.2.1",
			initialRolloutStatus:         apiv1.NumaflowControllerRolloutStatus{NameCount: ptr.To(int32(1))},
			existingNumaflowControllers:  []*apiv1.NumaflowController{createDefaultNumaflowController("1.2.3", apiv1.PhaseDeployed, true)},
			expectedRolloutPhase:         apiv1.PhasePending,
			expectedInProgressStrategy:   apiv1.UpgradeStrategyProgressive,
			expectedUpgradingAssessment:  ptr.To(apiv1.AssessmentResultUnknown),
			expectedControllers: map[string]expectedController{
				promotedName: {version: "1.2.3", instanceID: "", upgradeState: common.LabelValueUpgradePromoted},
				trialName:    {version: "3.2.1", instanceID: trialInstanceID, upgradeState: common.LabelValueUpgradeTrial},
			},
			expectedPromotedName:          promotedName,
			expectedPromotedInstance:      originalInstance,
			expectedUpgradingInstance:     trialInstance,
			expectedControllerInstanceIDs: [2]string{"", trialInstanceID},
		},
		{
			name:                         "healthy trial with no dependents is promoted",
			newNumaflowControllerVersion: "3.2.1",
			initialRolloutStatus:         assessingTrialStatus(),
			existingNumaflowControllers: []*apiv1.NumaflowController{
				createDefaultNumaflowController("1.2.3", apiv1.PhaseDeployed, true),
				createNumaflowController(trialName, "3.2.1", trialInstanceID, common.LabelValueUpgradeTrial, apiv1.PhaseDeployed, true, healthyChildResourcesCondition()),
			},
			expectedRolloutPhase:        apiv1.PhaseDeployed,
			expectedInProgressStrategy:  apiv1.UpgradeStrategyNoOp,
			expectedUpgradingAssessment: ptr.To(apiv1.AssessmentResultSuccess),
			expectedControllers: map[string]expectedController{
				promotedName: {version: "1.2.3", instanceID: "", upgradeState: common.LabelValueUpgradeRecyclable},
				trialName:    {version: "3.2.1", instanceID: trialInstanceID, upgradeState: common.LabelValueUpgradePromoted},
			},
			expectedPromotedName:          trialName,
			expectedPromotedInstance:      trialInstance,
			expectedUpgradingInstance:     trialInstance,
			expectedControllerInstanceIDs: [2]string{trialInstanceID, ""},
		},
		{
			name:                         "trial is not assessed while a dependent is still upgrading",
			newNumaflowControllerVersion: "3.2.1",
			initialRolloutStatus:         assessingTrialStatus(),
			existingNumaflowControllers: []*apiv1.NumaflowController{
				createDefaultNumaflowController("1.2.3", apiv1.PhaseDeployed, true),
				createNumaflowController(trialName, "3.2.1", trialInstanceID, common.LabelValueUpgradeTrial, apiv1.PhaseDeployed, true, healthyChildResourcesCondition()),
			},
			expectedRolloutPhase:        apiv1.PhasePending,
			expectedInProgressStrategy:  apiv1.UpgradeStrategyProgressive,
			expectedUpgradingAssessment: ptr.To(apiv1.AssessmentResultUnknown),
			expectedControllers: map[string]expectedController{
				promotedName: {version: "1.2.3", instanceID: "", upgradeState: common.LabelValueUpgradePromoted},
				trialName:    {version: "3.2.1", instanceID: trialInstanceID, upgradeState: common.LabelValueUpgradeTrial},
			},
			expectedPromotedName:          promotedName,
			expectedPromotedInstance:      originalInstance,
			expectedUpgradingInstance:     trialInstance,
			expectedControllerInstanceIDs: [2]string{"", trialInstanceID},
		},
		{
			name:                         "trial is promoted once dependents succeed on it",
			newNumaflowControllerVersion: "3.2.1",
			initialRolloutStatus:         assessingTrialStatus(),
			existingNumaflowControllers: []*apiv1.NumaflowController{
				createDefaultNumaflowController("1.2.3", apiv1.PhaseDeployed, true),
				createNumaflowController(trialName, "3.2.1", trialInstanceID, common.LabelValueUpgradeTrial, apiv1.PhaseDeployed, true, healthyChildResourcesCondition()),
			},
			expectedRolloutPhase:        apiv1.PhaseDeployed,
			expectedInProgressStrategy:  apiv1.UpgradeStrategyNoOp,
			expectedUpgradingAssessment: ptr.To(apiv1.AssessmentResultSuccess),
			expectedControllers: map[string]expectedController{
				promotedName: {version: "1.2.3", instanceID: "", upgradeState: common.LabelValueUpgradeRecyclable},
				trialName:    {version: "3.2.1", instanceID: trialInstanceID, upgradeState: common.LabelValueUpgradePromoted},
			},
			expectedPromotedName:          trialName,
			expectedPromotedInstance:      trialInstance,
			expectedUpgradingInstance:     trialInstance,
			expectedControllerInstanceIDs: [2]string{trialInstanceID, ""},
		},
		{
			name:                         "trial fails when a dependent fails on it",
			newNumaflowControllerVersion: "3.2.1",
			initialRolloutStatus:         assessingTrialStatus(),
			existingNumaflowControllers: []*apiv1.NumaflowController{
				createDefaultNumaflowController("1.2.3", apiv1.PhaseDeployed, true),
				createNumaflowController(trialName, "3.2.1", trialInstanceID, common.LabelValueUpgradeTrial, apiv1.PhaseDeployed, true, healthyChildResourcesCondition()),
			},
			expectedRolloutPhase:        apiv1.PhasePending,
			expectedInProgressStrategy:  apiv1.UpgradeStrategyProgressive,
			expectedUpgradingAssessment: ptr.To(apiv1.AssessmentResultFailure),
			expectedControllers: map[string]expectedController{
				promotedName: {version: "1.2.3", instanceID: "", upgradeState: common.LabelValueUpgradePromoted},
				trialName:    {version: "3.2.1", instanceID: trialInstanceID, upgradeState: common.LabelValueUpgradeTrial},
			},
			expectedPromotedName:      promotedName,
			expectedPromotedInstance:  originalInstance,
			expectedUpgradingInstance: trialInstance,
			// a failed trial's instance is still resolved, the same way a Pipeline resolves a trial ISBService
			// regardless of its assessment: see getControllerInstanceIDOfUpgradeState.
			expectedControllerInstanceIDs: [2]string{"", trialInstanceID},
		},
	}
	// dependents for the dependent-driven cases
	testCases[4].existingMonoVertexRollout, testCases[4].existingMonoVertex = monoVertexRolloutOnInstance(trialInstanceID, apiv1.AssessmentResultUnknown)
	testCases[5].existingMonoVertexRollout, testCases[5].existingMonoVertex = monoVertexRolloutOnInstance(trialInstanceID, apiv1.AssessmentResultSuccess)
	testCases[6].existingMonoVertexRollout, testCases[6].existingMonoVertex = monoVertexRolloutOnInstance(trialInstanceID, apiv1.AssessmentResultFailure)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// first delete any previous resources if they already exist, in Kubernetes
			deleteNumaflowControllerTestResources(ctx, t, client, k8sClientSet)
			_ = client.DeleteAllOf(ctx, &apiv1.MonoVertexRollout{}, crCli.InNamespace(ctlrcommon.DefaultTestNamespace))
			_ = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})

			// create NumaflowControllerRollout definition, in Kubernetes as well since the reconciler updates its Status while naming children
			nfcRollout := createNumaflowControllerRollout(tc.newNumaflowControllerVersion)
			nfcRollout.Status = tc.initialRolloutStatus
			ctlrcommon.CreateNumaflowControllerRolloutInK8S(ctx, t, client, nfcRollout)

			// the Reconcile() function does this, so we need to do it before calling reconcile() as well
			nfcRollout.Status.Init(nfcRollout.Generation)

			for _, nfc := range tc.existingNumaflowControllers {
				ctlrcommon.CreateNumaflowControllerInK8S(ctx, t, client, nfc)
			}
			if tc.existingMonoVertexRollout != nil {
				ctlrcommon.CreateMVRolloutInK8S(ctx, t, client, tc.existingMonoVertexRollout)
				ctlrcommon.CreateMonoVertexInK8S(ctx, t, numaflowClientSet, tc.existingMonoVertex)
			}

			r.inProgressStrategyMgr.Store.SetStrategy(k8stypes.NamespacedName{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestNumaflowControllerRolloutName}, tc.initialRolloutStatus.UpgradeInProgress)

			// call reconcile()
			_, err = r.reconcile(ctx, nfcRollout, ctlrcommon.DefaultTestNamespace, time.Now())
			assert.NoError(t, err)

			////// check results:
			assert.Equal(t, tc.expectedRolloutPhase, nfcRollout.Status.Phase)
			assert.Equal(t, tc.expectedInProgressStrategy, nfcRollout.Status.UpgradeInProgress)
			if tc.expectedUpgradingAssessment == nil {
				assert.Nil(t, nfcRollout.GetUpgradingChildStatus())
			} else if assert.NotNil(t, nfcRollout.GetUpgradingChildStatus()) {
				assert.Equal(t, trialName, nfcRollout.GetUpgradingChildStatus().Name)
				assert.Equal(t, *tc.expectedUpgradingAssessment, nfcRollout.GetUpgradingChildStatus().AssessmentResult)
				assert.Equal(t, tc.expectedUpgradingInstance, nfcRollout.Status.ProgressiveStatus.UpgradingNumaflowControllerStatus.ControllerInstanceStatus)
			}
			if assert.NotNil(t, nfcRollout.Status.ProgressiveStatus.PromotedNumaflowControllerStatus) {
				promotedStatus := nfcRollout.Status.ProgressiveStatus.PromotedNumaflowControllerStatus
				assert.Equal(t, tc.expectedPromotedName, promotedStatus.Name)
				assert.Equal(t, tc.expectedPromotedInstance, promotedStatus.ControllerInstanceStatus)
			}

			// Check the NumaflowController children
			nfcList := apiv1.NumaflowControllerList{}
			assert.NoError(t, client.List(ctx, &nfcList, &crCli.ListOptions{Namespace: ctlrcommon.DefaultTestNamespace}))
			assert.Len(t, nfcList.Items, len(tc.expectedControllers))
			for _, nfc := range nfcList.Items {
				expected, found := tc.expectedControllers[nfc.Name]
				if !assert.True(t, found, "unexpected NumaflowController %q", nfc.Name) {
					continue
				}
				assert.Equal(t, expected.version, nfc.Spec.Version, "version of %q", nfc.Name)
				assert.Equal(t, expected.instanceID, nfc.Spec.InstanceID, "instanceID of %q", nfc.Name)
				assert.Equal(t, string(expected.upgradeState), nfc.Labels[common.LabelKeyUpgradeState], "upgrade state of %q", nfc.Name)
				assert.Equal(t, ctlrcommon.DefaultTestNumaflowControllerRolloutName, nfc.Labels[common.LabelKeyParentRollout], "parent rollout label of %q", nfc.Name)
				assert.Equal(t, expected.instanceID, nfc.Labels[common.LabelKeyControllerInstanceID], "instance label of %q", nfc.Name)
			}

			// Check how dependents resolve the instances from the children's labels
			promotedInstanceID, trialInstanceID, err := ctlrcommon.GetControllerInstanceIDs(ctx, client, ctlrcommon.DefaultTestNamespace)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedControllerInstanceIDs, [2]string{promotedInstanceID, trialInstanceID})
		})
	}
}

type expectedController struct {
	version      string
	instanceID   string
	upgradeState common.UpgradeState
}

func Test_DeriveControllerInstanceID(t *testing.T) {
	testCases := []struct {
		version   string
		childName string
		expected  string
	}{
		{version: "1.5.2", childName: "numaflow-controller-3", expected: "1-5-2-3"},
		{version: "v1.5.2", childName: "my-controller-0", expected: "v1-5-2-0"},
		{version: "1.5.2-RC.1", childName: "numaflow-controller-12", expected: "1-5-2-rc-1-12"},
		{version: "1.5.2", childName: "numaflow-controller", expected: ""}, // pre-Progressive name: keeps the empty instance
		{version: "1.5.2", childName: "numaflow-controller-", expected: ""},
		{version: "1.5.2", childName: "3", expected: ""},
		{version: "...", childName: "numaflow-controller-1", expected: "1"},
		// 33 characters max: "1-1-...-1" truncated to 31 characters, then "-7"
		{version: strings.Repeat("1.", 40), childName: "numaflow-controller-7", expected: strings.TrimRight(strings.Repeat("1-", 40)[:31], "-") + "-7"},
	}
	for _, tc := range testCases {
		t.Run(tc.version+"/"+tc.childName, func(t *testing.T) {
			instanceID := DeriveControllerInstanceID(tc.version, tc.childName)
			assert.Equal(t, tc.expected, instanceID)
			if instanceID != "" {
				// the longest Service in the controller manifest is "numaflow-dex-server-<instance>"
				assert.Empty(t, validation.IsDNS1035Label("numaflow-dex-server-"+instanceID), "instance ID must be usable as a Service name suffix")
				assert.Empty(t, validation.IsValidLabelValue(instanceID), "instance ID must be usable as a label value")
			}
		})
	}
}

func Test_CheckForDifferences_IgnoresInstanceID(t *testing.T) {
	ctx := context.Background()
	r := &NumaflowControllerRolloutReconciler{}
	nfcRollout := createNumaflowControllerRollout("1.2.3")

	existing, err := makeNumaflowControllerDefinition(nfcRollout, "numaflow-controller-0", "0", common.LabelValueUpgradePromoted)
	assert.NoError(t, err)
	sameVersionOtherInstance, err := makeNumaflowControllerDefinition(nfcRollout, "numaflow-controller-1", "1-2-3-1", common.LabelValueUpgradeTrial)
	assert.NoError(t, err)
	requiredMetadata := map[string]interface{}{"labels": map[string]interface{}{common.LabelKeyParentRollout: nfcRollout.Name}}

	different, err := r.CheckForDifferences(ctx, nfcRollout, existing, sameVersionOtherInstance.Object, requiredMetadata, common.LabelValueUpgradePromoted)
	assert.NoError(t, err)
	assert.False(t, different, "a different instance alone is not a difference")

	nfcRollout.Spec.Controller.Version = "3.2.1"
	newVersion, err := makeNumaflowControllerDefinition(nfcRollout, "numaflow-controller-1", "3-2-1-1", common.LabelValueUpgradeTrial)
	assert.NoError(t, err)
	different, err = r.CheckForDifferences(ctx, nfcRollout, existing, newVersion.Object, requiredMetadata, common.LabelValueUpgradePromoted)
	assert.NoError(t, err)
	assert.True(t, different, "a different version is a difference")

	// the Rollout-based comparison carries over the existing child's instance
	different, err = r.CheckForDifferencesWithRolloutDef(ctx, existing, nfcRollout, common.LabelValueUpgradePromoted)
	assert.NoError(t, err)
	assert.True(t, different)
	nfcRollout.Spec.Controller.Version = "1.2.3"
	different, err = r.CheckForDifferencesWithRolloutDef(ctx, existing, nfcRollout, common.LabelValueUpgradePromoted)
	assert.NoError(t, err)
	assert.False(t, different)
}

func Test_SkipProgressiveAssessment(t *testing.T) {
	tests := []struct {
		name           string
		strategy       *apiv1.NumaflowControllerRolloutStrategy
		expectedSkip   bool
		expectedReason progressive.SkipProgressiveAssessmentReason
	}{
		{
			name:           "no strategy",
			strategy:       nil,
			expectedSkip:   false,
			expectedReason: progressive.SkipProgressiveAssessmentReasonUndefined,
		},
		{
			name:           "forcePromote not set",
			strategy:       &apiv1.NumaflowControllerRolloutStrategy{Progressive: apiv1.ProgressiveStrategy{AssessmentSchedule: "60,60,10"}},
			expectedSkip:   false,
			expectedReason: progressive.SkipProgressiveAssessmentReasonUndefined,
		},
		{
			name:           "forcePromote set",
			strategy:       &apiv1.NumaflowControllerRolloutStrategy{Progressive: apiv1.ProgressiveStrategy{ForcePromote: true}},
			expectedSkip:   true,
			expectedReason: progressive.SkipProgressiveAssessmentReasonRolloutConfiguration,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &NumaflowControllerRolloutReconciler{}
			nfcRollout := createNumaflowControllerRollout("1.2.3")
			nfcRollout.Spec.Strategy = tc.strategy

			skip, reason, err := r.SkipProgressiveAssessment(context.Background(), nfcRollout)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedSkip, skip)
			assert.Equal(t, tc.expectedReason, reason)
		})
	}
}

var monoVertexSpec = numaflowv1.MonoVertexSpec{
	Replicas: ptr.To(int32(1)),
	Source: &numaflowv1.Source{
		UDSource: &numaflowv1.UDSource{
			Container: &numaflowv1.Container{
				Image: "quay.io/numaio/numaflow-java/source-simple-source:stable",
			},
		},
	},
	Sink: &numaflowv1.Sink{
		AbstractSink: numaflowv1.AbstractSink{
			UDSink: &numaflowv1.UDSink{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-java/simple-sink:stable",
				},
			},
		},
	},
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
