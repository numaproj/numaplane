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

package isbservicerollout

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sRuntime "k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"

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
)

// test reconcile() for the case of PPND

func Test_reconcile_isbservicerollout_PPND(t *testing.T) {
	ctx := context.Background()

	numaLogger := logger.New()
	numaLogger.SetLevel(4)
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
		"interstepbufferservice": config.USDEResourceConfig{
			DataLoss: []config.SpecField{{Path: "spec", IncludeSubfields: true}},
		},
	}

	config.GetConfigManagerInstance().UpdateUSDEConfig(usdeConfig)

	if ctlrcommon.TestCustomMetrics == nil {
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics(numaLogger)
	}

	recorder := record.NewFakeRecorder(64)

	r := NewISBServiceRolloutReconciler(client, scheme.Scheme, ctlrcommon.TestCustomMetrics, recorder)

	trueValue := true
	falseValue := false

	pipelinerollout.PipelineROReconciler = &pipelinerollout.PipelineRolloutReconciler{Queue: util.NewWorkQueue("fake_queue")}

	testCases := []struct {
		name                      string
		newISBSvcSpec             numaflowv1.InterStepBufferServiceSpec
		existingISBSvcDef         *numaflowv1.InterStepBufferService
		existingStatefulSetDef    *appsv1.StatefulSet
		existingPipelineRollout   *apiv1.PipelineRollout
		existingPipeline          *numaflowv1.Pipeline
		existingPauseRequest      *bool // was ISBServiceRollout previously requesting pause?
		initialInProgressStrategy apiv1.UpgradeStrategy
		expectedPauseRequest      *bool // after reconcile(), should it be requesting pause?
		expectedRolloutPhase      apiv1.Phase
		// require these Conditions to be set (note that in real life, previous reconciliations may have set other Conditions from before which are still present)
		expectedConditionsSet      map[apiv1.ConditionType]metav1.ConditionStatus
		expectedISBSvcSpec         numaflowv1.InterStepBufferServiceSpec
		expectedInProgressStrategy apiv1.UpgradeStrategy
	}{
		{
			name:                   "new ISBService",
			newISBSvcSpec:          ctlrcommon.CreateDefaultISBServiceSpec("2.10.3"),
			existingISBSvcDef:      nil,
			existingStatefulSetDef: nil,
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
			expectedISBSvcSpec:         ctlrcommon.CreateDefaultISBServiceSpec("2.10.3"),
			expectedInProgressStrategy: apiv1.UpgradeStrategyNoOp,
		},
		{
			name:                   "existing ISBService - no change",
			newISBSvcSpec:          ctlrcommon.CreateDefaultISBServiceSpec("2.10.3"),
			existingISBSvcDef:      ctlrcommon.CreateDefaultISBService("2.10.3", numaflowv1.ISBSvcPhaseRunning, true),
			existingStatefulSetDef: createDefaultISBStatefulSet("2.10.3", true),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}, nil),
			existingPipeline:          ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhaseRunning),
			existingPauseRequest:      &falseValue,
			initialInProgressStrategy: apiv1.UpgradeStrategyNoOp,
			expectedPauseRequest:      &falseValue,
			expectedRolloutPhase:      apiv1.PhaseDeployed,
			expectedConditionsSet:     map[apiv1.ConditionType]metav1.ConditionStatus{}, // some Conditions may be set from before, but in any case nothing new to verify
			expectedISBSvcSpec:        ctlrcommon.CreateDefaultISBServiceSpec("2.10.3"),
		},
		{
			name:                   "existing ISBService - new spec - pipelines not paused",
			newISBSvcSpec:          ctlrcommon.CreateDefaultISBServiceSpec("2.10.11"),
			existingISBSvcDef:      ctlrcommon.CreateDefaultISBService("2.10.3", numaflowv1.ISBSvcPhaseRunning, true),
			existingStatefulSetDef: createDefaultISBStatefulSet("2.10.3", true),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}, nil),
			existingPipeline:           ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhaseRunning),
			existingPauseRequest:       &falseValue,
			initialInProgressStrategy:  apiv1.UpgradeStrategyNoOp,
			expectedPauseRequest:       &trueValue,
			expectedRolloutPhase:       apiv1.PhasePending,
			expectedConditionsSet:      map[apiv1.ConditionType]metav1.ConditionStatus{},
			expectedISBSvcSpec:         ctlrcommon.CreateDefaultISBServiceSpec("2.10.3"),
			expectedInProgressStrategy: apiv1.UpgradeStrategyPPND,
		},
		{
			name:                   "existing ISBService - new spec - pipelines paused",
			newISBSvcSpec:          ctlrcommon.CreateDefaultISBServiceSpec("2.10.11"),
			existingISBSvcDef:      ctlrcommon.CreateDefaultISBService("2.10.3", numaflowv1.ISBSvcPhaseRunning, true),
			existingStatefulSetDef: createDefaultISBStatefulSet("2.10.3", true),
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
			expectedISBSvcSpec:         ctlrcommon.CreateDefaultISBServiceSpec("2.10.11"),
			expectedInProgressStrategy: apiv1.UpgradeStrategyPPND,
		},
		{
			name:                   "existing ISBService - new spec - pipelines failed",
			newISBSvcSpec:          ctlrcommon.CreateDefaultISBServiceSpec("2.10.11"),
			existingISBSvcDef:      ctlrcommon.CreateDefaultISBService("2.10.3", numaflowv1.ISBSvcPhaseRunning, true),
			existingStatefulSetDef: createDefaultISBStatefulSet("2.10.3", true),
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
			expectedISBSvcSpec:         ctlrcommon.CreateDefaultISBServiceSpec("2.10.11"),
			expectedInProgressStrategy: apiv1.UpgradeStrategyPPND,
		},
		{
			name:                   "existing ISBService - new spec - pipelines set to allow data loss",
			newISBSvcSpec:          ctlrcommon.CreateDefaultISBServiceSpec("2.10.11"),
			existingISBSvcDef:      ctlrcommon.CreateDefaultISBService("2.10.3", numaflowv1.ISBSvcPhasePending, true),
			existingStatefulSetDef: createDefaultISBStatefulSet("2.10.3", true),
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
			expectedISBSvcSpec:         ctlrcommon.CreateDefaultISBServiceSpec("2.10.11"),
			expectedInProgressStrategy: apiv1.UpgradeStrategyPPND,
		},
		{
			name:                   "existing ISBService - spec already updated - isbsvc reconciling",
			newISBSvcSpec:          ctlrcommon.CreateDefaultISBServiceSpec("2.10.11"),
			existingISBSvcDef:      ctlrcommon.CreateDefaultISBService("2.10.11", numaflowv1.ISBSvcPhaseRunning, false),
			existingStatefulSetDef: createDefaultISBStatefulSet("2.10.3", false),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}, nil),
			existingPipeline:           ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhasePaused),
			existingPauseRequest:       &trueValue,
			initialInProgressStrategy:  apiv1.UpgradeStrategyPPND,
			expectedPauseRequest:       &trueValue,
			expectedRolloutPhase:       apiv1.PhaseDeployed,
			expectedConditionsSet:      map[apiv1.ConditionType]metav1.ConditionStatus{},
			expectedISBSvcSpec:         ctlrcommon.CreateDefaultISBServiceSpec("2.10.11"),
			expectedInProgressStrategy: apiv1.UpgradeStrategyPPND,
		},
		{
			name:                   "existing ISBService - spec already updated - isbsvc done reconciling",
			newISBSvcSpec:          ctlrcommon.CreateDefaultISBServiceSpec("2.10.11"),
			existingISBSvcDef:      ctlrcommon.CreateDefaultISBService("2.10.11", numaflowv1.ISBSvcPhaseRunning, true),
			existingStatefulSetDef: createDefaultISBStatefulSet("2.10.11", true),
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}, nil),
			existingPipeline:          ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhasePaused),
			existingPauseRequest:      &trueValue,
			initialInProgressStrategy: apiv1.UpgradeStrategyPPND,
			expectedPauseRequest:      &falseValue,
			expectedRolloutPhase:      apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
			},
			expectedISBSvcSpec:         ctlrcommon.CreateDefaultISBServiceSpec("2.10.11"),
			expectedInProgressStrategy: apiv1.UpgradeStrategyNoOp,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			// first delete any previous resources if they already exist, in Kubernetes
			_ = numaflowClientSet.NumaflowV1alpha1().InterStepBufferServices(ctlrcommon.DefaultTestNamespace).Delete(ctx, ctlrcommon.DefaultTestISBSvcName, metav1.DeleteOptions{})
			_ = k8sClientSet.AppsV1().StatefulSets(ctlrcommon.DefaultTestNamespace).Delete(ctx, deriveISBSvcStatefulSetName(ctlrcommon.DefaultTestISBSvcName), metav1.DeleteOptions{})
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).Delete(ctx, ctlrcommon.DefaultTestPipelineName, metav1.DeleteOptions{})
			_ = client.Delete(ctx, &apiv1.PipelineRollout{ObjectMeta: metav1.ObjectMeta{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestPipelineRolloutName}})
			_ = client.Delete(ctx, &apiv1.ISBServiceRollout{ObjectMeta: metav1.ObjectMeta{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestISBSvcRolloutName}})

			isbsvcList, err := numaflowClientSet.NumaflowV1alpha1().InterStepBufferServices(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Len(t, isbsvcList.Items, 0)
			ssList, err := k8sClientSet.AppsV1().StatefulSets(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Len(t, ssList.Items, 0)
			pipelineList, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Len(t, pipelineList.Items, 0)

			// create ISBServiceRollout definition
			rollout := ctlrcommon.CreateISBServiceRollout(tc.newISBSvcSpec, nil)
			ctlrcommon.CreateISBServiceRolloutInK8S(ctx, t, client, rollout)

			// the Reconcile() function does this, so we need to do it before calling reconcile() as well
			rollout.Status.Init(rollout.Generation)

			// create the already-existing ISBSvc in Kubernetes
			if tc.existingISBSvcDef != nil {
				ctlrcommon.CreateISBSvcInK8S(ctx, t, numaflowClientSet, tc.existingISBSvcDef)
			}

			// create the already-existing StatefulSet in Kubernetes
			if tc.existingStatefulSetDef != nil {
				ctlrcommon.CreateStatefulSetInK8S(ctx, t, k8sClientSet, tc.existingStatefulSetDef)
			}

			ctlrcommon.CreatePipelineRolloutInK8S(ctx, t, client, tc.existingPipelineRollout)

			ctlrcommon.CreatePipelineInK8S(ctx, t, numaflowClientSet, tc.existingPipeline)

			pm := ppnd.GetPauseModule()
			pm.PauseRequests[pm.GetISBServiceKey(ctlrcommon.DefaultTestNamespace, ctlrcommon.DefaultTestISBSvcRolloutName)] = tc.existingPauseRequest

			rollout.Status.UpgradeInProgress = tc.initialInProgressStrategy
			r.inProgressStrategyMgr.Store.SetStrategy(k8stypes.NamespacedName{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestPipelineRolloutName}, tc.initialInProgressStrategy)

			// call reconcile()
			_, err = r.reconcile(ctx, rollout, time.Now())
			assert.NoError(t, err)

			////// check results:

			// Check in-memory pause request:
			assert.Equal(t, tc.expectedPauseRequest, (pm.PauseRequests[pm.GetISBServiceKey(ctlrcommon.DefaultTestNamespace, ctlrcommon.DefaultTestISBSvcRolloutName)]))

			// Check Phase of Rollout:
			assert.Equal(t, tc.expectedRolloutPhase, rollout.Status.Phase)
			// Check In-Progress Strategy
			assert.Equal(t, tc.expectedInProgressStrategy, rollout.Status.UpgradeInProgress)
			// Check isbsvc
			resultISBSVC, err := numaflowClientSet.NumaflowV1alpha1().InterStepBufferServices(ctlrcommon.DefaultTestNamespace).Get(ctx, ctlrcommon.DefaultTestISBSvcName, metav1.GetOptions{})
			assert.NoError(t, err)
			assert.NotNil(t, resultISBSVC)
			assert.Equal(t, tc.expectedISBSvcSpec, resultISBSVC.Spec)

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

func createDefaultISBStatefulSet(jetstreamVersion string, fullyReconciled bool) *appsv1.StatefulSet {
	var status appsv1.StatefulSetStatus
	if fullyReconciled {
		status.ObservedGeneration = 1
		status.Replicas = 3
		status.UpdatedReplicas = 3
	} else {
		status.ObservedGeneration = 0
		status.Replicas = 3
	}
	replicas := int32(3)
	labels := map[string]string{
		"app.kubernetes.io/component":      "isbsvc",
		"app.kubernetes.io/managed-by":     "isbsvc-controller",
		"app.kubernetes.io/part-of":        "numaflow",
		"numaflow.numaproj.io/isbsvc-name": ctlrcommon.DefaultTestISBSvcName,
		"numaflow.numaproj.io/isbsvc-type": "jetstream",
	}
	selector := metav1.LabelSelector{MatchLabels: labels}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deriveISBSvcStatefulSetName(ctlrcommon.DefaultTestISBSvcName),
			Namespace: ctlrcommon.DefaultTestNamespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &selector,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: v1.PodSpec{Containers: []v1.Container{{
					Image: fmt.Sprintf("nats:%s", jetstreamVersion),
					Name:  "main",
				}}},
			},
		},
		Status: status,
	}
}

func deriveISBSvcStatefulSetName(isbsvcName string) string {
	return fmt.Sprintf("isbsvc-%s-js", isbsvcName)
}

// Technically, IsUpgradeReplacementRequired() function is in progressive.go file, but we test it here because we can take advantage of also testing code specific to the ISBServiceRollout controller.
func Test_ISBSvcRollout_IsUpgradeReplacementRequired(t *testing.T) {
	restConfig, _, client, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetClientSets(restConfig))

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig2"))
	assert.NoError(t, err)

	ctx := context.Background()

	// Create a real ISBServiceRolloutReconciler
	scheme := scheme.Scheme
	reconciler := NewISBServiceRolloutReconciler(client, scheme, nil, nil)

	// Create an ISBServiceSpec
	// For rollout spec, pass "{{.isbsvc-name}}" as nameTemplateValue
	// For child specs, pass the evaluated name like "my-isbsvc-0" as nameTemplateValue
	createISBServiceSpec := func(version string, nameTemplateValue string) numaflowv1.InterStepBufferServiceSpec {
		return numaflowv1.InterStepBufferServiceSpec{
			Redis: &numaflowv1.RedisBufferService{},
			JetStream: &numaflowv1.JetStreamBufferService{
				Version: version,
				Persistence: &numaflowv1.PersistenceStrategy{
					VolumeSize:       &numaflowv1.DefaultVolumeSize,
					StorageClassName: &nameTemplateValue,
				},
			},
		}
	}

	testCases := []struct {
		name                      string
		rolloutSpec               numaflowv1.InterStepBufferServiceSpec
		rolloutLabels             map[string]string
		rolloutAnnotations        map[string]string
		promotedChildSpec         numaflowv1.InterStepBufferServiceSpec
		promotedChildName         string
		promotedChildLabels       map[string]string
		promotedChildAnnotations  map[string]string
		upgradingChildSpec        numaflowv1.InterStepBufferServiceSpec
		upgradingChildName        string
		upgradingChildLabels      map[string]string
		upgradingChildAnnotations map[string]string
		expectedDiffFromUpgrading bool
		expectedDiffFromPromoted  bool
	}{

		{
			name:        "different from Upgrading only (different version) - rollout matches Promoted",
			rolloutSpec: createISBServiceSpec("2.10.11", "{{.isbsvc-name}}"),
			rolloutLabels: map[string]string{
				"my-label": "{{.isbsvc-name}}",
			},
			rolloutAnnotations: map[string]string{
				"my-annotation": "{{.isbsvc-name}}",
			},
			promotedChildSpec: createISBServiceSpec("2.10.11", "my-isbsvc-0"),
			promotedChildName: "my-isbsvc-0",
			promotedChildLabels: map[string]string{
				"my-label":   "my-isbsvc-0", // this is a match
				"my-label-2": "something",   // this is no problem to have an extra label
			},
			promotedChildAnnotations: map[string]string{
				"my-annotation":   "my-isbsvc-0", // this is a match
				"my-annotation-2": "something",   // this is no problem to have an extra annotation
			},
			upgradingChildSpec: createISBServiceSpec("2.10.3", "my-isbsvc-1"),
			upgradingChildName: "my-isbsvc-1",
			upgradingChildLabels: map[string]string{
				"my-label":   "my-isbsvc-1", // this is a match
				"my-label-2": "something",   // this is no problem to have an extra label
			},
			upgradingChildAnnotations: map[string]string{
				"my-annotation":   "my-isbsvc-1", // this is a match
				"my-annotation-2": "something",   // this is no problem to have an extra annotation
			},
			expectedDiffFromUpgrading: true,
			expectedDiffFromPromoted:  false,
		},
		{
			name:        "different from Promoted only (different version) - rollout matches Upgrading",
			rolloutSpec: createISBServiceSpec("2.10.3", "{{.isbsvc-name}}"),
			rolloutLabels: map[string]string{
				"my-label": "{{.isbsvc-name}}",
			},
			rolloutAnnotations: map[string]string{
				"my-annotation": "{{.isbsvc-name}}",
			},
			promotedChildSpec: createISBServiceSpec("2.9.0", "my-isbsvc-0"),
			promotedChildName: "my-isbsvc-0",
			promotedChildLabels: map[string]string{
				"my-label":   "my-isbsvc-0", // this is a match
				"my-label-2": "something",   // this is no problem to have an extra label
			},
			promotedChildAnnotations: map[string]string{
				"my-annotation":   "my-isbsvc-0", // this is a match
				"my-annotation-2": "something",   // this is no problem to have an extra annotation
			},
			upgradingChildSpec: createISBServiceSpec("2.10.3", "my-isbsvc-1"),
			upgradingChildName: "my-isbsvc-1",
			upgradingChildLabels: map[string]string{
				"my-label":   "my-isbsvc-1", // this is a match
				"my-label-2": "something",   // this is no problem to have an extra label
			},
			upgradingChildAnnotations: map[string]string{
				"my-annotation":   "my-isbsvc-1", // this is a match
				"my-annotation-2": "something",   // this is no problem to have an extra annotation
			},
			expectedDiffFromUpgrading: false,
			expectedDiffFromPromoted:  true,
		},
		{
			name:        "different from both - required metadata not present",
			rolloutSpec: createISBServiceSpec("2.9.0", "{{.isbsvc-name}}"),
			rolloutLabels: map[string]string{
				"my-label":       "{{.isbsvc-name}}",
				"required-label": "important-value",
			},
			rolloutAnnotations: map[string]string{
				"my-annotation":       "{{.isbsvc-name}}",
				"required-annotation": "important-value",
			},
			promotedChildSpec: createISBServiceSpec("2.9.0", "my-isbsvc-0"),
			promotedChildName: "my-isbsvc-0",
			promotedChildLabels: map[string]string{
				"my-label":       "my-isbsvc-0",     // this is a match
				"required-label": "important-value", // this is a match
			},
			promotedChildAnnotations: map[string]string{
				"my-annotation": "my-isbsvc-0", // this is a match
				// Missing "required-annotation"
			},
			upgradingChildSpec: createISBServiceSpec("2.9.0", "my-isbsvc-1"),
			upgradingChildName: "my-isbsvc-1",
			upgradingChildLabels: map[string]string{
				"my-label":       "my-isbsvc-1",               // this is a match
				"required-label": "different-important-value", // key is present but value is different
			},
			upgradingChildAnnotations: map[string]string{
				"my-annotation":       "my-isbsvc-1",     // this is a match
				"required-annotation": "important-value", // this is a match
			},
			expectedDiffFromUpgrading: true,
			expectedDiffFromPromoted:  true,
		},
	}

	// Helper function to create an ISBService child with required labels/annotations
	createISBServiceChild := func(spec numaflowv1.InterStepBufferServiceSpec, name string, labels, annotations map[string]string, upgradeState common.UpgradeState) *unstructured.Unstructured {
		isbService := ctlrcommon.CreateTestISBService(
			"",
			name,
			numaflowv1.ISBSvcPhaseRunning,
			true,
			labels,
			annotations,
		)
		// Override the spec with the provided spec
		isbService.Spec = spec
		// Add required labels
		if isbService.Labels == nil {
			isbService.Labels = make(map[string]string)
		}
		isbService.Labels[common.LabelKeyParentRollout] = ctlrcommon.DefaultTestISBSvcRolloutName
		isbService.Labels[common.LabelKeyUpgradeState] = string(upgradeState)
		if isbService.Annotations == nil {
			isbService.Annotations = make(map[string]string)
		}
		isbService.Annotations[common.AnnotationKeyNumaflowInstanceID] = "1"

		// Convert to unstructured
		unstructMap, _ := k8sRuntime.DefaultUnstructuredConverter.ToUnstructured(isbService)
		return &unstructured.Unstructured{Object: unstructMap}
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create ISBServiceRollout with template values
			isbsvcSpecRaw, err := json.Marshal(tc.rolloutSpec)
			assert.NoError(t, err)

			isbsvcRollout := &apiv1.ISBServiceRollout{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ctlrcommon.DefaultTestNamespace,
					Name:      ctlrcommon.DefaultTestISBSvcRolloutName,
					UID:       "uid",
				},
				Spec: apiv1.ISBServiceRolloutSpec{
					InterStepBufferService: apiv1.InterStepBufferService{
						Metadata: apiv1.Metadata{
							Labels:      tc.rolloutLabels,
							Annotations: tc.rolloutAnnotations,
						},
						Spec: k8sRuntime.RawExtension{
							Raw: isbsvcSpecRaw,
						},
					},
				},
				Status: apiv1.ISBServiceRolloutStatus{
					ProgressiveStatus: apiv1.ISBServiceProgressiveStatus{
						UpgradingISBServiceStatus: &apiv1.UpgradingISBServiceStatus{},
						PromotedISBServiceStatus:  &apiv1.PromotedISBServiceStatus{},
					},
				},
			}

			// Create Promoted and Upgrading ISBServices
			promotedChildUnstruct := createISBServiceChild(
				tc.promotedChildSpec,
				tc.promotedChildName,
				tc.promotedChildLabels,
				tc.promotedChildAnnotations,
				common.LabelValueUpgradePromoted,
			)
			upgradingChildUnstruct := createISBServiceChild(
				tc.upgradingChildSpec,
				tc.upgradingChildName,
				tc.upgradingChildLabels,
				tc.upgradingChildAnnotations,
				common.LabelValueUpgradeTrial,
			)

			// Call progressive.IsUpgradeReplacementRequired with the real controller
			differentFromUpgrading, differentFromPromoted, err := progressive.IsUpgradeReplacementRequired(
				ctx,
				isbsvcRollout,
				reconciler,
				promotedChildUnstruct,
				upgradingChildUnstruct,
				client,
			)

			// Verify results
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedDiffFromUpgrading, differentFromUpgrading,
				"differentFromUpgrading mismatch")
			assert.Equal(t, tc.expectedDiffFromPromoted, differentFromPromoted,
				"differentFromPromoted mismatch")
		})
	}
}
