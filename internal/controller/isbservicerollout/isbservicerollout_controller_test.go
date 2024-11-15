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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
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
)

// test reconcile() for the case of PPND

func Test_reconcile_isbservicerollout_PPND(t *testing.T) {
	ctx := context.Background()

	numaLogger := logger.New()
	numaLogger.SetLevel(3)
	logger.SetBaseLogger(numaLogger)
	ctx = logger.WithLogger(ctx, numaLogger)

	restConfig, numaflowClientSet, numaplaneClient, k8sClientSet, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetDynamicClient(restConfig))

	usdeConfig := config.USDEConfig{
		DefaultUpgradeStrategy:       config.PPNDStrategyID,
		ISBServiceSpecDataLossFields: []config.SpecDataLossField{{Path: "spec", IncludeSubfields: true}},
	}

	config.GetConfigManagerInstance().UpdateUSDEConfig(usdeConfig)

	// other tests may call this, but it fails if called more than once
	if ctlrcommon.TestCustomMetrics == nil {
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics()
	}

	recorder := record.NewFakeRecorder(64)

	r := NewISBServiceRolloutReconciler(numaplaneClient, scheme.Scheme, ctlrcommon.TestCustomMetrics, recorder)

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
			name:                      "new ISBService",
			newISBSvcSpec:             createDefaultISBServiceSpec("2.10.3"),
			existingISBSvcDef:         nil,
			existingStatefulSetDef:    nil,
			existingPipelineRollout:   ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName}, map[string]string{}, map[string]string{}),
			existingPipeline:          ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhaseRunning),
			existingPauseRequest:      nil,
			initialInProgressStrategy: apiv1.UpgradeStrategyNoOp,
			expectedPauseRequest:      nil,
			expectedRolloutPhase:      apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
			},
			expectedISBSvcSpec:         createDefaultISBServiceSpec("2.10.3"),
			expectedInProgressStrategy: apiv1.UpgradeStrategyNoOp,
		},
		{
			name:                      "existing ISBService - no change",
			newISBSvcSpec:             createDefaultISBServiceSpec("2.10.3"),
			existingISBSvcDef:         createDefaultISBService("2.10.3", numaflowv1.ISBSvcPhaseRunning, true),
			existingStatefulSetDef:    createDefaultISBStatefulSet("2.10.3", true),
			existingPipelineRollout:   ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName}, map[string]string{}, map[string]string{}),
			existingPipeline:          ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhaseRunning),
			existingPauseRequest:      &falseValue,
			initialInProgressStrategy: apiv1.UpgradeStrategyNoOp,
			expectedPauseRequest:      &falseValue,
			expectedRolloutPhase:      apiv1.PhaseDeployed,
			expectedConditionsSet:     map[apiv1.ConditionType]metav1.ConditionStatus{}, // some Conditions may be set from before, but in any case nothing new to verify
			expectedISBSvcSpec:        createDefaultISBServiceSpec("2.10.3"),
		},
		{
			name:                       "existing ISBService - new spec - pipelines not paused",
			newISBSvcSpec:              createDefaultISBServiceSpec("2.10.11"),
			existingISBSvcDef:          createDefaultISBService("2.10.3", numaflowv1.ISBSvcPhaseRunning, true),
			existingStatefulSetDef:     createDefaultISBStatefulSet("2.10.3", true),
			existingPipelineRollout:    ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName}, map[string]string{}, map[string]string{}),
			existingPipeline:           ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhaseRunning),
			existingPauseRequest:       &falseValue,
			initialInProgressStrategy:  apiv1.UpgradeStrategyNoOp,
			expectedPauseRequest:       &trueValue,
			expectedRolloutPhase:       apiv1.PhasePending,
			expectedConditionsSet:      map[apiv1.ConditionType]metav1.ConditionStatus{apiv1.ConditionPausingPipelines: metav1.ConditionTrue},
			expectedISBSvcSpec:         createDefaultISBServiceSpec("2.10.3"),
			expectedInProgressStrategy: apiv1.UpgradeStrategyPPND,
		},
		{
			name:                      "existing ISBService - new spec - pipelines paused",
			newISBSvcSpec:             createDefaultISBServiceSpec("2.10.11"),
			existingISBSvcDef:         createDefaultISBService("2.10.3", numaflowv1.ISBSvcPhaseRunning, true),
			existingStatefulSetDef:    createDefaultISBStatefulSet("2.10.3", true),
			existingPipelineRollout:   ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName}, map[string]string{}, map[string]string{}),
			existingPipeline:          ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhasePaused),
			existingPauseRequest:      &trueValue,
			initialInProgressStrategy: apiv1.UpgradeStrategyPPND,
			expectedPauseRequest:      &trueValue,
			expectedRolloutPhase:      apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
				apiv1.ConditionPausingPipelines:      metav1.ConditionTrue,
			},
			expectedISBSvcSpec:         createDefaultISBServiceSpec("2.10.11"),
			expectedInProgressStrategy: apiv1.UpgradeStrategyPPND,
		},
		{
			name:                      "existing ISBService - new spec - pipelines failed",
			newISBSvcSpec:             createDefaultISBServiceSpec("2.10.11"),
			existingISBSvcDef:         createDefaultISBService("2.10.3", numaflowv1.ISBSvcPhaseRunning, true),
			existingStatefulSetDef:    createDefaultISBStatefulSet("2.10.3", true),
			existingPipelineRollout:   ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName}, map[string]string{}, map[string]string{}),
			existingPipeline:          ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhaseFailed),
			existingPauseRequest:      &trueValue,
			initialInProgressStrategy: apiv1.UpgradeStrategyPPND,
			expectedPauseRequest:      &trueValue,
			expectedRolloutPhase:      apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
				apiv1.ConditionPausingPipelines:      metav1.ConditionTrue,
			},
			expectedISBSvcSpec:         createDefaultISBServiceSpec("2.10.11"),
			expectedInProgressStrategy: apiv1.UpgradeStrategyPPND,
		},
		{
			name:                      "existing ISBService - new spec - pipelines set to allow data loss",
			newISBSvcSpec:             createDefaultISBServiceSpec("2.10.11"),
			existingISBSvcDef:         createDefaultISBService("2.10.3", numaflowv1.ISBSvcPhasePending, true),
			existingStatefulSetDef:    createDefaultISBStatefulSet("2.10.3", true),
			existingPipelineRollout:   ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName}, map[string]string{common.LabelKeyAllowDataLoss: "true"}, map[string]string{}),
			existingPipeline:          ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhasePausing),
			existingPauseRequest:      &trueValue,
			initialInProgressStrategy: apiv1.UpgradeStrategyPPND,
			expectedPauseRequest:      &trueValue,
			expectedRolloutPhase:      apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
				apiv1.ConditionPausingPipelines:      metav1.ConditionTrue,
			},
			expectedISBSvcSpec:         createDefaultISBServiceSpec("2.10.11"),
			expectedInProgressStrategy: apiv1.UpgradeStrategyPPND,
		},
		{
			name:                      "existing ISBService - spec already updated - isbsvc reconciling",
			newISBSvcSpec:             createDefaultISBServiceSpec("2.10.11"),
			existingISBSvcDef:         createDefaultISBService("2.10.11", numaflowv1.ISBSvcPhaseRunning, false),
			existingStatefulSetDef:    createDefaultISBStatefulSet("2.10.3", false),
			existingPipelineRollout:   ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName}, map[string]string{}, map[string]string{}),
			existingPipeline:          ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhasePaused),
			existingPauseRequest:      &trueValue,
			initialInProgressStrategy: apiv1.UpgradeStrategyPPND,
			expectedPauseRequest:      &trueValue,
			expectedRolloutPhase:      apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionPausingPipelines: metav1.ConditionTrue,
			},
			expectedISBSvcSpec:         createDefaultISBServiceSpec("2.10.11"),
			expectedInProgressStrategy: apiv1.UpgradeStrategyPPND,
		},
		{
			name:                      "existing ISBService - spec already updated - isbsvc done reconciling",
			newISBSvcSpec:             createDefaultISBServiceSpec("2.10.11"),
			existingISBSvcDef:         createDefaultISBService("2.10.11", numaflowv1.ISBSvcPhaseRunning, true),
			existingStatefulSetDef:    createDefaultISBStatefulSet("2.10.11", true),
			existingPipelineRollout:   ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName}, map[string]string{}, map[string]string{}),
			existingPipeline:          ctlrcommon.CreateDefaultTestPipelineOfPhase(numaflowv1.PipelinePhasePaused),
			existingPauseRequest:      &trueValue,
			initialInProgressStrategy: apiv1.UpgradeStrategyPPND,
			expectedPauseRequest:      &falseValue,
			expectedRolloutPhase:      apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
			},
			expectedISBSvcSpec:         createDefaultISBServiceSpec("2.10.11"),
			expectedInProgressStrategy: apiv1.UpgradeStrategyNoOp,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			// first delete any previous resources if they already exist, in Kubernetes
			_ = numaflowClientSet.NumaflowV1alpha1().InterStepBufferServices(ctlrcommon.DefaultTestNamespace).Delete(ctx, ctlrcommon.DefaultTestISBSvcRolloutName, metav1.DeleteOptions{})
			_ = k8sClientSet.AppsV1().StatefulSets(ctlrcommon.DefaultTestNamespace).Delete(ctx, deriveISBSvcStatefulSetName(ctlrcommon.DefaultTestISBSvcRolloutName), metav1.DeleteOptions{})
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).Delete(ctx, ctlrcommon.DefaultTestPipelineName, metav1.DeleteOptions{})
			_ = numaplaneClient.Delete(ctx, &apiv1.PipelineRollout{ObjectMeta: metav1.ObjectMeta{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestPipelineRolloutName}})

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
			rollout := createISBServiceRollout(tc.newISBSvcSpec)

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

			ctlrcommon.CreatePipelineRolloutInK8S(ctx, t, numaplaneClient, tc.existingPipelineRollout)

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
			resultISBSVC, err := numaflowClientSet.NumaflowV1alpha1().InterStepBufferServices(ctlrcommon.DefaultTestNamespace).Get(ctx, ctlrcommon.DefaultTestISBSvcRolloutName, metav1.GetOptions{})
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

func createDefaultISBServiceSpec(jetstreamVersion string) numaflowv1.InterStepBufferServiceSpec {
	return numaflowv1.InterStepBufferServiceSpec{
		Redis: &numaflowv1.RedisBufferService{},
		JetStream: &numaflowv1.JetStreamBufferService{
			Version:     jetstreamVersion,
			Persistence: nil,
		},
	}
}

func createDefaultISBService(jetstreamVersion string, phase numaflowv1.ISBSvcPhase, fullyReconciled bool) *numaflowv1.InterStepBufferService {
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
			Name:      ctlrcommon.DefaultTestISBSvcRolloutName,
			Namespace: ctlrcommon.DefaultTestNamespace,
		},
		Spec:   createDefaultISBServiceSpec(jetstreamVersion),
		Status: status,
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
		"numaflow.numaproj.io/isbsvc-name": ctlrcommon.DefaultTestISBSvcRolloutName,
		"numaflow.numaproj.io/isbsvc-type": "jetstream",
	}
	selector := metav1.LabelSelector{MatchLabels: labels}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deriveISBSvcStatefulSetName(ctlrcommon.DefaultTestISBSvcRolloutName),
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

func createISBServiceRollout(isbsvcSpec numaflowv1.InterStepBufferServiceSpec) *apiv1.ISBServiceRollout {
	isbsSpecRaw, _ := json.Marshal(isbsvcSpec)
	return &apiv1.ISBServiceRollout{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         ctlrcommon.DefaultTestNamespace,
			Name:              ctlrcommon.DefaultTestISBSvcRolloutName,
			UID:               "some-uid",
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
	}
}
