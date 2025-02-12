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

/*
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
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics()
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


func Test_reconcile_isbservicerollout_Progressive(t *testing.T) {
	restConfig, numaflowClientSet, client, k8sClientSet, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetClientSets(restConfig))

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig2"))
	assert.NoError(t, err)

	config.GetConfigManagerInstance().UpdateUSDEConfig(config.USDEConfig{
		"interstepbufferservice": config.USDEResourceConfig{
			DataLoss: []config.SpecField{{Path: "spec.jetstream.version", IncludeSubfields: false}},
		},
	})
	ctx := context.Background()

	// other tests may call this, but it fails if called more than once
	if ctlrcommon.TestCustomMetrics == nil {
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics()
	}

	recorder := record.NewFakeRecorder(64)

	r := NewISBServiceRolloutReconciler(
		client,
		scheme.Scheme,
		ctlrcommon.TestCustomMetrics,
		recorder)

	defaultPromotedISBSvcName := ctlrcommon.DefaultTestISBSvcRolloutName + "-0"
	defaultUpgradingISBSvcName := ctlrcommon.DefaultTestISBSvcRolloutName + "-1"
	defaultPromotedPipelineName := ctlrcommon.DefaultTestPipelineRolloutName + "-0"
	defaultUpgradingPipelineName := ctlrcommon.DefaultTestPipelineRolloutName + "-1"
	defaultPromotedISBSvc := *ctlrcommon.CreateTestISBService(
		"2.10.3",
		defaultPromotedISBSvcName,
		numaflowv1.ISBSvcPhaseRunning,
		true,
		map[string]string{
			common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
			common.LabelKeyParentRollout: ctlrcommon.DefaultTestISBSvcRolloutName,
		},
		map[string]string{},
	)
	defaultPromotedPipeline := createPipelineForISBSvc(defaultPromotedPipelineName, defaultPromotedISBSvcName,
		numaflowv1.PipelinePhaseRunning, map[string]string{
			common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
			common.LabelKeyISBServiceChildNameForPipeline: defaultPromotedISBSvcName,
			common.LabelKeyUpgradeState:                   string(common.LabelValueUpgradePromoted),
			common.LabelKeyParentRollout:                  ctlrcommon.DefaultTestPipelineRolloutName,
		},
	)
	defaultUpgradingPipeline := createPipelineForISBSvc(defaultUpgradingPipelineName, defaultUpgradingISBSvcName,
		numaflowv1.PipelinePhaseRunning, map[string]string{
			common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
			common.LabelKeyISBServiceChildNameForPipeline: defaultUpgradingISBSvcName,
			common.LabelKeyUpgradeState:                   string(common.LabelValueUpgradeInProgress),
			common.LabelKeyParentRollout:                  ctlrcommon.DefaultTestPipelineRolloutName,
		},
	)
	defaultUpgradingISBSvc := ctlrcommon.CreateTestISBService(
		"2.10.11",
		defaultUpgradingISBSvcName,
		numaflowv1.ISBSvcPhaseRunning,
		true,
		map[string]string{
			common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeInProgress),
			common.LabelKeyParentRollout: ctlrcommon.DefaultTestISBSvcRolloutName,
		},
		map[string]string{},
	)
	defaultPromotedStatefulSet := *createDefaultISBStatefulSet("2.10.3", true)

	progressiveUpgradeStrategy := apiv1.UpgradeStrategyProgressive

	pipelinerollout.PipelineROReconciler = &pipelinerollout.PipelineRolloutReconciler{Queue: util.NewWorkQueue("fake_queue")}

	testCases := []struct {
		name                           string
		newISBSvcSpec                  numaflowv1.InterStepBufferServiceSpec
		existingPromotedISBSvcDef      numaflowv1.InterStepBufferService
		existingPromotedStatefulSetDef appsv1.StatefulSet
		existingUpgradingISBSvcDef     *numaflowv1.InterStepBufferService
		existingPipelineRollout        *apiv1.PipelineRollout
		existingPromotedPipelineDef    *numaflowv1.Pipeline
		existingUpgradingPipelineDef   *numaflowv1.Pipeline
		initialRolloutPhase            apiv1.Phase
		initialRolloutNameCount        int
		initialInProgressStrategy      *apiv1.UpgradeStrategy
		initialUpgradingChildStatus    *apiv1.UpgradingChildStatus

		expectedInProgressStrategy apiv1.UpgradeStrategy
		expectedRolloutPhase       apiv1.Phase

		expectedISBServices map[string]common.UpgradeState // after reconcile(), these are the only interstepbufferservices we expect to exist along with their expected UpgradeState

	}{
		{
			// the Rollout's specified jetstream version - 2.10.11 - differs from the running one - 2.10.3
			// that's a data loss field
			name:                           "spec difference results in Progressive",
			newISBSvcSpec:                  ctlrcommon.CreateDefaultISBServiceSpec("2.10.11"),
			existingPromotedISBSvcDef:      defaultPromotedISBSvc,
			existingPromotedStatefulSetDef: defaultPromotedStatefulSet,
			existingUpgradingISBSvcDef:     nil,
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}, nil),
			existingPromotedPipelineDef:  defaultPromotedPipeline,
			existingUpgradingPipelineDef: nil,
			initialRolloutPhase:          apiv1.PhaseDeployed,
			initialRolloutNameCount:      1,
			initialInProgressStrategy:    nil,
			initialUpgradingChildStatus:  nil,
			expectedInProgressStrategy:   apiv1.UpgradeStrategyProgressive,
			expectedRolloutPhase:         apiv1.PhasePending,
			expectedISBServices: map[string]common.UpgradeState{
				defaultPromotedISBSvcName:  common.LabelValueUpgradePromoted,
				defaultUpgradingISBSvcName: common.LabelValueUpgradeInProgress,
			},
		},
		{
			// the "upgrading" isbsvc succeeds (given that its pipeline succeeded)
			// the original pipeline was deleted, so the isbsvc in turn can also be deleted
			name:                           "Progressive succeeds",
			newISBSvcSpec:                  ctlrcommon.CreateDefaultISBServiceSpec("2.10.11"),
			existingPromotedISBSvcDef:      defaultPromotedISBSvc,
			existingPromotedStatefulSetDef: defaultPromotedStatefulSet,
			existingUpgradingISBSvcDef:     defaultUpgradingISBSvc,
			// Because the PipelineRollout was successful, therefore the ISBServiceRollout should be as well
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{},
				&apiv1.PipelineRolloutStatus{
					Status: apiv1.Status{
						ProgressiveStatus: apiv1.ProgressiveStatus{
							UpgradingChildStatus: &apiv1.UpgradingChildStatus{
								Name:             defaultUpgradingPipelineName,
								AssessmentResult: apiv1.AssessmentResultSuccess,
							},
						},
					},
				},
			),
			// our previously "upgrading" pipeline has been "promoted" and our previously "promoted" one was already deleted
			existingPromotedPipelineDef: createPipelineForISBSvc(defaultUpgradingPipelineName, defaultUpgradingISBSvcName,
				numaflowv1.PipelinePhaseRunning, map[string]string{
					common.LabelKeyISBServiceRONameForPipeline:    ctlrcommon.DefaultTestISBSvcRolloutName,
					common.LabelKeyISBServiceChildNameForPipeline: defaultUpgradingISBSvcName,
					common.LabelKeyUpgradeState:                   string(common.LabelValueUpgradePromoted),
					common.LabelKeyParentRollout:                  ctlrcommon.DefaultTestPipelineRolloutName,
				},
			),
			existingUpgradingPipelineDef: nil,
			initialRolloutPhase:          apiv1.PhasePending,
			initialRolloutNameCount:      2,
			initialInProgressStrategy:    &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: &apiv1.UpgradingChildStatus{
				Name:               ctlrcommon.DefaultTestISBSvcRolloutName + "-1",
				NextAssessmentTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
				AssessUntil:        &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
				AssessmentResult:   apiv1.AssessmentResultSuccess,
			},
			expectedInProgressStrategy: apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:       apiv1.PhaseDeployed,
			expectedISBServices: map[string]common.UpgradeState{
				// note: the original isbsvc got garbage collected so it's no longer there
				defaultUpgradingISBSvcName: common.LabelValueUpgradePromoted,
			},
		},
		{
			// the "upgrading" isbsvc fails because the "upgrading" pipeline on it has failed
			name:                           "Progressive fails",
			newISBSvcSpec:                  ctlrcommon.CreateDefaultISBServiceSpec("2.10.11"),
			existingPromotedISBSvcDef:      defaultPromotedISBSvc,
			existingPromotedStatefulSetDef: defaultPromotedStatefulSet,
			existingUpgradingISBSvcDef:     defaultUpgradingISBSvc,
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{},
				&apiv1.PipelineRolloutStatus{
					Status: apiv1.Status{
						ProgressiveStatus: apiv1.ProgressiveStatus{
							UpgradingChildStatus: &apiv1.UpgradingChildStatus{
								Name:             defaultUpgradingPipelineName,
								AssessmentResult: apiv1.AssessmentResultFailure,
							},
						},
					},
				},
			),
			existingPromotedPipelineDef:  defaultPromotedPipeline,
			existingUpgradingPipelineDef: defaultUpgradingPipeline,
			initialRolloutPhase:          apiv1.PhasePending,
			initialRolloutNameCount:      2,
			initialInProgressStrategy:    &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: &apiv1.UpgradingChildStatus{
				Name:               ctlrcommon.DefaultTestISBSvcRolloutName + "-1",
				NextAssessmentTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
				AssessUntil:        &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
				AssessmentResult:   apiv1.AssessmentResultFailure,
			},
			expectedInProgressStrategy: progressiveUpgradeStrategy,
			expectedRolloutPhase:       apiv1.PhasePending,
			expectedISBServices: map[string]common.UpgradeState{
				defaultPromotedISBSvcName:  common.LabelValueUpgradePromoted,
				defaultUpgradingISBSvcName: common.LabelValueUpgradeInProgress,
			},
		},
		{
			// jetstream version modified after the last "upgrading" pipeline was marked "failed"
			name: "Updated isbsvc spec after previous progressive failure",
			// this version is different from our current upgrading isbsvc, which is using 2.10.11 - this will create a new isbsvc
			newISBSvcSpec:                  ctlrcommon.CreateDefaultISBServiceSpec("2.10.12"),
			existingPromotedISBSvcDef:      defaultPromotedISBSvc,
			existingPromotedStatefulSetDef: defaultPromotedStatefulSet,
			existingUpgradingISBSvcDef:     defaultUpgradingISBSvc,
			existingPipelineRollout: ctlrcommon.CreateTestPipelineRollout(numaflowv1.PipelineSpec{InterStepBufferServiceName: ctlrcommon.DefaultTestISBSvcRolloutName},
				map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{},
				&apiv1.PipelineRolloutStatus{
					Status: apiv1.Status{
						ProgressiveStatus: apiv1.ProgressiveStatus{
							UpgradingChildStatus: &apiv1.UpgradingChildStatus{
								Name:             defaultUpgradingPipelineName,
								AssessmentResult: apiv1.AssessmentResultFailure,
							},
						},
					},
				},
			),
			existingPromotedPipelineDef:  defaultPromotedPipeline,
			existingUpgradingPipelineDef: defaultUpgradingPipeline,
			initialRolloutPhase:          apiv1.PhasePending,
			initialRolloutNameCount:      2,
			initialInProgressStrategy:    &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: &apiv1.UpgradingChildStatus{
				Name:               ctlrcommon.DefaultTestISBSvcRolloutName + "-1",
				NextAssessmentTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
				AssessUntil:        &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
				AssessmentResult:   apiv1.AssessmentResultFailure,
			},
			expectedInProgressStrategy: progressiveUpgradeStrategy,
			expectedRolloutPhase:       apiv1.PhasePending,
			expectedISBServices: map[string]common.UpgradeState{
				defaultPromotedISBSvcName:                      common.LabelValueUpgradePromoted,
				defaultUpgradingISBSvcName:                     common.LabelValueUpgradeRecyclable,
				ctlrcommon.DefaultTestISBSvcRolloutName + "-2": common.LabelValueUpgradeInProgress,
			},
		},
	}

	for _, tc := range testCases {

		t.Run(tc.name, func(t *testing.T) {
			// first delete all of the resources in case they already exist, in Kubernetes
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = numaflowClientSet.NumaflowV1alpha1().InterStepBufferServices(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = k8sClientSet.AppsV1().StatefulSets(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})

			_ = client.DeleteAllOf(ctx, &apiv1.PipelineRollout{}, &ctlrruntimeclient.DeleteAllOfOptions{ListOptions: ctlrruntimeclient.ListOptions{Namespace: ctlrcommon.DefaultTestNamespace}})
			_ = client.DeleteAllOf(ctx, &apiv1.ISBServiceRollout{}, &ctlrruntimeclient.DeleteAllOfOptions{ListOptions: ctlrruntimeclient.ListOptions{Namespace: ctlrcommon.DefaultTestNamespace}})

			// create ISBServiceRollout in K8S
			rollout := ctlrcommon.CreateISBServiceRollout(tc.newISBSvcSpec, &apiv1.ISBServiceRolloutStatus{
				Status: apiv1.Status{ProgressiveStatus: apiv1.ProgressiveStatus{UpgradingChildStatus: tc.initialUpgradingChildStatus}}})
			rollout.Status.Init(rollout.Generation)
			rollout.Status.Phase = tc.initialRolloutPhase
			if rollout.Status.NameCount == nil {
				rollout.Status.NameCount = new(int32)
			}
			*rollout.Status.NameCount = int32(tc.initialRolloutNameCount)
			ctlrcommon.CreateISBServiceRolloutInK8S(ctx, t, client, rollout)

			// create both "promoted" and "upgrading" interstepbufferservice (if it exists) in K8S
			ctlrcommon.CreateISBSvcInK8S(ctx, t, numaflowClientSet, &tc.existingPromotedISBSvcDef)
			if tc.existingUpgradingISBSvcDef != nil {
				ctlrcommon.CreateISBSvcInK8S(ctx, t, numaflowClientSet, tc.existingUpgradingISBSvcDef)
			}

			// create "promoted" StatefulSet in K8S
			// note: we don't bother to create the "upgrading" StatefulSet, since what happens will not be dependent on it
			ctlrcommon.CreateStatefulSetInK8S(ctx, t, k8sClientSet, &tc.existingPromotedStatefulSetDef)

			// create PipelineRollout in K8S
			ctlrcommon.CreatePipelineRolloutInK8S(ctx, t, client, tc.existingPipelineRollout)

			// create "promoted" as well as "upgrading" pipeline if they exist, in K8S
			if tc.existingPromotedPipelineDef != nil {
				ctlrcommon.CreatePipelineInK8S(ctx, t, numaflowClientSet, tc.existingPromotedPipelineDef)
			}
			if tc.existingUpgradingPipelineDef != nil {
				ctlrcommon.CreatePipelineInK8S(ctx, t, numaflowClientSet, tc.existingUpgradingPipelineDef)
			}

			// Set the In-Progress Strategy
			if tc.initialInProgressStrategy != nil {
				rollout.Status.UpgradeInProgress = *tc.initialInProgressStrategy
				r.inProgressStrategyMgr.Store.SetStrategy(k8stypes.NamespacedName{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestPipelineRolloutName}, *tc.initialInProgressStrategy)
			} else {
				rollout.Status.UpgradeInProgress = apiv1.UpgradeStrategyNoOp
				r.inProgressStrategyMgr.Store.SetStrategy(k8stypes.NamespacedName{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestPipelineRolloutName}, apiv1.UpgradeStrategyNoOp)
			}

			// call reconcile()
			_, err = r.reconcile(ctx, rollout, time.Now())
			assert.NoError(t, err)

			////// check results:
			// Check Phase of Rollout:
			assert.Equal(t, tc.expectedRolloutPhase, rollout.Status.Phase)
			// Check In-Progress Strategy
			assert.Equal(t, tc.expectedInProgressStrategy, rollout.Status.UpgradeInProgress)

			resultISBSvcList, err := numaflowClientSet.NumaflowV1alpha1().InterStepBufferServices(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Equal(t, len(tc.expectedISBServices), len(resultISBSvcList.Items), resultISBSvcList.Items)

			for _, isbsvc := range resultISBSvcList.Items {
				expectedUpgradeState, found := tc.expectedISBServices[isbsvc.Name]
				assert.True(t, found)
				resultUpgradeState, found := isbsvc.Labels[common.LabelKeyUpgradeState]
				assert.True(t, found)
				assert.Equal(t, string(expectedUpgradeState), resultUpgradeState)
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

func createPipelineForISBSvc(
	name string,
	isbsvcName string,
	phase numaflowv1.PipelinePhase,
	labels map[string]string,
) *numaflowv1.Pipeline {
	spec := numaflowv1.PipelineSpec{InterStepBufferServiceName: isbsvcName}
	return ctlrcommon.CreateTestPipelineOfSpec(spec, name, phase, numaflowv1.Status{}, false, labels, map[string]string{})
}
*/
