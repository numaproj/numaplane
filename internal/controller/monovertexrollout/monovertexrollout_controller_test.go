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

package monovertexrollout

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"k8s.io/utils/strings/slices"

	argorolloutsv1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"github.com/numaproj/numaplane/pkg/client/clientset/versioned/scheme"
	commontest "github.com/numaproj/numaplane/tests/common"
)

var (
	monoVertexSpec = numaflowv1.MonoVertexSpec{
		Replicas: ptr.To(int32(1)),
		Source: &numaflowv1.Source{
			UDSource: &numaflowv1.UDSource{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-java/source-simple-source:stable",
				},
			},
			UDTransformer: &numaflowv1.UDTransformer{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-rs/source-transformer-now:stable",
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

	defaultOriginalMonoVertexDef = *createMonoVertex(
		numaflowv1.MonoVertexPhaseRunning,
		numaflowv1.Status{},
		map[string]string{
			common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
			common.LabelKeyParentRollout: ctlrcommon.DefaultTestMonoVertexRolloutName,
		},
		map[string]string{
			common.AnnotationKeyNumaflowInstanceID: "0",
		})

	defaultUpgradingMonoVertexDef = ctlrcommon.CreateTestMonoVertexOfSpec(
		monoVertexSpec, ctlrcommon.DefaultTestMonoVertexRolloutName+"-1",
		numaflowv1.MonoVertexPhaseRunning,
		numaflowv1.Status{
			Conditions: []metav1.Condition{
				{
					Type:               string(numaflowv1.MonoVertexConditionDaemonHealthy),
					Status:             metav1.ConditionTrue,
					Reason:             "healthy",
					LastTransitionTime: metav1.NewTime(time.Now()),
				},
			},
		},
		map[string]string{
			common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeTrial),
			common.LabelKeyParentRollout: ctlrcommon.DefaultTestMonoVertexRolloutName,
		},
		map[string]string{
			common.AnnotationKeyNumaflowInstanceID: "1",
		})

	defaultPromotedChildStatus = &apiv1.PromotedMonoVertexStatus{
		PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
			PromotedChildStatus: apiv1.PromotedChildStatus{
				Name: ctlrcommon.DefaultTestMonoVertexRolloutName + "-0",
			},
			ScaleValues: map[string]apiv1.ScaleValues{ctlrcommon.DefaultTestMonoVertexRolloutName + "-0": {OriginalScaleMinMax: ctlrcommon.DefaultScaleJSONString, ScaleTo: ctlrcommon.DefaultScaleTo}},
		},
	}

	unassessedUpgradingChildStatus = apiv1.UpgradingChildStatus{
		Name:                     ctlrcommon.DefaultTestMonoVertexRolloutName + "-1",
		BasicAssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
		BasicAssessmentEndTime:   &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
		AssessmentResult:         apiv1.AssessmentResultUnknown,
		InitializationComplete:   true,
	}

	vectorTemplate = argorolloutsv1.AnalysisTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vector",
			Namespace: ctlrcommon.DefaultTestNamespace,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "AnalysisTemplate",
			APIVersion: "argoproj.io/v1alpha1",
		},
		Spec: argorolloutsv1.AnalysisTemplateSpec{
			Args: []argorolloutsv1.Argument{
				{Name: "upgrading-monovertex-name"},
				{Name: "monovertex-namespace"},
			},
			Metrics: []argorolloutsv1.Metric{
				{
					Name: "return-vector",
					Provider: argorolloutsv1.MetricProvider{
						Prometheus: &argorolloutsv1.PrometheusMetric{
							Address: " http://prometheus-kube-prometheus-prometheus.prometheus.svc.cluster.local:9090",
							Query:   "vector(1) == vector(2)",
						},
					},
					SuccessCondition: "true",
				},
			},
		},
	}

	resultTemplate = argorolloutsv1.AnalysisTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "result",
			Namespace: ctlrcommon.DefaultTestNamespace,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "AnalysisTemplate",
			APIVersion: "argoproj.io/v1alpha1",
		},
		Spec: argorolloutsv1.AnalysisTemplateSpec{
			Args: []argorolloutsv1.Argument{
				{Name: "upgrading-monovertex-name"},
				{Name: "monovertex-namespace"},
			},
			Metrics: []argorolloutsv1.Metric{
				{
					Name: "return-result",
					Provider: argorolloutsv1.MetricProvider{
						Prometheus: &argorolloutsv1.PrometheusMetric{
							Address: " http://prometheus-kube-prometheus-prometheus.prometheus.svc.cluster.local:9090",
							Query:   "result[0] == 1",
						},
					},
					SuccessCondition: "true",
				},
			},
		},
	}

	analysisRunName       = "monovertex-monovertexrollout-test-1"
	successfulAnalysisRun = argorolloutsv1.AnalysisRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      analysisRunName,
			Namespace: ctlrcommon.DefaultTestNamespace,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "AnalysisRun",
			APIVersion: "argoproj.io/v1alpha1",
		},
		Spec: argorolloutsv1.AnalysisRunSpec{
			Metrics: []argorolloutsv1.Metric{
				{
					Name: "return-true",
					Provider: argorolloutsv1.MetricProvider{
						Prometheus: &argorolloutsv1.PrometheusMetric{
							Address: " http://prometheus-kube-prometheus-prometheus.prometheus.svc.cluster.local:9090",
							Query:   "vector(1) == vector(2)",
						},
					},
					SuccessCondition: "true",
				},
			},
			Args: []argorolloutsv1.Argument{
				{Name: "upgrading-monovertex-name", Value: &analysisRunName},
				{Name: "monovertex-namespace", Value: &ctlrcommon.DefaultTestNamespace},
			},
		},
		Status: argorolloutsv1.AnalysisRunStatus{
			Phase:       argorolloutsv1.AnalysisPhaseSuccessful,
			StartedAt:   &metav1.Time{Time: time.Now().Add(-45 * time.Second)},
			CompletedAt: &metav1.Time{Time: time.Now().Add(-40 * time.Second)},
		},
	}

	failedAnalysisRunName = "monovertex-monovertexrollout-test-2"
	failedAnalysisRun     = argorolloutsv1.AnalysisRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      failedAnalysisRunName,
			Namespace: ctlrcommon.DefaultTestNamespace,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "AnalysisRun",
			APIVersion: "argoproj.io/v1alpha1",
		},
		Spec: argorolloutsv1.AnalysisRunSpec{
			Metrics: []argorolloutsv1.Metric{
				{
					Name: "return-true",
					Provider: argorolloutsv1.MetricProvider{
						Prometheus: &argorolloutsv1.PrometheusMetric{
							Address: " http://prometheus-kube-prometheus-prometheus.prometheus.svc.cluster.local:9090",
							Query:   "vector(1) == vector(2)",
						},
					},
					SuccessCondition: "true",
				},
			},
			Args: []argorolloutsv1.Argument{
				{Name: "upgrading-monovertex-name", Value: &failedAnalysisRunName},
				{Name: "monovertex-namespace", Value: &ctlrcommon.DefaultTestNamespace},
			},
		},
		Status: argorolloutsv1.AnalysisRunStatus{
			Phase:       argorolloutsv1.AnalysisPhaseFailed,
			StartedAt:   &metav1.Time{Time: time.Now().Add(-45 * time.Second)},
			CompletedAt: &metav1.Time{Time: time.Now().Add(-40 * time.Second)},
		},
	}
)

func fakeMonoVertexSpec(t *testing.T) numaflowv1.MonoVertexSpec {
	t.Helper()
	return numaflowv1.MonoVertexSpec{
		Replicas: ptr.To(int32(1)),
		Source: &numaflowv1.Source{
			UDSource: &numaflowv1.UDSource{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-java/source-simple-source:stable",
				},
			},
			UDTransformer: &numaflowv1.UDTransformer{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-rs/source-transformer-now:stable",
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
}

func fakeMonoVertex(t *testing.T, s numaflowv1.MonoVertexSpec) *unstructured.Unstructured {
	t.Helper()
	monoVertexDef := &unstructured.Unstructured{Object: make(map[string]interface{})}
	monoVertexDef.SetGroupVersionKind(numaflowv1.MonoVertexGroupVersionKind)
	monoVertexDef.SetName("test--mvtx")
	monoVertexDef.SetNamespace("test-ns")
	var monoVertexSpec map[string]interface{}
	if err := util.StructToStruct(s, &monoVertexSpec); err != nil {
		log.Fatal(err)
	}
	monoVertexDef.Object["spec"] = monoVertexSpec
	return monoVertexDef
}

func Test_withExistingMvtxReplicas(t *testing.T) {
	tests := []struct {
		name             string
		existingReplicas *int32
		newReplicas      *int32
		expected         *int32
	}{
		{
			name:             "nil existing replicas",
			existingReplicas: nil,
			newReplicas:      ptr.To(int32(2)),
			expected:         ptr.To(int32(2)),
		},
		{
			name:             "both nil",
			existingReplicas: nil,
			newReplicas:      nil,
			expected:         nil,
		},
		{
			name:             "existing replicas not nil, new replicas not nil",
			existingReplicas: ptr.To(int32(2)),
			newReplicas:      ptr.To(int32(1)),
			expected:         ptr.To(int32(2)),
		},
		{
			name:             "existing replicas not nil, new replicas nil",
			existingReplicas: ptr.To(int32(2)),
			newReplicas:      nil,
			expected:         ptr.To(int32(2)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existingMvtxSpec := fakeMonoVertexSpec(t)
			existingMvtxSpec.Replicas = tt.existingReplicas
			existingGenericMvtx := fakeMonoVertex(t, existingMvtxSpec)

			newMvtxSpec := fakeMonoVertexSpec(t)
			newMvtxSpec.Replicas = tt.newReplicas
			newGenericMvtx := fakeMonoVertex(t, newMvtxSpec)

			result, err := withExistingMvtxReplicas(existingGenericMvtx, newGenericMvtx)
			assert.NoError(t, err)

			expected, existing, err := unstructured.NestedFloat64(result.Object, "spec", "replicas")
			assert.NoError(t, err)
			assert.Equal(t, tt.expected != nil, existing)
			if tt.expected != nil {
				assert.Equal(t, *tt.expected, int32(expected))
			}
		})
	}

}

func createMonoVertex(phase numaflowv1.MonoVertexPhase, status numaflowv1.Status, labels map[string]string, annotations map[string]string) *numaflowv1.MonoVertex {
	return ctlrcommon.CreateTestMonoVertexOfSpec(monoVertexSpec, ctlrcommon.DefaultTestMonoVertexName, phase, status, labels, annotations)
}

// process an existing monoVertex's progressive upgrade and check that AnalysisRun with correct spec is created
func Test_processExistingMonoVertex_AnalysisRunGeneration(t *testing.T) {
	numaLogger := logger.New()
	numaLogger.SetLevel(4)
	restConfig, numaflowClientSet, client, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetClientSets(restConfig))

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig2"))
	assert.NoError(t, err)

	ctx := context.Background()

	if ctlrcommon.TestCustomMetrics == nil {
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics(numaLogger)
	}

	recorder := record.NewFakeRecorder(64)

	err = client.Create(ctx, &vectorTemplate)
	assert.NoError(t, err)
	err = client.Create(ctx, &resultTemplate)
	assert.NoError(t, err)

	r := NewMonoVertexRolloutReconciler(
		client,
		scheme.Scheme,
		ctlrcommon.TestCustomMetrics,
		recorder)

	progressiveUpgradeStrategy := apiv1.UpgradeStrategyProgressive

	testCases := []struct {
		name         string
		templateRefs []argorolloutsv1.AnalysisTemplateRef
		queries      []string
	}{
		{
			name: "vector template",
			templateRefs: []argorolloutsv1.AnalysisTemplateRef{
				{TemplateName: "vector", ClusterScope: false},
			},
			queries: []string{"vector(1) == vector(2)"},
		},
		{
			name: "result template",
			templateRefs: []argorolloutsv1.AnalysisTemplateRef{
				{TemplateName: "result", ClusterScope: false},
			},
			queries: []string{"result[0] == 1"},
		},
		{
			name: "multiple templates",
			templateRefs: []argorolloutsv1.AnalysisTemplateRef{
				{TemplateName: "result", ClusterScope: false},
				{TemplateName: "vector", ClusterScope: false},
			},
			queries: []string{"result[0] == 1", "vector(1) == vector(2)"},
		},
	}

	for _, tc := range testCases {

		t.Run(tc.name, func(t *testing.T) {

			// first delete MonoVertex MonoVertexRollout and AnalysisRun in case they already exist, in Kubernetes
			_ = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = client.Delete(ctx, &argorolloutsv1.AnalysisRun{ObjectMeta: metav1.ObjectMeta{Namespace: ctlrcommon.DefaultTestNamespace, Name: analysisRunName}})

			monoVertexList, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Len(t, monoVertexList.Items, 0)

			rollout := ctlrcommon.CreateTestMVRollout(monoVertexSpec, map[string]string{}, map[string]string{},
				map[string]string{common.AnnotationKeyNumaflowInstanceID: "1"}, map[string]string{},
				&apiv1.MonoVertexRolloutStatus{ProgressiveStatus: apiv1.MonoVertexProgressiveStatus{
					UpgradingMonoVertexStatus: &apiv1.UpgradingMonoVertexStatus{
						UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
							UpgradingChildStatus: unassessedUpgradingChildStatus,
							Analysis:             apiv1.AnalysisStatus{},
						},
					},
					PromotedMonoVertexStatus: defaultPromotedChildStatus,
				}})
			_ = client.Delete(ctx, rollout)

			rollout.Status.Phase = apiv1.PhasePending
			if rollout.Status.NameCount == nil {
				rollout.Status.NameCount = new(int32)
			}
			*rollout.Status.NameCount = int32(2)

			rollout.Status.UpgradeInProgress = progressiveUpgradeStrategy
			r.inProgressStrategyMgr.Store.SetStrategy(k8stypes.NamespacedName{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestMonoVertexRolloutName}, progressiveUpgradeStrategy)

			// the Reconcile() function does this, so we need to do it before calling reconcile() as well
			rollout.Status.Init(rollout.Generation)

			// reference templates in rollout spec
			rollout.Spec.Strategy = &apiv1.PipelineTypeRolloutStrategy{
				PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
					Analysis: apiv1.Analysis{
						Templates: tc.templateRefs,
					},
				},
			}

			rolloutCopy := *rollout
			err = client.Create(ctx, rollout)
			assert.NoError(t, err)
			// update Status subresource
			rollout.Status = rolloutCopy.Status
			err = client.Status().Update(ctx, rollout)
			assert.NoError(t, err)

			// create the already-existing MonoVertex in Kubernetes
			// this updates everything but the Status subresource
			existingMonoVertexDef := &defaultOriginalMonoVertexDef
			existingMonoVertexDef.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rollout.GetObjectMeta(), apiv1.MonoVertexRolloutGroupVersionKind)}
			monoVertex, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).Create(ctx, existingMonoVertexDef, metav1.CreateOptions{})
			assert.NoError(t, err)
			// update Status subresource
			monoVertex.Status = defaultOriginalMonoVertexDef.Status
			_, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).UpdateStatus(ctx, monoVertex, metav1.UpdateOptions{})
			assert.NoError(t, err)

			existingUpgradeMonoVertexDef := defaultUpgradingMonoVertexDef
			existingUpgradeMonoVertexDef.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rollout.GetObjectMeta(), apiv1.MonoVertexRolloutGroupVersionKind)}
			monoVertex, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).Create(ctx, existingUpgradeMonoVertexDef, metav1.CreateOptions{})
			assert.NoError(t, err)

			// update Status subresource
			monoVertex.Status = defaultUpgradingMonoVertexDef.Status
			_, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).UpdateStatus(ctx, monoVertex, metav1.UpdateOptions{})
			assert.NoError(t, err)

			_, err = r.reconcile(context.Background(), rollout, time.Now())
			assert.NoError(t, err)

			// verify AnalysisRun generation
			analysisRun := &argorolloutsv1.AnalysisRun{}
			err = client.Get(ctx, k8stypes.NamespacedName{Name: analysisRunName, Namespace: rollout.Namespace}, analysisRun)
			assert.NoError(t, err)

			// verify the correct number of metrics in AnalysisRun
			assert.Equal(t, len(tc.templateRefs), len(analysisRun.Spec.Metrics))

			// verify a correct query is set
			for _, metric := range analysisRun.Spec.Metrics {
				query := metric.Provider.Prometheus.Query
				assert.True(t, slices.Contains(tc.queries, query))
			}

		})
	}
}

// process an existing monoVertex in this test, the user preferred strategy is Progressive
func Test_processExistingMonoVertex_Progressive(t *testing.T) {
	numaLogger := logger.New()
	numaLogger.SetLevel(4)
	restConfig, numaflowClientSet, client, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetClientSets(restConfig))

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig2"))
	assert.NoError(t, err)

	ctx := context.Background()

	// other tests may call this, but it fails if called more than once
	if ctlrcommon.TestCustomMetrics == nil {
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics(numaLogger)
	}

	recorder := record.NewFakeRecorder(64)

	err = client.Create(ctx, &successfulAnalysisRun)
	assert.NoError(t, err)

	err = client.Create(ctx, &failedAnalysisRun)
	assert.NoError(t, err)

	r := NewMonoVertexRolloutReconciler(
		client,
		scheme.Scheme,
		ctlrcommon.TestCustomMetrics,
		recorder)

	progressiveUpgradeStrategy := apiv1.UpgradeStrategyProgressive

	successfulUpgradingChildStatus := *unassessedUpgradingChildStatus.DeepCopy()
	successfulUpgradingChildStatus.AssessmentResult = apiv1.AssessmentResultSuccess
	failedUpgradingChildStatus := *unassessedUpgradingChildStatus.DeepCopy()
	failedUpgradingChildStatus.AssessmentResult = apiv1.AssessmentResultFailure

	testCases := []struct {
		name                           string
		newControllerInstanceID        string
		existingOriginalMonoVertexDef  *numaflowv1.MonoVertex
		existingUpgradingMonoVertexDef *numaflowv1.MonoVertex
		initialRolloutPhase            apiv1.Phase
		initialRolloutNameCount        int
		initialInProgressStrategy      *apiv1.UpgradeStrategy
		initialUpgradingChildStatus    *apiv1.UpgradingMonoVertexStatus
		initialPromotedChildStatus     *apiv1.PromotedMonoVertexStatus
		analysisRun                    bool

		expectedInProgressStrategy   apiv1.UpgradeStrategy
		expectedRolloutPhase         apiv1.Phase
		expectedProgressiveCondition metav1.ConditionStatus

		expectedMonoVertices            map[string]common.UpgradeState // after reconcile(), these are the only monoVertexs we expect to exist along with their expected UpgradeState
		expectedMonoVerticesResultState map[string]common.ResultState
	}{
		{
			name:                           "Instance annotation difference results in Progressive",
			newControllerInstanceID:        "1",
			existingOriginalMonoVertexDef:  &defaultOriginalMonoVertexDef,
			existingUpgradingMonoVertexDef: nil,
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialRolloutNameCount:        1,
			initialInProgressStrategy:      nil,
			initialUpgradingChildStatus:    nil,
			initialPromotedChildStatus:     defaultPromotedChildStatus,
			analysisRun:                    false,
			expectedInProgressStrategy:     apiv1.UpgradeStrategyProgressive,
			expectedRolloutPhase:           apiv1.PhasePending,
			expectedProgressiveCondition:   metav1.ConditionUnknown,
			expectedMonoVertices: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-0": common.LabelValueUpgradePromoted,
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-1": common.LabelValueUpgradeTrial,
			},
			expectedMonoVerticesResultState: map[string]common.ResultState{},
		},
		{
			name:                           "Progressive deployed successfully",
			newControllerInstanceID:        "1",
			existingOriginalMonoVertexDef:  &defaultOriginalMonoVertexDef,
			existingUpgradingMonoVertexDef: defaultUpgradingMonoVertexDef,
			initialRolloutPhase:            apiv1.PhasePending,
			initialRolloutNameCount:        2,
			initialInProgressStrategy:      &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: &apiv1.UpgradingMonoVertexStatus{
				UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
					UpgradingChildStatus: successfulUpgradingChildStatus,
				},
			},
			initialPromotedChildStatus:   defaultPromotedChildStatus,
			analysisRun:                  false,
			expectedInProgressStrategy:   apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:         apiv1.PhaseDeployed,
			expectedProgressiveCondition: metav1.ConditionTrue,
			// original MonoVertex deleted, new one promoted
			expectedMonoVertices: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-1": common.LabelValueUpgradePromoted,
			},
			expectedMonoVerticesResultState: map[string]common.ResultState{
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-1": common.LabelValueResultStateSucceeded,
			},
		},
		{
			name:                          "Progressive deployment failed",
			newControllerInstanceID:       "1",
			existingOriginalMonoVertexDef: &defaultOriginalMonoVertexDef,
			existingUpgradingMonoVertexDef: ctlrcommon.CreateTestMonoVertexOfSpec(
				monoVertexSpec, ctlrcommon.DefaultTestMonoVertexRolloutName+"-1",
				numaflowv1.MonoVertexPhaseFailed,
				numaflowv1.Status{
					Conditions: []metav1.Condition{},
				},
				map[string]string{
					common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeTrial),
					common.LabelKeyParentRollout: ctlrcommon.DefaultTestMonoVertexRolloutName,
				},
				map[string]string{
					common.AnnotationKeyNumaflowInstanceID: "1",
				}),
			initialRolloutPhase:       apiv1.PhasePending,
			initialRolloutNameCount:   2,
			initialInProgressStrategy: &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: &apiv1.UpgradingMonoVertexStatus{
				UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
					UpgradingChildStatus: failedUpgradingChildStatus,
				},
			},
			initialPromotedChildStatus:   defaultPromotedChildStatus,
			analysisRun:                  false,
			expectedInProgressStrategy:   apiv1.UpgradeStrategyProgressive,
			expectedRolloutPhase:         apiv1.PhasePending,
			expectedProgressiveCondition: metav1.ConditionFalse,
			expectedMonoVertices: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-0": common.LabelValueUpgradePromoted,
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-1": common.LabelValueUpgradeTrial,
			},
			expectedMonoVerticesResultState: map[string]common.ResultState{
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-1": common.LabelValueResultStateFailed,
			},
		},
		{
			name:                           "AnalysisRun successful",
			newControllerInstanceID:        "1",
			existingOriginalMonoVertexDef:  &defaultOriginalMonoVertexDef,
			existingUpgradingMonoVertexDef: defaultUpgradingMonoVertexDef,
			initialRolloutPhase:            apiv1.PhasePending,
			initialRolloutNameCount:        2,
			initialInProgressStrategy:      &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: &apiv1.UpgradingMonoVertexStatus{
				UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
					UpgradingChildStatus: unassessedUpgradingChildStatus,
					Analysis: apiv1.AnalysisStatus{
						AnalysisRunName: ctlrcommon.DefaultTestMonoVertexRolloutName + "-1",
						StartTime:       &metav1.Time{Time: time.Now().Add(-45 * time.Second)},
						EndTime:         &metav1.Time{Time: time.Now().Add(-40 * time.Second)},
						Phase:           argorolloutsv1.AnalysisPhaseSuccessful,
					},
				},
			},
			initialPromotedChildStatus:   defaultPromotedChildStatus,
			analysisRun:                  true,
			expectedInProgressStrategy:   apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:         apiv1.PhaseDeployed,
			expectedProgressiveCondition: metav1.ConditionTrue,
			// original MonoVertex deleted, new one promoted
			expectedMonoVertices: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-1": common.LabelValueUpgradePromoted,
			},
			expectedMonoVerticesResultState: map[string]common.ResultState{
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-1": common.LabelValueResultStateSucceeded,
			},
		},
		{
			name:                    "AnalysisRun failed",
			newControllerInstanceID: "2",
			existingOriginalMonoVertexDef: ctlrcommon.CreateTestMonoVertexOfSpec(
				monoVertexSpec, ctlrcommon.DefaultTestMonoVertexRolloutName+"-1",
				numaflowv1.MonoVertexPhaseRunning,
				numaflowv1.Status{},
				map[string]string{
					common.LabelKeyUpgradeState:           string(common.LabelValueUpgradePromoted),
					common.LabelKeyParentRollout:          ctlrcommon.DefaultTestMonoVertexRolloutName,
					common.LabelKeyProgressiveResultState: string(common.LabelValueResultStateSucceeded),
				},
				map[string]string{
					common.AnnotationKeyNumaflowInstanceID: "1",
				}),
			existingUpgradingMonoVertexDef: ctlrcommon.CreateTestMonoVertexOfSpec(
				monoVertexSpec, ctlrcommon.DefaultTestMonoVertexRolloutName+"-2",
				numaflowv1.MonoVertexPhaseRunning,
				numaflowv1.Status{
					Conditions: []metav1.Condition{
						{
							Type:               string(numaflowv1.MonoVertexConditionDaemonHealthy),
							Status:             metav1.ConditionTrue,
							Reason:             "healthy",
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
				},
				map[string]string{
					common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeTrial),
					common.LabelKeyParentRollout: ctlrcommon.DefaultTestMonoVertexRolloutName,
				},
				map[string]string{
					common.AnnotationKeyNumaflowInstanceID: "2",
				}),
			initialRolloutPhase:       apiv1.PhasePending,
			initialRolloutNameCount:   2,
			initialInProgressStrategy: &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: &apiv1.UpgradingMonoVertexStatus{
				UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                     ctlrcommon.DefaultTestMonoVertexRolloutName + "-2",
						BasicAssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
						BasicAssessmentEndTime:   &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
						AssessmentResult:         apiv1.AssessmentResultFailure,
						InitializationComplete:   true,
					},
					Analysis: apiv1.AnalysisStatus{
						AnalysisRunName: ctlrcommon.DefaultTestMonoVertexRolloutName + "-2",
						StartTime:       &metav1.Time{Time: time.Now().Add(-45 * time.Second)},
						EndTime:         &metav1.Time{Time: time.Now().Add(-40 * time.Second)},
						Phase:           argorolloutsv1.AnalysisPhaseFailed,
					},
				},
			},
			initialPromotedChildStatus: &apiv1.PromotedMonoVertexStatus{
				PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
					PromotedChildStatus: apiv1.PromotedChildStatus{
						Name: ctlrcommon.DefaultTestMonoVertexRolloutName + "-1",
					},
					ScaleValues: map[string]apiv1.ScaleValues{ctlrcommon.DefaultTestMonoVertexRolloutName + "-1": {OriginalScaleMinMax: ctlrcommon.DefaultScaleJSONString, ScaleTo: ctlrcommon.DefaultScaleTo}},
				},
			},
			analysisRun:                  true,
			expectedInProgressStrategy:   apiv1.UpgradeStrategyProgressive,
			expectedRolloutPhase:         apiv1.PhasePending,
			expectedProgressiveCondition: metav1.ConditionFalse,
			expectedMonoVertices: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-1": common.LabelValueUpgradePromoted,
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-2": common.LabelValueUpgradeTrial,
			},
			expectedMonoVerticesResultState: map[string]common.ResultState{
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-1": common.LabelValueResultStateSucceeded,
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-2": common.LabelValueResultStateFailed,
			},
		},
		{
			name:                           "Handle user deletion of promoted monovertex during Progressive",
			newControllerInstanceID:        "1",
			existingOriginalMonoVertexDef:  nil,
			existingUpgradingMonoVertexDef: defaultUpgradingMonoVertexDef,
			initialRolloutPhase:            apiv1.PhasePending,
			initialRolloutNameCount:        2,
			initialInProgressStrategy:      &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: &apiv1.UpgradingMonoVertexStatus{
				UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
					UpgradingChildStatus: unassessedUpgradingChildStatus,
				},
			},
			initialPromotedChildStatus:   defaultPromotedChildStatus,
			expectedInProgressStrategy:   apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:         apiv1.PhaseDeployed,
			expectedProgressiveCondition: metav1.ConditionTrue,
			// original MonoVertex deleted, new one promoted
			expectedMonoVertices: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-2": common.LabelValueUpgradePromoted,
			},
			expectedMonoVerticesResultState: map[string]common.ResultState{},
		},
	}

	for _, tc := range testCases {

		t.Run(tc.name, func(t *testing.T) {

			// first delete MonoVertex and MonoVertexRollout in case they already exist, in Kubernetes
			_ = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})

			monoVertexList, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Len(t, monoVertexList.Items, 0)

			rollout := ctlrcommon.CreateTestMVRollout(monoVertexSpec, map[string]string{}, map[string]string{},
				map[string]string{common.AnnotationKeyNumaflowInstanceID: tc.newControllerInstanceID}, map[string]string{},
				&apiv1.MonoVertexRolloutStatus{ProgressiveStatus: apiv1.MonoVertexProgressiveStatus{
					UpgradingMonoVertexStatus: tc.initialUpgradingChildStatus,
					PromotedMonoVertexStatus:  tc.initialPromotedChildStatus,
				}})
			_ = client.Delete(ctx, rollout)

			rollout.Status.Phase = tc.initialRolloutPhase
			if rollout.Status.NameCount == nil {
				rollout.Status.NameCount = new(int32)
			}
			*rollout.Status.NameCount = int32(tc.initialRolloutNameCount)

			if tc.initialInProgressStrategy != nil {
				rollout.Status.UpgradeInProgress = *tc.initialInProgressStrategy
				r.inProgressStrategyMgr.Store.SetStrategy(k8stypes.NamespacedName{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestMonoVertexRolloutName}, *tc.initialInProgressStrategy)
			} else {
				rollout.Status.UpgradeInProgress = apiv1.UpgradeStrategyNoOp
				r.inProgressStrategyMgr.Store.SetStrategy(k8stypes.NamespacedName{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestMonoVertexRolloutName}, apiv1.UpgradeStrategyNoOp)
			}

			// the Reconcile() function does this, so we need to do it before calling reconcile() as well
			rollout.Status.Init(rollout.Generation)

			// AnalysisRun related case
			if tc.analysisRun {
				rollout.Spec.Strategy = &apiv1.PipelineTypeRolloutStrategy{
					PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
						Analysis: apiv1.Analysis{
							Templates: []argorolloutsv1.AnalysisTemplateRef{
								{TemplateName: "test", ClusterScope: false},
							},
						},
					},
				}
			}

			rolloutCopy := *rollout
			err = client.Create(ctx, rollout)
			assert.NoError(t, err)
			// update Status subresource
			rollout.Status = rolloutCopy.Status
			err = client.Status().Update(ctx, rollout)
			assert.NoError(t, err)

			// create the already-existing MonoVertex in Kubernetes
			// this updates everything but the Status subresource

			existingMonoVertexDef := tc.existingOriginalMonoVertexDef
			if existingMonoVertexDef != nil {
				existingMonoVertexDef.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rollout.GetObjectMeta(), apiv1.MonoVertexRolloutGroupVersionKind)}
				monoVertex, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).Create(ctx, existingMonoVertexDef, metav1.CreateOptions{})
				assert.NoError(t, err)
				// update Status subresource
				monoVertex.Status = tc.existingOriginalMonoVertexDef.Status
				_, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).UpdateStatus(ctx, monoVertex, metav1.UpdateOptions{})
				assert.NoError(t, err)
			}

			if tc.existingUpgradingMonoVertexDef != nil {
				existingUpgradeMonoVertexDef := tc.existingUpgradingMonoVertexDef
				existingUpgradeMonoVertexDef.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rollout.GetObjectMeta(), apiv1.MonoVertexRolloutGroupVersionKind)}
				monoVertex, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).Create(ctx, existingUpgradeMonoVertexDef, metav1.CreateOptions{})
				assert.NoError(t, err)

				// update Status subresource
				monoVertex.Status = tc.existingUpgradingMonoVertexDef.Status
				_, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).UpdateStatus(ctx, monoVertex, metav1.UpdateOptions{})
				assert.NoError(t, err)
			}

			_, err = r.reconcile(context.Background(), rollout, time.Now())
			assert.NoError(t, err)

			// check results:
			// Check Phase of Rollout:
			assert.Equal(t, tc.expectedRolloutPhase, rollout.Status.Phase)
			// Check In-Progress Strategy
			assert.Equal(t, tc.expectedInProgressStrategy, rollout.Status.UpgradeInProgress)
			// Check ProgressiveUpgradeSucceeded Condition
			for _, cond := range rollout.Status.Conditions {
				if cond.Type == "ProgressiveUpgradeSucceeded" {
					assert.Equal(t, cond.Status, tc.expectedProgressiveCondition)
				}
			}

			resultMonoVertexList, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Equal(t, len(tc.expectedMonoVertices), len(resultMonoVertexList.Items), resultMonoVertexList.Items)

			for _, monoVertex := range resultMonoVertexList.Items {
				expectedMonoVertexUpgradeState, found := tc.expectedMonoVertices[monoVertex.Name]
				assert.True(t, found)
				resultUpgradeState, found := monoVertex.Labels[common.LabelKeyUpgradeState]
				assert.True(t, found)
				assert.Equal(t, string(expectedMonoVertexUpgradeState), resultUpgradeState)

				if len(tc.expectedMonoVerticesResultState) > 0 {
					expectedMonoVertexResultState, found := tc.expectedMonoVerticesResultState[monoVertex.Name]
					if found {
						resultState, labelFound := monoVertex.Labels[common.LabelKeyProgressiveResultState]
						assert.True(t, labelFound)
						assert.Equal(t, string(expectedMonoVertexResultState), resultState)
					}
				}
			}
		})
	}
}

func Test_CheckForDifferences(t *testing.T) {
	ctx := context.Background()
	numaLogger := logger.FromContext(ctx)

	tests := []struct {
		name                      string
		from                      *unstructured.Unstructured
		to                        *unstructured.Unstructured
		existingChildUpgradeState common.UpgradeState
		originalScaleDefinition   string // only used when existingChildUpgradeState=trial
		expectedError             bool
		expectedResult            bool
	}{
		{
			name: "ObjectsEqual",
			from: func() *unstructured.Unstructured {
				obj := &unstructured.Unstructured{
					Object: map[string]interface{}{
						"spec": map[string]interface{}{
							"some_map": map[string]interface{}{
								"key": "value1",
							},
						},
					},
				}
				obj.SetLabels(map[string]string{"key": "value1"})
				return obj
			}(),
			to: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"spec": map[string]interface{}{
						"some_map": map[string]interface{}{
							"key": "value1",
						},
					},
				},
			},
			existingChildUpgradeState: common.LabelValueUpgradePromoted,
			expectedError:             false,
			expectedResult:            false,
		},
		{
			name: "RequiredLabelsNotPresent",
			from: func() *unstructured.Unstructured {
				obj := &unstructured.Unstructured{}
				obj.SetLabels(map[string]string{"key": "value1"})
				return obj
			}(),
			to: func() *unstructured.Unstructured {
				obj := &unstructured.Unstructured{}
				obj.SetLabels(map[string]string{"key": "value2"})
				return obj
			}(),
			existingChildUpgradeState: common.LabelValueUpgradePromoted,
			expectedError:             false,
			expectedResult:            true,
		},
		{
			name: "RequiredAnnotationsNotPresent",
			from: func() *unstructured.Unstructured {
				obj := &unstructured.Unstructured{}
				return obj
			}(),
			to: func() *unstructured.Unstructured {
				obj := &unstructured.Unstructured{}
				obj.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "1"})
				return obj
			}(),
			existingChildUpgradeState: common.LabelValueUpgradePromoted,
			expectedError:             false,
			expectedResult:            true,
		},
		{
			name: "SpecsDiffer",
			from: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"spec": map[string]interface{}{
						"some_map": map[string]interface{}{},
					},
				},
			},
			to: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"spec": map[string]interface{}{
						"some_map": map[string]interface{}{
							"key": "value1",
						},
					},
				},
			},
			existingChildUpgradeState: common.LabelValueUpgradePromoted,
			expectedError:             false,
			expectedResult:            true,
		},
		{
			name: "Scales Differ - Promoted child",
			from: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"spec": map[string]interface{}{
						"scale": map[string]interface{}{
							"min": int64(1),
							"max": int64(2),
						},
						"some_map": map[string]interface{}{
							"key": "value1",
						},
					},
				},
			},
			to: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"spec": map[string]interface{}{
						"some_map": map[string]interface{}{
							"key": "value1",
						},
					},
				},
			},
			existingChildUpgradeState: common.LabelValueUpgradePromoted,
			expectedError:             false,
			expectedResult:            false, // scale fields are excluded from comparison for promoted children
		},
		{
			name: "Scales Match between Upgrading child and Rollout definition",
			// this is our Upgrading child
			from: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"name": "test-mv",
					},
					"spec": map[string]interface{}{
						"scale": map[string]interface{}{
							"min": int64(5), // current scale (modified by progressive)
							"max": int64(5),
						},
						"some_map": map[string]interface{}{
							"key": "value1",
						},
					},
				},
			},
			// this is basically our Rollout definition
			to: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"spec": map[string]interface{}{
						"scale": map[string]interface{}{
							"min": int64(1), // rollout definition
							"max": int64(2),
						},
						"some_map": map[string]interface{}{
							"key": "value1",
						},
					},
				},
			},
			existingChildUpgradeState: common.LabelValueUpgradeTrial,
			originalScaleDefinition:   `{"min":1,"max":2}`, // original matches rollout
			expectedError:             false,
			expectedResult:            false, // no difference because original matches rollout
		},
		{
			name: "Scales Differ between Upgrading child and Rollout definition",
			// this is our Upgrading child
			from: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"name": "test-mv",
					},
					"spec": map[string]interface{}{
						"scale": map[string]interface{}{
							"min": int64(5), // current scale (modified by progressive)
							"max": int64(5),
						},
						"some_map": map[string]interface{}{
							"key": "value1",
						},
					},
				},
			},
			// this is basically our Rollout definition
			to: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"spec": map[string]interface{}{
						"scale": map[string]interface{}{
							"min": int64(3), // new rollout definition
							"max": int64(4),
						},
						"some_map": map[string]interface{}{
							"key": "value1",
						},
					},
				},
			},
			existingChildUpgradeState: common.LabelValueUpgradeTrial,
			originalScaleDefinition:   `{"min":1,"max":2}`, // original differs from new rollout
			expectedError:             false,
			expectedResult:            true, // difference because original scale != new rollout scale
		},
		{
			name: "Scales Match between Upgrading child and Rollout definition - neither defined with scale",
			// this is our Upgrading child
			from: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"name": "test-mv",
					},
					"spec": map[string]interface{}{
						"scale": map[string]interface{}{
							"min": int64(5), // current scale (modified by progressive)
							"max": int64(5),
						},
						"some_map": map[string]interface{}{
							"key": "value1",
						},
					},
				},
			},
			// this is basically our Rollout definition
			to: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"spec": map[string]interface{}{
						"some_map": map[string]interface{}{
							"key": "value1",
						},
					},
				},
			},
			existingChildUpgradeState: common.LabelValueUpgradeTrial,
			originalScaleDefinition:   "null", // original had no scale
			expectedError:             false,
			expectedResult:            false, // no difference because original (null) matches rollout (no scale)
		},
		{
			name: "Scales Differ between Upgrading child and Rollout definition - Upgrading child was not defined with scale",
			// this is our Upgrading child
			from: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"name": "test-mv",
					},
					"spec": map[string]interface{}{
						"scale": map[string]interface{}{
							"min": int64(5), // current scale (modified by progressive)
							"max": int64(5),
						},
						"some_map": map[string]interface{}{
							"key": "value1",
						},
					},
				},
			},
			// this is basically our Rollout definition
			to: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"spec": map[string]interface{}{
						"scale": map[string]interface{}{
							"min": int64(1),
							"max": int64(2),
						},
						"some_map": map[string]interface{}{
							"key": "value1",
						},
					},
				},
			},
			existingChildUpgradeState: common.LabelValueUpgradeTrial,
			originalScaleDefinition:   "null", // original had no scale, but rollout now has scale
			expectedError:             false,
			expectedResult:            true, // difference because original (null) != rollout (has scale)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reconciler := &MonoVertexRolloutReconciler{}

			// Extract spec from unstructured object and convert to RawExtension
			specData, found, err := unstructured.NestedMap(tt.to.Object, "spec")
			if err != nil {
				t.Fatalf("Failed to extract spec from unstructured object: %v", err)
			}
			if !found {
				specData = make(map[string]interface{})
			}

			specBytes, err := json.Marshal(specData)
			if err != nil {
				t.Fatalf("Failed to marshal spec to JSON: %v", err)
			}

			mvRollout := &apiv1.MonoVertexRollout{
				Spec: apiv1.MonoVertexRolloutSpec{
					MonoVertex: apiv1.MonoVertex{
						Spec: runtime.RawExtension{
							Raw: specBytes,
						},
						Metadata: apiv1.Metadata{
							Annotations: tt.to.GetAnnotations(),
							Labels:      tt.to.GetLabels(),
						},
					},
				},
			}

			// If comparing to an upgrading child, set up the UpgradingMonoVertexStatus
			if tt.existingChildUpgradeState == common.LabelValueUpgradeTrial {
				mvRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus = &apiv1.UpgradingMonoVertexStatus{
					OriginalScaleDefinition: tt.originalScaleDefinition,
				}
				mvRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus.Name = tt.from.GetName()
			}

			needsUpdate, err := reconciler.CheckForDifferencesWithRolloutDef(ctx, tt.from, mvRollout, tt.existingChildUpgradeState)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expectedResult, needsUpdate)
			numaLogger.Debugf("Test %s passed", tt.name)
		})
	}
}

func TestGetScaleValuesFromMonoVertexSpec(t *testing.T) {
	one := int64(1)
	ten := int64(10)
	tests := []struct {
		name                    string
		input                   map[string]interface{}
		expectedScaleDefinition *apiv1.ScaleDefinition
		expectError             bool
	}{
		{
			name: "BothValuesPresent",
			input: map[string]interface{}{
				"scale": map[string]interface{}{
					"min":        int64(1),
					"max":        int64(10),
					"anotherKey": "anotherValue",
				},
			},
			expectedScaleDefinition: &apiv1.ScaleDefinition{Min: &one, Max: &ten},
			expectError:             false,
		},
		{
			name:                    "NoScalePresent",
			input:                   map[string]interface{}{},
			expectedScaleDefinition: nil,
			expectError:             false,
		},
		{
			name: "NoValuesPresent",
			input: map[string]interface{}{
				"scale": map[string]interface{}{},
			},
			expectedScaleDefinition: &apiv1.ScaleDefinition{Min: nil, Max: nil},
			expectError:             false,
		},
		{
			name: "OneValuePresent",
			input: map[string]interface{}{
				"scale": map[string]interface{}{
					"min": int64(1),
				},
			},
			expectedScaleDefinition: &apiv1.ScaleDefinition{Min: &one, Max: nil},
			expectError:             false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scaleDefinition, err := getScaleValuesFromMonoVertexSpec(tt.input)
			if (err != nil) != tt.expectError {
				t.Errorf("Expected error: %v, got: %v", tt.expectError, err)
			}
			assert.Equal(t, tt.expectedScaleDefinition, scaleDefinition)
		})
	}
}

func Test_scaleMonoVertex(t *testing.T) {
	restConfig, numaflowClientSet, client, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetClientSets(restConfig))

	ctx := context.Background()
	two32 := int32(2)
	four32 := int32(4)
	eight32 := int32(8)
	four64 := int64(4)
	eight64 := int64(8)
	tenUint := uint32(10)

	tests := []struct {
		name               string
		originalScale      numaflowv1.Scale
		newScaleDefinition *apiv1.ScaleDefinition
		expectedScale      numaflowv1.Scale
	}{
		{
			name: "newMin,newMax",
			originalScale: numaflowv1.Scale{
				Min:             &two32,
				Max:             &four32,
				LookbackSeconds: &tenUint,
			},
			newScaleDefinition: &apiv1.ScaleDefinition{
				Min: &four64,
				Max: &eight64,
			},
			expectedScale: numaflowv1.Scale{
				Min:             &four32,
				Max:             &eight32,
				LookbackSeconds: &tenUint,
			},
		},
		{
			name: "newNullValues",
			originalScale: numaflowv1.Scale{
				Min:             &two32,
				Max:             &four32,
				LookbackSeconds: &tenUint,
			},
			newScaleDefinition: &apiv1.ScaleDefinition{
				Min: nil,
				Max: nil,
			},
			expectedScale: numaflowv1.Scale{
				Min:             nil,
				Max:             nil,
				LookbackSeconds: &tenUint,
			},
		},
		{
			name: "newMin,nullMax",
			originalScale: numaflowv1.Scale{
				Min:             &two32,
				Max:             &four32,
				LookbackSeconds: &tenUint,
			},
			newScaleDefinition: &apiv1.ScaleDefinition{
				Min: &four64,
				Max: nil,
			},
			expectedScale: numaflowv1.Scale{
				Min:             &four32,
				Max:             nil,
				LookbackSeconds: &tenUint,
			},
		},
		{
			name: "newMax,nullMin",
			originalScale: numaflowv1.Scale{
				Min:             &two32,
				Max:             &four32,
				LookbackSeconds: &tenUint,
			},
			newScaleDefinition: &apiv1.ScaleDefinition{
				Min: nil,
				Max: &eight64,
			},
			expectedScale: numaflowv1.Scale{
				Min:             nil,
				Max:             &eight32,
				LookbackSeconds: &tenUint,
			},
		},
		{
			name: "nullScale",
			originalScale: numaflowv1.Scale{
				Min:             &two32,
				Max:             &four32,
				LookbackSeconds: &tenUint,
			},
			newScaleDefinition: nil,

			expectedScale: numaflowv1.Scale{
				Min:             nil,
				Max:             nil,
				LookbackSeconds: nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// first delete MonoVertex in case they already exist, in Kubernetes
			_ = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})

			_, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).Create(ctx, createMonoVertexOfScale(tt.originalScale), metav1.CreateOptions{})
			assert.NoError(t, err)

			// get the monovertex as Unstructured type
			mvUnstruc := &unstructured.Unstructured{}
			mvUnstruc.SetGroupVersionKind(schema.GroupVersionKind{
				Kind:    common.NumaflowMonoVertexKind,
				Group:   common.NumaflowAPIGroup,
				Version: common.NumaflowAPIVersion,
			})

			namespacedName := k8stypes.NamespacedName{Namespace: ctlrcommon.DefaultTestNamespace, Name: ctlrcommon.DefaultTestMonoVertexName}
			err = client.Get(ctx, namespacedName, mvUnstruc)
			assert.NoError(t, err)

			err = scaleMonoVertex(ctx, mvUnstruc, tt.newScaleDefinition, client)
			assert.NoError(t, err)

			// Get result MonoVertex
			resultMonoVertex, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).Get(ctx, ctlrcommon.DefaultTestMonoVertexName, metav1.GetOptions{})
			assert.NoError(t, err)
			assert.NotNil(t, resultMonoVertex)
			assert.Equal(t, tt.expectedScale, resultMonoVertex.Spec.Scale)
		})
	}
}

func createMonoVertexOfScale(scaleDefinition numaflowv1.Scale) *numaflowv1.MonoVertex {
	mv := createMonoVertex(numaflowv1.MonoVertexPhaseRunning, numaflowv1.Status{}, map[string]string{}, map[string]string{})
	mv.Spec.Scale = scaleDefinition
	return mv
}

func TestProgressiveUnsupported(t *testing.T) {
	ctx := context.Background()

	// Create a test reconciler
	reconciler := &MonoVertexRolloutReconciler{}

	tests := []struct {
		name     string
		riders   []apiv1.Rider
		expected bool
	}{
		{
			name:     "No riders",
			riders:   []apiv1.Rider{},
			expected: false,
		},
		{
			name: "ConfigMap rider only",
			riders: []apiv1.Rider{
				{
					Progressive: true,
					Definition: runtime.RawExtension{
						Raw: createConfigMapRawExtension(t),
					},
				},
			},
			expected: false,
		},
		{
			name: "HPA rider - should return true",
			riders: []apiv1.Rider{
				{
					Progressive: true,
					Definition: runtime.RawExtension{
						Raw: createHPARawExtension(t),
					},
				},
			},
			expected: true,
		},
		{
			name: "Mixed riders with HPA - should return true",
			riders: []apiv1.Rider{
				{
					Progressive: true,
					Definition: runtime.RawExtension{
						Raw: createConfigMapRawExtension(t),
					},
				},
				{
					Progressive: true,
					Definition: runtime.RawExtension{
						Raw: createHPARawExtension(t),
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a MonoVertexRollout with the test riders
			monoVertexRollout := &apiv1.MonoVertexRollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rollout",
					Namespace: "test-namespace",
				},
				Spec: apiv1.MonoVertexRolloutSpec{
					Riders: tt.riders,
				},
			}

			result := reconciler.progressiveUnsupported(ctx, monoVertexRollout)
			assert.Equal(t, tt.expected, result, "ProgressiveUnsupported should return %v for test case: %s", tt.expected, tt.name)
		})
	}
}

// Helper functions to create RawExtension objects for different resource types

func createConfigMapRawExtension(t *testing.T) []byte {
	t.Helper()
	configMap := corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-configmap",
		},
		Data: map[string]string{
			"key": "value",
		},
	}
	raw, err := json.Marshal(configMap)
	assert.NoError(t, err)
	return raw
}

func createHPARawExtension(t *testing.T) []byte {
	t.Helper()
	hpa := autoscalingv2.HorizontalPodAutoscaler{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "autoscaling/v2",
			Kind:       "HorizontalPodAutoscaler",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-hpa",
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			MinReplicas: ptr.To(int32(1)),
			MaxReplicas: 10,
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "numaflow.numaproj.io/v1alpha1",
				Kind:       "MonoVertex",
				Name:       "test-monovertex",
			},
		},
	}
	raw, err := json.Marshal(hpa)
	assert.NoError(t, err)
	return raw
}

func Test_SkipProgressiveAssessment(t *testing.T) {
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

	recorder := record.NewFakeRecorder(64)
	r := NewMonoVertexRolloutReconciler(
		client,
		scheme.Scheme,
		ctlrcommon.TestCustomMetrics,
		recorder)

	// Helper function to create a MonoVertex spec with desiredPhase set
	createMonoVertexSpecWithLifecycle := func(desiredPhase string) numaflowv1.MonoVertexSpec {
		spec := monoVertexSpec.DeepCopy()
		if desiredPhase != "" {
			spec.Lifecycle.DesiredPhase = numaflowv1.MonoVertexPhase(desiredPhase)
		}
		return *spec
	}

	// Helper function to create a MonoVertex spec scaled to 0
	createMonoVertexSpecScaledToZero := func() numaflowv1.MonoVertexSpec {
		spec := monoVertexSpec.DeepCopy()
		zero := int32(0)
		spec.Scale = numaflowv1.Scale{Max: &zero}
		return *spec
	}

	testCases := []struct {
		name                   string
		monoVertexSpec         numaflowv1.MonoVertexSpec
		forcePromoteConfigured bool
		riders                 []apiv1.Rider
		expectedSkip           bool
	}{
		{
			name:                   "MonoVertex can ingest data, no ForcePromote, no HPA rider - should NOT skip",
			monoVertexSpec:         monoVertexSpec,
			forcePromoteConfigured: false,
			riders:                 nil,
			expectedSkip:           false,
		},
		{
			name:                   "MonoVertex paused - should skip",
			monoVertexSpec:         createMonoVertexSpecWithLifecycle("Paused"),
			forcePromoteConfigured: false,
			riders:                 nil,
			expectedSkip:           true,
		},
		{
			name:                   "MonoVertex scaled to 0 - should skip",
			monoVertexSpec:         createMonoVertexSpecScaledToZero(),
			forcePromoteConfigured: false,
			riders:                 nil,
			expectedSkip:           true,
		},
		{
			name:                   "ForcePromote set to true - should skip",
			monoVertexSpec:         monoVertexSpec,
			forcePromoteConfigured: true,
			riders:                 nil,
			expectedSkip:           true,
		},
		{
			name:                   "HPA rider present - should skip",
			monoVertexSpec:         monoVertexSpec,
			forcePromoteConfigured: false,
			riders: []apiv1.Rider{
				{
					Progressive: true,
					Definition: runtime.RawExtension{
						Raw: createHPARawExtension(t),
					},
				},
			},
			expectedSkip: true,
		},
		{
			name:                   "ConfigMap rider only (no HPA) - should NOT skip",
			monoVertexSpec:         monoVertexSpec,
			forcePromoteConfigured: false,
			riders: []apiv1.Rider{
				{
					Progressive: true,
					Definition: runtime.RawExtension{
						Raw: createConfigMapRawExtension(t),
					},
				},
			},
			expectedSkip: false,
		},
		{
			name:                   "Mixed riders including HPA - should skip",
			monoVertexSpec:         monoVertexSpec,
			forcePromoteConfigured: false,
			riders: []apiv1.Rider{
				{
					Progressive: true,
					Definition: runtime.RawExtension{
						Raw: createConfigMapRawExtension(t),
					},
				},
				{
					Progressive: true,
					Definition: runtime.RawExtension{
						Raw: createHPARawExtension(t),
					},
				},
			},
			expectedSkip: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Marshal monoVertexSpec to RawExtension
			monoVertexSpecBytes, err := json.Marshal(tc.monoVertexSpec)
			assert.NoError(t, err)

			// Create MonoVertexRollout
			monoVertexRollout := &apiv1.MonoVertexRollout{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ctlrcommon.DefaultTestNamespace,
					Name:      "test-monovertex-rollout",
				},
				Spec: apiv1.MonoVertexRolloutSpec{
					MonoVertex: apiv1.MonoVertex{
						Spec: runtime.RawExtension{Raw: monoVertexSpecBytes},
					},
					Riders: tc.riders,
				},
			}

			// Set ForcePromote if needed
			if tc.forcePromoteConfigured {
				monoVertexRollout.Spec.Strategy = &apiv1.PipelineTypeRolloutStrategy{
					PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
						Progressive: apiv1.ProgressiveStrategy{
							ForcePromote: true,
						},
					},
				}
			}

			skip, _, err := r.SkipProgressiveAssessment(ctx, monoVertexRollout)

			assert.NoError(t, err)
			assert.Equal(t, tc.expectedSkip, skip, "skip result mismatch")
		})
	}
}

// Technically, IsUpgradeReplacementRequired() function is in progressive.go file, but we test it here because we can take advantage of also testing code specific to the MonoVertexRollout controller.
func Test_MVRollout_IsUpgradeReplacementRequired(t *testing.T) {
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

	// Create a real MonoVertexRolloutReconciler
	scheme := scheme.Scheme
	reconciler := NewMonoVertexRolloutReconciler(client, scheme, nil, nil)

	// Create a MonoVertexSpec
	// For rollout spec, pass "{{.monovertex-name}}" as nameTemplateValue
	// For child specs, pass the evaluated name like "my-monovertex-0" as nameTemplateValue
	createMonoVertexSpec := func(image string, nameTemplateValue string) numaflowv1.MonoVertexSpec {
		return numaflowv1.MonoVertexSpec{
			Replicas: ptr.To(int32(1)),
			Source: &numaflowv1.Source{
				UDSource: &numaflowv1.UDSource{
					Container: &numaflowv1.Container{
						Image: image,
					},
				},
				UDTransformer: &numaflowv1.UDTransformer{
					Container: &numaflowv1.Container{
						Image: "quay.io/numaio/numaflow-rs/source-transformer-now:stable",
						Env: []corev1.EnvVar{
							{
								Name:  "my-key",
								Value: nameTemplateValue,
							},
						},
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
	}

	testCases := []struct {
		name                      string
		rolloutSpec               numaflowv1.MonoVertexSpec
		rolloutLabels             map[string]string
		rolloutAnnotations        map[string]string
		promotedChildSpec         numaflowv1.MonoVertexSpec
		promotedChildName         string
		promotedChildLabels       map[string]string
		promotedChildAnnotations  map[string]string
		upgradingChildSpec        numaflowv1.MonoVertexSpec
		upgradingChildName        string
		upgradingChildLabels      map[string]string
		upgradingChildAnnotations map[string]string
		upgradingMonoVertexStatus *apiv1.UpgradingMonoVertexStatus
		expectedDiffFromUpgrading bool
		expectedDiffFromPromoted  bool
	}{

		{
			name:        "different from Upgrading only (different image)- rollout matches Promoted",
			rolloutSpec: createMonoVertexSpec("quay.io/numaio/numaflow-java/source-simple-source:v2.0.0", "{{.monovertex-name}}"),
			rolloutLabels: map[string]string{
				"my-label": "{{.monovertex-name}}",
			},
			rolloutAnnotations: map[string]string{
				"my-annotation": "{{.monovertex-name}}",
			},
			promotedChildSpec: createMonoVertexSpec("quay.io/numaio/numaflow-java/source-simple-source:v2.0.0", "my-monovertex-0"),
			promotedChildName: "my-monovertex-0",
			promotedChildLabels: map[string]string{
				"my-label":   "my-monovertex-0", // this is a match
				"my-label-2": "something",       // this is no problem to have an extra label
			},
			promotedChildAnnotations: map[string]string{
				"my-annotation":   "my-monovertex-0", // this is a match
				"my-annotation-2": "something",       // this is no problem to have an extra annotation
			},
			upgradingChildSpec: createMonoVertexSpec("quay.io/numaio/numaflow-java/source-simple-source:v1.5.0", "my-monovertex-1"),
			upgradingChildName: "my-monovertex-1",
			upgradingChildLabels: map[string]string{
				"my-label":   "my-monovertex-1", // this is a match
				"my-label-2": "something",       // this is no problem to have an extra label
			},
			upgradingChildAnnotations: map[string]string{
				"my-annotation":   "my-monovertex-1", // this is a match
				"my-annotation-2": "something",       // this is no problem to have an extra annotation
			},
			expectedDiffFromUpgrading: true,
			expectedDiffFromPromoted:  false,
		},
		{
			name:        "different from Promoted only (different image) - rollout matches Upgrading",
			rolloutSpec: createMonoVertexSpec("quay.io/numaio/numaflow-java/source-simple-source:v1.5.0", "{{.monovertex-name}}"),
			rolloutLabels: map[string]string{
				"my-label": "{{.monovertex-name}}",
			},
			rolloutAnnotations: map[string]string{
				"my-annotation": "{{.monovertex-name}}",
			},
			promotedChildSpec: createMonoVertexSpec("quay.io/numaio/numaflow-java/source-simple-source:v1.0.0", "my-monovertex-0"),
			promotedChildName: "my-monovertex-0",
			promotedChildLabels: map[string]string{
				"my-label":   "my-monovertex-0", // this is a match
				"my-label-2": "something",       // this is no problem to have an extra label
			},
			promotedChildAnnotations: map[string]string{
				"my-annotation":   "my-monovertex-0", // this is a match
				"my-annotation-2": "something",       // this is no problem to have an extra annotation
			},
			upgradingChildSpec: createMonoVertexSpec("quay.io/numaio/numaflow-java/source-simple-source:v1.5.0", "my-monovertex-1"),
			upgradingChildName: "my-monovertex-1",
			upgradingChildLabels: map[string]string{
				"my-label":   "my-monovertex-1", // this is a match
				"my-label-2": "something",       // this is no problem to have an extra label
			},
			upgradingChildAnnotations: map[string]string{
				"my-annotation":   "my-monovertex-1", // this is a match
				"my-annotation-2": "something",       // this is no problem to have an extra annotation
			},
			expectedDiffFromUpgrading: false,
			expectedDiffFromPromoted:  true,
		},
		{
			name:        "different from both - required metadata not present",
			rolloutSpec: createMonoVertexSpec("quay.io/numaio/numaflow-java/source-simple-source:v1.0.0", "{{.monovertex-name}}"),
			rolloutLabels: map[string]string{
				"my-label":       "{{.monovertex-name}}",
				"required-label": "important-value",
			},
			rolloutAnnotations: map[string]string{
				"my-annotation":       "{{.monovertex-name}}",
				"required-annotation": "important-value",
			},
			promotedChildSpec: createMonoVertexSpec("quay.io/numaio/numaflow-java/source-simple-source:v1.0.0", "my-monovertex-0"),
			promotedChildName: "my-monovertex-0",
			promotedChildLabels: map[string]string{
				"my-label":       "my-monovertex-0", // this is a match
				"required-label": "important-value", // this is a match
			},
			promotedChildAnnotations: map[string]string{
				"my-annotation": "my-monovertex-0", // this is a match
				// Missing "required-annotation"
			},
			upgradingChildSpec: createMonoVertexSpec("quay.io/numaio/numaflow-java/source-simple-source:v1.0.0", "my-monovertex-1"),
			upgradingChildName: "my-monovertex-1",
			upgradingChildLabels: map[string]string{
				"my-label":       "my-monovertex-1",           // this is a match
				"required-label": "different-important-value", // key is present but value is different
			},
			upgradingChildAnnotations: map[string]string{
				"my-annotation":       "my-monovertex-1", // this is a match
				"required-annotation": "important-value", // this is a match
			},
			expectedDiffFromUpgrading: true,
			expectedDiffFromPromoted:  true,
		},
	}

	// Helper function to create a MonoVertex child with required labels/annotations
	createMonoVertexChild := func(spec numaflowv1.MonoVertexSpec, name string, labels, annotations map[string]string, upgradeState common.UpgradeState) *unstructured.Unstructured {
		monoVertex := ctlrcommon.CreateTestMonoVertexOfSpec(
			spec,
			name,
			numaflowv1.MonoVertexPhaseRunning,
			numaflowv1.Status{
				Conditions: []metav1.Condition{
					{
						Type:               string(numaflowv1.MonoVertexConditionDaemonHealthy),
						Status:             metav1.ConditionTrue,
						Reason:             "healthy",
						LastTransitionTime: metav1.NewTime(time.Now()),
					},
				},
			},
			labels,
			annotations,
		)
		// Add required labels
		if monoVertex.Labels == nil {
			monoVertex.Labels = make(map[string]string)
		}
		monoVertex.Labels[common.LabelKeyParentRollout] = ctlrcommon.DefaultTestMonoVertexRolloutName
		monoVertex.Labels[common.LabelKeyUpgradeState] = string(upgradeState)
		if monoVertex.Annotations == nil {
			monoVertex.Annotations = make(map[string]string)
		}
		monoVertex.Annotations[common.AnnotationKeyNumaflowInstanceID] = "1"

		// Convert to unstructured
		unstructMap, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(monoVertex)
		return &unstructured.Unstructured{Object: unstructMap}
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create MonoVertexRollout with template values
			upgradingStatus := tc.upgradingMonoVertexStatus
			if upgradingStatus == nil {
				upgradingStatus = &apiv1.UpgradingMonoVertexStatus{}
			}
			mvRollout := ctlrcommon.CreateTestMVRollout(
				tc.rolloutSpec,
				map[string]string{}, // rollout annotations
				map[string]string{}, // rollout labels
				tc.rolloutAnnotations,
				tc.rolloutLabels,
				&apiv1.MonoVertexRolloutStatus{
					ProgressiveStatus: apiv1.MonoVertexProgressiveStatus{
						UpgradingMonoVertexStatus: upgradingStatus,
						PromotedMonoVertexStatus:  &apiv1.PromotedMonoVertexStatus{},
					},
				},
			)

			// Create Promoted and Upgrading MonoVertices
			promotedChildUnstruct := createMonoVertexChild(
				tc.promotedChildSpec,
				tc.promotedChildName,
				tc.promotedChildLabels,
				tc.promotedChildAnnotations,
				common.LabelValueUpgradePromoted,
			)
			upgradingChildUnstruct := createMonoVertexChild(
				tc.upgradingChildSpec,
				tc.upgradingChildName,
				tc.upgradingChildLabels,
				tc.upgradingChildAnnotations,
				common.LabelValueUpgradeTrial,
			)

			// Call progressive.IsUpgradeReplacementRequired with the real controller
			differentFromUpgrading, differentFromPromoted, err := progressive.IsUpgradeReplacementRequired(
				ctx,
				mvRollout,
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
