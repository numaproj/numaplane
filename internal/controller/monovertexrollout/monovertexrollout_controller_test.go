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
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	argorolloutsv1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/config"
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

	// TODO: will use in AnalysisRun test later
	// testTemplate = argorolloutsv1.AnalysisTemplate{
	// 	ObjectMeta: metav1.ObjectMeta{
	// 		Name:      "test",
	// 		Namespace: ctlrcommon.DefaultTestNamespace,
	// 	},
	// 	TypeMeta: metav1.TypeMeta{
	// 		Kind:       "AnalysisTemplate",
	// 		APIVersion: "argoproj.io/v1alpha1",
	// 	},
	// 	Spec: argorolloutsv1.AnalysisTemplateSpec{
	// 		Args: []argorolloutsv1.Argument{
	// 			{Name: "monovertex-name"},
	// 			{Name: "monovertex-namespace"},
	// 		},
	// 		Metrics: []argorolloutsv1.Metric{
	// 			{
	// 				Name: "return-true",
	// 				Provider: argorolloutsv1.MetricProvider{
	// 					Prometheus: &argorolloutsv1.PrometheusMetric{
	// 						Address: " http://prometheus-kube-prometheus-prometheus.prometheus.svc.cluster.local:9090",
	// 						Query:   "vector(1) == vector(2)",
	// 					},
	// 				},
	// 				SuccessCondition: "true",
	// 			},
	// 		},
	// 	},
	// }

	analysisRunName = "monovertexrollout-test-1"
	testAnalysisRun = argorolloutsv1.AnalysisRun{
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
				{Name: "monovertex-name", Value: &analysisRunName},
				{Name: "monovertex-namespace", Value: &ctlrcommon.DefaultTestNamespace},
			},
		},
		Status: argorolloutsv1.AnalysisRunStatus{
			Phase:       argorolloutsv1.AnalysisPhaseSuccessful,
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

// process an existing monoVertex in this test, the user preferred strategy is Progressive
func Test_processExistingMonoVertex_Progressive(t *testing.T) {
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
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics()
	}

	recorder := record.NewFakeRecorder(64)

	err = client.Create(ctx, &testAnalysisRun)
	assert.NoError(t, err)

	r := NewMonoVertexRolloutReconciler(
		client,
		scheme.Scheme,
		ctlrcommon.TestCustomMetrics,
		recorder)

	progressiveUpgradeStrategy := apiv1.UpgradeStrategyProgressive

	defaultOriginalMonoVertexDef := *createMonoVertex(
		numaflowv1.MonoVertexPhaseRunning,
		numaflowv1.Status{},
		map[string]string{
			common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
			common.LabelKeyParentRollout: ctlrcommon.DefaultTestMonoVertexRolloutName,
		},
		map[string]string{
			common.AnnotationKeyNumaflowInstanceID: "0",
		})
	defaultUpgradingMonoVertexDef := ctlrcommon.CreateTestMonoVertexOfSpec(
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
			common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeInProgress),
			common.LabelKeyParentRollout: ctlrcommon.DefaultTestMonoVertexRolloutName,
		},
		map[string]string{
			common.AnnotationKeyNumaflowInstanceID: "1",
		})

	testCases := []struct {
		name                           string
		newControllerInstanceID        string
		existingOriginalMonoVertexDef  numaflowv1.MonoVertex
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

		expectedMonoVertices map[string]common.UpgradeState // after reconcile(), these are the only monoVertexs we expect to exist along with their expected UpgradeState

	}{
		{
			name:                           "Instance annotation difference results in Progressive",
			newControllerInstanceID:        "1",
			existingOriginalMonoVertexDef:  defaultOriginalMonoVertexDef,
			existingUpgradingMonoVertexDef: nil,
			initialRolloutPhase:            apiv1.PhaseDeployed,
			initialRolloutNameCount:        1,
			initialInProgressStrategy:      nil,
			initialUpgradingChildStatus:    nil,
			initialPromotedChildStatus: &apiv1.PromotedMonoVertexStatus{
				PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
					PromotedChildStatus: apiv1.PromotedChildStatus{
						Name: ctlrcommon.DefaultTestMonoVertexRolloutName + "-0",
					},
					AllVerticesScaledDown: true,
				},
			},
			analysisRun:                  false,
			expectedInProgressStrategy:   apiv1.UpgradeStrategyProgressive,
			expectedRolloutPhase:         apiv1.PhasePending,
			expectedProgressiveCondition: metav1.ConditionUnknown,
			expectedMonoVertices: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-0": common.LabelValueUpgradePromoted,
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-1": common.LabelValueUpgradeInProgress,
			},
		},
		{
			name:                           "AnalysisRun successful",
			newControllerInstanceID:        "1",
			existingOriginalMonoVertexDef:  defaultOriginalMonoVertexDef,
			existingUpgradingMonoVertexDef: defaultUpgradingMonoVertexDef,
			initialRolloutPhase:            apiv1.PhasePending,
			initialRolloutNameCount:        2,
			initialInProgressStrategy:      &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: &apiv1.UpgradingMonoVertexStatus{
				UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                   ctlrcommon.DefaultTestMonoVertexRolloutName + "-1",
						AssessmentStartTime:    &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
						AssessmentEndTime:      &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
						AssessmentResult:       apiv1.AssessmentResultUnknown,
						InitializationComplete: true,
					},
					Analysis: apiv1.AnalysisStatus{
						AnalysisRunName: ctlrcommon.DefaultTestMonoVertexRolloutName + "-1",
						StartTime:       &metav1.Time{Time: time.Now().Add(-45 * time.Second)},
						EndTime:         &metav1.Time{Time: time.Now().Add(-40 * time.Second)},
						Phase:           argorolloutsv1.AnalysisPhaseSuccessful,
					},
				},
			},
			initialPromotedChildStatus: &apiv1.PromotedMonoVertexStatus{
				PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
					PromotedChildStatus: apiv1.PromotedChildStatus{
						Name: ctlrcommon.DefaultTestMonoVertexRolloutName + "-0",
					},
				},
			},
			analysisRun:                  true,
			expectedInProgressStrategy:   apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:         apiv1.PhaseDeployed,
			expectedProgressiveCondition: metav1.ConditionTrue,
			// original MonoVertex deleted, new one promoted
			expectedMonoVertices: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-1": common.LabelValueUpgradePromoted,
			},
		},
		{
			name:                           "Progressive deployed successfully",
			newControllerInstanceID:        "1",
			existingOriginalMonoVertexDef:  defaultOriginalMonoVertexDef,
			existingUpgradingMonoVertexDef: defaultUpgradingMonoVertexDef,
			initialRolloutPhase:            apiv1.PhasePending,
			initialRolloutNameCount:        2,
			initialInProgressStrategy:      &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: &apiv1.UpgradingMonoVertexStatus{
				UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                   ctlrcommon.DefaultTestMonoVertexRolloutName + "-1",
						AssessmentStartTime:    &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
						AssessmentEndTime:      &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
						AssessmentResult:       apiv1.AssessmentResultSuccess,
						InitializationComplete: true,
					},
				},
			},
			initialPromotedChildStatus: &apiv1.PromotedMonoVertexStatus{
				PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
					PromotedChildStatus: apiv1.PromotedChildStatus{
						Name: ctlrcommon.DefaultTestMonoVertexRolloutName + "-0",
					},
					AllVerticesScaledDown: true,
				},
			},
			analysisRun:                  false,
			expectedInProgressStrategy:   apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:         apiv1.PhaseDeployed,
			expectedProgressiveCondition: metav1.ConditionTrue,
			// original MonoVertex deleted, new one promoted
			expectedMonoVertices: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-1": common.LabelValueUpgradePromoted,
			},
		},
		{
			name:                          "Progressive deployment failed",
			newControllerInstanceID:       "1",
			existingOriginalMonoVertexDef: defaultOriginalMonoVertexDef,
			existingUpgradingMonoVertexDef: ctlrcommon.CreateTestMonoVertexOfSpec(
				monoVertexSpec, ctlrcommon.DefaultTestMonoVertexRolloutName+"-1",
				numaflowv1.MonoVertexPhaseFailed,
				numaflowv1.Status{
					Conditions: []metav1.Condition{},
				},
				map[string]string{
					common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeInProgress),
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
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                   ctlrcommon.DefaultTestMonoVertexRolloutName + "-1",
						AssessmentStartTime:    &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
						AssessmentEndTime:      &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
						AssessmentResult:       apiv1.AssessmentResultFailure,
						InitializationComplete: true,
					},
				},
			},
			initialPromotedChildStatus: &apiv1.PromotedMonoVertexStatus{
				PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
					PromotedChildStatus: apiv1.PromotedChildStatus{
						Name: ctlrcommon.DefaultTestMonoVertexRolloutName + "-0",
					},
					AllVerticesScaledDown: true,
					ScaleValues:           map[string]apiv1.ScaleValues{ctlrcommon.DefaultTestMonoVertexRolloutName + "-0": {OriginalScaleMinMax: ctlrcommon.DefaultScaleJSONString, ScaleTo: ctlrcommon.DefaultScaleTo}},
				},
			},
			analysisRun:                  false,
			expectedInProgressStrategy:   apiv1.UpgradeStrategyProgressive,
			expectedRolloutPhase:         apiv1.PhasePending,
			expectedProgressiveCondition: metav1.ConditionFalse,
			expectedMonoVertices: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-0": common.LabelValueUpgradePromoted,
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-1": common.LabelValueUpgradeInProgress,
			},
		},
		{
			name:                           "AnalysisRun failed",
			newControllerInstanceID:        "1",
			existingOriginalMonoVertexDef:  defaultOriginalMonoVertexDef,
			existingUpgradingMonoVertexDef: defaultUpgradingMonoVertexDef,
			initialRolloutPhase:            apiv1.PhasePending,
			initialRolloutNameCount:        2,
			initialInProgressStrategy:      &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: &apiv1.UpgradingMonoVertexStatus{
				UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                   ctlrcommon.DefaultTestMonoVertexRolloutName + "-1",
						AssessmentStartTime:    &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
						AssessmentResult:       apiv1.AssessmentResultUnknown,
						InitializationComplete: true,
					},
					Analysis: apiv1.AnalysisStatus{
						AnalysisRunName: ctlrcommon.DefaultTestMonoVertexRolloutName + "-1",
						StartTime:       &metav1.Time{Time: time.Now().Add(-45 * time.Second)},
						EndTime:         &metav1.Time{Time: time.Now().Add(-40 * time.Second)},
						Phase:           argorolloutsv1.AnalysisPhaseFailed,
					},
				},
			},
			initialPromotedChildStatus: &apiv1.PromotedMonoVertexStatus{
				PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
					PromotedChildStatus: apiv1.PromotedChildStatus{
						Name: ctlrcommon.DefaultTestMonoVertexRolloutName + "-0",
					},
					ScaleValues: map[string]apiv1.ScaleValues{ctlrcommon.DefaultTestMonoVertexRolloutName + "-0": {OriginalScaleMinMax: ctlrcommon.DefaultScaleJSONString, ScaleTo: ctlrcommon.DefaultScaleTo}},
				},
			},
			analysisRun:                  true,
			expectedInProgressStrategy:   apiv1.UpgradeStrategyProgressive,
			expectedRolloutPhase:         apiv1.PhasePending,
			expectedProgressiveCondition: metav1.ConditionFalse,
			expectedMonoVertices: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-0": common.LabelValueUpgradePromoted,
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-1": common.LabelValueUpgradeInProgress,
			},
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
			existingMonoVertexDef := &tc.existingOriginalMonoVertexDef
			existingMonoVertexDef.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rollout.GetObjectMeta(), apiv1.MonoVertexRolloutGroupVersionKind)}
			monoVertex, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).Create(ctx, existingMonoVertexDef, metav1.CreateOptions{})
			assert.NoError(t, err)
			// update Status subresource
			monoVertex.Status = tc.existingOriginalMonoVertexDef.Status
			_, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).UpdateStatus(ctx, monoVertex, metav1.UpdateOptions{})
			assert.NoError(t, err)

			if tc.existingUpgradingMonoVertexDef != nil {
				existingUpgradeMonoVertexDef := tc.existingUpgradingMonoVertexDef
				existingUpgradeMonoVertexDef.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rollout.GetObjectMeta(), apiv1.MonoVertexRolloutGroupVersionKind)}
				monoVertex, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).Create(ctx, existingUpgradeMonoVertexDef, metav1.CreateOptions{})
				assert.NoError(t, err)

				// update Status subresource
				monoVertex.Status = tc.existingUpgradingMonoVertexDef.Status
				_, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).UpdateStatus(ctx, monoVertex, metav1.UpdateOptions{})
				assert.NoError(t, err)
			}

			_, err = r.reconcile(context.Background(), rollout, time.Now())
			assert.NoError(t, err)

			////// check results:
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
			}
		})
	}
}

func TestChildNeedsUpdating(t *testing.T) {
	ctx := context.Background()
	numaLogger := logger.FromContext(ctx)

	tests := []struct {
		name           string
		from           *unstructured.Unstructured
		to             *unstructured.Unstructured
		expectedError  bool
		expectedResult bool
	}{
		{
			name: "ObjectsEqual",

			from: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"spec": map[string]interface{}{
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
			expectedError:  false,
			expectedResult: false,
		},
		{
			name: "LabelsDiffer",
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
			expectedError:  false,
			expectedResult: true,
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
			expectedError:  false,
			expectedResult: true,
		},
		{
			name: "OnlyReplicasDiffer",
			from: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"spec": map[string]interface{}{
						"replicas": 1,
						"some_map": map[string]interface{}{
							"key": "value1",
						},
					},
				},
			},
			to: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"spec": map[string]interface{}{
						"replicas": 2,
						"some_map": map[string]interface{}{
							"key": "value1",
						},
					},
				},
			},
			expectedError:  false,
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reconciler := &MonoVertexRolloutReconciler{}
			needsUpdate, err := reconciler.ChildNeedsUpdating(ctx, tt.from, tt.to)

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
