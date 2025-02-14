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
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

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

	r := NewMonoVertexRolloutReconciler(
		client,
		scheme.Scheme,
		ctlrcommon.TestCustomMetrics,
		recorder)

	progressiveUpgradeStrategy := apiv1.UpgradeStrategyProgressive

	testCases := []struct {
		name                          string
		newControllerInstanceID       string
		existingOriginalMonoVertexDef numaflowv1.MonoVertex
		existingUpgradeMonoVertexDef  *numaflowv1.MonoVertex
		initialRolloutPhase           apiv1.Phase
		initialRolloutNameCount       int
		initialInProgressStrategy     *apiv1.UpgradeStrategy
		initialUpgradingChildStatus   *apiv1.UpgradingMonoVertexStatus
		initialPromotedChildStatus    *apiv1.PromotedMonoVertexStatus

		expectedInProgressStrategy apiv1.UpgradeStrategy
		expectedRolloutPhase       apiv1.Phase

		expectedMonoVertices map[string]common.UpgradeState // after reconcile(), these are the only monoVertexs we expect to exist along with their expected UpgradeState

	}{
		{
			name:                    "Instance annotation difference results in Progressive",
			newControllerInstanceID: "1",
			existingOriginalMonoVertexDef: *createMonoVertex(
				numaflowv1.MonoVertexPhaseRunning,
				numaflowv1.Status{},
				map[string]string{
					common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
					common.LabelKeyParentRollout: ctlrcommon.DefaultTestMonoVertexRolloutName,
				},
				map[string]string{
					common.AnnotationKeyNumaflowInstanceID: "0",
				}),
			existingUpgradeMonoVertexDef: nil,
			initialRolloutPhase:          apiv1.PhaseDeployed,
			initialRolloutNameCount:      1,
			initialInProgressStrategy:    nil,
			initialUpgradingChildStatus:  nil,
			initialPromotedChildStatus: &apiv1.PromotedMonoVertexStatus{
				PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
					PromotedChildStatus: apiv1.PromotedChildStatus{
						Name: ctlrcommon.DefaultTestMonoVertexRolloutName + "-0",
					},
					AllSourceVerticesScaledDown: true,
				},
			},
			expectedInProgressStrategy: apiv1.UpgradeStrategyProgressive,
			expectedRolloutPhase:       apiv1.PhasePending,

			expectedMonoVertices: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-0": common.LabelValueUpgradePromoted,
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-1": common.LabelValueUpgradeInProgress,
			},
		},
		{
			name:                    "Progressive deployed successfully",
			newControllerInstanceID: "1",
			existingOriginalMonoVertexDef: *createMonoVertex(
				numaflowv1.MonoVertexPhaseRunning,
				numaflowv1.Status{},
				map[string]string{
					common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
					common.LabelKeyParentRollout: ctlrcommon.DefaultTestMonoVertexRolloutName,
				},
				map[string]string{
					common.AnnotationKeyNumaflowInstanceID: "0",
				}),
			existingUpgradeMonoVertexDef: ctlrcommon.CreateTestMonoVertexOfSpec(
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
				}),
			initialRolloutPhase:       apiv1.PhasePending,
			initialRolloutNameCount:   2,
			initialInProgressStrategy: &progressiveUpgradeStrategy,
			initialUpgradingChildStatus: &apiv1.UpgradingMonoVertexStatus{
				UpgradingChildStatus: apiv1.UpgradingChildStatus{
					Name:               ctlrcommon.DefaultTestMonoVertexRolloutName + "-1",
					NextAssessmentTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
					AssessUntil:        &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
					AssessmentResult:   apiv1.AssessmentResultSuccess,
				},
			},
			initialPromotedChildStatus: &apiv1.PromotedMonoVertexStatus{
				PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
					PromotedChildStatus: apiv1.PromotedChildStatus{
						Name: ctlrcommon.DefaultTestMonoVertexRolloutName + "-0",
					},
					AllSourceVerticesScaledDown: true,
				},
			},
			expectedInProgressStrategy: apiv1.UpgradeStrategyNoOp,
			expectedRolloutPhase:       apiv1.PhaseDeployed,

			// original MonoVertex deleted, new one promoted
			expectedMonoVertices: map[string]common.UpgradeState{
				ctlrcommon.DefaultTestMonoVertexRolloutName + "-1": common.LabelValueUpgradePromoted,
			},
		},
		{
			name:                    "Progressive deployment failed",
			newControllerInstanceID: "1",
			existingOriginalMonoVertexDef: *createMonoVertex(
				numaflowv1.MonoVertexPhaseRunning,
				numaflowv1.Status{},
				map[string]string{
					common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
					common.LabelKeyParentRollout: ctlrcommon.DefaultTestMonoVertexRolloutName,
				},
				map[string]string{
					common.AnnotationKeyNumaflowInstanceID: "0",
				}),
			existingUpgradeMonoVertexDef: ctlrcommon.CreateTestMonoVertexOfSpec(
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
				UpgradingChildStatus: apiv1.UpgradingChildStatus{
					Name:               ctlrcommon.DefaultTestMonoVertexRolloutName + "-1",
					NextAssessmentTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
					AssessUntil:        &metav1.Time{Time: time.Now().Add(-30 * time.Second)},
					AssessmentResult:   apiv1.AssessmentResultFailure,
				},
			},
			initialPromotedChildStatus: &apiv1.PromotedMonoVertexStatus{
				PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
					PromotedChildStatus: apiv1.PromotedChildStatus{
						Name: ctlrcommon.DefaultTestMonoVertexRolloutName + "-0",
					},
					AllSourceVerticesScaledDown: true,
					ScaleValues:                 map[string]apiv1.ScaleValues{ctlrcommon.DefaultTestMonoVertexRolloutName + "-0": {OriginalScaleDefinition: &ctlrcommon.DefaultScaleJSONString, ScaleTo: &ctlrcommon.DefaultScaleTo}},
				},
			},
			expectedInProgressStrategy: apiv1.UpgradeStrategyProgressive,
			expectedRolloutPhase:       apiv1.PhasePending,

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

			if tc.existingUpgradeMonoVertexDef != nil {
				existingUpgradeMonoVertexDef := tc.existingUpgradeMonoVertexDef
				existingUpgradeMonoVertexDef.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rollout.GetObjectMeta(), apiv1.MonoVertexRolloutGroupVersionKind)}
				monoVertex, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).Create(ctx, existingUpgradeMonoVertexDef, metav1.CreateOptions{})
				assert.NoError(t, err)

				// update Status subresource
				monoVertex.Status = tc.existingUpgradeMonoVertexDef.Status
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
