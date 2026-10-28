package progressive

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	argorolloutsv1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/controller/common/riders"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"
)

type fakeProgressiveController struct {
	skipProgressiveAssessment bool
}

func (fpc fakeProgressiveController) UpdateProgressiveMetrics(rolloutObject ProgressiveRolloutObject) {
}

// specChangeController reports that the rollout spec differs from the current children.
// ProcessResource must promote an annotated trial before asking this controller to replace it.
type specChangeController struct {
	fakeProgressiveController
	replacementAttempted bool
}

func (s *specChangeController) CheckForDifferences(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, childDef *unstructured.Unstructured, requiredSpec map[string]interface{}, requiredMetadata map[string]interface{}, existingChildUpgradeState common.UpgradeState) (bool, error) {
	s.replacementAttempted = true
	return true, nil
}

func (s *specChangeController) CheckForDifferencesWithRolloutDef(ctx context.Context, existingChild *unstructured.Unstructured, rolloutObject ctlrcommon.RolloutObject, existingChildUpgradeState common.UpgradeState) (bool, error) {
	s.replacementAttempted = true
	return true, nil
}

func (s *specChangeController) CreateUpgradingChildDefinition(ctx context.Context, rolloutObject ProgressiveRolloutObject, name string) (*unstructured.Unstructured, error) {
	s.replacementAttempted = true
	return nil, assert.AnError
}

func (s *specChangeController) IncrementChildCount(ctx context.Context, rolloutObject ctlrcommon.RolloutObject) (int32, error) {
	s.replacementAttempted = true
	return 0, assert.AnError
}

func (fpc fakeProgressiveController) CreateUpgradingChildDefinition(ctx context.Context, rolloutObject ProgressiveRolloutObject, name string) (*unstructured.Unstructured, error) {
	return nil, nil
}

func (fpc fakeProgressiveController) IncrementChildCount(ctx context.Context, rolloutObject ctlrcommon.RolloutObject) (int32, error) {
	return 0, nil
}

func (fpc fakeProgressiveController) Recycle(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, childObject *unstructured.Unstructured) (bool, error) {
	return false, nil
}

func (fpc fakeProgressiveController) GetDesiredRiders(rolloutObject ctlrcommon.RolloutObject, name string, childDef *unstructured.Unstructured) ([]riders.Rider, error) {
	desiredRiders := []riders.Rider{}
	return desiredRiders, nil
}
func (fpc fakeProgressiveController) GetExistingRiders(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, upgrading bool) (unstructured.UnstructuredList, error) {
	return unstructured.UnstructuredList{}, nil
}

func (fpc fakeProgressiveController) CheckForDifferences(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, childDef *unstructured.Unstructured, requiredSpec map[string]interface{}, requiredMetadata map[string]interface{}, existingChildUpgradeState common.UpgradeState) (bool, error) {
	return false, nil
}

func (fpc fakeProgressiveController) CheckForDifferencesWithRolloutDef(ctx context.Context, existingChild *unstructured.Unstructured, rolloutObject ctlrcommon.RolloutObject, existingChildUpgradeState common.UpgradeState) (bool, error) {
	return false, nil
}

func (fpc fakeProgressiveController) AssessUpgradingChild(ctx context.Context, rolloutObject ProgressiveRolloutObject, existingUpgradingChildDef *unstructured.Unstructured, schedule config.AssessmentSchedule) (apiv1.AssessmentResult, string, error) {
	switch existingUpgradingChildDef.GetName() {
	case "test-success", "test-analysis-success":
		return apiv1.AssessmentResultSuccess, "", nil
	case "test-failure", "test-analysis-failure":
		return apiv1.AssessmentResultFailure, "test-fail-reason", nil
	default:
		return apiv1.AssessmentResultUnknown, "", nil
	}
}

func (fpc fakeProgressiveController) AssessResourceChain(ctx context.Context, rolloutObject ProgressiveRolloutObject, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, []apiv1.ResourceChainMemberStatus, []string, error) {
	// These tests have no dependent rollouts, so the chain is the resource assessment.
	if status := rolloutObject.GetUpgradingChildStatus(); status != nil && status.ResourceAssessmentResult != "" {
		member := NewChainMember("TestRollout", rolloutObject.GetRolloutObjectMeta().Name, status.Name, status.ResourceAssessmentResult)
		if status.ResourceAssessmentResult == apiv1.AssessmentResultFailure {
			return apiv1.AssessmentResultFailure, []apiv1.ResourceChainMemberStatus{member}, []string{"test-fail-reason"}, nil
		}
		return status.ResourceAssessmentResult, []apiv1.ResourceChainMemberStatus{member}, nil, nil
	}
	result, reason, err := fpc.AssessUpgradingChild(ctx, rolloutObject, existingUpgradingChildDef, config.AssessmentSchedule{})
	member := NewChainMember("TestRollout", rolloutObject.GetRolloutObjectMeta().Name, existingUpgradingChildDef.GetName(), result)
	if result == apiv1.AssessmentResultFailure && reason != "" {
		return result, []apiv1.ResourceChainMemberStatus{member}, []string{reason}, err
	}
	return result, []apiv1.ResourceChainMemberStatus{member}, nil, err
}

func (fpc fakeProgressiveController) ProcessPromotedChildPreUpgrade(ctx context.Context, rolloutObject ProgressiveRolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) (bool, error) {
	return false, nil
}

func (fpc fakeProgressiveController) ProcessPromotedChildPostUpgradeStart(ctx context.Context, rolloutObject ProgressiveRolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) (bool, error) {
	return false, nil
}

func (fpc fakeProgressiveController) ProcessPromotedChildPostFailure(ctx context.Context, rolloutObject ProgressiveRolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) (bool, error) {
	return false, nil
}

func (fpc fakeProgressiveController) ProcessUpgradingChildPostFailure(ctx context.Context, rolloutObject ProgressiveRolloutObject, upgradingChildDef *unstructured.Unstructured, c client.Client) (bool, error) {
	return false, nil
}

func (fpc fakeProgressiveController) ProcessUpgradingChildPreUpgrade(ctx context.Context, rolloutObject ProgressiveRolloutObject, upgradingChildDef *unstructured.Unstructured, c client.Client) (bool, error) {
	return false, nil
}
func (fpc fakeProgressiveController) ProcessUpgradingChildPostUpgradeStart(ctx context.Context, rolloutObject ProgressiveRolloutObject, upgradingChildDef *unstructured.Unstructured, c client.Client) (bool, error) {
	return false, nil
}

func (fpc fakeProgressiveController) SetCurrentRiderList(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, riders []riders.Rider) {

}

func (fpc fakeProgressiveController) SkipProgressiveAssessment(ctx context.Context, rolloutObject ProgressiveRolloutObject) (bool, SkipProgressiveAssessmentReason, error) {
	if fpc.skipProgressiveAssessment {
		return true, SkipProgressiveAssessmentReasonRolloutConfiguration, nil
	}
	return false, SkipProgressiveAssessmentReasonUndefined, nil
}

func (fpc fakeProgressiveController) GetTemplateArguments(pipeline *unstructured.Unstructured) map[string]interface{} {
	return map[string]interface{}{}
}

func Test_processUpgradingChild(t *testing.T) {
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

	defaultExistingPromotedChildDef := createMonoVertex("test")

	testCases := []struct {
		name          string
		rolloutObject ProgressiveRolloutObject
		// if set to true, the rolloutController will indicate that we should skip the progressive assessment and force promote
		skipProgressiveAssessment bool
		existingUpgradingChildDef *numaflowv1.MonoVertex
		expectedDone              bool
		expectedFailed            bool
		expectedRequeueDelay      time.Duration
		expectedError             error
	}{
		{
			name: "can assess, success",
			rolloutObject: setMonoVertexProgressiveStatus(
				defaultMonoVertexRollout.DeepCopy(),
				&apiv1.UpgradingMonoVertexStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{
							Name:                     "test-success",
							AssessmentResult:         apiv1.AssessmentResultUnknown,
							BasicAssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
							BasicAssessmentEndTime:   &metav1.Time{Time: time.Now()},
							InitializationComplete:   true,
						},
					},
				},
				nil,
			),
			skipProgressiveAssessment: false,
			existingUpgradingChildDef: createMonoVertex("test-success"),
			expectedDone:              true,
			expectedFailed:            false,
			expectedRequeueDelay:      0,
			expectedError:             nil,
		},
		{
			name: "failure",
			rolloutObject: setMonoVertexProgressiveStatus(
				defaultMonoVertexRollout.DeepCopy(),
				&apiv1.UpgradingMonoVertexStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{
							Name:                     "test-failure",
							AssessmentResult:         apiv1.AssessmentResultFailure,
							BasicAssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
							InitializationComplete:   true,
							ResourceChainMembers: []apiv1.ResourceChainMemberStatus{
								{Kind: "MonoVertexRollout", Name: "test", ChildName: "test-failure", AssessmentResult: apiv1.AssessmentResultFailure},
							},
						},
					},
				},
				&apiv1.PromotedMonoVertexStatus{
					PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
						PromotedChildStatus: apiv1.PromotedChildStatus{
							Name: defaultExistingPromotedChildDef.GetName(),
						},
						ScaleValuesRestoredToOriginal: true,
					},
				},
			),
			skipProgressiveAssessment: false,
			existingUpgradingChildDef: createMonoVertex("test-failure"),
			expectedDone:              true,
			expectedFailed:            true,
			expectedRequeueDelay:      10 * time.Second,
			expectedError:             nil,
		},
		{
			name: "force promote a failed progressive upgrade",
			rolloutObject: setMonoVertexProgressiveStatus(
				forcePromoteMonoVertexRollout.DeepCopy(),
				&apiv1.UpgradingMonoVertexStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{
							Name:                     "test-force-promote",
							AssessmentResult:         apiv1.AssessmentResultFailure,
							BasicAssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
							InitializationComplete:   true,
						},
					},
				},
				&apiv1.PromotedMonoVertexStatus{
					PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
						PromotedChildStatus: apiv1.PromotedChildStatus{
							Name: defaultExistingPromotedChildDef.GetName(),
						},
						ScaleValuesRestoredToOriginal: true,
					},
				},
			),
			skipProgressiveAssessment: true,
			existingUpgradingChildDef: createMonoVertex("test-force-promote"),
			expectedDone:              true,
			expectedFailed:            false,
			expectedRequeueDelay:      0,
			expectedError:             nil,
		},
		{
			name: "analysis status set - success",
			rolloutObject: setMonoVertexProgressiveStatus(
				analysisTmplMonoVertexRollout.DeepCopy(),
				&apiv1.UpgradingMonoVertexStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{
							Name:                     "test-analysis-success",
							AssessmentResult:         apiv1.AssessmentResultUnknown,
							BasicAssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
							BasicAssessmentEndTime:   &metav1.Time{Time: time.Now()},
							InitializationComplete:   true,
						},
						Analysis: apiv1.AnalysisStatus{
							AnalysisRunName: "monovertex-test-analysis-success",
							StartTime:       &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
							EndTime:         &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
							Phase:           argorolloutsv1.AnalysisPhaseSuccessful,
						},
					},
				},
				nil,
			),
			skipProgressiveAssessment: false,
			existingUpgradingChildDef: createMonoVertex("test-analysis-success"),
			expectedDone:              true,
			expectedFailed:            false,
			expectedRequeueDelay:      0,
			expectedError:             nil,
		},
		{
			name: "analysis status set - failure",
			rolloutObject: setMonoVertexProgressiveStatus(
				analysisTmplMonoVertexRollout.DeepCopy(),
				&apiv1.UpgradingMonoVertexStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{
							Name:                     "test-analysis-failure",
							AssessmentResult:         apiv1.AssessmentResultUnknown,
							BasicAssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
							BasicAssessmentEndTime:   &metav1.Time{Time: time.Now()},
							InitializationComplete:   true,
						},
						Analysis: apiv1.AnalysisStatus{
							AnalysisRunName: "monovertex-test-analysis-failure",
							StartTime:       &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
							EndTime:         &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
							Phase:           argorolloutsv1.AnalysisPhaseFailed,
						},
					},
				},
				nil,
			),
			skipProgressiveAssessment: false,
			existingUpgradingChildDef: createMonoVertex("test-analysis-failure"),
			expectedDone:              true,
			expectedFailed:            true,
			expectedRequeueDelay:      10 * time.Second,
			expectedError:             nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			// first delete MonoVertex and MonoVertexRollout in case they already exist, in Kubernetes
			_ = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})

			monoVertexList, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Len(t, monoVertexList.Items, 0)

			// creating existingPromotedChild and existingUpgradingChild MonoVertices, in Kubernetes
			_, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).Create(ctx, tc.existingUpgradingChildDef, metav1.CreateOptions{})
			assert.NoError(t, err)

			if tc.existingUpgradingChildDef.Name != defaultExistingPromotedChildDef.Name {
				_, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).Create(ctx, defaultExistingPromotedChildDef, metav1.CreateOptions{})
				assert.NoError(t, err)
			}

			actualDone, actualFailed, actualRequeueDelay, actualErr := processUpgradingChild(
				ctx, tc.rolloutObject, fakeProgressiveController{skipProgressiveAssessment: tc.skipProgressiveAssessment}, monoVertexToUnstruct(defaultExistingPromotedChildDef), monoVertexToUnstruct(tc.existingUpgradingChildDef), client)

			if tc.expectedError != nil {
				assert.Error(t, actualErr)
				assert.False(t, actualDone)
				assert.Zero(t, actualRequeueDelay)
			} else {
				assert.Nil(t, actualErr)
				assert.Equal(t, tc.expectedDone, actualDone)
				assert.Equal(t, tc.expectedFailed, actualFailed)
				assert.Equal(t, tc.expectedRequeueDelay, actualRequeueDelay)
			}

			// Cases that run AssessResourceChain must persist resourceChainMembers on the rollout status.
			if tc.name == "can assess, success" || tc.name == "force promote a failed progressive upgrade" ||
				tc.name == "analysis status set - success" || tc.name == "analysis status set - failure" {
				childStatus := tc.rolloutObject.GetUpgradingChildStatus()
				require.NotNil(t, childStatus)
				require.NotEmpty(t, childStatus.ResourceChainMembers, "resourceChainMembers should be set after progressive assessment")
				assert.Equal(t, "TestRollout", childStatus.ResourceChainMembers[0].Kind)
				assert.Equal(t, tc.existingUpgradingChildDef.Name, childStatus.ResourceChainMembers[0].ChildName)
			}
			// A later reconcile of an already-failed child skips assessment. Keep the members recorded earlier.
			if tc.name == "failure" {
				childStatus := tc.rolloutObject.GetUpgradingChildStatus()
				require.Equal(t, []apiv1.ResourceChainMemberStatus{
					{Kind: "MonoVertexRollout", Name: "test", ChildName: "test-failure", AssessmentResult: apiv1.AssessmentResultFailure},
				}, childStatus.ResourceChainMembers)
			}
		})
	}
}

// otherMemberFailedController reports a chain that another member failed, with this resource's own
// assessment set by ownResult, and counts post-failure processing calls.
type otherMemberFailedController struct {
	fakeProgressiveController
	ownResult        apiv1.AssessmentResult
	postFailureCalls int
}

func (o *otherMemberFailedController) AssessUpgradingChild(ctx context.Context, rolloutObject ProgressiveRolloutObject, existingUpgradingChildDef *unstructured.Unstructured, schedule config.AssessmentSchedule) (apiv1.AssessmentResult, string, error) {
	return o.ownResult, "", nil
}

func (o *otherMemberFailedController) AssessResourceChain(ctx context.Context, rolloutObject ProgressiveRolloutObject, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, []apiv1.ResourceChainMemberStatus, []string, error) {
	members := []apiv1.ResourceChainMemberStatus{
		NewChainMember("MonoVertexRollout", rolloutObject.GetRolloutObjectMeta().Name, existingUpgradingChildDef.GetName(), o.ownResult),
		NewChainMember("PipelineRollout", "other", "other-1", apiv1.AssessmentResultFailure),
	}
	return apiv1.AssessmentResultFailure, members, []string{"PipelineRollout other failed"}, nil
}

func (o *otherMemberFailedController) ProcessPromotedChildPostFailure(ctx context.Context, rolloutObject ProgressiveRolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) (bool, error) {
	o.postFailureCalls++
	return false, nil
}

func (o *otherMemberFailedController) ProcessUpgradingChildPostFailure(ctx context.Context, rolloutObject ProgressiveRolloutObject, upgradingChildDef *unstructured.Unstructured, c client.Client) (bool, error) {
	o.postFailureCalls++
	return false, nil
}

func TestProcessUpgradingChildFinishesOwnAssessmentBeforePostFailure(t *testing.T) {
	restConfig, numaflowClientSet, k8sClient, _, err := commontest.PrepareK8SEnvironment()
	require.NoError(t, err)
	require.NoError(t, kubernetes.SetClientSets(restConfig))

	getwd, err := os.Getwd()
	require.NoError(t, err)
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	require.NoError(t, config.GetConfigManagerInstance().LoadAllConfigs(func(error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig2")))

	ctx := context.Background()
	namespace := ctlrcommon.DefaultTestNamespace

	for _, tc := range []struct {
		name                     string
		ownResult                apiv1.AssessmentResult
		stampFailed              bool
		expectedPostFailureCalls int
	}{
		{name: "own assessment still running", ownResult: apiv1.AssessmentResultUnknown, expectedPostFailureCalls: 0},
		{name: "own assessment succeeded", ownResult: apiv1.AssessmentResultSuccess, expectedPostFailureCalls: 2},
		{name: "own assessment failed", ownResult: apiv1.AssessmentResultFailure, expectedPostFailureCalls: 2},
		{name: "own assessment succeeded clears a chain-stamped failure", ownResult: apiv1.AssessmentResultSuccess, stampFailed: true, expectedPostFailureCalls: 2},
		{name: "own assessment still running clears a chain-stamped failure", ownResult: apiv1.AssessmentResultUnknown, stampFailed: true, expectedPostFailureCalls: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{}))
			promoted := createMonoVertex("test")
			trial := createMonoVertex("test-trial")
			if tc.stampFailed {
				trial.Labels[common.LabelKeyProgressiveResultState] = string(common.LabelValueResultStateFailed)
			}
			for _, mv := range []*numaflowv1.MonoVertex{promoted, trial} {
				_, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Create(ctx, mv, metav1.CreateOptions{})
				require.NoError(t, err)
			}

			rollout := setMonoVertexProgressiveStatus(
				defaultMonoVertexRollout.DeepCopy(),
				&apiv1.UpgradingMonoVertexStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{
							Name:                     trial.Name,
							BasicAssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
							InitializationComplete:   true,
						},
					},
				},
				&apiv1.PromotedMonoVertexStatus{
					PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
						PromotedChildStatus:           apiv1.PromotedChildStatus{Name: promoted.Name},
						ScaleValuesRestoredToOriginal: true,
					},
				},
			)

			controller := &otherMemberFailedController{ownResult: tc.ownResult}
			done, failed, delay, err := processUpgradingChild(ctx, rollout, controller, monoVertexToUnstruct(promoted), monoVertexToUnstruct(trial), k8sClient)
			require.NoError(t, err)
			assert.True(t, done)
			assert.True(t, failed)
			assert.Equal(t, 10*time.Second, delay)
			assert.Equal(t, tc.expectedPostFailureCalls, controller.postFailureCalls)

			childStatus := rollout.GetUpgradingChildStatus()
			assert.Equal(t, tc.ownResult, childStatus.ResourceAssessmentResult)
			assert.Equal(t, apiv1.AssessmentResultFailure, childStatus.ResourceChainAssessmentResult)
			assert.Contains(t, childStatus.FailureReasons, "PipelineRollout other failed")
			condition := rollout.Status.GetCondition(apiv1.ConditionProgressiveUpgradeSucceeded)
			require.NotNil(t, condition)
			assert.Equal(t, metav1.ConditionFalse, condition.Status)

			got, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Get(ctx, trial.Name, metav1.GetOptions{})
			require.NoError(t, err)
			if tc.ownResult == apiv1.AssessmentResultFailure {
				assert.Equal(t, string(common.LabelValueResultStateFailed), got.Labels[common.LabelKeyProgressiveResultState])
			} else {
				_, markedFailed := got.Labels[common.LabelKeyProgressiveResultState]
				assert.False(t, markedFailed)
			}
			_, chainLatched := got.Labels[common.LabelKeyProgressiveChainFailed]
			assert.False(t, chainLatched)
		})
	}

	t.Run("controller chain failure follows the controller's own assessment", func(t *testing.T) {
		for _, tc := range []struct {
			name        string
			childName   string
			own         apiv1.AssessmentResult
			chain       apiv1.AssessmentResult
			stampFailed bool
			wantFailed  bool
			wantLatch   bool
			wantRollout bool
		}{
			{name: "healthy controller in a failed chain", childName: "controller-healthy", own: apiv1.AssessmentResultSuccess, chain: apiv1.AssessmentResultFailure, wantLatch: true, wantRollout: true},
			{name: "failed controller", childName: "controller-failed", own: apiv1.AssessmentResultFailure, chain: apiv1.AssessmentResultFailure, wantFailed: true, wantLatch: true, wantRollout: true},
			{name: "reassessment keeps the hold and clears a stale failure", childName: "controller-reassess", own: apiv1.AssessmentResultSuccess, chain: apiv1.AssessmentResultUnknown, stampFailed: true, wantLatch: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				child := ctlrcommon.CreateTestNumaflowController(namespace, "controller", tc.childName, common.LabelValueUpgradeTrial, "trial")
				if tc.stampFailed {
					child.Labels[common.LabelKeyProgressiveResultState] = string(common.LabelValueResultStateFailed)
				}
				ctlrcommon.CreateNumaflowControllerInK8S(ctx, t, k8sClient, child)
				objMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(child)
				require.NoError(t, err)
				childObject := &unstructured.Unstructured{Object: objMap}
				childObject.SetGroupVersionKind(apiv1.NumaflowControllerGroupVersionKind)

				rollout := setMonoVertexProgressiveStatus(
					defaultMonoVertexRollout.DeepCopy(),
					&apiv1.UpgradingMonoVertexStatus{
						UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
							UpgradingChildStatus: apiv1.UpgradingChildStatus{
								Name:                     tc.childName,
								BasicAssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
								InitializationComplete:   true,
							},
						},
					},
					nil,
				)
				assessmentController := &scriptedAssessmentController{own: tc.own, chain: tc.chain}
				_, _, _, err = processUpgradingChild(ctx, rollout, assessmentController, childObject, childObject, k8sClient)
				require.NoError(t, err)

				var got apiv1.NumaflowController
				require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: tc.childName}, &got))
				if tc.wantFailed {
					assert.Equal(t, string(common.LabelValueResultStateFailed), got.Labels[common.LabelKeyProgressiveResultState])
				} else {
					_, markedFailed := got.Labels[common.LabelKeyProgressiveResultState]
					assert.False(t, markedFailed)
				}
				assert.Equal(t, tc.wantLatch, got.Labels[common.LabelKeyProgressiveChainFailed] == "true")
				if tc.wantRollout {
					condition := rollout.Status.GetCondition(apiv1.ConditionProgressiveUpgradeSucceeded)
					require.NotNil(t, condition)
					assert.Equal(t, metav1.ConditionFalse, condition.Status)
				}
			})
		}
	})
}

// scriptedAssessmentController returns fixed own and chain assessment results.
type scriptedAssessmentController struct {
	fakeProgressiveController
	own   apiv1.AssessmentResult
	chain apiv1.AssessmentResult
}

func (s *scriptedAssessmentController) AssessUpgradingChild(ctx context.Context, rolloutObject ProgressiveRolloutObject, existingUpgradingChildDef *unstructured.Unstructured, schedule config.AssessmentSchedule) (apiv1.AssessmentResult, string, error) {
	return s.own, "", nil
}

func (s *scriptedAssessmentController) AssessResourceChain(ctx context.Context, rolloutObject ProgressiveRolloutObject, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, []apiv1.ResourceChainMemberStatus, []string, error) {
	member := NewChainMember("NumaflowControllerRollout", rolloutObject.GetRolloutObjectMeta().Name, existingUpgradingChildDef.GetName(), s.own)
	if s.chain == apiv1.AssessmentResultFailure {
		return s.chain, []apiv1.ResourceChainMemberStatus{member}, []string{"chain failed"}, nil
	}
	return s.chain, []apiv1.ResourceChainMemberStatus{member}, nil, nil
}

func setMonoVertexProgressiveStatus(mvRollout *apiv1.MonoVertexRollout, upgradingChildStatus *apiv1.UpgradingMonoVertexStatus, promotedChildStatus *apiv1.PromotedMonoVertexStatus) *apiv1.MonoVertexRollout {
	mvRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus = upgradingChildStatus
	mvRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus = promotedChildStatus
	return mvRollout
}

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

func createMonoVertex(name string) *numaflowv1.MonoVertex {

	return ctlrcommon.CreateTestMonoVertexOfSpec(
		monoVertexSpec, name,
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
			common.LabelKeyParentRollout: ctlrcommon.DefaultTestMonoVertexRolloutName,
		},
		map[string]string{
			common.AnnotationKeyNumaflowInstanceID: "1",
		})

}

func monoVertexToUnstruct(mvtx *numaflowv1.MonoVertex) *unstructured.Unstructured {
	unstructMap, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(mvtx)
	return &unstructured.Unstructured{Object: unstructMap}
}

var defaultMonoVertexRollout = &apiv1.MonoVertexRollout{
	ObjectMeta: metav1.ObjectMeta{
		Name: "test",
	},
	Status: apiv1.MonoVertexRolloutStatus{
		ProgressiveStatus: apiv1.MonoVertexProgressiveStatus{
			UpgradingMonoVertexStatus: nil,
			PromotedMonoVertexStatus:  nil,
		},
	},
}

var forcePromoteMonoVertexRollout = &apiv1.MonoVertexRollout{
	ObjectMeta: metav1.ObjectMeta{
		Name: "test",
	},
	Status: apiv1.MonoVertexRolloutStatus{
		ProgressiveStatus: apiv1.MonoVertexProgressiveStatus{
			UpgradingMonoVertexStatus: nil,
			PromotedMonoVertexStatus:  nil,
		},
	},
	Spec: apiv1.MonoVertexRolloutSpec{
		Strategy: &apiv1.PipelineTypeRolloutStrategy{
			PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
				Progressive: apiv1.ProgressiveStrategy{
					ForcePromote: true,
				},
			},
		},
	},
}

var analysisTmplMonoVertexRollout = &apiv1.MonoVertexRollout{
	ObjectMeta: metav1.ObjectMeta{
		Name: "test",
	},
	Status: apiv1.MonoVertexRolloutStatus{
		ProgressiveStatus: apiv1.MonoVertexProgressiveStatus{
			UpgradingMonoVertexStatus: nil,
			PromotedMonoVertexStatus:  nil,
		},
	},
	Spec: apiv1.MonoVertexRolloutSpec{
		Strategy: &apiv1.PipelineTypeRolloutStrategy{
			PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
				Analysis: apiv1.Analysis{
					Templates: []argorolloutsv1.AnalysisTemplateRef{
						{TemplateName: "test", ClusterScope: false},
					},
				},
			},
		},
	},
}

func Test_getAnalysisRunTimeout(t *testing.T) {

	testCases := []struct {
		name            string
		configToTest    string
		expectedTimeout time.Duration
		expectedErr     bool
	}{
		{
			name:            "default timeout",
			configToTest:    "testconfig",
			expectedTimeout: time.Duration(1200) * time.Second,
			expectedErr:     false,
		},
		{
			name:            "custom timeout",
			configToTest:    "testconfig2",
			expectedTimeout: time.Duration(600) * time.Second,
			expectedErr:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			getwd, err := os.Getwd()
			assert.Nil(t, err, "Failed to get working directory")
			configPath := filepath.Join(getwd, "../../../", "tests", "config")
			configManager := config.GetConfigManagerInstance()
			err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName(tc.configToTest))
			assert.NoError(t, err)

			analysisRunTimeout, err := getAnalysisRunTimeout(context.Background())
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedTimeout, analysisRunTimeout)
			}
		})
	}

}

func Test_getChildStatusAssessmentSchedule(t *testing.T) {

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig"))
	assert.NoError(t, err)

	testCases := []struct {
		name             string
		rolloutSchedule  string
		expectedSchedule config.AssessmentSchedule
		expectedError    bool
	}{
		{
			name:            "rollout defines schedule",
			rolloutSchedule: "300,600,200,10",
			expectedSchedule: config.AssessmentSchedule{
				Delay:    300 * time.Second,
				End:      600 * time.Second,
				Period:   200 * time.Second,
				Interval: 10 * time.Second,
			},
			expectedError: false,
		},
		{
			name:            "rollout doesn't define schedule, use default for the Kind",
			rolloutSchedule: "",
			expectedSchedule: config.AssessmentSchedule{
				Delay:    120 * time.Second,
				End:      360 * time.Second,
				Period:   0 * time.Second,
				Interval: 10 * time.Second,
			},
			expectedError: false,
		},
		{
			name:            "rollout defines invalid format, so default is used for the Kind",
			rolloutSchedule: "10,20",
			expectedSchedule: config.AssessmentSchedule{
				Delay:    120 * time.Second,
				End:      360 * time.Second,
				Period:   0 * time.Second,
				Interval: 10 * time.Second,
			},
			expectedError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			pipelineRollout := apiv1.PipelineRollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "my-pipeline",
					Namespace: "my-namespace",
				},
				Spec: apiv1.PipelineRolloutSpec{
					Pipeline: apiv1.Pipeline{
						// not needed for test
					},
					Strategy: &apiv1.PipelineStrategy{
						PipelineTypeRolloutStrategy: apiv1.PipelineTypeRolloutStrategy{
							PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
								Progressive: apiv1.ProgressiveStrategy{
									AssessmentSchedule: tc.rolloutSchedule,
								},
							},
						},
					},
				},
			}

			resultSchedule, err := getChildStatusAssessmentSchedule(context.Background(), &pipelineRollout)
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedSchedule, resultSchedule)
			}
		})
	}
}

func Test_ExtractOriginalScaleMinMaxAsJSONString(t *testing.T) {
	testCases := []struct {
		name               string
		object             map[string]any
		pathToScale        []string
		expectedJSONString string
		expectedError      error
	}{
		{
			name:               "scale unset",
			object:             map[string]any{"spec": map[string]any{"somefield": int64(2)}},
			pathToScale:        []string{"spec", "scale"},
			expectedJSONString: "null",
			expectedError:      nil,
		},
		{
			name:               "min set",
			object:             map[string]any{"scale": map[string]any{"min": int64(1)}},
			pathToScale:        []string{"scale"},
			expectedJSONString: "{\"disabled\":null,\"max\":null,\"min\":1}",
			expectedError:      nil,
		},
		{
			name:               "max set",
			object:             map[string]any{"scale": map[string]any{"max": int64(5)}},
			pathToScale:        []string{"scale"},
			expectedJSONString: "{\"disabled\":null,\"max\":5,\"min\":null}",
			expectedError:      nil,
		},
		{
			name:               "both min and max set",
			object:             map[string]any{"scale": map[string]any{"min": int64(3), "max": int64(7)}},
			pathToScale:        []string{"scale"},
			expectedJSONString: "{\"disabled\":null,\"max\":7,\"min\":3}",
			expectedError:      nil,
		},
		{
			name:               "both min and max set and also another field",
			object:             map[string]any{"scale": map[string]any{"min": int64(3), "max": int64(7), "zeroReplicaSleepSeconds": int64(123)}},
			pathToScale:        []string{"scale"},
			expectedJSONString: "{\"disabled\":null,\"max\":7,\"min\":3}",
			expectedError:      nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualJSONString, actualErr := ExtractScaleMinMaxAsJSONString(tc.object, tc.pathToScale)

			if tc.expectedError != nil {
				assert.Error(t, actualErr)
				assert.Equal(t, "", actualJSONString)
			} else {
				assert.Nil(t, actualErr)
				assert.Equal(t, tc.expectedJSONString, actualJSONString)
			}
		})
	}
}

func Test_AreVertexReplicasReady(t *testing.T) {
	testCases := []struct {
		name            string
		desiredReplicas *int64
		readyReplicas   *int64
		expectedResult  bool
		failureReasons  []string
	}{
		{
			name:            "desired = 0, ready = 0",
			desiredReplicas: ptr.To(int64(0)),
			readyReplicas:   ptr.To(int64(0)),
			expectedResult:  true,
		},
		{
			name:            "desired > 0, ready = 0",
			desiredReplicas: ptr.To(int64(3)),
			readyReplicas:   ptr.To(int64(0)),
			expectedResult:  false,
			failureReasons:  []string{"readyReplicas=0 is less than desiredReplicas=3"},
		},
		{
			name:            "desired = 0, ready > 0",
			desiredReplicas: ptr.To(int64(0)),
			readyReplicas:   ptr.To(int64(3)),
			expectedResult:  true,
		},
		{
			name:            "desired > 0, ready > 0, desired < ready",
			desiredReplicas: ptr.To(int64(2)),
			readyReplicas:   ptr.To(int64(3)),
			expectedResult:  true,
		},
		{
			name:            "desired > 0, ready > 0, desired > ready",
			desiredReplicas: ptr.To(int64(3)),
			readyReplicas:   ptr.To(int64(2)),
			expectedResult:  false,
			failureReasons:  []string{"readyReplicas=2 is less than desiredReplicas=3"},
		},
		{
			name:            "desired > 0, ready > 0, desired = ready",
			desiredReplicas: ptr.To(int64(3)),
			readyReplicas:   ptr.To(int64(3)),
			expectedResult:  true,
		},
		{
			name:            "desired = nil, ready = nil",
			desiredReplicas: nil,
			readyReplicas:   nil,
			expectedResult:  true,
		},
		{
			name:            "desired = nil, ready = 0",
			desiredReplicas: nil,
			readyReplicas:   ptr.To(int64(0)),
			expectedResult:  true,
		},
		{
			name:            "desired = 0, ready = nil",
			desiredReplicas: ptr.To(int64(0)),
			readyReplicas:   nil,
			expectedResult:  true,
		},
		{
			name:            "desired = nil, ready > 0",
			desiredReplicas: nil,
			readyReplicas:   ptr.To(int64(3)),
			expectedResult:  true,
		},
		{
			name:            "desired > 0, ready = nil",
			desiredReplicas: ptr.To(int64(3)),
			readyReplicas:   nil,
			expectedResult:  false,
			failureReasons:  []string{"readyReplicas=0 is less than desiredReplicas=3"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			unstr := &unstructured.Unstructured{
				Object: map[string]any{
					"status": map[string]any{},
				},
			}

			if tc.desiredReplicas != nil {
				_ = unstructured.SetNestedField(unstr.Object, *tc.desiredReplicas, "status", "desiredReplicas")
			}

			if tc.readyReplicas != nil {
				_ = unstructured.SetNestedField(unstr.Object, *tc.readyReplicas, "status", "readyReplicas")
			}

			actualResult, failureReasons, actualErr := AreVertexReplicasReady(unstr)

			assert.Equal(t, tc.failureReasons, failureReasons)
			assert.Nil(t, actualErr)
			assert.Equal(t, tc.expectedResult, actualResult)
		})
	}
}

func Test_ExtractScaleMinMax(t *testing.T) {
	two := int64(2)
	three := int64(3)
	testCases := []struct {
		name                    string
		objAsJson               string
		path                    []string
		expectedScaleDefinition *apiv1.ScaleDefinition
		expectedErr             bool
	}{
		{
			name: "min and max present",
			objAsJson: `
			{
				"something": 
				{
					"name": "my-source",
					"scale": 
					{
						"min": 2,
						"max": 3
					}
				}
			}
			`,
			path:                    []string{"something", "scale"},
			expectedScaleDefinition: &apiv1.ScaleDefinition{Min: &two, Max: &three},
			expectedErr:             false,
		},
		{
			name: "scale not present",
			objAsJson: `
			{
				"something": 
				{
					"name": "my-source"
				}
			}
			`,
			path:                    []string{"something", "scale"},
			expectedScaleDefinition: nil,
			expectedErr:             false,
		},
		{
			name: "min and max not present",
			objAsJson: `
			{
				"something": 
				{
					"name": "my-source",
					"scale": 
					{
					}
				}
			}
			`,
			path:                    []string{"something", "scale"},
			expectedScaleDefinition: &apiv1.ScaleDefinition{Min: nil, Max: nil},
			expectedErr:             false,
		},
		{
			name: "invalid type",
			objAsJson: `
			{
				"something": 
				{
					"name": "my-source",
					"scale": 
					{
						"min": "wrongtype"
					}
				}
			}
			`,
			path:        []string{"something", "scale"},
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			obj := map[string]interface{}{}
			err := json.Unmarshal([]byte(tc.objAsJson), &obj)
			assert.NoError(t, err)
			scaleDefinition, err := numaflowtypes.ExtractScaleMinMax(obj, tc.path)
			if !tc.expectedErr {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedScaleDefinition, scaleDefinition)
			} else {
				assert.Error(t, err)
			}
		})
	}

}

func TestControllerInstanceBindingDiffers(t *testing.T) {
	t.Run("empty desired annotation is not a difference when the live child is bound", func(t *testing.T) {
		desired := &unstructured.Unstructured{}
		existing := &unstructured.Unstructured{}
		existing.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "old"})
		existing.SetLabels(map[string]string{common.LabelKeyControllerInstanceID: "old"})
		assert.False(t, controllerInstanceBindingDiffers(desired, existing))
	})

	t.Run("extra instance annotation on the live child is not a difference", func(t *testing.T) {
		desired := &unstructured.Unstructured{}
		existing := &unstructured.Unstructured{}
		existing.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "old"})
		assert.False(t, controllerInstanceBindingDiffers(desired, existing))
	})

	t.Run("instance changed", func(t *testing.T) {
		desired := &unstructured.Unstructured{}
		desired.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "new"})
		desired.SetLabels(map[string]string{common.LabelKeyControllerInstanceID: "new"})
		existing := &unstructured.Unstructured{}
		existing.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "old"})
		existing.SetLabels(map[string]string{common.LabelKeyControllerInstanceID: "old"})
		assert.True(t, controllerInstanceBindingDiffers(desired, existing))
	})

	t.Run("matching annotation without the lookup label is not a difference", func(t *testing.T) {
		desired := &unstructured.Unstructured{}
		desired.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "same"})
		desired.SetLabels(map[string]string{common.LabelKeyControllerInstanceID: "same"})
		existing := &unstructured.Unstructured{}
		existing.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "same"})
		assert.False(t, controllerInstanceBindingDiffers(desired, existing))
	})
}

func TestProcessResourcePromotesAnnotatedTrialBeforeReplacement(t *testing.T) {
	restConfig, numaflowClientSet, k8sClient, _, err := commontest.PrepareK8SEnvironment()
	require.NoError(t, err)
	require.NoError(t, kubernetes.SetClientSets(restConfig))

	getwd, err := os.Getwd()
	require.NoError(t, err)
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	require.NoError(t, config.GetConfigManagerInstance().LoadAllConfigs(func(error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig2")))

	ctx := context.Background()
	namespace := ctlrcommon.DefaultTestNamespace
	require.NoError(t, numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{}))

	promoted := createMonoVertex("mv-promoted")
	promoted.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradePromoted)
	trial := createMonoVertex("mv-trial")
	trial.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeTrial)
	recyclable := createMonoVertex("mv-old")
	recyclable.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeRecyclable)

	createdPromoted, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Create(ctx, promoted, metav1.CreateOptions{})
	require.NoError(t, err)
	createdTrial, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Create(ctx, trial, metav1.CreateOptions{})
	require.NoError(t, err)
	createdOld, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Create(ctx, recyclable, metav1.CreateOptions{})
	require.NoError(t, err)

	if createdTrial.Annotations == nil {
		createdTrial.Annotations = map[string]string{}
	}
	createdTrial.Annotations[common.AnnotationKeyChainPromote] = ChainPromoteAnnotationValue(createdTrial.Name, string(createdTrial.UID))
	_, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Update(ctx, createdTrial, metav1.UpdateOptions{})
	require.NoError(t, err)

	if createdOld.Annotations == nil {
		createdOld.Annotations = map[string]string{}
	}
	createdOld.Annotations[common.AnnotationKeyChainPromote] = ChainPromoteAnnotationValue(createdOld.Name, string(createdOld.UID))
	_, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Update(ctx, createdOld, metav1.UpdateOptions{})
	require.NoError(t, err)

	rollout := setMonoVertexProgressiveStatus(
		&apiv1.MonoVertexRollout{
			ObjectMeta: metav1.ObjectMeta{
				Name:       ctlrcommon.DefaultTestMonoVertexRolloutName,
				Namespace:  namespace,
				Generation: 3,
			},
		},
		&apiv1.UpgradingMonoVertexStatus{
			UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
				UpgradingChildStatus: apiv1.UpgradingChildStatus{
					Name:                          createdTrial.Name,
					AssessmentResult:              apiv1.AssessmentResultFailure,
					ResourceAssessmentResult:      apiv1.AssessmentResultFailure,
					ResourceChainAssessmentResult: apiv1.AssessmentResultFailure,
					InitializationComplete:        true,
				},
			},
		},
		&apiv1.PromotedMonoVertexStatus{
			PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
				PromotedChildStatus: apiv1.PromotedChildStatus{Name: createdPromoted.Name},
			},
		},
	)
	rollout.Status.MarkProgressiveUpgradeFailed("progressive upgrade failed", rollout.Generation)

	promotedUn := monoVertexToUnstruct(createdPromoted)
	promotedUn.SetGroupVersionKind(numaflowv1.MonoVertexGroupVersionKind)

	controller := &specChangeController{}
	done, failed, delay, err := ProcessResource(ctx, rollout, promotedUn, true, controller, k8sClient)
	require.NoError(t, err)
	assert.True(t, done)
	assert.False(t, failed)
	assert.Zero(t, delay)
	assert.False(t, controller.replacementAttempted, "a spec change must wait until the annotated trial is promoted")

	gotTrial, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Get(ctx, createdTrial.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, string(common.LabelValueUpgradePromoted), gotTrial.Labels[common.LabelKeyUpgradeState])
	assert.Equal(t, string(common.LabelValueResultStateSucceeded), gotTrial.Labels[common.LabelKeyProgressiveResultState])

	gotPromoted, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Get(ctx, createdPromoted.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, string(common.LabelValueUpgradeRecyclable), gotPromoted.Labels[common.LabelKeyUpgradeState])
	assert.Equal(t, string(common.LabelValueProgressiveSuccess), gotPromoted.Labels[common.LabelKeyUpgradeStateReason])

	gotOld, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Get(ctx, createdOld.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, string(common.LabelValueUpgradeRecyclable), gotOld.Labels[common.LabelKeyUpgradeState])
	_, promotedByLabel := gotOld.Labels[common.LabelKeyUpgradeStateReason]
	assert.False(t, promotedByLabel)

	list, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, list.Items, 3)

	childStatus := rollout.GetUpgradingChildStatus()
	require.NotNil(t, childStatus)
	assert.Equal(t, apiv1.AssessmentResultSuccess, childStatus.AssessmentResult)
	assert.Equal(t, apiv1.AssessmentResultSuccess, childStatus.ResourceAssessmentResult)
	assert.Equal(t, apiv1.AssessmentResultSuccess, childStatus.ResourceChainAssessmentResult)
	condition := rollout.Status.GetCondition(apiv1.ConditionProgressiveUpgradeSucceeded)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionTrue, condition.Status)
	assert.Contains(t, condition.Message, createdTrial.Name)
	assert.Equal(t, apiv1.PhaseDeployed, rollout.Status.Phase)
}

func TestProcessResourcePromotesForcePromoteChainTrialBeforeReplacement(t *testing.T) {
	restConfig, numaflowClientSet, k8sClient, _, err := commontest.PrepareK8SEnvironment()
	require.NoError(t, err)
	require.NoError(t, kubernetes.SetClientSets(restConfig))

	getwd, err := os.Getwd()
	require.NoError(t, err)
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	require.NoError(t, config.GetConfigManagerInstance().LoadAllConfigs(func(error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig2")))

	ctx := context.Background()
	namespace := ctlrcommon.DefaultTestNamespace
	require.NoError(t, numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{}))

	promoted := createMonoVertex("mv-promoted")
	promoted.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradePromoted)
	trial := createMonoVertex("mv-trial")
	trial.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeTrial)
	trial.Labels[common.LabelKeyForcePromoteChain] = "true"

	createdPromoted, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Create(ctx, promoted, metav1.CreateOptions{})
	require.NoError(t, err)
	createdTrial, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Create(ctx, trial, metav1.CreateOptions{})
	require.NoError(t, err)

	rollout := setMonoVertexProgressiveStatus(
		&apiv1.MonoVertexRollout{
			ObjectMeta: metav1.ObjectMeta{
				Name:       ctlrcommon.DefaultTestMonoVertexRolloutName,
				Namespace:  namespace,
				Generation: 3,
			},
		},
		&apiv1.UpgradingMonoVertexStatus{
			UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
				UpgradingChildStatus: apiv1.UpgradingChildStatus{
					Name:                          createdTrial.Name,
					AssessmentResult:              apiv1.AssessmentResultFailure,
					ResourceAssessmentResult:      apiv1.AssessmentResultFailure,
					ResourceChainAssessmentResult: apiv1.AssessmentResultFailure,
					BasicAssessmentStartTime:      &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
					InitializationComplete:        true,
				},
			},
		},
		&apiv1.PromotedMonoVertexStatus{
			PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
				PromotedChildStatus: apiv1.PromotedChildStatus{Name: createdPromoted.Name},
			},
		},
	)
	rollout.Status.MarkProgressiveUpgradeFailed("progressive upgrade failed", rollout.Generation)

	promotedUn := monoVertexToUnstruct(createdPromoted)
	promotedUn.SetGroupVersionKind(numaflowv1.MonoVertexGroupVersionKind)

	controller := &specChangeController{}
	done, failed, _, err := ProcessResource(ctx, rollout, promotedUn, true, controller, k8sClient)
	require.NoError(t, err)
	assert.True(t, done)
	assert.False(t, failed)
	assert.False(t, controller.replacementAttempted, "a spec change must wait until the force-promote-chain trial is promoted")

	gotTrial, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Get(ctx, createdTrial.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, string(common.LabelValueUpgradePromoted), gotTrial.Labels[common.LabelKeyUpgradeState])
	assert.Equal(t, string(common.LabelValueResultStateForcePromoted), gotTrial.Labels[common.LabelKeyProgressiveResultState])

	gotPromoted, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Get(ctx, createdPromoted.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, string(common.LabelValueUpgradeRecyclable), gotPromoted.Labels[common.LabelKeyUpgradeState])

	childStatus := rollout.GetUpgradingChildStatus()
	require.NotNil(t, childStatus)
	assert.True(t, childStatus.ForcedSuccess)
	assert.Equal(t, apiv1.AssessmentResultSuccess, childStatus.ResourceChainAssessmentResult)
}

func TestDiscontinueTrialOnRecyclableController(t *testing.T) {
	restConfig, numaflowClientSet, k8sClient, _, err := commontest.PrepareK8SEnvironment()
	require.NoError(t, err)
	require.NoError(t, kubernetes.SetClientSets(restConfig))

	ctx := context.Background()
	namespace := ctlrcommon.DefaultTestNamespace
	require.NoError(t, numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{}))
	require.NoError(t, k8sClient.DeleteAllOf(ctx, &apiv1.NumaflowController{}, client.InNamespace(namespace)))

	// createMonoVertex binds both children to controller instance "1".
	promoted := createMonoVertex("mv-promoted")
	promoted.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradePromoted)
	trial := createMonoVertex("mv-trial")
	trial.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeTrial)
	_, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Create(ctx, promoted, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Create(ctx, trial, metav1.CreateOptions{})
	require.NoError(t, err)

	rollout := setMonoVertexProgressiveStatus(
		&apiv1.MonoVertexRollout{
			ObjectMeta: metav1.ObjectMeta{Name: ctlrcommon.DefaultTestMonoVertexRolloutName, Namespace: namespace, Generation: 3},
		},
		&apiv1.UpgradingMonoVertexStatus{
			UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
				UpgradingChildStatus: apiv1.UpgradingChildStatus{
					Name:                          trial.Name,
					ResourceAssessmentResult:      apiv1.AssessmentResultSuccess,
					ResourceChainAssessmentResult: apiv1.AssessmentResultFailure,
					InitializationComplete:        true,
				},
			},
		},
		nil,
	)
	rollout.Status.MarkProgressiveUpgradeFailed("progressive upgrade failed", rollout.Generation)

	// A recyclable controller on another instance leaves the trial alone.
	require.NoError(t, k8sClient.Create(ctx, ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-0", common.LabelValueUpgradeRecyclable, "0")))
	discontinued, err := DiscontinueTrialOnRecyclableController(ctx, rollout, fakeProgressiveController{}, k8sClient)
	require.NoError(t, err)
	assert.False(t, discontinued)
	gotTrial, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Get(ctx, trial.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, string(common.LabelValueUpgradeTrial), gotTrial.Labels[common.LabelKeyUpgradeState])

	// The trial controller is reverted: the trial bound to it is discontinued, the promoted child stays.
	require.NoError(t, k8sClient.Create(ctx, ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeRecyclable, "1")))
	discontinued, err = DiscontinueTrialOnRecyclableController(ctx, rollout, fakeProgressiveController{}, k8sClient)
	require.NoError(t, err)
	assert.True(t, discontinued)

	gotTrial, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Get(ctx, trial.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, string(common.LabelValueUpgradeRecyclable), gotTrial.Labels[common.LabelKeyUpgradeState])
	assert.Equal(t, string(common.LabelValueDiscontinueProgressive), gotTrial.Labels[common.LabelKeyUpgradeStateReason])
	gotPromoted, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(namespace).Get(ctx, promoted.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, string(common.LabelValueUpgradePromoted), gotPromoted.Labels[common.LabelKeyUpgradeState])

	assert.True(t, rollout.GetUpgradingChildStatus().Discontinued)
	condition := rollout.Status.GetCondition(apiv1.ConditionProgressiveUpgradeSucceeded)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionTrue, condition.Status)
	assert.Equal(t, apiv1.PhaseDeployed, rollout.Status.Phase)
}

func TestApplyDesiredControllerInstanceAnnotation(t *testing.T) {
	t.Run("replaces rollout annotation X with desired Y", func(t *testing.T) {
		required := map[string]interface{}{
			"annotations": map[string]interface{}{
				common.AnnotationKeyNumaflowInstanceID: "X",
				"keep-me":                              "user",
			},
		}
		desired := &unstructured.Unstructured{}
		desired.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "Y"})
		desired.SetLabels(map[string]string{common.LabelKeyControllerInstanceID: "Y"})

		applyDesiredControllerInstanceAnnotation(required, desired)

		annotations := required["annotations"].(map[string]interface{})
		assert.Equal(t, "Y", annotations[common.AnnotationKeyNumaflowInstanceID])
		assert.Equal(t, "user", annotations["keep-me"])
		_, hasLabel := required["labels"]
		assert.False(t, hasLabel)
	})

	t.Run("removes instance annotation when desired has unbound", func(t *testing.T) {
		required := map[string]interface{}{
			"annotations": map[string]interface{}{
				common.AnnotationKeyNumaflowInstanceID: "X",
			},
		}
		desired := &unstructured.Unstructured{}

		applyDesiredControllerInstanceAnnotation(required, desired)

		annotations := required["annotations"].(map[string]interface{})
		_, found := annotations[common.AnnotationKeyNumaflowInstanceID]
		assert.False(t, found)
	})
}
