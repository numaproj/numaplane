package progressive

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type fakeProgressiveController struct{}

func (fpc fakeProgressiveController) CreateUpgradingChildDefinition(ctx context.Context, rolloutObject ProgressiveRolloutObject, name string) (*unstructured.Unstructured, error) {
	return nil, nil
}

func (fpc fakeProgressiveController) IncrementChildCount(ctx context.Context, rolloutObject ctlrcommon.RolloutObject) (int32, error) {
	return 0, nil
}

func (fpc fakeProgressiveController) Recycle(ctx context.Context, childObject *unstructured.Unstructured, c client.Client) (bool, error) {
	return false, nil
}

func (fpc fakeProgressiveController) ChildNeedsUpdating(ctx context.Context, existingChild, newChildDefinition *unstructured.Unstructured) (bool, error) {
	return false, nil
}

func (fpc fakeProgressiveController) AssessUpgradingChild(ctx context.Context, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, error) {
	switch existingUpgradingChildDef.GetName() {
	case "test-success":
		return apiv1.AssessmentResultSuccess, nil
	case "test-failure":
		return apiv1.AssessmentResultFailure, nil
	default:
		return apiv1.AssessmentResultUnknown, nil
	}
}

func (fpc fakeProgressiveController) ProcessPromotedChildPreUpgrade(ctx context.Context, rolloutObject ProgressiveRolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) (bool, error) {
	return false, nil
}

func (fpc fakeProgressiveController) ProcessPromotedChildPostFailure(ctx context.Context, rolloutObject ProgressiveRolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) (bool, error) {
	return false, nil
}

func Test_processUpgradingChild(t *testing.T) {
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

	globalConfig, err := config.GetConfigManagerInstance().GetConfig()
	assert.NoError(t, err)

	_, _, assessmentInterval, err := globalConfig.GetChildStatusAssessmentSchedule()
	assert.NoError(t, err)

	defaultExistingPromotedChildDef := &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": "test"}}}

	testCases := []struct {
		name                      string
		rolloutObject             ProgressiveRolloutObject
		existingUpgradingChildDef *unstructured.Unstructured
		expectedDone              bool
		expectedNewChildCreated   bool
		expectedRequeueDelay      time.Duration
		expectedError             error
	}{
		{
			name:                      "no upgrading child status on the live rollout",
			rolloutObject:             defaultMonoVertexRollout.DeepCopy(),
			existingUpgradingChildDef: &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": "test"}}},
			expectedDone:              false,
			expectedNewChildCreated:   false,
			expectedRequeueDelay:      assessmentInterval,
			expectedError:             nil,
		},
		{
			name: "preset upgrading child status on the live rollout - different name",
			rolloutObject: setMonoVertexProgressiveStatus(
				defaultMonoVertexRollout.DeepCopy(),
				&apiv1.UpgradingMonoVertexStatus{UpgradingChildStatus: apiv1.UpgradingChildStatus{Name: "test"}},
				nil),
			//setRolloutObjectChildStatus(defaultMonoVertexRollout.DeepCopy(), &apiv1.UpgradingChildStatus{Name: "test"}, &apiv1.PromotedChildStatus{}),
			existingUpgradingChildDef: &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": "test-1"}}},
			expectedDone:              false,
			expectedNewChildCreated:   false,
			expectedRequeueDelay:      assessmentInterval,
			expectedError:             nil,
		},
		{
			name: "preset upgrading child status on the live rollout - same name, can assess, success",
			rolloutObject: setMonoVertexProgressiveStatus(
				defaultMonoVertexRollout.DeepCopy(),
				&apiv1.UpgradingMonoVertexStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                "test-success",
						AssessmentResult:    apiv1.AssessmentResultUnknown,
						AssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
					},
				},
				nil,
			),
			existingUpgradingChildDef: &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": "test-success"}}},
			expectedDone:              false,
			expectedNewChildCreated:   false,
			expectedRequeueDelay:      assessmentInterval,
			expectedError:             nil,
		},
		{
			name: "preset upgrading child status on the live rollout - same name, failure",
			rolloutObject: setMonoVertexProgressiveStatus(
				defaultMonoVertexRollout.DeepCopy(),
				&apiv1.UpgradingMonoVertexStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                "test-failure",
						AssessmentResult:    apiv1.AssessmentResultFailure,
						AssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
					},
				},
				&apiv1.PromotedMonoVertexStatus{
					PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
						PromotedChildStatus: apiv1.PromotedChildStatus{
							Name: defaultExistingPromotedChildDef.GetName(),
						},
						ScaleValuesRestoredToDesired: true,
					},
				},
			),
			existingUpgradingChildDef: &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": "test-failure"}}},
			expectedDone:              false,
			expectedNewChildCreated:   false,
			expectedRequeueDelay:      0,
			expectedError:             nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualDone, actualNewChildCreated, actualRequeueDelay, actualErr := processUpgradingChild(
				ctx, tc.rolloutObject, fakeProgressiveController{}, defaultExistingPromotedChildDef, tc.existingUpgradingChildDef, client)

			if tc.expectedError != nil {
				assert.Error(t, actualErr)
				assert.False(t, actualDone)
				assert.False(t, actualNewChildCreated)
				assert.Zero(t, actualRequeueDelay)
			} else {
				assert.Nil(t, actualErr)
				assert.Equal(t, tc.expectedDone, actualDone)
				assert.Equal(t, tc.expectedNewChildCreated, actualNewChildCreated)
				assert.Equal(t, tc.expectedRequeueDelay, actualRequeueDelay)
			}
		})
	}
}

func setMonoVertexProgressiveStatus(mvRollout *apiv1.MonoVertexRollout, upgradingChildStatus *apiv1.UpgradingMonoVertexStatus, promotedChildStatus *apiv1.PromotedMonoVertexStatus) *apiv1.MonoVertexRollout {
	mvRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus = upgradingChildStatus
	mvRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus = promotedChildStatus
	return mvRollout
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
