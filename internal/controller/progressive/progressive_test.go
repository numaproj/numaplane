package progressive

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
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

// This tests the FindMostCurrentChildOfUpgradeState() function
// This function both finds the most current child of the upgrade-state, but also recycles any others that are found
// So, it should only be called if there in fact should be only one child of that upgrade-state in existence
func TestFindMostCurrentChildOfUpgradeState(t *testing.T) {

	restConfig, numaflowClientSet, client, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetClientSets(restConfig))

	ctx := context.TODO()
	pipelineRollout := &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-pipeline",
			Namespace: ctlrcommon.DefaultTestNamespace,
		},
	}
	checkLive := false

	tests := []struct {
		name          string
		pipelines     []*numaflowv1.Pipeline
		expectedName  string
		expectedError error
	}{
		{
			name: "Multiple children with valid indices",
			pipelines: []*numaflowv1.Pipeline{
				createPipeline("my-pipeline-1", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted),
				createPipeline("my-pipeline-2", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted),
				createPipeline("my-pipeline-3", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted),
			},

			expectedName:  "my-pipeline-3",
			expectedError: nil,
		},
		{
			name:          "No children",
			pipelines:     []*numaflowv1.Pipeline{},
			expectedName:  "",
			expectedError: nil,
		},
		{
			name: "Backward compatibility with older code",
			pipelines: []*numaflowv1.Pipeline{
				createPipeline("my-pipeline-1", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted),
				createPipeline("my-pipeline-2", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted),
				createPipeline("my-pipeline-3", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted),
				createPipeline("my-pipeline", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted),
			},

			expectedName:  "my-pipeline-3",
			expectedError: nil,
		},
		{
			name: "Backward compatibility with older code, and just one pipeline exists",
			pipelines: []*numaflowv1.Pipeline{
				createPipeline("my-pipeline", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted),
			},

			expectedName:  "my-pipeline",
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// Clean up any pipelines on the namespace beforehand
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			pipelineList, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Len(t, pipelineList.Items, 0)

			// Create the Pipelines in Kubernetes
			for _, pipeline := range tt.pipelines {
				_, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).Create(ctx, pipeline, metav1.CreateOptions{})
				assert.NoError(t, err)
			}

			mostCurrentChild, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, pipelineRollout, common.LabelValueUpgradePromoted, checkLive, client)
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Nil(t, mostCurrentChild)
			} else {
				assert.NoError(t, err)
				if tt.expectedName == "" {
					assert.Nil(t, mostCurrentChild)
				} else {
					// verify the most current child is correct
					assert.NotNil(t, mostCurrentChild)
					assert.Equal(t, tt.expectedName, mostCurrentChild.GetName())

					// verify all other children have been marked "recyclable"
					for _, p := range tt.pipelines {
						retrievedPipeline, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).Get(ctx, p.Name, metav1.GetOptions{})
						assert.NoError(t, err)

						retrievedUpgradeState, found := retrievedPipeline.GetLabels()[common.LabelKeyUpgradeState]
						assert.True(t, found)
						if p.Name == tt.expectedName {
							assert.Equal(t, string(common.LabelValueUpgradePromoted), retrievedUpgradeState)
						} else {
							assert.Equal(t, string(common.LabelValueUpgradeRecyclable), retrievedUpgradeState)
						}
					}
				}
			}
		})
	}
}

var (
	defaultISBSVCRolloutName = "my-isbsvc"
	pipelineSpec             = numaflowv1.PipelineSpec{
		InterStepBufferServiceName: defaultISBSVCRolloutName + "-0",
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: "in",
				Source: &numaflowv1.Source{
					Generator: &numaflowv1.GeneratorSource{},
				},
			},
			{
				Name: "out",
				Sink: &numaflowv1.Sink{
					AbstractSink: numaflowv1.AbstractSink{
						Log: &numaflowv1.Log{},
					},
				},
			},
		},
		Edges: []numaflowv1.Edge{
			{
				From: "in",
				To:   "out",
			},
		},
	}
)

func createPipeline(pipelineName string, pipelineRolloutName string, isbsvcRolloutName string, upgradeState common.UpgradeState) *numaflowv1.Pipeline {
	return ctlrcommon.CreateTestPipelineOfSpec(pipelineSpec, pipelineName, numaflowv1.PipelinePhaseRunning, numaflowv1.Status{}, false,
		map[string]string{
			common.LabelKeyParentRollout:               pipelineRolloutName,
			common.LabelKeyISBServiceRONameForPipeline: isbsvcRolloutName,
			common.LabelKeyUpgradeState:                string(upgradeState),
		},
		map[string]string{})
}

var defaultMonoVertexRollout = &apiv1.MonoVertexRollout{
	ObjectMeta: metav1.ObjectMeta{
		Name: "test",
	},
	Status: apiv1.MonoVertexRolloutStatus{
		Status: apiv1.Status{
			ProgressiveStatus: apiv1.ProgressiveStatus{
				UpgradingChildStatus: nil,
			},
		},
	},
}

type fakeProgressiveController struct{}

func (fpc fakeProgressiveController) CreateUpgradingChildDefinition(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, name string) (*unstructured.Unstructured, error) {
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

	testCases := []struct {
		name                      string
		liveRolloutObject         ctlrcommon.RolloutObject
		existingUpgradingChildDef *unstructured.Unstructured
		expectedDone              bool
		expectedNewChildCreated   bool
		expectedRequeueDelay      time.Duration
		expectedError             error
	}{
		{
			name:                      "no upgrading child status on the live rollout",
			liveRolloutObject:         defaultMonoVertexRollout.DeepCopy(),
			existingUpgradingChildDef: &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": "test"}}},
			expectedDone:              false,
			expectedNewChildCreated:   false,
			expectedRequeueDelay:      assessmentInterval,
			expectedError:             nil,
		},
		{
			name:                      "preset upgrading child status on the live rollout - different name",
			liveRolloutObject:         setRolloutObjectUpgradingChildStatus(defaultMonoVertexRollout.DeepCopy(), &apiv1.ChildStatus{Name: "test"}),
			existingUpgradingChildDef: &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": "test-1"}}},
			expectedDone:              false,
			expectedNewChildCreated:   false,
			expectedRequeueDelay:      assessmentInterval,
			expectedError:             nil,
		},
		{
			name: "preset upgrading child status on the live rollout - same name, can assess, success",
			liveRolloutObject: setRolloutObjectUpgradingChildStatus(defaultMonoVertexRollout.DeepCopy(), &apiv1.ChildStatus{
				Name:               "test-success",
				AssessmentResult:   apiv1.AssessmentResultUnknown,
				NextAssessmentTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
			}),
			existingUpgradingChildDef: &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": "test-success"}}},
			expectedDone:              false,
			expectedNewChildCreated:   false,
			expectedRequeueDelay:      assessmentInterval,
			expectedError:             nil,
		},
		{
			name: "preset upgrading child status on the live rollout - same name, failure",
			liveRolloutObject: setRolloutObjectUpgradingChildStatus(defaultMonoVertexRollout.DeepCopy(), &apiv1.ChildStatus{
				Name:               "test-failure",
				AssessmentResult:   apiv1.AssessmentResultFailure,
				NextAssessmentTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
			}),
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
				ctx, defaultMonoVertexRollout, tc.liveRolloutObject, fakeProgressiveController{}, nil, tc.existingUpgradingChildDef, client)

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

func setRolloutObjectUpgradingChildStatus(rolloutObject ctlrcommon.RolloutObject, childStatus *apiv1.ChildStatus) ctlrcommon.RolloutObject {
	rolloutObject.GetRolloutStatus().ProgressiveStatus.UpgradingChildStatus = childStatus
	return rolloutObject
}
