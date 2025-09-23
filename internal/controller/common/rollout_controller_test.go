package common

import (
	"context"
	"testing"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
			Namespace: DefaultTestNamespace,
		},
	}
	checkLive := false

	reasonProgressiveSuccess := common.LabelValueProgressiveSuccess
	reasonUpgradingReplaced := common.LabelValueProgressiveReplaced

	// Base time for creating pipelines with different timestamps
	baseTime := time.Now()

	tests := []struct {
		name               string
		pipelines          []*numaflowv1.Pipeline
		upgradeState       common.UpgradeState
		upgradeStateReason *common.UpgradeStateReason
		expectedName       string
		expectedError      error
	}{
		{
			name: "Multiple children with different creation timestamps",
			pipelines: []*numaflowv1.Pipeline{
				createPipelineWithTimestamp("my-pipeline-1", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, nil, baseTime.Add(-3*time.Minute)), // oldest
				createPipelineWithTimestamp("my-pipeline-2", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, nil, baseTime.Add(-2*time.Minute)),
				createPipelineWithTimestamp("my-pipeline-3", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, nil, baseTime.Add(-1*time.Minute)), // newest
				createPipelineWithTimestamp("my-pipeline-4", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradeInProgress, nil, baseTime),                   // different upgrade state
			},
			upgradeState:  common.LabelValueUpgradePromoted,
			expectedName:  "my-pipeline-3", // newest timestamp
			expectedError: nil,
		},
		{
			name: "Multiple children with upgrade strategy reason specified",
			pipelines: []*numaflowv1.Pipeline{
				createPipelineWithTimestamp("my-pipeline-1", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, &reasonUpgradingReplaced, baseTime.Add(-2*time.Minute)), // older
				createPipelineWithTimestamp("my-pipeline-2", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, &reasonUpgradingReplaced, baseTime.Add(-1*time.Minute)), // newer
				createPipelineWithTimestamp("my-pipeline-3", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, &reasonProgressiveSuccess, baseTime),                    // different reason
				createPipelineWithTimestamp("my-pipeline-4", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, nil, baseTime),                                          // no reason
			},
			upgradeState:       common.LabelValueUpgradePromoted,
			upgradeStateReason: &reasonUpgradingReplaced,
			expectedName:       "my-pipeline-2", // newest with matching reason
			expectedError:      nil,
		},
		{
			name:          "No children exist",
			pipelines:     []*numaflowv1.Pipeline{},
			upgradeState:  common.LabelValueUpgradePromoted,
			expectedName:  "",
			expectedError: nil,
		},
		{
			name: "No children found",
			pipelines: []*numaflowv1.Pipeline{
				createPipelineWithTimestamp("my-pipeline-3", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, &reasonProgressiveSuccess, baseTime), // different reason
				createPipelineWithTimestamp("my-pipeline-4", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, nil, baseTime),                       // no reason
				createPipelineWithTimestamp("my-pipeline-4", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradeInProgress, nil, baseTime),                     // different upgrade state
			},
			upgradeState:       common.LabelValueUpgradePromoted,
			upgradeStateReason: &reasonUpgradingReplaced,
			expectedName:       "",
			expectedError:      nil,
		},
		{
			name: "One child",
			pipelines: []*numaflowv1.Pipeline{
				createPipelineWithTimestamp("my-pipeline-2", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, &reasonUpgradingReplaced, baseTime.Add(-1*time.Minute)),
			},
			upgradeState:  common.LabelValueUpgradePromoted,
			expectedName:  "my-pipeline-2",
			expectedError: nil,
		},
		{
			name: "One child with upgrade strategy reason specified",
			pipelines: []*numaflowv1.Pipeline{
				createPipelineWithTimestamp("my-pipeline-2", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, &reasonUpgradingReplaced, baseTime.Add(-1*time.Minute)),
			},
			upgradeState:       common.LabelValueUpgradePromoted,
			upgradeStateReason: &reasonUpgradingReplaced,
			expectedName:       "my-pipeline-2",
			expectedError:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// Clean up any pipelines on the namespace beforehand
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			pipelineList, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(DefaultTestNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Len(t, pipelineList.Items, 0)

			// Create the Pipelines in Kubernetes
			for _, pipeline := range tt.pipelines {
				_, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(DefaultTestNamespace).Create(ctx, pipeline, metav1.CreateOptions{})
				assert.NoError(t, err)
			}

			mostCurrentChild, err := FindMostCurrentChildOfUpgradeState(ctx, pipelineRollout, tt.upgradeState, tt.upgradeStateReason, checkLive, client)
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

					// verify all other children of the same upgrade state and upgrade state reason have been marked "recyclable"
					for _, p := range tt.pipelines {
						if p.Labels[common.LabelKeyUpgradeState] == string(tt.upgradeState) && (tt.upgradeStateReason == nil || p.Labels[common.LabelKeyUpgradeStateReason] == string(*tt.upgradeStateReason)) {
							retrievedPipeline, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(DefaultTestNamespace).Get(ctx, p.Name, metav1.GetOptions{})
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

func createPipelineWithTimestamp(pipelineName string, pipelineRolloutName string, isbsvcRolloutName string, upgradeState common.UpgradeState, upgradeStateReason *common.UpgradeStateReason, creationTime time.Time) *numaflowv1.Pipeline {
	labels := map[string]string{
		common.LabelKeyParentRollout:               pipelineRolloutName,
		common.LabelKeyISBServiceRONameForPipeline: isbsvcRolloutName,
		common.LabelKeyUpgradeState:                string(upgradeState),
	}
	if upgradeStateReason != nil {
		labels[common.LabelKeyUpgradeStateReason] = string(*upgradeStateReason)
	}
	pipeline := CreateTestPipelineOfSpec(pipelineSpec, pipelineName, numaflowv1.PipelinePhaseRunning, numaflowv1.Status{}, false, labels, map[string]string{})
	// Set the creation timestamp
	pipeline.ObjectMeta.CreationTimestamp = metav1.NewTime(creationTime)
	return pipeline
}
