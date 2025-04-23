package common

import (
	"context"
	"testing"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// This tests the ResolveTemplateSpec function
// This function resolves any templated definition with the provided arguments
func TestResolveTemplateSpec(t *testing.T) {

	tests := []struct {
		name           string
		data           any
		args           map[string]interface{}
		expectedOutput map[string]interface{}
	}{
		{
			name: "multiple arguments",
			data: map[string]interface{}{
				"spec": map[string]interface{}{
					"configMap": map[string]interface{}{
						"name":      "test-volume-{{.monovertex-name}}",
						"namespace": "{{.monovertex-namespace}}",
					},
				},
			},
			args: map[string]interface{}{
				".monovertex-name":      "my-monovertex-0",
				".monovertex-namespace": "test-namespace",
			},
			expectedOutput: map[string]interface{}{
				"spec": map[string]interface{}{
					"configMap": map[string]interface{}{
						"name":      "test-volume-my-monovertex-0",
						"namespace": "test-namespace",
					},
				},
			},
		},
		{
			name: "argument is used multiple times",
			data: map[string]interface{}{
				"annotations": map[string]interface{}{
					"link.argocd.argoproj.io/external-link": "https://splunk.intuit.com/en-US/app/search/search?q=search%20(index%3Daccounting-ledger%20source%3Diks2%2Fsbg-qbo-ppd-usw2-k8s%2F*%2F{{.monovertex-namespace}}%2F*)%20OR%20(index%3Diks%20source%3Diks2%2Fsbg-qbo-ppd-usw2-k8s%2F*%2F{{.monovertex-namespace}}%2F*)%20kubernetes_pod%3D{{.monovertex-name}}*&display.page.search.mode=fast&dispatch.sample_ratio=1&earliest=-5m&latest=now",
				},
				"spec": map[string]interface{}{
					"configMap": map[string]interface{}{
						"name":      "test-volume-{{.monovertex-name}}",
						"namespace": "{{.monovertex-namespace}}",
					},
				},
			},
			args: map[string]interface{}{
				".monovertex-name":      "my-monovertex-0",
				".monovertex-namespace": "test-namespace",
			},
			expectedOutput: map[string]interface{}{
				"annotations": map[string]interface{}{
					"link.argocd.argoproj.io/external-link": "https://splunk.intuit.com/en-US/app/search/search?q=search%20(index%3Daccounting-ledger%20source%3Diks2%2Fsbg-qbo-ppd-usw2-k8s%2F*%2Ftest-namespace%2F*)%20OR%20(index%3Diks%20source%3Diks2%2Fsbg-qbo-ppd-usw2-k8s%2F*%2Ftest-namespace%2F*)%20kubernetes_pod%3Dmy-monovertex-0*&display.page.search.mode=fast&dispatch.sample_ratio=1&earliest=-5m&latest=now",
				},
				"spec": map[string]interface{}{
					"configMap": map[string]interface{}{
						"name":      "test-volume-my-monovertex-0",
						"namespace": "test-namespace",
					},
				},
			},
		},
		{
			name: "input is not templated",
			data: map[string]interface{}{
				"spec": map[string]interface{}{
					"configMap": map[string]interface{}{
						"name":      "test-volume-my-monovertex-0",
						"namespace": "test-namespace",
					},
				},
			},
			args: map[string]interface{}{
				".monovertex-name":      "my-monovertex-0",
				".monovertex-namespace": "test-namespace",
			},
			expectedOutput: map[string]interface{}{
				"spec": map[string]interface{}{
					"configMap": map[string]interface{}{
						"name":      "test-volume-my-monovertex-0",
						"namespace": "test-namespace",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ResolveTemplateSpec(tt.data, tt.args)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedOutput, result)
		})
	}

}

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
	reasonProgressiveFailure := common.LabelValueProgressiveFailure

	tests := []struct {
		name               string
		pipelines          []*numaflowv1.Pipeline
		upgradeState       common.UpgradeState
		upgradeStateReason *common.UpgradeStateReason
		expectedName       string
		expectedError      error
	}{
		{
			name: "Multiple children with valid indices",
			pipelines: []*numaflowv1.Pipeline{
				createPipeline("my-pipeline-1", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, nil),
				createPipeline("my-pipeline-2", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, nil),
				createPipeline("my-pipeline-3", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, nil),
				createPipeline("my-pipeline-4", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradeInProgress, nil),
			},
			upgradeState:  common.LabelValueUpgradePromoted,
			expectedName:  "my-pipeline-3",
			expectedError: nil,
		},
		{
			name: "Multiple children with valid indices - upgrade strategy reason specified",
			pipelines: []*numaflowv1.Pipeline{
				createPipeline("my-pipeline-1", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, &reasonProgressiveFailure),
				createPipeline("my-pipeline-2", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, &reasonProgressiveFailure),
				createPipeline("my-pipeline-3", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, &reasonProgressiveSuccess),
				createPipeline("my-pipeline-4", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, nil),
			},
			upgradeState:       common.LabelValueUpgradePromoted,
			upgradeStateReason: &reasonProgressiveFailure,
			expectedName:       "my-pipeline-2",
			expectedError:      nil,
		},
		{
			name:          "No children",
			pipelines:     []*numaflowv1.Pipeline{},
			upgradeState:  common.LabelValueUpgradePromoted,
			expectedName:  "",
			expectedError: nil,
		},
		{
			name: "Backward compatibility with older code",
			pipelines: []*numaflowv1.Pipeline{
				createPipeline("my-pipeline-1", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, nil),
				createPipeline("my-pipeline-2", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, nil),
				createPipeline("my-pipeline-3", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, nil),
				// backward compatibility test tests for case of "my-pipeline" with no suffix
				createPipeline("my-pipeline", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, nil),
			},
			upgradeState: common.LabelValueUpgradePromoted,

			expectedName:  "my-pipeline-3",
			expectedError: nil,
		},
		{
			name: "Backward compatibility with older code, and just one pipeline exists",
			pipelines: []*numaflowv1.Pipeline{
				// backward compatibility test tests for case of "my-pipeline" with no suffix
				createPipeline("my-pipeline", "my-pipeline", defaultISBSVCRolloutName, common.LabelValueUpgradePromoted, nil),
			},
			upgradeState: common.LabelValueUpgradePromoted,

			expectedName:  "my-pipeline",
			expectedError: nil,
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

func createPipeline(pipelineName string, pipelineRolloutName string, isbsvcRolloutName string, upgradeState common.UpgradeState, upgradeStateReason *common.UpgradeStateReason) *numaflowv1.Pipeline {
	labels := map[string]string{
		common.LabelKeyParentRollout:               pipelineRolloutName,
		common.LabelKeyISBServiceRONameForPipeline: isbsvcRolloutName,
		common.LabelKeyUpgradeState:                string(upgradeState),
	}
	if upgradeStateReason != nil {
		labels[common.LabelKeyUpgradeStateReason] = string(*upgradeStateReason)
	}
	return CreateTestPipelineOfSpec(pipelineSpec, pipelineName, numaflowv1.PipelinePhaseRunning, numaflowv1.Status{}, false, labels, map[string]string{})
}
