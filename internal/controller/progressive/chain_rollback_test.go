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

package progressive

import (
	"context"
	"encoding/json"
	"testing"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func TestRollbackChainControllerTrial(t *testing.T) {
	const (
		namespace        = "test"
		promotedInstance = "promoted-1"
		trialInstance    = "trial-2"
	)
	ctx := context.Background()

	controllerRollout := &apiv1.NumaflowControllerRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: namespace},
		Status: apiv1.NumaflowControllerRolloutStatus{
			ProgressiveStatus: apiv1.NumaflowControllerProgressiveStatus{
				UpgradingNumaflowControllerStatus: &apiv1.UpgradingNumaflowControllerStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:             "controller-2",
						AssessmentResult: apiv1.AssessmentResultUnknown,
					},
				},
			},
		},
	}
	trialController := ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-2", common.LabelValueUpgradeTrial, trialInstance)
	trialController.Labels[common.LabelKeyControllerInstanceID] = trialInstance
	promotedController := ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradePromoted, promotedInstance)

	monoVertexRollout := &apiv1.MonoVertexRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "mv", Namespace: namespace},
		Status: apiv1.MonoVertexRolloutStatus{
			ProgressiveStatus: apiv1.MonoVertexProgressiveStatus{
				UpgradingMonoVertexStatus: &apiv1.UpgradingMonoVertexStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{Name: "mv-1", AssessmentResult: apiv1.AssessmentResultUnknown},
					},
				},
			},
		},
	}
	trialMonoVertex := &numaflowv1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mv-1",
			Namespace: namespace,
			Labels: map[string]string{
				common.LabelKeyParentRollout: monoVertexRollout.Name,
				common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeTrial),
			},
			Annotations: map[string]string{common.AnnotationKeyNumaflowInstanceID: trialInstance},
		},
	}
	source := &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "pipeline", Namespace: namespace},
		Status: apiv1.PipelineRolloutStatus{
			ProgressiveStatus: apiv1.PipelineProgressiveStatus{
				UpgradingPipelineStatus: &apiv1.UpgradingPipelineStatus{
					InterStepBufferServiceName: "promoted-isb",
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{Name: "pipeline-1", AssessmentResult: apiv1.AssessmentResultUnknown},
					},
				},
			},
		},
	}
	upgradingPipeline := &unstructured.Unstructured{}
	upgradingPipeline.SetGroupVersionKind(numaflowv1.PipelineGroupVersionKind)
	upgradingPipeline.SetName("pipeline-1")
	upgradingPipeline.SetNamespace(namespace)
	upgradingPipeline.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: trialInstance})

	c := newChainClient(t, controllerRollout, trialController, promotedController, monoVertexRollout, trialMonoVertex, source)
	require.NoError(t, c.Status().Update(ctx, controllerRollout))
	require.NoError(t, c.Status().Update(ctx, monoVertexRollout))
	require.NoError(t, c.Status().Update(ctx, source))

	require.NoError(t, RollbackChain(ctx, c, source, upgradingPipeline))

	updatedController := &apiv1.NumaflowControllerRollout{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(controllerRollout), updatedController))
	require.NotNil(t, updatedController.GetUpgradingChildStatus())
	assert.Equal(t, apiv1.AssessmentResultFailure, updatedController.GetUpgradingChildStatus().AssessmentResult)

	updatedTrial := &apiv1.NumaflowController{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(trialController), updatedTrial))
	assert.Equal(t, string(common.LabelValueResultStateFailed), updatedTrial.Labels[common.LabelKeyProgressiveResultState])
	assert.Equal(t, string(common.LabelValueUpgradeTrial), updatedTrial.Labels[common.LabelKeyUpgradeState])

	updatedMonoVertexRollout := &apiv1.MonoVertexRollout{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(monoVertexRollout), updatedMonoVertexRollout))
	require.NotNil(t, updatedMonoVertexRollout.GetUpgradingChildStatus())
	assert.Equal(t, apiv1.AssessmentResultFailure, updatedMonoVertexRollout.GetUpgradingChildStatus().AssessmentResult)

	updatedMonoVertex := &numaflowv1.MonoVertex{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(trialMonoVertex), updatedMonoVertex))
	assert.Equal(t, string(common.LabelValueResultStateFailed), updatedMonoVertex.Labels[common.LabelKeyProgressiveResultState])

	// The source is left for its own failure path.
	updatedSource := &apiv1.PipelineRollout{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(source), updatedSource))
	assert.Equal(t, apiv1.AssessmentResultUnknown, updatedSource.GetUpgradingChildStatus().AssessmentResult)

	desired, err := ctlrcommon.GetDesiredControllerInstanceID(ctx, c, namespace)
	require.NoError(t, err)
	assert.Equal(t, promotedInstance, desired)
}

func TestRollbackChainLeavesControllerWhenPipelineIsNotOnTheTrial(t *testing.T) {
	const namespace = "test"
	ctx := context.Background()

	controllerRollout := &apiv1.NumaflowControllerRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: namespace},
		Status: apiv1.NumaflowControllerRolloutStatus{
			ProgressiveStatus: apiv1.NumaflowControllerProgressiveStatus{
				UpgradingNumaflowControllerStatus: &apiv1.UpgradingNumaflowControllerStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:             "controller-2",
						AssessmentResult: apiv1.AssessmentResultUnknown,
					},
				},
			},
		},
	}
	trialController := ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-2", common.LabelValueUpgradeTrial, "trial-2")
	source := &apiv1.PipelineRollout{ObjectMeta: metav1.ObjectMeta{Name: "pipeline", Namespace: namespace}}
	upgradingPipeline := &unstructured.Unstructured{}
	upgradingPipeline.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "promoted-1"})

	c := newChainClient(t, controllerRollout, trialController, source)
	require.NoError(t, c.Status().Update(ctx, controllerRollout))

	require.NoError(t, RollbackChain(ctx, c, source, upgradingPipeline))

	updatedController := &apiv1.NumaflowControllerRollout{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(controllerRollout), updatedController))
	assert.Equal(t, apiv1.AssessmentResultUnknown, updatedController.GetUpgradingChildStatus().AssessmentResult)
}

func TestRollbackChainISBServiceTrial(t *testing.T) {
	const (
		namespace = "test"
		isbName   = "default-1"
	)
	ctx := context.Background()

	isbRollout := &apiv1.ISBServiceRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: namespace},
		Status: apiv1.ISBServiceRolloutStatus{
			ProgressiveStatus: apiv1.ISBServiceProgressiveStatus{
				UpgradingISBServiceStatus: &apiv1.UpgradingISBServiceStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{Name: isbName, AssessmentResult: apiv1.AssessmentResultUnknown},
				},
			},
		},
	}
	trialISB := &numaflowv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      isbName,
			Namespace: namespace,
			Labels: map[string]string{
				common.LabelKeyParentRollout: isbRollout.Name,
				common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeTrial),
			},
		},
	}
	source := pipelineOnISB("pipeline-a", namespace, isbName, apiv1.AssessmentResultFailure)
	sibling := pipelineOnISB("pipeline-b", namespace, isbName, apiv1.AssessmentResultUnknown)
	siblingPipeline := &numaflowv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pipeline-b-1",
			Namespace: namespace,
			Labels: map[string]string{
				common.LabelKeyParentRollout: sibling.Name,
				common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeTrial),
			},
		},
		Spec: numaflowv1.PipelineSpec{InterStepBufferServiceName: isbName},
	}

	c := newChainClient(t, isbRollout, trialISB, source, sibling, siblingPipeline)
	require.NoError(t, c.Status().Update(ctx, isbRollout))
	require.NoError(t, c.Status().Update(ctx, source))
	require.NoError(t, c.Status().Update(ctx, sibling))

	require.NoError(t, RollbackChain(ctx, c, source, nil))

	updatedISBRollout := &apiv1.ISBServiceRollout{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(isbRollout), updatedISBRollout))
	assert.Equal(t, apiv1.AssessmentResultFailure, updatedISBRollout.GetUpgradingChildStatus().AssessmentResult)

	updatedISB := &numaflowv1.InterStepBufferService{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(trialISB), updatedISB))
	assert.Equal(t, string(common.LabelValueResultStateFailed), updatedISB.Labels[common.LabelKeyProgressiveResultState])

	updatedSibling := &apiv1.PipelineRollout{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(sibling), updatedSibling))
	assert.Equal(t, apiv1.AssessmentResultFailure, updatedSibling.GetUpgradingChildStatus().AssessmentResult)

	updatedSiblingPipeline := &numaflowv1.Pipeline{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(siblingPipeline), updatedSiblingPipeline))
	assert.Equal(t, string(common.LabelValueResultStateFailed), updatedSiblingPipeline.Labels[common.LabelKeyProgressiveResultState])

	updatedSource := &apiv1.PipelineRollout{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(source), updatedSource))
	assert.Equal(t, apiv1.AssessmentResultFailure, updatedSource.GetUpgradingChildStatus().AssessmentResult)
}

func TestShouldHoldNewProgressive(t *testing.T) {
	const namespace = "test"
	ctx := context.Background()

	t.Run("holds while the controller trial assessment is failed", func(t *testing.T) {
		controllerRollout := &apiv1.NumaflowControllerRollout{ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: namespace}}
		failedTrial := ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-2", common.LabelValueUpgradeTrial, "trial-2")
		failedTrial.Labels[common.LabelKeyProgressiveResultState] = string(common.LabelValueResultStateFailed)
		isbRollout := &apiv1.ISBServiceRollout{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: namespace}}
		c := newChainClient(t, controllerRollout, failedTrial, isbRollout)

		hold, err := ShouldHoldNewProgressive(ctx, c, isbRollout)
		require.NoError(t, err)
		assert.True(t, hold)
	})

	t.Run("holds a pipeline while its ISBService trial assessment is failed", func(t *testing.T) {
		isbRollout := &apiv1.ISBServiceRollout{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: namespace}}
		failedISB := &numaflowv1.InterStepBufferService{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "default-1",
				Namespace: namespace,
				Labels: map[string]string{
					common.LabelKeyParentRollout:          isbRollout.Name,
					common.LabelKeyUpgradeState:           string(common.LabelValueUpgradeTrial),
					common.LabelKeyProgressiveResultState: string(common.LabelValueResultStateFailed),
				},
			},
		}
		pipeline := pipelineOnISB("pipeline", namespace, "default-1", apiv1.AssessmentResultUnknown)
		spec, err := json.Marshal(numaflowv1.PipelineSpec{InterStepBufferServiceName: isbRollout.Name})
		require.NoError(t, err)
		pipeline.Spec.Pipeline.Spec.Raw = spec
		c := newChainClient(t, isbRollout, failedISB, pipeline)

		hold, err := ShouldHoldNewProgressive(ctx, c, pipeline)
		require.NoError(t, err)
		assert.True(t, hold)
	})

	t.Run("does not hold when there is no failed trial", func(t *testing.T) {
		isbRollout := &apiv1.ISBServiceRollout{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: namespace}}
		c := newChainClient(t, isbRollout)

		hold, err := ShouldHoldNewProgressive(ctx, c, isbRollout)
		require.NoError(t, err)
		assert.False(t, hold)
	})
}

func pipelineOnISB(name, namespace, isbName string, assessment apiv1.AssessmentResult) *apiv1.PipelineRollout {
	return &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Status: apiv1.PipelineRolloutStatus{
			ProgressiveStatus: apiv1.PipelineProgressiveStatus{
				UpgradingPipelineStatus: &apiv1.UpgradingPipelineStatus{
					InterStepBufferServiceName: isbName,
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{Name: name + "-1", AssessmentResult: assessment},
					},
				},
			},
		},
	}
}

func newChainClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))
	require.NoError(t, numaflowv1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(
		&apiv1.NumaflowControllerRollout{},
		&apiv1.ISBServiceRollout{},
		&apiv1.PipelineRollout{},
		&apiv1.MonoVertexRollout{},
	).WithObjects(objects...).Build()
}
