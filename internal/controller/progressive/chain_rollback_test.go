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
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

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
