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
	"os"
	"path/filepath"
	"testing"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"github.com/numaproj/numaplane/pkg/client/clientset/versioned/scheme"
	commontest "github.com/numaproj/numaplane/tests/common"
)

// A cluster-scoped Numaflow controller is not managed by a NumaflowControllerRollout.
// A MonoVertex spec change must still start a progressive upgrade, and the children must
// stay unbound so that controller reconciles them.
func TestReconcileProgressiveWithoutControllerRollout(t *testing.T) {
	restConfig, numaflowClientSet, k8sClient, _, err := commontest.PrepareK8SEnvironment()
	require.NoError(t, err)
	require.NoError(t, kubernetes.SetClientSets(restConfig))

	getwd, err := os.Getwd()
	require.NoError(t, err)
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	require.NoError(t, config.GetConfigManagerInstance().LoadAllConfigs(
		func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig2")))
	config.GetConfigManagerInstance().UpdateUSDEConfig(config.USDEConfig{
		"monovertex": config.USDEResourceConfig{
			Progressive: []config.SpecField{{Path: "spec.sink.udsink.container.image"}},
		},
	})

	ctx := context.Background()
	if ctlrcommon.TestCustomMetrics == nil {
		ctlrcommon.TestCustomMetrics = metrics.RegisterCustomMetrics(logger.New())
	}

	require.NoError(t, k8sClient.DeleteAllOf(ctx, &apiv1.NumaflowControllerRollout{}, client.InNamespace(ctlrcommon.DefaultTestNamespace)))
	_ = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})

	rollout := ctlrcommon.CreateTestMVRollout(monoVertexSpec, map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}, nil)
	_ = k8sClient.Delete(ctx, rollout)
	rollout.Status.Init(rollout.Generation)
	rolloutCopy := *rollout
	require.NoError(t, k8sClient.Create(ctx, rollout))
	rollout.Status = rolloutCopy.Status
	require.NoError(t, k8sClient.Status().Update(ctx, rollout))

	reconciler := NewMonoVertexRolloutReconciler(k8sClient, scheme.Scheme, ctlrcommon.TestCustomMetrics, record.NewFakeRecorder(64))

	_, err = reconciler.reconcile(ctx, rollout, time.Now())
	require.NoError(t, err)

	promoted, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, promoted.Items, 1)
	assert.Equal(t, string(common.LabelValueUpgradePromoted), promoted.Items[0].Labels[common.LabelKeyUpgradeState])
	assertMonoVertexUnbound(t, &promoted.Items[0])

	updated := monoVertexSpec.DeepCopy()
	updated.Sink.UDSink.Container.Image = "quay.io/numaio/numaflow-java/simple-sink:updated"
	updatedRaw, err := json.Marshal(updated)
	require.NoError(t, err)
	rollout.Spec.MonoVertex.Spec.Raw = updatedRaw
	require.NoError(t, k8sClient.Update(ctx, rollout))

	// The first progressive reconcile records the promoted scale and requeues. The next one creates the trial.
	_, err = reconciler.reconcile(ctx, rollout, time.Now())
	require.NoError(t, err)
	_, err = reconciler.reconcile(ctx, rollout, time.Now())
	require.NoError(t, err)

	children, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, children.Items, 2)

	var sawPromoted, sawTrial bool
	for i := range children.Items {
		assertMonoVertexUnbound(t, &children.Items[i])
		switch children.Items[i].Labels[common.LabelKeyUpgradeState] {
		case string(common.LabelValueUpgradePromoted):
			sawPromoted = true
			assert.Equal(t, "quay.io/numaio/numaflow-java/simple-sink:stable", children.Items[i].Spec.Sink.UDSink.Container.Image)
		case string(common.LabelValueUpgradeTrial):
			sawTrial = true
			assert.Equal(t, "quay.io/numaio/numaflow-java/simple-sink:updated", children.Items[i].Spec.Sink.UDSink.Container.Image)
		}
	}
	assert.True(t, sawPromoted)
	assert.True(t, sawTrial)
	assert.Equal(t, apiv1.UpgradeStrategyProgressive, rollout.Status.UpgradeInProgress)

	rollouts := &apiv1.NumaflowControllerRolloutList{}
	require.NoError(t, k8sClient.List(ctx, rollouts))
	assert.Empty(t, rollouts.Items)
}

func assertMonoVertexUnbound(t *testing.T, monoVertex *numaflowv1.MonoVertex) {
	t.Helper()
	_, annFound := monoVertex.Annotations[common.AnnotationKeyNumaflowInstanceID]
	_, labelFound := monoVertex.Labels[common.LabelKeyControllerInstanceID]
	assert.False(t, annFound, "monovertex %s should not carry a controller instance annotation", monoVertex.Name)
	assert.False(t, labelFound, "monovertex %s should not carry a controller instance label", monoVertex.Name)
}
