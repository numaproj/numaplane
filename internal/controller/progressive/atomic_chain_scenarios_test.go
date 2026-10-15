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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"
)

const scenarioSpecKey = "test-spec"

// TestAtomicChainRollbackScenarios covers the manual cases in
// https://github.com/numaproj/numaplane/issues/1054
func TestAtomicChainRollbackScenarios(t *testing.T) {
	restConfig, _, c, _, err := commontest.PrepareK8SEnvironment()
	require.NoError(t, err)
	require.NoError(t, kubernetes.SetClientSets(restConfig))
	ctx := context.Background()

	t.Run("bad controller then a pipeline change", func(t *testing.T) {
		ns := "scenario-pipeline-after-controller"
		require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
		addFailedController(t, ctx, c, ns)
		pipeline, promoted, trial := addPipeline(t, ctx, c, ns, "pipeline", "promoted", "trial")

		requeue, done, err := replace(ctx, c, pipeline, promoted, trial, "changed")
		require.NoError(t, err)
		assert.True(t, requeue)
		assert.False(t, done)
		assert.Equal(t, apiv1.PhasePending, pipeline.Status.Phase)
		assert.Equal(t, FailedTrialPendingMessage, pipeline.Status.Message)
		assert.Equal(t, string(common.LabelValueUpgradeTrial), childState(t, ctx, c, pipeline.GetChildGVK(), ns, trial.GetName()))

		require.NoError(t, c.Delete(ctx, controllerChild(ns, "controller-trial", common.LabelValueUpgradeTrial, "trial")))

		requeue, done, err = replace(ctx, c, pipeline, promoted, trial, "promoted")
		require.NoError(t, err)
		assert.False(t, requeue)
		assert.True(t, done)
		assert.Equal(t, string(common.LabelValueUpgradeRecyclable), childState(t, ctx, c, pipeline.GetChildGVK(), ns, trial.GetName()))
		assert.Empty(t, childrenInState(t, ctx, c, pipeline.GetChildGVK(), ns, pipeline.Name, common.LabelValueUpgradeTrial))
	})

	t.Run("only the controller changed", func(t *testing.T) {
		ns := "scenario-controller-only"
		require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
		addFailedController(t, ctx, c, ns)
		pipeline, promoted, trial := addPipeline(t, ctx, c, ns, "pipeline", "promoted", "followed-trial")

		_, _, err := replace(ctx, c, pipeline, promoted, trial, "promoted")
		require.NoError(t, err)
		assert.Equal(t, apiv1.PhasePending, pipeline.Status.Phase)
		assert.Equal(t, string(common.LabelValueUpgradeTrial), childState(t, ctx, c, pipeline.GetChildGVK(), ns, trial.GetName()))

		require.NoError(t, c.Delete(ctx, controllerChild(ns, "controller-trial", common.LabelValueUpgradeTrial, "trial")))
		_, done, err := replace(ctx, c, pipeline, promoted, trial, "promoted")
		require.NoError(t, err)
		assert.True(t, done)
		assert.Empty(t, childrenInState(t, ctx, c, pipeline.GetChildGVK(), ns, pipeline.Name, common.LabelValueUpgradeTrial))
	})

	t.Run("controller and pipeline both changed", func(t *testing.T) {
		ns := "scenario-controller-and-pipeline"
		require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
		addFailedController(t, ctx, c, ns)
		pipeline, promoted, trial := addPipeline(t, ctx, c, ns, "pipeline", "promoted", "trial")

		_, _, err := replace(ctx, c, pipeline, promoted, trial, "changed")
		require.NoError(t, err)
		assert.Equal(t, string(common.LabelValueUpgradeTrial), childState(t, ctx, c, pipeline.GetChildGVK(), ns, trial.GetName()))

		require.NoError(t, c.Delete(ctx, controllerChild(ns, "controller-trial", common.LabelValueUpgradeTrial, "trial")))
		_, _, err = replace(ctx, c, pipeline, promoted, trial, "changed")
		require.NoError(t, err)
		assert.Equal(t, string(common.LabelValueUpgradeRecyclable), childState(t, ctx, c, pipeline.GetChildGVK(), ns, trial.GetName()))
		assert.Equal(t, []string{"pipeline-2"}, childrenInState(t, ctx, c, pipeline.GetChildGVK(), ns, pipeline.Name, common.LabelValueUpgradeTrial))
	})

	t.Run("bad pipeline is replaced after the others are force-promoted", func(t *testing.T) {
		ns := "scenario-bad-pipeline"
		require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
		addForcePromotedController(t, ctx, c, ns)
		pipeline, promoted, trial := addPipeline(t, ctx, c, ns, "pipeline", "promoted", "bad")

		_, _, err := replace(ctx, c, pipeline, promoted, trial, "fixed")
		require.NoError(t, err)
		assert.Equal(t, string(common.LabelValueUpgradeRecyclable), childState(t, ctx, c, pipeline.GetChildGVK(), ns, trial.GetName()))
		assert.Equal(t, string(common.LabelValueProgressiveReplacedFailed), childReason(t, ctx, c, pipeline.GetChildGVK(), ns, trial.GetName()))
		assert.Equal(t, []string{"pipeline-2"}, childrenInState(t, ctx, c, pipeline.GetChildGVK(), ns, pipeline.Name, common.LabelValueUpgradeTrial))
	})

	t.Run("undo the bad pipeline", func(t *testing.T) {
		ns := "scenario-undo-pipeline"
		require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
		pipeline, promoted, trial := addPipeline(t, ctx, c, ns, "pipeline", "promoted", "bad")

		_, done, err := replace(ctx, c, pipeline, promoted, trial, "promoted")
		require.NoError(t, err)
		assert.True(t, done)
		assert.Equal(t, string(common.LabelValueUpgradeRecyclable), childState(t, ctx, c, pipeline.GetChildGVK(), ns, trial.GetName()))
		assert.Equal(t, []string{"pipeline-0"}, childrenInState(t, ctx, c, pipeline.GetChildGVK(), ns, pipeline.Name, common.LabelValueUpgradePromoted))
	})

	t.Run("moving the failed controller forward keeps its trial", func(t *testing.T) {
		ns := "scenario-controller-forward"
		require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
		controller, promoted, trial := addControllerRollout(t, ctx, c, ns, true)
		pipeline, pipelinePromoted, pipelineTrial := addPipeline(t, ctx, c, ns, "pipeline", "promoted", "trial")

		requeue, done, err := replace(ctx, c, controller, promoted, trial, "another")
		require.NoError(t, err)
		assert.False(t, requeue)
		assert.False(t, done)
		assert.Equal(t, string(common.LabelValueUpgradeTrial), childState(t, ctx, c, controller.GetChildGVK(), ns, trial.GetName()))
		assert.Equal(t, []string{"controller-trial"}, childrenInState(t, ctx, c, controller.GetChildGVK(), ns, controller.Name, common.LabelValueUpgradeTrial))

		_, _, err = replace(ctx, c, pipeline, pipelinePromoted, pipelineTrial, "changed")
		require.NoError(t, err)
		assert.Equal(t, apiv1.PhasePending, pipeline.Status.Phase)
		assert.Equal(t, string(common.LabelValueUpgradeTrial), childState(t, ctx, c, pipeline.GetChildGVK(), ns, pipelineTrial.GetName()))
	})

	t.Run("bad isbservice holds a pipeline and not a monovertex", func(t *testing.T) {
		ns := "scenario-bad-isb"
		require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
		addFailedISB(t, ctx, c, ns, "default")
		pipeline, promoted, trial := addPipelineOnISB(t, ctx, c, ns, "pipeline", "default", "promoted", "trial")
		monoVertex := &apiv1.MonoVertexRollout{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "mono-vertex"}}

		hold, err := ShouldHoldNewProgressive(ctx, c, pipeline)
		require.NoError(t, err)
		assert.True(t, hold)
		hold, err = ShouldHoldNewProgressive(ctx, c, monoVertex)
		require.NoError(t, err)
		assert.False(t, hold)

		_, _, err = replace(ctx, c, pipeline, promoted, trial, "changed")
		require.NoError(t, err)
		assert.Equal(t, apiv1.PhasePending, pipeline.Status.Phase)
		assert.Equal(t, string(common.LabelValueUpgradeTrial), childState(t, ctx, c, pipeline.GetChildGVK(), ns, trial.GetName()))

		require.NoError(t, c.Delete(ctx, &numaflowv1.InterStepBufferService{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "default-1"},
		}))
		_, _, err = replace(ctx, c, pipeline, promoted, trial, "changed")
		require.NoError(t, err)
		assert.Equal(t, []string{"pipeline-2"}, childrenInState(t, ctx, c, pipeline.GetChildGVK(), ns, pipeline.Name, common.LabelValueUpgradeTrial))

		unchanged, unchangedPromoted, unchangedTrial := addPipelineOnISB(t, ctx, c, ns, "unchanged", "default", "promoted", "followed-trial")
		_, done, err := replace(ctx, c, unchanged, unchangedPromoted, unchangedTrial, "promoted")
		require.NoError(t, err)
		assert.True(t, done)
		assert.Empty(t, childrenInState(t, ctx, c, unchanged.GetChildGVK(), ns, unchanged.Name, common.LabelValueUpgradeTrial))
	})

	t.Run("bad controller blocks monovertex and isbservice", func(t *testing.T) {
		ns := "scenario-controller-blocks-dependents"
		require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
		addFailedController(t, ctx, c, ns)
		monoVertex, mvPromoted, mvTrial := addMonoVertex(t, ctx, c, ns, "mono-vertex", "promoted", "trial")
		isb, isbPromoted, isbTrial := addISB(t, ctx, c, ns, "isb", "promoted", "trial")

		_, _, err := replace(ctx, c, monoVertex, mvPromoted, mvTrial, "changed")
		require.NoError(t, err)
		_, _, err = replace(ctx, c, isb, isbPromoted, isbTrial, "changed")
		require.NoError(t, err)
		assert.Equal(t, apiv1.PhasePending, monoVertex.Status.Phase)
		assert.Equal(t, apiv1.PhasePending, isb.Status.Phase)
		assert.Equal(t, string(common.LabelValueUpgradeTrial), childState(t, ctx, c, monoVertex.GetChildGVK(), ns, mvTrial.GetName()))
		assert.Equal(t, string(common.LabelValueUpgradeTrial), childState(t, ctx, c, isb.GetChildGVK(), ns, isbTrial.GetName()))

		require.NoError(t, c.Delete(ctx, controllerChild(ns, "controller-trial", common.LabelValueUpgradeTrial, "trial")))
		_, _, err = replace(ctx, c, monoVertex, mvPromoted, mvTrial, "changed")
		require.NoError(t, err)
		_, done, err := replace(ctx, c, isb, isbPromoted, isbTrial, "promoted")
		require.NoError(t, err)
		assert.Equal(t, []string{"mono-vertex-2"}, childrenInState(t, ctx, c, monoVertex.GetChildGVK(), ns, monoVertex.Name, common.LabelValueUpgradeTrial))
		assert.True(t, done)
		assert.Empty(t, childrenInState(t, ctx, c, isb.GetChildGVK(), ns, isb.Name, common.LabelValueUpgradeTrial))
	})

	t.Run("force-promote the controller", func(t *testing.T) {
		ns := "scenario-force-promote-controller"
		require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
		addFailedController(t, ctx, c, ns)
		changed, changedPromoted, changedTrial := addPipeline(t, ctx, c, ns, "changed", "promoted", "trial")
		unchanged, unchangedPromoted, unchangedTrial := addPipeline(t, ctx, c, ns, "unchanged", "promoted", "followed-trial")

		_, _, err := replace(ctx, c, changed, changedPromoted, changedTrial, "changed")
		require.NoError(t, err)
		assert.Equal(t, apiv1.PhasePending, changed.Status.Phase)
		assert.Equal(t, string(common.LabelValueUpgradeTrial), childState(t, ctx, c, changed.GetChildGVK(), ns, changedTrial.GetName()))

		trial := &apiv1.NumaflowController{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "controller-trial"}, trial))
		trial.Labels[common.LabelKeyProgressiveResultState] = string(common.LabelValueResultStateForcePromoted)
		require.NoError(t, c.Update(ctx, trial))

		_, _, err = replace(ctx, c, changed, changedPromoted, changedTrial, "changed")
		require.NoError(t, err)
		_, done, err := replace(ctx, c, unchanged, unchangedPromoted, unchangedTrial, "promoted")
		require.NoError(t, err)
		assert.Equal(t, []string{"changed-2"}, childrenInState(t, ctx, c, changed.GetChildGVK(), ns, changed.Name, common.LabelValueUpgradeTrial))
		assert.True(t, done)
		assert.Empty(t, childrenInState(t, ctx, c, unchanged.GetChildGVK(), ns, unchanged.Name, common.LabelValueUpgradeTrial))
	})
}

type markedSpecController struct {
	fakeProgressiveController
	gvk     schema.GroupVersionKind
	desired string
}

func (m markedSpecController) CreateUpgradingChildDefinition(_ context.Context, rollout ProgressiveRolloutObject, name string) (*unstructured.Unstructured, error) {
	child := &unstructured.Unstructured{}
	child.SetGroupVersionKind(m.gvk)
	child.SetNamespace(rollout.GetRolloutObjectMeta().Namespace)
	child.SetName(name)
	child.SetLabels(map[string]string{
		common.LabelKeyParentRollout: rollout.GetRolloutObjectMeta().Name,
		common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeTrial),
	})
	child.SetAnnotations(map[string]string{scenarioSpecKey: m.desired})
	child.Object["spec"] = map[string]interface{}{}
	return child, nil
}

func (m markedSpecController) CheckForDifferences(_ context.Context, _ ctlrcommon.RolloutObject, existing *unstructured.Unstructured, _ map[string]interface{}, _ map[string]interface{}, _ common.UpgradeState) (bool, error) {
	return existing.GetAnnotations()[scenarioSpecKey] != m.desired, nil
}

func (m markedSpecController) IncrementChildCount(context.Context, ctlrcommon.RolloutObject) (int32, error) {
	return 2, nil
}

func replace(ctx context.Context, c client.Client, rollout ProgressiveRolloutObject, promoted, trial *unstructured.Unstructured, desired string) (bool, bool, error) {
	return checkForUpgradeReplacement(ctx, rollout, markedSpecController{
		gvk:     rollout.GetChildGVK(),
		desired: desired,
	}, promoted, trial, c)
}

func addFailedController(t *testing.T, ctx context.Context, c client.Client, namespace string) {
	t.Helper()
	_, _, _ = addControllerRollout(t, ctx, c, namespace, true)
}

func addForcePromotedController(t *testing.T, ctx context.Context, c client.Client, namespace string) {
	t.Helper()
	controller := &apiv1.NumaflowControllerRollout{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "controller"}}
	require.NoError(t, c.Create(ctx, controller))
	trial := ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-trial", common.LabelValueUpgradeTrial, "trial")
	trial.Labels[common.LabelKeyProgressiveResultState] = string(common.LabelValueResultStateForcePromoted)
	require.NoError(t, c.Create(ctx, ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-promoted", common.LabelValueUpgradePromoted, "promoted")))
	require.NoError(t, c.Create(ctx, trial))
}

func addControllerRollout(t *testing.T, ctx context.Context, c client.Client, namespace string, failed bool) (*apiv1.NumaflowControllerRollout, *unstructured.Unstructured, *unstructured.Unstructured) {
	t.Helper()
	controller := &apiv1.NumaflowControllerRollout{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "controller"}}
	controller.Status.ProgressiveStatus.UpgradingNumaflowControllerStatus = &apiv1.UpgradingNumaflowControllerStatus{
		UpgradingChildStatus: apiv1.UpgradingChildStatus{Name: "controller-trial", AssessmentResult: apiv1.AssessmentResultFailure},
	}
	require.NoError(t, c.Create(ctx, controller))
	promoted := controllerChild(namespace, "controller-promoted", common.LabelValueUpgradePromoted, "promoted")
	trial := controllerChild(namespace, "controller-trial", common.LabelValueUpgradeTrial, "trial")
	if failed {
		labels := trial.GetLabels()
		labels[common.LabelKeyProgressiveResultState] = string(common.LabelValueResultStateFailed)
		trial.SetLabels(labels)
	}
	require.NoError(t, c.Create(ctx, promoted))
	require.NoError(t, c.Create(ctx, trial))
	return controller, promoted, trial
}

func controllerChild(namespace, name string, state common.UpgradeState, spec string) *unstructured.Unstructured {
	obj := child(apiv1.NumaflowControllerGroupVersionKind, namespace, "controller", name, state, spec)
	obj.Object["spec"] = map[string]interface{}{"instanceID": spec, "version": "1.2.3"}
	return obj
}

func addPipeline(t *testing.T, ctx context.Context, c client.Client, namespace, name, promotedSpec, trialSpec string) (*apiv1.PipelineRollout, *unstructured.Unstructured, *unstructured.Unstructured) {
	t.Helper()
	return addPipelineOnISB(t, ctx, c, namespace, name, "absent-isb", promotedSpec, trialSpec)
}

func addPipelineOnISB(t *testing.T, ctx context.Context, c client.Client, namespace, name, isbName, promotedSpec, trialSpec string) (*apiv1.PipelineRollout, *unstructured.Unstructured, *unstructured.Unstructured) {
	t.Helper()
	raw, err := json.Marshal(map[string]string{"interStepBufferServiceName": isbName})
	require.NoError(t, err)
	pipeline := &apiv1.PipelineRollout{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	pipeline.Spec.Pipeline.Spec.Raw = raw
	pipeline.Status.ProgressiveStatus.UpgradingPipelineStatus = &apiv1.UpgradingPipelineStatus{
		UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
			UpgradingChildStatus: apiv1.UpgradingChildStatus{Name: name + "-1", AssessmentResult: apiv1.AssessmentResultFailure},
		},
	}
	promoted := child(pipeline.GetChildGVK(), namespace, name, name+"-0", common.LabelValueUpgradePromoted, promotedSpec)
	trial := child(pipeline.GetChildGVK(), namespace, name, name+"-1", common.LabelValueUpgradeTrial, trialSpec)
	require.NoError(t, c.Create(ctx, promoted))
	require.NoError(t, c.Create(ctx, trial))
	return pipeline, promoted, trial
}

func addMonoVertex(t *testing.T, ctx context.Context, c client.Client, namespace, name, promotedSpec, trialSpec string) (*apiv1.MonoVertexRollout, *unstructured.Unstructured, *unstructured.Unstructured) {
	t.Helper()
	rollout := &apiv1.MonoVertexRollout{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	rollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus = &apiv1.UpgradingMonoVertexStatus{
		UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
			UpgradingChildStatus: apiv1.UpgradingChildStatus{Name: name + "-1", AssessmentResult: apiv1.AssessmentResultFailure},
		},
	}
	promoted := child(rollout.GetChildGVK(), namespace, name, name+"-0", common.LabelValueUpgradePromoted, promotedSpec)
	trial := child(rollout.GetChildGVK(), namespace, name, name+"-1", common.LabelValueUpgradeTrial, trialSpec)
	require.NoError(t, c.Create(ctx, promoted))
	require.NoError(t, c.Create(ctx, trial))
	return rollout, promoted, trial
}

func addISB(t *testing.T, ctx context.Context, c client.Client, namespace, name, promotedSpec, trialSpec string) (*apiv1.ISBServiceRollout, *unstructured.Unstructured, *unstructured.Unstructured) {
	t.Helper()
	rollout := &apiv1.ISBServiceRollout{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	rollout.Status.ProgressiveStatus.UpgradingISBServiceStatus = &apiv1.UpgradingISBServiceStatus{
		UpgradingChildStatus: apiv1.UpgradingChildStatus{Name: name + "-1", AssessmentResult: apiv1.AssessmentResultFailure},
	}
	promoted := child(rollout.GetChildGVK(), namespace, name, name+"-0", common.LabelValueUpgradePromoted, promotedSpec)
	trial := child(rollout.GetChildGVK(), namespace, name, name+"-1", common.LabelValueUpgradeTrial, trialSpec)
	require.NoError(t, c.Create(ctx, promoted))
	require.NoError(t, c.Create(ctx, trial))
	return rollout, promoted, trial
}

func addFailedISB(t *testing.T, ctx context.Context, c client.Client, namespace, name string) {
	t.Helper()
	raw, err := json.Marshal(map[string]interface{}{"jetstream": map[string]string{"version": "latest"}})
	require.NoError(t, err)
	rollout := &apiv1.ISBServiceRollout{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	rollout.Spec.InterStepBufferService.Spec.Raw = raw
	require.NoError(t, c.Create(ctx, rollout))
	failed := child(rollout.GetChildGVK(), namespace, name, name+"-1", common.LabelValueUpgradeTrial, "bad")
	labels := failed.GetLabels()
	labels[common.LabelKeyProgressiveResultState] = string(common.LabelValueResultStateFailed)
	failed.SetLabels(labels)
	require.NoError(t, c.Create(ctx, failed))
}

func child(gvk schema.GroupVersionKind, namespace, rolloutName, name string, state common.UpgradeState, spec string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	obj.SetLabels(map[string]string{
		common.LabelKeyParentRollout: rolloutName,
		common.LabelKeyUpgradeState:  string(state),
	})
	obj.SetAnnotations(map[string]string{scenarioSpecKey: spec})
	obj.Object["spec"] = map[string]interface{}{}
	return obj
}

func childState(t *testing.T, ctx context.Context, c client.Client, gvk schema.GroupVersionKind, namespace, name string) string {
	t.Helper()
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, obj))
	return obj.GetLabels()[common.LabelKeyUpgradeState]
}

func childReason(t *testing.T, ctx context.Context, c client.Client, gvk schema.GroupVersionKind, namespace, name string) string {
	t.Helper()
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, obj))
	return obj.GetLabels()[common.LabelKeyUpgradeStateReason]
}

func childrenInState(t *testing.T, ctx context.Context, c client.Client, gvk schema.GroupVersionKind, namespace, rolloutName string, state common.UpgradeState) []string {
	t.Helper()
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(gvk)
	require.NoError(t, c.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels{
		common.LabelKeyParentRollout: rolloutName,
		common.LabelKeyUpgradeState:  string(state),
	}))
	names := make([]string, 0, len(list.Items))
	for _, item := range list.Items {
		names = append(names, item.GetName())
	}
	return names
}
