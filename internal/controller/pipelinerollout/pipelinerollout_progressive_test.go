package pipelinerollout

import (
	"context"
	"fmt"
	"testing"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// TestAssessVertexReplicaReadiness checks that short vertices keep their replica failure reasons.
func TestAssessVertexReplicaReadiness(t *testing.T) {
	ready := vertexWithReplicas("in", 4, 4)
	shortIn := vertexWithReplicas("in", 4, 2)
	shortCat := vertexWithReplicas("cat", 3, 0)
	shortOut := vertexWithReplicas("out", 1, 0)

	tests := []struct {
		name            string
		vertices        []unstructured.Unstructured
		expectedReady   bool
		expectedReasons []string
		expectErr       bool
	}{
		{
			name:          "no vertices",
			vertices:      nil,
			expectedReady: true,
		},
		{
			name:          "all vertices ready",
			vertices:      []unstructured.Unstructured{ready, vertexWithReplicas("out", 1, 1)},
			expectedReady: true,
		},
		{
			name:          "one vertex short on replicas",
			vertices:      []unstructured.Unstructured{shortIn},
			expectedReady: false,
			expectedReasons: []string{
				"readyReplicas=2 is less than desiredReplicas=4 (vertex: in)",
			},
		},
		{
			name: "reasons accumulate across vertices and skip ready ones",
			vertices: []unstructured.Unstructured{
				shortIn,
				vertexWithReplicas("udf", 2, 2),
				shortCat,
				shortOut,
			},
			expectedReady: false,
			expectedReasons: []string{
				"readyReplicas=2 is less than desiredReplicas=4 (vertex: in)",
				"readyReplicas=0 is less than desiredReplicas=3 (vertex: cat)",
				"readyReplicas=0 is less than desiredReplicas=1 (vertex: out)",
			},
		},
		{
			name: "invalid replica status returns an error",
			vertices: []unstructured.Unstructured{{
				Object: map[string]any{
					"metadata": map[string]any{"name": "in"},
					"status":   map[string]any{"desiredReplicas": "not-a-number"},
				},
			}},
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ready, reasons, err := assessVertexReplicaReadiness(tc.vertices)
			if tc.expectErr {
				require.Error(t, err)
				assert.False(t, ready)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expectedReady, ready)
			assert.Equal(t, tc.expectedReasons, reasons)
		})
	}
}

// TestAssessUpgradingChildRecordsVertexReplicaFailureReasons confirms a failed Pipeline trial
// stores each short vertex on status.progressiveStatus.upgradingPipelineStatus.failureReasons.
func TestAssessUpgradingChildRecordsVertexReplicaFailureReasons(t *testing.T) {
	const (
		namespace    = "test"
		pipelineName = "pipeline-1"
	)
	ctx := context.Background()
	openWindow := config.AssessmentSchedule{End: time.Hour, Period: time.Minute}

	t.Run("short vertices are recorded and other pipelines are ignored", func(t *testing.T) {
		withVertexClient(t,
			liveVertex(namespace, pipelineName, "in", 4, 2),
			liveVertex(namespace, pipelineName, "udf", 2, 2),
			liveVertex(namespace, pipelineName, "out", 1, 0),
			liveVertex(namespace, "other-pipeline", "other-in", 5, 0),
		)

		rollout := upgradingPipelineRollout(namespace, pipelineName)
		result, _, err := (&PipelineRolloutReconciler{}).AssessUpgradingChild(ctx, rollout, runningPipeline(namespace, pipelineName), openWindow)

		require.NoError(t, err)
		assert.Equal(t, apiv1.AssessmentResultUnknown, result)
		assert.Equal(t, []string{
			"readyReplicas=2 is less than desiredReplicas=4 (vertex: in)",
			"readyReplicas=0 is less than desiredReplicas=1 (vertex: out)",
		}, rollout.Status.ProgressiveStatus.UpgradingPipelineStatus.FailureReasons)
	})

	t.Run("a vertex list error is returned", func(t *testing.T) {
		client := withVertexClient(t)
		client.PrependReactor("list", numaflowv1.VertexGroupVersionResource.Resource, func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, fmt.Errorf("list vertices failed")
		})

		rollout := upgradingPipelineRollout(namespace, pipelineName)
		result, _, err := (&PipelineRolloutReconciler{}).AssessUpgradingChild(ctx, rollout, runningPipeline(namespace, pipelineName), openWindow)

		require.Error(t, err)
		assert.Equal(t, apiv1.AssessmentResultUnknown, result)
		assert.Empty(t, rollout.Status.ProgressiveStatus.UpgradingPipelineStatus.FailureReasons)
	})
}

func withVertexClient(t *testing.T, vertices ...*unstructured.Unstructured) *fake.FakeDynamicClient {
	t.Helper()
	scheme := runtime.NewScheme()
	gvk := numaflowv1.VertexGroupVersionKind
	scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(gvk.GroupVersion().WithKind("VertexList"), &unstructured.UnstructuredList{})
	// The fake client guesses the plural "vertexs". Store under the real "vertices" resource that assessment lists.
	client := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		numaflowv1.VertexGroupVersionResource: "VertexList",
	})
	for _, vertex := range vertices {
		require.NoError(t, client.Tracker().Create(numaflowv1.VertexGroupVersionResource, vertex, vertex.GetNamespace()))
	}

	previous := kubernetes.DynamicClient
	t.Cleanup(func() { kubernetes.DynamicClient = previous })
	kubernetes.DynamicClient = client
	return client
}

func upgradingPipelineRollout(namespace, pipelineName string) *apiv1.PipelineRollout {
	return &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "pipeline", Namespace: namespace},
		Status: apiv1.PipelineRolloutStatus{
			ProgressiveStatus: apiv1.PipelineProgressiveStatus{
				UpgradingPipelineStatus: &apiv1.UpgradingPipelineStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{
							Name:                     pipelineName,
							BasicAssessmentStartTime: &metav1.Time{Time: time.Now()},
						},
					},
				},
			},
		},
	}
}

func runningPipeline(namespace, name string) *unstructured.Unstructured {
	pipeline := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{"phase": "Running"},
	}}
	pipeline.SetNamespace(namespace)
	pipeline.SetName(name)
	return pipeline
}

func liveVertex(namespace, pipelineName, name string, desired, ready int64) *unstructured.Unstructured {
	vertex := vertexWithReplicas(name, desired, ready)
	vertex.SetNamespace(namespace)
	vertex.SetLabels(map[string]string{
		common.LabelKeyNumaflowPipelineName: pipelineName,
	})
	vertex.SetGroupVersionKind(numaflowv1.VertexGroupVersionKind)
	return &vertex
}

// vertexWithReplicas builds a Vertex whose status reports the given replica counts.
func vertexWithReplicas(name string, desired, ready int64) unstructured.Unstructured {
	vertex := unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{
			"desiredReplicas": desired,
			"readyReplicas":   ready,
		},
	}}
	vertex.SetName(name)
	return vertex
}
