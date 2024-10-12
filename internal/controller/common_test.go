package controller

import (
	"context"
	"testing"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	numaflowversioned "github.com/numaproj/numaflow/pkg/client/clientset/versioned"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sclientgo "k8s.io/client-go/kubernetes"
)

func createISBSvcInK8S(ctx context.Context, t *testing.T, numaflowClientSet *numaflowversioned.Clientset, isbsvc *numaflowv1.InterStepBufferService) {
	updatedISBSvc, err := numaflowClientSet.NumaflowV1alpha1().InterStepBufferServices(defaultNamespace).Create(ctx, isbsvc, metav1.CreateOptions{})
	assert.NoError(t, err)
	// update Status subresource
	updatedISBSvc.Status = isbsvc.Status
	_, err = numaflowClientSet.NumaflowV1alpha1().InterStepBufferServices(defaultNamespace).UpdateStatus(ctx, updatedISBSvc, metav1.UpdateOptions{})
	assert.NoError(t, err)
}

func createStatefulSetInK8S(ctx context.Context, t *testing.T, k8sClientSet *k8sclientgo.Clientset, statefulSet *appsv1.StatefulSet) {
	ss, err := k8sClientSet.AppsV1().StatefulSets(defaultNamespace).Create(ctx, statefulSet, metav1.CreateOptions{})
	assert.NoError(t, err)
	// update Status subresource
	ss.Status = statefulSet.Status
	_, err = k8sClientSet.AppsV1().StatefulSets(defaultNamespace).UpdateStatus(ctx, ss, metav1.UpdateOptions{})
	assert.NoError(t, err)
}

func createPipelineInK8S(ctx context.Context, t *testing.T, numaflowClientSet *numaflowversioned.Clientset, pipeline *numaflowv1.Pipeline) {
	updatedPipeline, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).Create(ctx, pipeline, metav1.CreateOptions{})
	assert.NoError(t, err)
	updatedPipeline.Status = pipeline.Status

	// updating the Status subresource is a separate operation
	_, err = numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).UpdateStatus(ctx, updatedPipeline, metav1.UpdateOptions{})
	assert.NoError(t, err)
}
