package controller

import (
	"context"
	"os"
	"testing"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	numaflowversioned "github.com/numaproj/numaflow/pkg/client/clientset/versioned"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	k8sclientgo "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func createPipelineRolloutInK8S(ctx context.Context, t *testing.T, numaplaneClient client.Client, pipelineRollout *apiv1.PipelineRollout) {
	err := numaplaneClient.Create(ctx, pipelineRollout)
	assert.NoError(t, err)
	err = numaplaneClient.Status().Update(ctx, pipelineRollout)
	assert.NoError(t, err)
}

func createPipelineInK8S(ctx context.Context, t *testing.T, numaflowClientSet *numaflowversioned.Clientset, pipeline *numaflowv1.Pipeline) {
	resultPipeline, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).Create(ctx, pipeline, metav1.CreateOptions{})
	assert.NoError(t, err)
	resultPipeline.Status = pipeline.Status

	// updating the Status subresource is a separate operation
	_, err = numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).UpdateStatus(ctx, resultPipeline, metav1.UpdateOptions{})
	assert.NoError(t, err)
}

func createISBSvcInK8S(ctx context.Context, t *testing.T, numaflowClientSet *numaflowversioned.Clientset, isbsvc *numaflowv1.InterStepBufferService) {
	resultISBSvc, err := numaflowClientSet.NumaflowV1alpha1().InterStepBufferServices(defaultNamespace).Create(ctx, isbsvc, metav1.CreateOptions{})
	assert.NoError(t, err)
	// update Status subresource
	resultISBSvc.Status = isbsvc.Status
	_, err = numaflowClientSet.NumaflowV1alpha1().InterStepBufferServices(defaultNamespace).UpdateStatus(ctx, resultISBSvc, metav1.UpdateOptions{})
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

func createDeploymentInK8S(ctx context.Context, t *testing.T, k8sClientSet *k8sclientgo.Clientset, deployment *appsv1.Deployment) {
	resultDeployment, err := k8sClientSet.AppsV1().Deployments(defaultNamespace).Create(ctx, deployment, metav1.CreateOptions{})
	assert.NoError(t, err)
	resultDeployment.Status = deployment.Status
	_, err = k8sClientSet.AppsV1().Deployments(defaultNamespace).UpdateStatus(ctx, resultDeployment, metav1.UpdateOptions{})
	assert.NoError(t, err)
}

func createDefaultPipelineOfPhase(phase numaflowv1.PipelinePhase) *numaflowv1.Pipeline {
	return &numaflowv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:              defaultPipelineName,
			Namespace:         defaultNamespace,
			UID:               "some-uid",
			CreationTimestamp: metav1.NewTime(time.Now()),
			Generation:        1,
			Labels: map[string]string{
				common.LabelKeyISBServiceNameForPipeline: defaultISBSvcRolloutName,
				common.LabelKeyParentRollout:             defaultPipelineRolloutName},
		},
		Spec: numaflowv1.PipelineSpec{
			InterStepBufferServiceName: defaultISBSvcRolloutName,
		},
		Status: numaflowv1.PipelineStatus{
			Phase: phase,
		},
	}
}

func getNumaflowControllerDefinitions(definitionsFile string) (*config.NumaflowControllerDefinitionConfig, error) {
	// Read definitions config file
	configData, err := os.ReadFile(definitionsFile)
	if err != nil {
		return nil, err
	}
	var controllerConfig config.NumaflowControllerDefinitionConfig
	err = yaml.Unmarshal(configData, &controllerConfig)
	if err != nil {
		return nil, err
	}

	return &controllerConfig, nil
}
