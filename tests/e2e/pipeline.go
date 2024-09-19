package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/yaml"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func verifyPipelineSpec(namespace string, pipelineName string, f func(numaflowv1.PipelineSpec) bool) {

	document("verifying Pipeline Spec")
	var retrievedPipelineSpec numaflowv1.PipelineSpec
	Eventually(func() bool {
		unstruct, err := dynamicClient.Resource(getGVRForPipeline()).Namespace(namespace).Get(ctx, pipelineName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		if retrievedPipelineSpec, err = getPipelineSpec(unstruct); err != nil {
			return false
		}

		return f(retrievedPipelineSpec)
	}, testTimeout, testPollingInterval).Should(BeTrue())
}

func verifyPipelineStatus(namespace string, pipelineName string, f func(numaflowv1.PipelineSpec, kubernetes.GenericStatus) bool) {

	// document("verifying PipelineStatus")
	var retrievedPipelineSpec numaflowv1.PipelineSpec
	var retrievedPipelineStatus kubernetes.GenericStatus
	Eventually(func() bool {
		unstruct, err := dynamicClient.Resource(getGVRForPipeline()).Namespace(namespace).Get(ctx, pipelineName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		if retrievedPipelineSpec, err = getPipelineSpec(unstruct); err != nil {
			return false
		}
		if retrievedPipelineStatus, err = getNumaflowResourceStatus(unstruct); err != nil {
			return false
		}

		return f(retrievedPipelineSpec, retrievedPipelineStatus)
	}, testTimeout).Should(BeTrue())
}

func verifyPipelineRolloutReady(pipelineRolloutName string) {
	document("Verifying that the PipelineRollout is ready")

	Eventually(func() bool {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return rollout.Status.Phase == apiv1.PhaseDeployed
	}, testTimeout, testPollingInterval).Should(BeTrue())

	Eventually(func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionChildResourceDeployed)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))

	Eventually(func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionChildResourceHealthy)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))

	Eventually(func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionFalse))

}

func verifyPipelineReady(namespace string, pipelineName string, numVertices int) {
	document("Verifying that the Pipeline is running")
	verifyPipelineStatus(namespace, pipelineName,
		func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus kubernetes.GenericStatus) bool {
			return retrievedPipelineStatus.Phase == string(numaflowv1.PipelinePhaseRunning)
		})

	vertexLabelSelector := fmt.Sprintf("%s=%s,%s=%s", numaflowv1.KeyPipelineName, pipelineName, numaflowv1.KeyComponent, "vertex")
	daemonLabelSelector := fmt.Sprintf("%s=%s,%s=%s", numaflowv1.KeyPipelineName, pipelineName, numaflowv1.KeyComponent, "daemon")

	// Get Pipeline Pods to verify they're all up
	document("Verifying that the Pipeline is ready")
	// check "vertex" Pods
	verifyPodsRunning(namespace, numVertices, vertexLabelSelector)
	verifyPodsRunning(namespace, 1, daemonLabelSelector)

}

// Get PipelineSpec from Unstructured type
func getPipelineSpec(u *unstructured.Unstructured) (numaflowv1.PipelineSpec, error) {
	specMap := u.Object["spec"]
	var pipelineSpec numaflowv1.PipelineSpec
	err := util.StructToStruct(&specMap, &pipelineSpec)
	return pipelineSpec, err
}

func getGVRForPipeline() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "pipelines",
	}
}

func updatePipelineRolloutInK8S(namespace string, name string, f func(apiv1.PipelineRollout) (apiv1.PipelineRollout, error)) {
	document("updating PipelineRollout")
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rollout, err := pipelineRolloutClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		*rollout, err = f(*rollout)
		if err != nil {
			return err
		}
		_, err = pipelineRolloutClient.Update(ctx, rollout, metav1.UpdateOptions{})
		return err
	})
	Expect(err).ShouldNot(HaveOccurred())
}

func updatePipelineSpecInK8S(namespace string, pipelineName string, f func(numaflowv1.PipelineSpec) (numaflowv1.PipelineSpec, error)) {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {

		unstruct, err := dynamicClient.Resource(getGVRForPipeline()).Namespace(namespace).Get(ctx, pipelineName, metav1.GetOptions{})
		Expect(err).ShouldNot(HaveOccurred())
		retrievedPipeline := unstruct

		// modify Pipeline Spec to verify it gets auto-healed
		err = updatePipelineSpec(retrievedPipeline, f)
		Expect(err).ShouldNot(HaveOccurred())
		_, err = dynamicClient.Resource(getGVRForPipeline()).Namespace(namespace).Update(ctx, retrievedPipeline, metav1.UpdateOptions{})
		return err
	})
	Expect(err).ShouldNot(HaveOccurred())
}

// Take a Pipeline Unstructured type and update the PipelineSpec in some way
func updatePipelineSpec(u *unstructured.Unstructured, f func(numaflowv1.PipelineSpec) (numaflowv1.PipelineSpec, error)) error {

	// get PipelineSpec from unstructured object
	specMap := u.Object["spec"]
	var pipelineSpec numaflowv1.PipelineSpec
	err := util.StructToStruct(&specMap, &pipelineSpec)
	if err != nil {
		return err
	}
	// update PipelineSpec
	pipelineSpec, err = f(pipelineSpec)
	if err != nil {
		return err
	}
	var newMap map[string]interface{}
	err = util.StructToStruct(&pipelineSpec, &newMap)
	if err != nil {
		return err
	}
	u.Object["spec"] = newMap
	return nil
}

// build watcher functions for both Pipeline and PipelineRollout
func watchPipelineRollout() {

	defer wg.Done()
	watcher, err := pipelineRolloutClient.Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		fmt.Printf("Failed to start watcher: %v\n", err)
		return
	}
	defer watcher.Stop()

	file, err := os.OpenFile(filepath.Join(ResourceChangesOutputPath, "pipeline_rollout.yaml"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Failed to open log file: %v\n", err)
		return
	}
	defer file.Close()

	for {
		select {
		case event := <-watcher.ResultChan():
			if event.Type == watch.Modified {
				if rollout, ok := event.Object.(*apiv1.PipelineRollout); ok {
					rollout.ManagedFields = nil
					rl := Output{
						APIVersion: NumaplaneAPIVersion,
						Kind:       "PipelineRollout",
						Metadata:   rollout.ObjectMeta,
						Spec:       rollout.Spec,
						Status:     rollout.Status,
					}
					bytes, _ := yaml.Marshal(rl)
					updateLog := fmt.Sprintf("PipelineRollout update time: %v\n%s\n", time.Now().Format(time.RFC3339), string(bytes))
					file.WriteString(updateLog)
				}
			}
		case <-stopCh:
			return
		}
	}
}

func watchPipeline() {

	defer wg.Done()
	watcher, err := dynamicClient.Resource(getGVRForPipeline()).Namespace(Namespace).Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		fmt.Printf("Failed to start watcher: %v\n", err)
		return
	}
	defer watcher.Stop()

	file, err := os.OpenFile(filepath.Join(ResourceChangesOutputPath, "pipeline.yaml"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Failed to open log file: %v\n", err)
		return
	}
	defer file.Close()

	for {
		select {
		case event := <-watcher.ResultChan():
			if event.Type == watch.Modified {
				if obj, ok := event.Object.(*unstructured.Unstructured); ok {
					pl := numaflowv1.Pipeline{}
					err = util.StructToStruct(&obj, &pl)
					if err != nil {
						fmt.Printf("Failed to convert unstruct: %v\n", err)
						return
					}
					pl.ManagedFields = nil
					output := Output{
						APIVersion: NumaflowAPIVersion,
						Kind:       "Pipeline",
						Metadata:   pl.ObjectMeta,
						Spec:       pl.Spec,
						Status:     pl.Status,
					}
					bytes, _ := yaml.Marshal(output)
					updateLog := fmt.Sprintf("Pipeline update time: %v\nSpec: %s\n", time.Now().Format(time.RFC3339), string(bytes))
					file.WriteString(updateLog)
				}
			}
		case <-stopCh:
			return
		}
	}

}
