package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

func verifyPipelineStatusEventually(namespace string, pipelineName string, f func(numaflowv1.PipelineSpec, numaflowv1.PipelineStatus) bool) {

	Eventually(func() bool {

		_, retrievedPipelineSpec, retrievedPipelineStatus, err := getPipelineFromK8S(namespace, pipelineName)

		return err == nil && f(retrievedPipelineSpec, retrievedPipelineStatus)
	}, testTimeout).Should(BeTrue())
}

func verifyPipelineStatusConsistently(namespace string, pipelineName string, f func(numaflowv1.PipelineSpec, numaflowv1.PipelineStatus) bool) {

	Consistently(func() bool {
		_, retrievedPipelineSpec, retrievedPipelineStatus, err := getPipelineFromK8S(namespace, pipelineName)

		return err == nil && f(retrievedPipelineSpec, retrievedPipelineStatus)
	}, 30*time.Second, testPollingInterval).Should(BeTrue())

}

func verifyPipelineRolloutDeployed(pipelineRolloutName string) {
	document("Verifying that the PipelineRollout is Deployed")
	Eventually(func() bool {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return rollout.Status.Phase == apiv1.PhaseDeployed
	}, testTimeout, testPollingInterval).Should(BeTrue())

	Eventually(func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionChildResourceDeployed)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))

}

func verifyPipelineRolloutHealthy(pipelineRolloutName string) {
	document("Verifying that the PipelineRollout Child Condition is Healthy")
	Eventually(func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionChildResourceHealthy)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))
}

func verifyPipelineRunning(namespace string, pipelineName string, numVertices int) {
	document("Verifying that the Pipeline is running")
	verifyPipelineStatusEventually(namespace, pipelineName,
		func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
			return retrievedPipelineStatus.Phase == numaflowv1.PipelinePhaseRunning
		})
	Eventually(func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineName, metav1.GetOptions{})
		return getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
	}, testTimeout).Should(Not(Equal(metav1.ConditionTrue)))

	// Get Pipeline Pods to verify they're all up
	document("Verifying that the Pipeline is ready")
	// check "vertex" Pods
	verifyPodsRunning(namespace, numVertices, getVertexLabelSelector(pipelineName))
	verifyPodsRunning(namespace, 1, getDaemonLabelSelector(pipelineName))

}

func verifyPipelinePaused(namespace string, pipelineRolloutName string, pipelineName string) {

	document("Verify that Pipeline Rollout condition is Pausing/Paused")
	Eventually(func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
	}, testTimeout).Should(Equal(metav1.ConditionTrue))

	document("Verify that Pipeline is paused and fully drained")
	verifyPipelineStatusEventually(namespace, pipelineName,
		func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
			return retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePaused && retrievedPipelineStatus.DrainedOnPause // TODO: uncomment: this needs to be working but in current Numaflow it isn't reliably
		})
	// this happens too fast to verify it:
	//verifyPodsRunning(namespace, 0, getVertexLabelSelector(pipelineName))
}

func verifyInProgressStrategy(pipelineRolloutName string, inProgressStrategy apiv1.UpgradeStrategy) {
	document("Verifying InProgressStrategy")
	Eventually(func() bool {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return rollout.Status.UpgradeInProgress == inProgressStrategy
	}, testTimeout, testPollingInterval).Should(BeTrue())
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

func getGVRForVertex() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "vertices",
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

func getPipelineFromK8S(namespace string, pipelineName string) (*unstructured.Unstructured, numaflowv1.PipelineSpec, numaflowv1.PipelineStatus, error) {

	var retrievedPipelineSpec numaflowv1.PipelineSpec
	var retrievedPipelineStatus numaflowv1.PipelineStatus

	unstruct, err := dynamicClient.Resource(getGVRForPipeline()).Namespace(namespace).Get(ctx, pipelineName, metav1.GetOptions{})
	if err != nil {
		return nil, retrievedPipelineSpec, retrievedPipelineStatus, err
	}
	retrievedPipelineSpec, err = getPipelineSpec(unstruct)
	if err != nil {
		return unstruct, retrievedPipelineSpec, retrievedPipelineStatus, err
	}

	retrievedPipelineStatus, err = getPipelineStatus(unstruct)

	if err != nil {
		return unstruct, retrievedPipelineSpec, retrievedPipelineStatus, err
	}
	return unstruct, retrievedPipelineSpec, retrievedPipelineStatus, nil
}

func getPipelineStatus(u *unstructured.Unstructured) (numaflowv1.PipelineStatus, error) {
	statusMap := u.Object["status"]
	var status numaflowv1.PipelineStatus
	err := util.StructToStruct(&statusMap, &status)
	return status, err
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

func getVertexLabelSelector(pipelineName string) string {
	return fmt.Sprintf("%s=%s,%s=%s", numaflowv1.KeyPipelineName, pipelineName, numaflowv1.KeyComponent, "vertex")
}

func getDaemonLabelSelector(pipelineName string) string {
	return fmt.Sprintf("%s=%s,%s=%s", numaflowv1.KeyPipelineName, pipelineName, numaflowv1.KeyComponent, "daemon")
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

	file, err := os.OpenFile(filepath.Join(ResourceChangesPipelineOutputPath, "pipeline_rollout.yaml"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
					updateLog := fmt.Sprintf("%s\n%v\n\n%s\n", LogSpacer, time.Now().Format(time.RFC3339Nano), string(bytes))
					_, err = file.WriteString(updateLog)
					if err != nil {
						fmt.Printf("Failed to write to log file: %v\n", err)
						return
					}
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

	file, err := os.OpenFile(filepath.Join(ResourceChangesPipelineOutputPath, "pipeline.yaml"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
					updateLog := fmt.Sprintf("%s\n%v\n\n%s\n", LogSpacer, time.Now().Format(time.RFC3339Nano), string(bytes))
					_, err = file.WriteString(updateLog)
					if err != nil {
						fmt.Printf("Failed to write to log file: %v\n", err)
						return
					}
				}
			}
		case <-stopCh:
			return
		}
	}

}

func watchVertices() {

	defer wg.Done()
	watcher, err := dynamicClient.Resource(getGVRForVertex()).Namespace(Namespace).Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		fmt.Printf("Failed to start watcher: %v\n", err)
		return
	}
	defer watcher.Stop()

	for {
		select {
		case event := <-watcher.ResultChan():
			if event.Type == watch.Modified {
				if obj, ok := event.Object.(*unstructured.Unstructured); ok {
					vtx := numaflowv1.Vertex{}
					err = util.StructToStruct(&obj, &vtx)
					if err != nil {
						fmt.Printf("Failed to convert unstruct: %v\n", err)
						return
					}
					vtx.ManagedFields = nil
					output := Output{
						APIVersion: NumaflowAPIVersion,
						Kind:       "Vertex",
						Metadata:   vtx.ObjectMeta,
						Spec:       vtx.Spec,
						Status:     vtx.Status,
					}
					bytes, _ := yaml.Marshal(output)
					updateLog := fmt.Sprintf("%s\n%v\n\n%s\n", LogSpacer, time.Now().Format(time.RFC3339Nano), string(bytes))
					fileName := filepath.Join(ResourceChangesPipelineOutputPath, "vertices", strings.Join([]string{vtx.Name, ".yaml"}, ""))
					file, err := os.OpenFile(filepath.Join(fileName), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
					if err != nil {
						fmt.Printf("Failed to open log file: %v\n", err)
						return
					}
					defer file.Close()
					_, err = file.WriteString(updateLog)
					if err != nil {
						fmt.Printf("Failed to write to log file: %v\n", err)
						return
					}
				}
			}
		case <-stopCh:
			return
		}
	}

}
