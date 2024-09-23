package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"

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

func verifyPipelinePaused(namespace string, pipelineRolloutName string, pipelineName string, ppnd bool) {
	/*if ppnd {
		document("Verify that in-progress-strategy gets set to PPND")
		verifyInProgressStrategy("pause-and-drain")
	}*/

	document("Verify that Pipeline Rollout condition is Pausing/Paused")
	Eventually(func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
	}, testTimeout).Should(Equal(metav1.ConditionTrue))

	document("Verify that Pipeline is paused and fully drained")
	verifyPipelineStatusEventually(Namespace, pipelineName,
		func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
			return retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePaused && retrievedPipelineStatus.DrainedOnPause
		})
	verifyPodsRunning(namespace, 0, getVertexLabelSelector(pipelineName))
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
