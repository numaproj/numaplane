package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/retry"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func GetGVRForISBService() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "interstepbufferservices",
	}
}

func GetPromotedISBService(namespace, isbServiceRolloutName string) (*unstructured.Unstructured, error) {
	return getChildResource(GetGVRForISBService(), namespace, isbServiceRolloutName)
}

// Get ISBServiceSpec from Unstructured type
func GetISBServiceSpec(u *unstructured.Unstructured) (numaflowv1.InterStepBufferServiceSpec, error) {
	specMap := u.Object["spec"]
	var isbServiceSpec numaflowv1.InterStepBufferServiceSpec
	err := util.StructToStruct(&specMap, &isbServiceSpec)
	return isbServiceSpec, err
}

func GetISBServiceName(namespace, isbServiceRolloutName string) (string, error) {
	isbsvc, err := GetPromotedISBService(namespace, isbServiceRolloutName)
	if err != nil {
		return "", err
	}
	return isbsvc.GetName(), nil
}

func VerifyISBServiceSpec(namespace string, isbServiceRolloutName string, f func(numaflowv1.InterStepBufferServiceSpec) bool) {
	var retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec
	CheckEventually("verifying ISBService Spec", func() bool {
		unstruct, err := GetPromotedISBService(namespace, isbServiceRolloutName)
		if err != nil {
			return false
		}
		if retrievedISBServiceSpec, err = GetISBServiceSpec(unstruct); err != nil {
			return false
		}

		return f(retrievedISBServiceSpec)
	}).WithTimeout(8 * time.Minute).Should(BeTrue())
}

func VerifyISBSvcRolloutReady(isbServiceRolloutName string) {
	By("Verifying that the ISBServiceRollout is ready")

	CheckEventually("verify ISBServiceRollout is deployed", func() bool {
		rollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
		return rollout.Status.Phase == apiv1.PhaseDeployed
	}).Should(BeTrue())

	CheckEventually("verify ISBServiceRollout child resource is deployed", func() metav1.ConditionStatus {
		rollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceDeployed)
	}).Should(Equal(metav1.ConditionTrue))

	CheckEventually("verify ISBServiceRollout child resource is healthy", func() metav1.ConditionStatus {
		rollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceHealthy)
	}).WithTimeout(4 * time.Minute).Should(Equal(metav1.ConditionTrue))

	if UpgradeStrategy == config.PPNDStrategyID {
		CheckEventually("Verifying that the ISBServiceRollout PausingPipelines condition is as expected", func() metav1.ConditionStatus {
			rollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPausingPipelines)
		}).Should(Or(Equal(metav1.ConditionFalse), Equal(metav1.ConditionUnknown)))
	}

}

func VerifyISBSvcReady(namespace string, isbServiceRolloutName string, nodeSize int) {

	var isbsvcName string
	CheckEventually("Verifying that the ISBService exists", func() error {
		unstruct, err := GetPromotedISBService(namespace, isbServiceRolloutName)
		if err == nil {
			isbsvcName = unstruct.GetName()
		}
		return err
	}).Should(Succeed())

	// TODO: eventually we can use ISBServiceRollout.Status.Conditions(ChildResourcesHealthy) to get this instead

	CheckEventually("Verifying that the StatefulSet exists and is ready", func() bool {
		statefulSet, _ := kubeClient.AppsV1().StatefulSets(namespace).Get(ctx, fmt.Sprintf("isbsvc-%s-js", isbsvcName), metav1.GetOptions{})
		return statefulSet != nil && statefulSet.Generation == statefulSet.Status.ObservedGeneration && statefulSet.Status.UpdatedReplicas == int32(nodeSize)
	}).Should(BeTrue())

	CheckEventually("Verifying that the StatefulSet Pods are in Running phase", func() bool {
		podList, _ := kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", numaflowv1.KeyISBSvcName, isbsvcName)})
		podsInRunning := 0
		if podList != nil {
			for _, pod := range podList.Items {
				if pod.Status.Phase == corev1.PodRunning {
					podsInRunning += 1
				}
			}
		}
		return podsInRunning == nodeSize
	}).Should(BeTrue())
}

func UpdateISBServiceRolloutInK8S(name string, f func(apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error)) {
	By("updating ISBServiceRollout")
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rollout, err := isbServiceRolloutClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		*rollout, err = f(*rollout)
		if err != nil {
			return err
		}
		_, err = isbServiceRolloutClient.Update(ctx, rollout, metav1.UpdateOptions{})
		return err
	})
	Expect(err).ShouldNot(HaveOccurred())
}

func VerifyInProgressStrategyISBService(namespace string, isbsvcRolloutName string, inProgressStrategy apiv1.UpgradeStrategy) {
	CheckEventually("Verifying InProgressStrategy for ISBService", func() bool {
		rollout, _ := isbServiceRolloutClient.Get(ctx, isbsvcRolloutName, metav1.GetOptions{})
		return rollout.Status.UpgradeInProgress == inProgressStrategy
	}).Should(BeTrue())
}

func watchISBServiceRollout() {

	watchResourceType(func() (watch.Interface, error) {
		watcher, err := isbServiceRolloutClient.Watch(context.Background(), metav1.ListOptions{})
		return watcher, err
	}, func(o runtime.Object) Output {
		if rollout, ok := o.(*apiv1.ISBServiceRollout); ok {
			rollout.ManagedFields = nil
			return Output{
				APIVersion: NumaplaneAPIVersion,
				Kind:       "ISBServiceRollout",
				Metadata:   rollout.ObjectMeta,
				Spec:       rollout.Spec,
				Status:     rollout.Status,
			}
		}
		return Output{}
	})

}

func watchISBService() {

	watchResourceType(func() (watch.Interface, error) {
		watcher, err := dynamicClient.Resource(GetGVRForISBService()).Namespace(Namespace).Watch(context.Background(), metav1.ListOptions{})
		return watcher, err
	}, func(o runtime.Object) Output {
		if obj, ok := o.(*unstructured.Unstructured); ok {
			isbsvc := numaflowv1.InterStepBufferService{}
			err := util.StructToStruct(&obj, &isbsvc)
			if err != nil {
				fmt.Printf("Failed to convert unstruct: %v\n", err)
				return Output{}
			}
			isbsvc.ManagedFields = nil
			return Output{
				APIVersion: NumaflowAPIVersion,
				Kind:       "InterStepBufferService",
				Metadata:   isbsvc.ObjectMeta,
				Spec:       isbsvc.Spec,
				Status:     isbsvc.Status,
			}
		}
		return Output{}
	})

}

func watchStatefulSet() {

	watchResourceType(func() (watch.Interface, error) {
		watcher, err := kubeClient.AppsV1().StatefulSets(Namespace).Watch(context.Background(), metav1.ListOptions{LabelSelector: "app.kubernetes.io/part-of=numaflow"})
		return watcher, err
	}, func(o runtime.Object) Output {
		if sts, ok := o.(*appsv1.StatefulSet); ok {
			sts.ManagedFields = nil
			return Output{
				APIVersion: "v1",
				Kind:       "StatefulSet",
				Metadata:   sts.ObjectMeta,
				Spec:       sts.Spec,
				Status:     sts.Status,
			}
		}
		return Output{}
	})

}

func startISBServiceRolloutWatches() {
	wg.Add(1)
	go watchISBServiceRollout()

	wg.Add(1)
	go watchISBService()

	wg.Add(1)
	go watchStatefulSet()
}

// shared functions

// create an ISBServiceRollout of a given version and name and make sure it's running
func CreateISBServiceRollout(name string, isbServiceSpec numaflowv1.InterStepBufferServiceSpec) {

	isbServiceRolloutSpec := createISBServiceRolloutSpec(name, Namespace, isbServiceSpec)
	_, err := isbServiceRolloutClient.Create(ctx, isbServiceRolloutSpec, metav1.CreateOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	CheckEventually("Verifying that the ISBServiceRollout was created", func() error {
		_, err := isbServiceRolloutClient.Get(ctx, name, metav1.GetOptions{})
		return err
	}).Should(Succeed())

	VerifyISBSvcRolloutReady(name)

	VerifyISBSvcReady(Namespace, name, 3)

}

func createISBServiceRolloutSpec(name, namespace string, isbServiceSpec numaflowv1.InterStepBufferServiceSpec) *apiv1.ISBServiceRollout {

	rawSpec, err := json.Marshal(isbServiceSpec)
	Expect(err).ShouldNot(HaveOccurred())

	isbServiceRollout := &apiv1.ISBServiceRollout{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "numaplane.numaproj.io/v1alpha1",
			Kind:       "ISBServiceRollout",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: apiv1.ISBServiceRolloutSpec{
			InterStepBufferService: apiv1.InterStepBufferService{
				Spec: runtime.RawExtension{
					Raw: rawSpec,
				},
			},
		},
	}

	return isbServiceRollout

}

// delete ISBServiceRollout and verify deletion
func DeleteISBServiceRollout(name string) {
	By("Deleting ISBServiceRollout")
	err := isbServiceRolloutClient.Delete(ctx, name, metav1.DeleteOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	CheckEventually("Verifying ISBServiceRollout deletion", func() bool {
		_, err := isbServiceRolloutClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if !errors.IsNotFound(err) {
				Fail("An unexpected error occurred when fetching the ISBServiceRollout: " + err.Error())
			}
			return false
		}
		return true
	}).WithTimeout(TestTimeout).Should(BeFalse(), "The ISBServiceRollout should have been deleted but it was found.")

	CheckEventually("Verifying ISBService deletion", func() bool {
		list, err := dynamicClient.Resource(GetGVRForISBService()).Namespace(Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false
		}
		if len(list.Items) == 0 {
			return true
		}
		return false
	}).WithTimeout(TestTimeout).Should(BeTrue(), "The ISBService should have been deleted but it was found.")
}

// pipelineRolloutNames is an array of pipelinerollout names that are checked to see if it is pausing or not after update
// newSpec is the updated spec of the ISBService defined in the rollout
// verifySpecFunc is passed to the verifyISBServiceSpec func which verifies the ISBService spec defined in the updated rollout
// matches what we expect
// dataLossFieldChanged determines if the change of spec incurs a potential data loss or not, as defined in the USDE config: https://github.com/numaproj/numaplane/blob/main/config/manager/usde-config.yaml
// recreateFieldChanged determines if the change of spec should result in a recreation of InterstepBufferService and its underlying pipelines - also defined in the USDE config
// pipelineIsFailed informs us if any dependent pipelines are currently failed and to not check if they are running
func UpdateISBServiceRollout(
	isbServiceRolloutName string,
	pipelineRollouts []PipelineRolloutInfo,
	newSpec numaflowv1.InterStepBufferServiceSpec,
	verifySpecFunc func(numaflowv1.InterStepBufferServiceSpec) bool,
	dataLossFieldChanged bool,
	recreateFieldChanged bool) {

	rawSpec, err := json.Marshal(newSpec)
	Expect(err).ShouldNot(HaveOccurred())

	// get name of isbservice and name of pipeline before the change so we can check them after the change to see if they're the same or if they've changed
	originalISBServiceName, err := GetISBServiceName(Namespace, isbServiceRolloutName)
	Expect(err).ShouldNot(HaveOccurred())

	// need to iterate over rollout names
	originalPipelineNames := make(map[string]string)
	for _, rolloutInfo := range pipelineRollouts {
		originalPipelineName, err := GetPipelineName(Namespace, rolloutInfo.PipelineRolloutName)
		Expect(err).ShouldNot(HaveOccurred())

		originalPipelineNames[rolloutInfo.PipelineRolloutName] = originalPipelineName
	}

	UpdateISBServiceRolloutInK8S(isbServiceRolloutName, func(rollout apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
		rollout.Spec.InterStepBufferService.Spec.Raw = rawSpec
		return rollout, nil
	})

	// both "dataLoss" fields and "recreate" fields have risk of data loss
	dataLossRisk := dataLossFieldChanged || recreateFieldChanged
	if UpgradeStrategy == config.PPNDStrategyID {

		verifyNotPausing := func() bool {
			isbRollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			isbCondStatus := getRolloutConditionStatus(isbRollout.Status.Conditions, apiv1.ConditionPausingPipelines)
			for _, rolloutInfo := range pipelineRollouts {
				_, _, retrievedPipelineStatus, err := GetPipelineSpecAndStatus(Namespace, rolloutInfo.PipelineRolloutName)
				if err != nil {
					return false
				}
				if retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePaused {
					return false
				}
				plRollout, _ := pipelineRolloutClient.Get(ctx, rolloutInfo.PipelineRolloutName, metav1.GetOptions{})
				plCondStatus := getRolloutConditionStatus(plRollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
				if isbCondStatus == metav1.ConditionTrue || plCondStatus == metav1.ConditionTrue {
					return false
				}
				if isbRollout.Status.UpgradeInProgress != apiv1.UpgradeStrategyNoOp || plRollout.Status.UpgradeInProgress != apiv1.UpgradeStrategyNoOp {
					return false
				}
			}
			return true
		}

		if dataLossRisk {

			By("Verify that in-progress-strategy gets set to PPND")
			VerifyInProgressStrategyISBService(Namespace, isbServiceRolloutName, apiv1.UpgradeStrategyPPND)
			// if we expect the pipeline to be healthy after update
			for _, rolloutInfo := range pipelineRollouts {
				if !rolloutInfo.PipelineIsFailed {
					VerifyInProgressStrategy(rolloutInfo.PipelineRolloutName, apiv1.UpgradeStrategyPPND)
					VerifyPipelinePaused(Namespace, rolloutInfo.PipelineRolloutName)
				}
			}

			CheckEventually("Verify that the pipelines are unpaused by checking the PPND conditions on ISBService Rollout and PipelineRollout", verifyNotPausing).Should(BeTrue())

		} else {
			CheckConsistently("Verify that dependent Pipeline is not paused when an update to ISBService not requiring pause is made", verifyNotPausing).WithTimeout(15 * time.Second).Should(BeTrue())
		}
	}

	VerifyISBServiceSpec(Namespace, isbServiceRolloutName, verifySpecFunc)

	VerifyISBSvcRolloutReady(isbServiceRolloutName)
	VerifyISBSvcReady(Namespace, isbServiceRolloutName, 3)

	for _, rolloutInfo := range pipelineRollouts {
		VerifyInProgressStrategy(rolloutInfo.PipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		// check that pipeline will be failed if we expect it to be
		if rolloutInfo.PipelineIsFailed {
			VerifyPipelineFailed(Namespace, rolloutInfo.PipelineRolloutName)
		} else {
			VerifyPipelineRunning(Namespace, rolloutInfo.PipelineRolloutName)
		}
	}

	By("getting new isbservice name")
	newISBServiceName, err := GetISBServiceName(Namespace, isbServiceRolloutName)
	Expect(err).ShouldNot(HaveOccurred())

	for _, rolloutInfo := range pipelineRollouts {

		rolloutName := rolloutInfo.PipelineRolloutName

		By("getting new pipeline name")
		newPipelineName, err := GetPipelineName(Namespace, rolloutName)
		Expect(err).ShouldNot(HaveOccurred())

		if recreateFieldChanged || (dataLossFieldChanged && UpgradeStrategy == config.ProgressiveStrategyID) {
			// make sure the names of isbsvc and pipeline have changed
			By(fmt.Sprintf("verifying new isbservice name is different from original %s and new pipeline name is different from original %s", originalISBServiceName, originalPipelineNames[rolloutName]))
			Expect(originalISBServiceName != newISBServiceName).To(BeTrue())
			Expect(originalPipelineNames[rolloutName] != newPipelineName).To(BeTrue())
		} else {
			// make sure the names of isbsvc and pipeline have not changed
			By(fmt.Sprintf("verifying new isbservice name matches original %s and new pipeline name matches original %s", originalISBServiceName, originalPipelineNames[rolloutName]))
			Expect(originalISBServiceName == newISBServiceName).To(BeTrue())
			Expect(originalPipelineNames[rolloutName] == newPipelineName).To(BeTrue())
		}

	}

}
