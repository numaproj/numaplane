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
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/retry"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	"github.com/numaproj/numaplane/internal/common"
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

func GetUpgradingISBServices(namespace, isbServiceRolloutName string) (*unstructured.UnstructuredList, error) {
	return GetChildrenOfUpgradeStrategy(GetGVRForISBService(), namespace, isbServiceRolloutName, common.LabelValueUpgradeTrial)

}

func GetPromotedISBServiceSpecAndStatus(namespace string, isbsvcRolloutName string) (*unstructured.Unstructured, numaflowv1.InterStepBufferServiceSpec, numaflowv1.InterStepBufferServiceStatus, error) {

	var retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec
	var retrievedISBServiceStatus numaflowv1.InterStepBufferServiceStatus

	unstruct, err := GetPromotedISBService(namespace, isbsvcRolloutName)
	if err != nil {
		return nil, retrievedISBServiceSpec, retrievedISBServiceStatus, err
	}
	retrievedISBServiceSpec, err = GetISBServiceSpec(unstruct)
	if err != nil {
		return unstruct, retrievedISBServiceSpec, retrievedISBServiceStatus, err
	}

	retrievedISBServiceStatus, err = GetISBServiceStatus(unstruct)

	if err != nil {
		return unstruct, retrievedISBServiceSpec, retrievedISBServiceStatus, err
	}
	return unstruct, retrievedISBServiceSpec, retrievedISBServiceStatus, nil
}

// Get ISBServiceSpec from Unstructured type
func GetISBServiceSpec(u *unstructured.Unstructured) (numaflowv1.InterStepBufferServiceSpec, error) {
	specMap := u.Object["spec"]
	var isbServiceSpec numaflowv1.InterStepBufferServiceSpec
	err := util.StructToStruct(&specMap, &isbServiceSpec)
	return isbServiceSpec, err
}

func GetISBServiceStatus(u *unstructured.Unstructured) (numaflowv1.InterStepBufferServiceStatus, error) {
	statusMap := u.Object["status"]
	var status numaflowv1.InterStepBufferServiceStatus
	err := util.StructToStruct(&statusMap, &status)
	return status, err
}

func GetPromotedISBServiceName(namespace, isbServiceRolloutName string) (string, error) {
	isbsvc, err := GetPromotedISBService(namespace, isbServiceRolloutName)
	if err != nil {
		return "", err
	}
	return isbsvc.GetName(), nil
}

func VerifyPromotedISBServiceExists(namespace, isbServiceRolloutName string) {
	CheckEventually(fmt.Sprintf("Verifying that a promoted ISBService exists for ISBServiceRollout %s", isbServiceRolloutName), func() bool {
		_, err := GetPromotedISBService(namespace, isbServiceRolloutName)
		return err == nil
	}).Should(BeTrue())
}

func VerifyPromotedISBServiceSpec(namespace string, isbServiceRolloutName string, f func(numaflowv1.InterStepBufferServiceSpec) bool) {
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
	VerifyISBSvcRolloutDeployed(isbServiceRolloutName)
	VerifyISBSvcRolloutHealthy(isbServiceRolloutName)
}

func VerifyISBSvcRolloutDeployed(isbServiceRolloutName string) {

	CheckEventually("verify ISBServiceRollout is deployed", func() bool {
		rollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
		return rollout.Status.Phase == apiv1.PhaseDeployed
	}).Should(BeTrue())

	CheckEventually("verify ISBServiceRollout child resource is deployed", func() metav1.ConditionStatus {
		rollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceDeployed)
	}).Should(Equal(metav1.ConditionTrue))

}
func VerifyISBSvcRolloutHealthy(isbServiceRolloutName string) {

	CheckEventually("verify ISBServiceRollout child resource is True", func() metav1.ConditionStatus {
		rollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceHealthy)
	}).WithTimeout(4 * time.Minute).Should(Equal(metav1.ConditionTrue))

	if UpgradeStrategy == config.PPNDStrategyID {
		CheckEventually("Verifying that the ISBServiceRollout PausingPipelines condition is as expected", func() metav1.ConditionStatus {
			rollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPausingPipelines)
		}).Should(Or(Equal(metav1.ConditionFalse), Equal(metav1.ConditionUnknown)))
	}

	if UpgradeStrategy == config.ProgressiveStrategyID {
		CheckEventually("Verifying that the ISBServiceRollout ProgressiveUpgrade condition is True", func() metav1.ConditionStatus {
			rollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionProgressiveUpgradeSucceeded)
		}).Should(Or(Equal(metav1.ConditionTrue), Equal(metav1.ConditionUnknown)))
	}

}

func VerifyPromotedISBSvcReady(namespace string, isbServiceRolloutName string, nodeSize int) {

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

	// Use a custom (longer) backoff schedule than the default (10ms with 5 steps) to avoid getting into resource
	// conflict errors that can cause e2e tests to fail
	backoff := wait.Backoff{
		Steps:    10,
		Duration: 100 * time.Millisecond,
		Factor:   2.0,
		Jitter:   0.1,
	}

	err := retry.RetryOnConflict(backoff, func() error {
		rollout, err := isbServiceRolloutClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if rollout == nil {
			return fmt.Errorf("ISBServiceRollout %s is nil", name)
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

func UpdateISBServiceInK8S(name string, f func(*unstructured.Unstructured) (*unstructured.Unstructured, error)) {
	By("updating ISBService")

	// Use a custom (longer) backoff schedule than the default (10ms with 5 steps) to avoid getting into resource
	// conflict errors that can cause e2e tests to fail
	backoff := wait.Backoff{
		Steps:    10,
		Duration: 100 * time.Millisecond,
		Factor:   2.0,
		Jitter:   0.1,
	}

	err := retry.RetryOnConflict(backoff, func() error {
		isbservice, err := dynamicClient.Resource(GetGVRForISBService()).Namespace(Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		updatedISBService, err := f(isbservice)
		if err != nil {
			return err
		}

		_, err = dynamicClient.Resource(GetGVRForISBService()).Namespace(Namespace).Update(ctx, updatedISBService, metav1.UpdateOptions{})
		return err
	})
	Expect(err).ShouldNot(HaveOccurred())
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

	VerifyPromotedISBSvcReady(Namespace, name, 3)

	By("getting new isbservice name")
	newISBServiceName, err := GetPromotedISBServiceName(Namespace, name)
	Expect(err).ShouldNot(HaveOccurred())

	VerifyPDBForISBService(Namespace, newISBServiceName)
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
	foregroundDeletion := metav1.DeletePropagationForeground
	err := isbServiceRolloutClient.Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &foregroundDeletion})
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
	}).WithTimeout(DefaultTestTimeout).Should(BeFalse(), "The ISBServiceRollout should have been deleted but it was found.")

	CheckEventually("Verifying ISBService deletion", func() bool {
		list, err := dynamicClient.Resource(GetGVRForISBService()).Namespace(Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false
		}
		if len(list.Items) == 0 {
			return true
		}
		return false
	}).WithTimeout(DefaultTestTimeout).Should(BeTrue(), "The ISBService should have been deleted but it was found.")
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
	recreateFieldChanged bool,
	progressiveFieldChanged bool,
) {

	rawSpec, err := json.Marshal(newSpec)
	Expect(err).ShouldNot(HaveOccurred())

	// get name of isbservice and name of pipeline before the change so we can check them after the change to see if they're the same or if they've changed
	originalISBServiceName, err := GetPromotedISBServiceName(Namespace, isbServiceRolloutName)
	Expect(err).ShouldNot(HaveOccurred())

	// need to iterate over rollout names
	originalPipelineNames := make(map[string]string)
	originalPipelineCount := make(map[string]int)
	for _, pipelineRollout := range pipelineRollouts {
		originalPipelineName, err := GetPromotedPipelineName(Namespace, pipelineRollout.PipelineRolloutName)
		Expect(err).ShouldNot(HaveOccurred())

		originalPipelineNames[pipelineRollout.PipelineRolloutName] = originalPipelineName

		originalPipelineCount[pipelineRollout.PipelineRolloutName] = GetCurrentPipelineCount(pipelineRollout.PipelineRolloutName)
	}

	UpdateISBServiceRolloutInK8S(isbServiceRolloutName, func(rollout apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
		rollout.Spec.InterStepBufferService.Spec.Raw = rawSpec
		return rollout, nil
	})

	// both "dataLoss" fields and "recreate" fields have risk of data loss
	dataLossRisk := dataLossFieldChanged || recreateFieldChanged
	progressiveRequiredField := dataLossRisk || recreateFieldChanged || progressiveFieldChanged
	if UpgradeStrategy == config.PPNDStrategyID {

		verifyNotPausing := func() bool {
			isbRollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			isbCondStatus := getRolloutConditionStatus(isbRollout.Status.Conditions, apiv1.ConditionPausingPipelines)
			for _, rolloutInfo := range pipelineRollouts {
				_, _, retrievedPipelineStatus, err := GetPromotedPipelineSpecAndStatus(Namespace, rolloutInfo.PipelineRolloutName)
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
			VerifyISBServiceRolloutInProgressStrategy(isbServiceRolloutName, apiv1.UpgradeStrategyPPND)
			// if we expect the pipeline to be healthy after update
			for _, rolloutInfo := range pipelineRollouts {
				if !rolloutInfo.PipelineIsFailed {
					VerifyPipelineRolloutInProgressStrategy(rolloutInfo.PipelineRolloutName, apiv1.UpgradeStrategyPPND)
					VerifyPromotedPipelinePaused(Namespace, rolloutInfo.PipelineRolloutName)
				}
			}

			CheckEventually("Verify that the pipelines are unpaused by checking the PPND conditions on ISBService Rollout and PipelineRollout", verifyNotPausing).Should(BeTrue())

		} else {
			CheckConsistently("Verify that dependent Pipeline is not paused when an update to ISBService not requiring pause is made", verifyNotPausing).WithTimeout(15 * time.Second).Should(BeTrue())
		}
	}

	if UpgradeStrategy == config.ProgressiveStrategyID && progressiveRequiredField {
		VerifyISBServiceRolloutInProgressStrategy(isbServiceRolloutName, apiv1.UpgradeStrategyProgressive)

		// Perform Progressive checks for all pipelines associated to the ISBService
		for _, pipelineRollout := range pipelineRollouts {

			expectedPromotedName := originalPipelineNames[pipelineRollout.PipelineRolloutName]
			expectedUpgradingName := fmt.Sprintf("%s-%d", pipelineRollout.PipelineRolloutName, originalPipelineCount[pipelineRollout.PipelineRolloutName])
			PipelineTransientProgressiveChecks(pipelineRollout.PipelineRolloutName, expectedPromotedName, expectedUpgradingName)
		}
	}

	VerifyPromotedISBServiceSpec(Namespace, isbServiceRolloutName, verifySpecFunc)

	VerifyISBSvcRolloutReady(isbServiceRolloutName)
	VerifyPromotedISBSvcReady(Namespace, isbServiceRolloutName, 3)

	for _, rolloutInfo := range pipelineRollouts {
		VerifyPipelineRolloutInProgressStrategy(rolloutInfo.PipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		// check that pipeline will be failed if we expect it to be
		if rolloutInfo.PipelineIsFailed {
			VerifyPromotedPipelineFailed(Namespace, rolloutInfo.PipelineRolloutName)
		} else {
			VerifyPromotedPipelineRunning(Namespace, rolloutInfo.PipelineRolloutName)
		}
	}

	By("getting new isbservice name")
	newISBServiceName, err := GetPromotedISBServiceName(Namespace, isbServiceRolloutName)
	Expect(err).ShouldNot(HaveOccurred())

	VerifyPDBForISBService(Namespace, newISBServiceName)

	for _, pipelineRollout := range pipelineRollouts {

		rolloutName := pipelineRollout.PipelineRolloutName

		By("getting new pipeline name")
		newPipelineName, err := GetPromotedPipelineName(Namespace, rolloutName)
		Expect(err).ShouldNot(HaveOccurred())

		if (recreateFieldChanged && UpgradeStrategy != config.ProgressiveStrategyID) || (UpgradeStrategy == config.ProgressiveStrategyID && progressiveRequiredField) {
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

		// for progressive strategy, make sure the pipelines look right (original should be cleaned up, etc)
		if UpgradeStrategy == config.ProgressiveStrategyID && progressiveRequiredField {
			expectedPromotedName := fmt.Sprintf("%s-%d", pipelineRollout.PipelineRolloutName, originalPipelineCount[pipelineRollout.PipelineRolloutName]-1)
			expectedUpgradingName := fmt.Sprintf("%s-%d", pipelineRollout.PipelineRolloutName, originalPipelineCount[pipelineRollout.PipelineRolloutName])
			pipeline, err := GetPipelineByName(Namespace, expectedUpgradingName)
			Expect(err).ShouldNot(HaveOccurred())
			pipelineSpec, err := GetPipelineSpec(pipeline)
			Expect(err).ShouldNot(HaveOccurred())
			PipelineFinalProgressiveChecks(pipelineRollout.PipelineRolloutName, expectedPromotedName, expectedUpgradingName, true, pipelineSpec)
		}

	}

}

func VerifyISBServiceRolloutInProgressStrategy(isbServiceRolloutName string, inProgressStrategy apiv1.UpgradeStrategy) {
	CheckEventually(fmt.Sprintf("Verifying InProgressStrategy for ISBService is %q", string(inProgressStrategy)), func() bool {
		isbServiceRollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
		return isbServiceRollout.Status.UpgradeInProgress == inProgressStrategy
	}).Should(BeTrue())
}

func VerifyISBServiceRolloutInProgressStrategyConsistently(isbsvcRolloutName string, inProgressStrategy apiv1.UpgradeStrategy) {
	CheckConsistently(fmt.Sprintf("Verifying InProgressStrategy for ISBService is consistently %q", string(inProgressStrategy)), func() bool {
		rollout, _ := isbServiceRolloutClient.Get(ctx, isbsvcRolloutName, metav1.GetOptions{})
		return rollout.Status.UpgradeInProgress == inProgressStrategy
	}).WithTimeout(10 * time.Second).Should(BeTrue())
}

func VerifyISBServiceRolloutProgressiveCondition(isbsvcRolloutName string, success metav1.ConditionStatus) {
	CheckEventually(fmt.Sprintf("Verify that ISBServiceRollout ProgressiveUpgradeSucceeded condition is %s", success), func() metav1.ConditionStatus {
		rollout, _ := isbServiceRolloutClient.Get(ctx, isbsvcRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionProgressiveUpgradeSucceeded)
	}).Should(Equal(success))
}

func VerifyPDBForISBService(namespace string, isbServiceName string) {
	// verify there's a PDB selecting pods for this isbsvc
	CheckEventually("Verifying PDB", func() bool {
		pdbList, err := kubeClient.PolicyV1().PodDisruptionBudgets(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false
		}
		for _, pdb := range pdbList.Items {
			By(fmt.Sprintf("found PDB with Match Labels: %+v", pdb.Spec.Selector.MatchLabels))
			fmt.Printf("found PDB with Match Labels: %+v", pdb.Spec.Selector.MatchLabels)
			if pdb.Spec.Selector.MatchLabels["app.kubernetes.io/component"] == "isbsvc" &&
				pdb.Spec.Selector.MatchLabels["numaflow.numaproj.io/isbsvc-name"] == isbServiceName {
				return true
			}
		}
		return false

	}).Should(BeTrue())
}

func VerifyISBServiceDeletion(isbsvcName string) {
	CheckEventually(fmt.Sprintf("Verifying that the InterstepBufferService was deleted (%s)", isbsvcName), func() bool {
		pipeline, err := dynamicClient.Resource(GetGVRForISBService()).Namespace(Namespace).Get(ctx, isbsvcName, metav1.GetOptions{})
		if err != nil {
			return errors.IsNotFound(err)
		}

		return pipeline == nil
	}).WithTimeout(DefaultTestTimeout).Should(BeTrue(), fmt.Sprintf("The InterstepBufferService %s/%s should have been deleted but it was found.", Namespace, isbsvcName))
}

func VerifyISBServiceProgressiveFailure(isbsvcRolloutName string, promotedISBSvcName string, upgradingISBSvcName string) {
	VerifyISBServiceRolloutProgressiveStatus(isbsvcRolloutName, promotedISBSvcName, upgradingISBSvcName, apiv1.AssessmentResultFailure)
	VerifyISBServiceRolloutProgressiveCondition(isbsvcRolloutName, metav1.ConditionFalse)
}

func VerifyISBServiceProgressiveSuccess(isbsvcRolloutName string, promotedISBSvcName string, upgradingISBSvcName string) {
	VerifyISBServiceRolloutProgressiveStatus(isbsvcRolloutName, promotedISBSvcName, upgradingISBSvcName, apiv1.AssessmentResultSuccess)
	VerifyISBServiceRolloutProgressiveCondition(isbsvcRolloutName, metav1.ConditionTrue)
}

func UpdateISBService(isbServiceRolloutName string, spec numaflowv1.InterStepBufferServiceSpec) {
	rawSpec, err := json.Marshal(spec)
	Expect(err).ShouldNot(HaveOccurred())
	UpdateISBServiceRolloutInK8S(isbServiceRolloutName, func(isbSvcRollout apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
		isbSvcRollout.Spec.InterStepBufferService.Spec.Raw = rawSpec
		return isbSvcRollout, nil
	})
}
