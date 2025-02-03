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

func getGVRForISBService() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "interstepbufferservices",
	}
}

func getISBService(namespace, isbServiceRolloutName string) (*unstructured.Unstructured, error) {
	return getChildResource(getGVRForISBService(), namespace, isbServiceRolloutName)
}

// Get ISBServiceSpec from Unstructured type
func getISBServiceSpec(u *unstructured.Unstructured) (numaflowv1.InterStepBufferServiceSpec, error) {
	specMap := u.Object["spec"]
	var isbServiceSpec numaflowv1.InterStepBufferServiceSpec
	err := util.StructToStruct(&specMap, &isbServiceSpec)
	return isbServiceSpec, err
}

func verifyISBServiceSpec(namespace string, isbServiceRolloutName string, f func(numaflowv1.InterStepBufferServiceSpec) bool) {

	document("verifying ISBService Spec")
	var retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec
	Eventually(func() bool {
		unstruct, err := getISBService(namespace, isbServiceRolloutName)
		if err != nil {
			return false
		}
		if retrievedISBServiceSpec, err = getISBServiceSpec(unstruct); err != nil {
			return false
		}

		return f(retrievedISBServiceSpec)
	}, testTimeout, testPollingInterval).Should(BeTrue())
}

func verifyISBSvcRolloutReady(isbServiceRolloutName string) {
	document("Verifying that the ISBServiceRollout is ready")

	Eventually(func() bool {
		rollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
		return rollout.Status.Phase == apiv1.PhaseDeployed
	}, testTimeout, testPollingInterval).Should(BeTrue())

	Eventually(func() metav1.ConditionStatus {
		rollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceDeployed)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))

	Eventually(func() metav1.ConditionStatus {
		rollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceHealthy)
	}, 4*time.Minute, testPollingInterval).Should(Equal(metav1.ConditionTrue))

	if upgradeStrategy == config.PPNDStrategyID {
		document("Verifying that the ISBServiceRollout PausingPipelines condition is as expected")
		Eventually(func() metav1.ConditionStatus {
			rollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPausingPipelines)
		}, testTimeout, testPollingInterval).Should(Or(Equal(metav1.ConditionFalse), Equal(metav1.ConditionUnknown)))
	}

}

func verifyISBSvcReady(namespace string, isbServiceRolloutName string, nodeSize int) {

	var isbsvcName string

	document("Verifying that the ISBService exists")
	Eventually(func() error {
		unstruct, err := getISBService(namespace, isbServiceRolloutName)
		if err == nil {
			isbsvcName = unstruct.GetName()
		}
		return err
	}, testTimeout, testPollingInterval).Should(Succeed())

	// TODO: eventually we can use ISBServiceRollout.Status.Conditions(ChildResourcesHealthy) to get this instead

	document("Verifying that the StatefulSet exists and is ready")
	Eventually(func() bool {
		statefulSet, _ := kubeClient.AppsV1().StatefulSets(namespace).Get(ctx, fmt.Sprintf("isbsvc-%s-js", isbsvcName), metav1.GetOptions{})
		return statefulSet != nil && statefulSet.Generation == statefulSet.Status.ObservedGeneration && statefulSet.Status.UpdatedReplicas == int32(nodeSize)
	}, testTimeout, testPollingInterval).Should(BeTrue())

	document("Verifying that the StatefulSet Pods are in Running phase")
	Eventually(func() bool {
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
	}, testTimeout, testPollingInterval).Should(BeTrue())
}

func updateISBServiceRolloutInK8S(name string, f func(apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error)) {
	document("updating ISBServiceRollout")
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

func verifyInProgressStrategyISBService(namespace string, isbsvcRolloutName string, inProgressStrategy apiv1.UpgradeStrategy) {
	document("Verifying InProgressStrategy for ISBService")
	Eventually(func() bool {
		rollout, _ := isbServiceRolloutClient.Get(ctx, isbsvcRolloutName, metav1.GetOptions{})
		return rollout.Status.UpgradeInProgress == inProgressStrategy
	}, testTimeout, testPollingInterval).Should(BeTrue())
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
		watcher, err := dynamicClient.Resource(getGVRForISBService()).Namespace(Namespace).Watch(context.Background(), metav1.ListOptions{})
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
func createISBServiceRollout(name string, isbServiceSpec numaflowv1.InterStepBufferServiceSpec) {

	isbServiceRolloutSpec := createISBServiceRolloutSpec(name, Namespace, isbServiceSpec)
	_, err := isbServiceRolloutClient.Create(ctx, isbServiceRolloutSpec, metav1.CreateOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	document("Verifying that the ISBServiceRollout was created")
	Eventually(func() error {
		_, err := isbServiceRolloutClient.Get(ctx, name, metav1.GetOptions{})
		return err
	}, testTimeout, testPollingInterval).Should(Succeed())

	verifyISBSvcRolloutReady(name)

	verifyISBSvcReady(Namespace, name, 3)

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
func deleteISBServiceRollout(name string) {
	document("Deleting ISBServiceRollout")
	err := isbServiceRolloutClient.Delete(ctx, name, metav1.DeleteOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	document("Verifying ISBServiceRollout deletion")
	Eventually(func() bool {
		_, err := isbServiceRolloutClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if !errors.IsNotFound(err) {
				Fail("An unexpected error occurred when fetching the ISBServiceRollout: " + err.Error())
			}
			return false
		}
		return true
	}).WithTimeout(testTimeout).Should(BeFalse(), "The ISBServiceRollout should have been deleted but it was found.")

	document("Verifying ISBService deletion")
	Eventually(func() bool {
		list, err := dynamicClient.Resource(getGVRForISBService()).Namespace(Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false
		}
		if len(list.Items) == 0 {
			return true
		}
		return false
	}).WithTimeout(testTimeout).Should(BeTrue(), "The ISBService should have been deleted but it was found.")
}

func updateISBServiceRollout(isbServiceRolloutName, pipelineRolloutName string, newSpec numaflowv1.InterStepBufferServiceSpec, f func(numaflowv1.InterStepBufferServiceSpec) bool, dataLoss bool) {

	rawSpec, err := json.Marshal(newSpec)
	Expect(err).ShouldNot(HaveOccurred())

	updateISBServiceRolloutInK8S(isbServiceRolloutName, func(rollout apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
		rollout.Spec.InterStepBufferService.Spec.Raw = rawSpec
		return rollout, nil
	})

	if upgradeStrategy == config.PPNDStrategyID && dataLoss {

		document("Verify that in-progress-strategy gets set to PPND")
		verifyInProgressStrategyISBService(Namespace, isbServiceRolloutName, apiv1.UpgradeStrategyPPND)
		verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyPPND)
		verifyPipelinePaused(Namespace, pipelineRolloutName)

		document("Verify that the pipelines are unpaused by checking the PPND conditions on ISBService Rollout and PipelineRollout")
		Eventually(func() bool {
			isbRollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			isbCondStatus := getRolloutConditionStatus(isbRollout.Status.Conditions, apiv1.ConditionPausingPipelines)
			plRollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			plCondStatus := getRolloutConditionStatus(plRollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
			if isbCondStatus != metav1.ConditionTrue || plCondStatus != metav1.ConditionTrue {
				return false
			}
			return true
		}, testTimeout).Should(BeTrue())

	}

	if upgradeStrategy == config.PPNDStrategyID && !dataLoss {
		document("Verify that dependent Pipeline is not paused when an update to ISBService not requiring pause is made")
		verifyNotPausing := func() bool {
			_, _, retrievedPipelineStatus, err := getPipelineSpecAndStatus(Namespace, pipelineRolloutName)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(retrievedPipelineStatus.Phase != numaflowv1.PipelinePhasePaused).To(BeTrue())
			isbRollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			isbCondStatus := getRolloutConditionStatus(isbRollout.Status.Conditions, apiv1.ConditionPausingPipelines)
			plRollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			plCondStatus := getRolloutConditionStatus(plRollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
			if isbCondStatus == metav1.ConditionTrue || plCondStatus == metav1.ConditionTrue {
				return false
			}
			if isbRollout.Status.UpgradeInProgress != apiv1.UpgradeStrategyNoOp || plRollout.Status.UpgradeInProgress != apiv1.UpgradeStrategyNoOp {
				return false
			}
			return true
		}

		Consistently(verifyNotPausing, 30*time.Second).Should(BeTrue())
	}

	verifyISBServiceSpec(Namespace, isbServiceRolloutName, f)

	verifyISBSvcRolloutReady(isbServiceRolloutName)
	verifyISBSvcReady(Namespace, isbServiceRolloutName, 3)

	verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
	verifyPipelineRunning(Namespace, pipelineRolloutName)

}
