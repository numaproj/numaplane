package e2e

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/retry"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	"github.com/numaproj/numaplane/internal/common"
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
	return getChildResource(getGVRForISBService(), namespace, isbServiceRolloutName, common.LabelValueUpgradePromoted)
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
