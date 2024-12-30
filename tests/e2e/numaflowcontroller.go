package e2e

import (
	"context"

	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/retry"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var (
	numaflowControllerRolloutName = "numaflow-controller"
)

// verify that the Deployment matches some criteria
func verifyNumaflowControllerDeployment(namespace string, f func(appsv1.Deployment) bool) {
	document("verifying Numaflow Controller Deployment")
	Eventually(func() bool {
		deployment, err := kubeClient.AppsV1().Deployments(namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		return f(*deployment)
	}, testTimeout, testPollingInterval).Should(BeTrue())
}

func verifyNumaflowControllerRolloutReady() {
	document("Verifying that the NumaflowControllerRollout is ready")

	Eventually(func() bool {
		rollout, _ := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		return rollout.Status.Phase == apiv1.PhaseDeployed
	}, testTimeout, testPollingInterval).Should(BeTrue())

	Eventually(func() metav1.ConditionStatus {
		rollout, _ := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceDeployed)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))

	Eventually(func() metav1.ConditionStatus {
		rollout, _ := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceHealthy)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))

	if ppnd == "true" {
		document("Verifying that the NumaflowControllerRollout PausingPipelines condition is as expected")
		Eventually(func() metav1.ConditionStatus {
			rollout, _ := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPausingPipelines)
		}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionFalse))
	}

}

// verify that the NumaflowControllerRollout matches some criteria
func verifyNumaflowControllerRollout(namespace string, f func(apiv1.NumaflowControllerRollout) bool) {
	document("verifying Numaflow Controller Rollout")
	Eventually(func() bool {
		rollout, err := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		return f(*rollout)
	}, testTimeout, testPollingInterval).Should(BeTrue())
}

/*
// verify that the NumaflowController matches some criteria
func verifyNumaflowController(namespace string, f func(apiv1.NumaflowController) bool) {
	document("verifying Numaflow Controller")
	Eventually(func() bool {
		nc, err := numaflowControllerClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		return f(*nc)
	}, testTimeout, testPollingInterval).Should(BeTrue())
}*/

func verifyNumaflowControllerExists(namespace string) {
	document("Verifying that the Numaflow Controller Deployment exists")
	Eventually(func() error {
		_, err := kubeClient.AppsV1().Deployments(namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		return err
	}, testTimeout, testPollingInterval).Should(Succeed())
}

func updateNumaflowControllerRolloutInK8S(f func(apiv1.NumaflowControllerRollout) (apiv1.NumaflowControllerRollout, error)) {
	document("updating NumaflowControllerRollout")
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rollout, err := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		if err != nil {
			return err
		}

		*rollout, err = f(*rollout)
		if err != nil {
			return err
		}
		_, err = numaflowControllerRolloutClient.Update(ctx, rollout, metav1.UpdateOptions{})
		return err
	})
	Expect(err).ShouldNot(HaveOccurred())
}

func watchNumaflowControllerRollout() {

	go watchResourceType(func() (watch.Interface, error) {
		watcher, err := numaflowControllerRolloutClient.Watch(context.Background(), metav1.ListOptions{})
		return watcher, err
	}, func(o runtime.Object) Output {
		if rollout, ok := o.(*apiv1.NumaflowControllerRollout); ok {
			rollout.ManagedFields = nil
			return Output{
				APIVersion: NumaplaneAPIVersion,
				Kind:       "NumaflowControllerRollout",
				Metadata:   rollout.ObjectMeta,
				Spec:       rollout.Spec,
				Status:     rollout.Status,
			}
		}
		return Output{}
	})

}
