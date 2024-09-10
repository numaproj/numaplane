package e2e

import (
	"time"

	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	}).WithTimeout(testTimeout).WithPolling(1 * time.Second).Should(BeTrue())
}

func verifyNumaflowControllerReady(namespace string) {
	document("Verifying that the Numaflow Controller Deployment exists")
	Eventually(func() error {
		_, err := kubeClient.AppsV1().Deployments(namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		return err
	}).WithTimeout(testTimeout).WithPolling(1 * time.Second).Should(Succeed())

	document("Verifying that the Numaflow ControllerRollout is ready")
	Eventually(func() bool {
		rollout, _ := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		if rollout == nil {
			return false
		}
		childResourcesHealthyCondition := rollout.Status.GetCondition(apiv1.ConditionChildResourceHealthy)
		if childResourcesHealthyCondition == nil {
			return false
		}
		return childResourcesHealthyCondition.Status == metav1.ConditionTrue

	}).WithTimeout(testTimeout).WithPolling(1 * time.Second).Should(BeTrue())
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
