package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/gomega"
	"sigs.k8s.io/yaml"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		return getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionChildResourceDeployed)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))

	Eventually(func() metav1.ConditionStatus {
		rollout, _ := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		return getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionChildResourceHealthy)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))

	if dataLossPrevention == "true" {
		document("Verifying that the NumaflowControllerRollout PausingPipelines condition is as expected")
		Eventually(func() metav1.ConditionStatus {
			rollout, _ := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			return getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionPausingPipelines)
		}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionFalse))
	}

}

func verifyNumaflowControllerReady(namespace string) {
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

	defer wg.Done()
	watcher, err := numaflowControllerRolloutClient.Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		fmt.Printf("Failed to start watcher: %v\n", err)
		return
	}
	defer watcher.Stop()

	file, err := os.OpenFile(filepath.Join(ResourceChangesNumaflowControllerOutputPath, "numaflowcontroller_rollout.yaml"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Failed to open log file: %v\n", err)
		return
	}
	defer file.Close()

	for {
		select {
		case event := <-watcher.ResultChan():
			if event.Type == watch.Modified {
				if rollout, ok := event.Object.(*apiv1.NumaflowControllerRollout); ok {
					rollout.ManagedFields = nil
					rl := Output{
						APIVersion: NumaplaneAPIVersion,
						Kind:       "NumaflowControllerRollout",
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
