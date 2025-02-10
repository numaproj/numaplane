package e2e

import (
	"context"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/retry"

	"github.com/numaproj/numaplane/internal/controller/config"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var (
	numaflowControllerRolloutName = "numaflow-controller"
)

// verify that the Deployment matches some criteria
func VerifyNumaflowControllerDeployment(namespace string, f func(appsv1.Deployment) bool) {
	Document("verifying Numaflow Controller Deployment")
	Eventually(func() bool {
		deployment, err := kubeClient.AppsV1().Deployments(namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		return f(*deployment)
	}, testTimeout, testPollingInterval).Should(BeTrue())
}

func verifyNumaflowControllerRolloutReady() {
	Document("Verifying that the NumaflowControllerRollout is ready")

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

	if UpgradeStrategy == config.PPNDStrategyID {
		Document("Verifying that the NumaflowControllerRollout PausingPipelines condition is as expected")
		Eventually(func() metav1.ConditionStatus {
			rollout, _ := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPausingPipelines)
		}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionFalse))
	}

}

// verify that the NumaflowControllerRollout matches some criteria
func verifyNumaflowControllerRollout(namespace string, f func(apiv1.NumaflowControllerRollout) bool) {
	Document("verifying Numaflow Controller Rollout")
	Eventually(func() bool {
		rollout, err := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		return f(*rollout)
	}, testTimeout, testPollingInterval).Should(BeTrue())
}

func verifyNumaflowControllerExists(namespace string) {
	Document("Verifying that the Numaflow Controller Deployment exists")
	Eventually(func() error {
		_, err := kubeClient.AppsV1().Deployments(namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		return err
	}, testTimeout, testPollingInterval).Should(Succeed())
}

func UpdateNumaflowControllerRolloutInK8S(f func(apiv1.NumaflowControllerRollout) (apiv1.NumaflowControllerRollout, error)) {
	Document("updating NumaflowControllerRollout")
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

// shared functions

// create NumaflowControllerRollout of any given version and be sure it's running
func CreateNumaflowControllerRollout(version string) {

	numaflowControllerRolloutSpec := createNumaflowControllerRolloutSpec(numaflowControllerRolloutName, Namespace, version)
	_, err := numaflowControllerRolloutClient.Create(ctx, numaflowControllerRolloutSpec, metav1.CreateOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	Document("Verifying that the NumaflowControllerRollout was created")
	Eventually(func() error {
		_, err := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		return err
	}, testTimeout, testPollingInterval).Should(Succeed())

	verifyNumaflowControllerRolloutReady()

	verifyNumaflowControllerExists(Namespace)

}

func createNumaflowControllerRolloutSpec(name, namespace, version string) *apiv1.NumaflowControllerRollout {

	controllerRollout := &apiv1.NumaflowControllerRollout{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "numaplane.numaproj.io/v1alpha1",
			Kind:       "NumaflowControllerRollout",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: apiv1.NumaflowControllerRolloutSpec{
			Controller: apiv1.Controller{Version: version},
		},
	}

	return controllerRollout

}

// delete NumaflowControllerRollout and verify deletion
func DeleteNumaflowControllerRollout() {
	Document("Deleting NumaflowControllerRollout")
	err := numaflowControllerRolloutClient.Delete(ctx, numaflowControllerRolloutName, metav1.DeleteOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	Document("Verifying NumaflowControllerRollout deletion")
	Eventually(func() bool {
		_, err := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		if err != nil {
			if !errors.IsNotFound(err) {
				Fail("An unexpected error occurred when fetching the NumaflowControllerRollout: " + err.Error())
			}
			return false
		}
		return true
	}).WithTimeout(testTimeout).Should(BeFalse(), "The NumaflowControllerRollout should have been deleted but it was found.")

	Document("Verifying Numaflow Controller deletion")
	Eventually(func() bool {
		_, err := kubeClient.AppsV1().Deployments(Namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		if err != nil {
			if !errors.IsNotFound(err) {
				Fail("An unexpected error occurred when fetching the deployment: " + err.Error())
			}
			return false
		}
		return true
	}).WithTimeout(testTimeout).Should(BeFalse(), "The deployment should have been deleted but it was found.")
}

// TODO: pipelinerolloutname should be an array of names to verify multiple pipelines should be paused
// originalVersion is the original version of the current NumaflowController defined in the rollout
// newVersion is the new version the updated NumaflowController should have if it is a valid version
// pipelineRolloutName is the pipeline we check to be sure that it is pausing during the update
// valid boolean indicates if newVersion is a valid version or not (which will change the check we make)
// pipelineIsFailed informs us if any dependent pipelines are currently failed and to not check if they are running
func UpdateNumaflowControllerRollout(originalVersion, newVersion, pipelineRolloutName string, valid bool, pipelineIsFailed bool) {
	// new NumaflowController spec
	updatedNumaflowControllerROSpec := apiv1.NumaflowControllerRolloutSpec{
		Controller: apiv1.Controller{Version: newVersion},
	}

	UpdateNumaflowControllerRolloutInK8S(func(rollout apiv1.NumaflowControllerRollout) (apiv1.NumaflowControllerRollout, error) {
		rollout.Spec = updatedNumaflowControllerROSpec
		return rollout, nil
	})

	// NOTE: we are only checking the "valid" case because in the "non-valid" case the pipeline pausing conditions on
	// the NumaflowController and Pipeline rollouts change too rapidly making the test flaky (intermittently pass or fail)
	if UpgradeStrategy == config.PPNDStrategyID && valid {

		if !pipelineIsFailed {
			Document("Verify that in-progress-strategy gets set to PPND")
			VerifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyPPND)
			VerifyPipelinePaused(Namespace, pipelineRolloutName)
		}

		Document("Verify that the pipelines are unpaused by checking the PPND conditions on NumaflowController Rollout and PipelineRollout")
		Eventually(func() bool {
			ncRollout, _ := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			ncCondStatus := getRolloutConditionStatus(ncRollout.Status.Conditions, apiv1.ConditionPausingPipelines)
			plRollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			plCondStatus := getRolloutConditionStatus(plRollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
			if ncCondStatus == metav1.ConditionTrue || plCondStatus == metav1.ConditionTrue {
				return false
			}
			return true
		}, testTimeout).Should(BeTrue())
	}

	var versionToCheck string
	if valid {
		versionToCheck = newVersion
	} else {
		versionToCheck = originalVersion
	}
	VerifyNumaflowControllerDeployment(Namespace, func(d appsv1.Deployment) bool {
		colon := strings.Index(d.Spec.Template.Spec.Containers[0].Image, ":")
		return colon != -1 && d.Spec.Template.Spec.Containers[0].Image[colon+1:] == "v"+versionToCheck
	})

	if valid {
		verifyNumaflowControllerRolloutReady()
	} else {
		// verify NumaflowControllerRollout ChildResourcesHealthy condition == false but NumaflowControllerRollout itself is marked "Deployed"
		verifyNumaflowControllerRollout(Namespace, func(rollout apiv1.NumaflowControllerRollout) bool {
			healthCondition := getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionChildResourceHealthy)
			return rollout.Status.Phase == apiv1.PhaseDeployed && healthCondition != nil && healthCondition.Status == metav1.ConditionFalse && healthCondition.Reason == "Failed"
		})
	}

	VerifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
	// need to track vertices of pipeline spec
	if pipelineIsFailed {
		VerifyPipelineFailed(Namespace, pipelineRolloutName)
	} else {
		VerifyPipelineRunning(Namespace, pipelineRolloutName, false)
	}

}
