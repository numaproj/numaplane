package e2e

import (
	"context"
	"fmt"
	"path/filepath"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/retry"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func getGVRForMonoVertex() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "monovertices",
	}
}

// Get MonoVertexSpec from Unstructured type
func getMonoVertexSpec(u *unstructured.Unstructured) (numaflowv1.MonoVertexSpec, error) {
	specMap := u.Object["spec"]
	var monoVertexSpec numaflowv1.MonoVertexSpec
	err := util.StructToStruct(&specMap, &monoVertexSpec)
	return monoVertexSpec, err
}

func verifyMonoVertexSpec(namespace, name string, f func(numaflowv1.MonoVertexSpec) bool) {

	document("verifying MonoVertex Spec")
	var retrievedMonoVertexSpec numaflowv1.MonoVertexSpec
	Eventually(func() bool {
		unstruct, err := dynamicClient.Resource(getGVRForMonoVertex()).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		if retrievedMonoVertexSpec, err = getMonoVertexSpec(unstruct); err != nil {
			return false
		}
		return f(retrievedMonoVertexSpec)
	}, testTimeout, testPollingInterval).Should(BeTrue())

}

func verifyMonoVertexRolloutReady(monoVertexRolloutName string) {
	document("verifying that the MonoVertexRollout is ready")

	Eventually(func() bool {
		rollout, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
		return rollout.Status.Phase == apiv1.PhaseDeployed
	}, testTimeout, testPollingInterval).Should(BeTrue())

	Eventually(func() metav1.ConditionStatus {
		rollout, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
		return getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionChildResourceDeployed)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))

	Eventually(func() metav1.ConditionStatus {
		rollout, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
		return getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionChildResourceHealthy)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))
}

func verifyMonoVertexReady(namespace, monoVertexName string) {

	document("Verifying that the MonoVertex is running")
	verifyMonoVertexStatus(namespace, monoVertexName,
		func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec, retrievedMonoVertexStatus kubernetes.GenericStatus) bool {
			return retrievedMonoVertexStatus.Phase == string(numaflowv1.MonoVertexPhaseRunning)
		})

	vertexLabelSelector := fmt.Sprintf("%s=%s,%s=%s", numaflowv1.KeyMonoVertexName, monoVertexName, numaflowv1.KeyComponent, "mono-vertex")
	daemonLabelSelector := fmt.Sprintf("%s=%s,%s=%s", numaflowv1.KeyMonoVertexName, monoVertexName, numaflowv1.KeyComponent, "mono-vertex-daemon")

	document("Verifying that the MonoVertex is ready")
	verifyPodsRunning(namespace, 1, vertexLabelSelector)
	verifyPodsRunning(namespace, 1, daemonLabelSelector)

}

func verifyMonoVertexStatus(namespace, monoVertexName string, f func(numaflowv1.MonoVertexSpec, kubernetes.GenericStatus) bool) {

	document("verifying MonoVertexStatus")
	var retrievedMonoVertexSpec numaflowv1.MonoVertexSpec
	var retrievedMonoVertexStatus kubernetes.GenericStatus
	Eventually(func() bool {
		unstruct, err := dynamicClient.Resource(getGVRForMonoVertex()).Namespace(namespace).Get(ctx, monoVertexName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		if retrievedMonoVertexSpec, err = getMonoVertexSpec(unstruct); err != nil {
			return false
		}
		if retrievedMonoVertexStatus, err = getNumaflowResourceStatus(unstruct); err != nil {
			return false
		}
		return f(retrievedMonoVertexSpec, retrievedMonoVertexStatus)
	}, testTimeout, testPollingInterval).Should(BeTrue())

}

func updateMonoVertexRolloutInK8S(name string, f func(apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error)) {

	document("updating MonoVertexRollout")
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rollout, err := monoVertexRolloutClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		*rollout, err = f(*rollout)
		if err != nil {
			return err
		}

		_, err = monoVertexRolloutClient.Update(ctx, rollout, metav1.UpdateOptions{})
		return err
	})
	Expect(err).ShouldNot(HaveOccurred())
}

func watchMonoVertexRollout() {

	defer wg.Done()
	watcher, err := monoVertexRolloutClient.Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		fmt.Printf("Failed to start watcher: %v\n", err)
		return
	}
	defer watcher.Stop()

	for {
		select {
		case event := <-watcher.ResultChan():
			if event.Type == watch.Modified {
				if rollout, ok := event.Object.(*apiv1.MonoVertexRollout); ok {
					rollout.ManagedFields = nil
					rl := Output{
						APIVersion: NumaplaneAPIVersion,
						Kind:       "MonoVertexRollout",
						Metadata:   rollout.ObjectMeta,
						Spec:       rollout.Spec,
						Status:     rollout.Status,
					}

					err := writeToFile(filepath.Join(ResourceChangesMonoVertexOutputPath, "monovertex_rollout.yaml"), rl)
					if err != nil {
						return
					}

				}
			}
		case <-stopCh:
			return
		}
	}
}

func watchMonoVertex() {

	defer wg.Done()
	watcher, err := dynamicClient.Resource(getGVRForMonoVertex()).Namespace(Namespace).Watch(context.Background(), metav1.ListOptions{})
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
					mvtx := numaflowv1.MonoVertex{}
					err = util.StructToStruct(&obj, &mvtx)
					if err != nil {
						fmt.Printf("Failed to convert unstruct: %v\n", err)
						return
					}
					mvtx.ManagedFields = nil
					output := Output{
						APIVersion: NumaflowAPIVersion,
						Kind:       "MonoVertex",
						Metadata:   mvtx.ObjectMeta,
						Spec:       mvtx.Spec,
						Status:     mvtx.Status,
					}

					err = writeToFile(filepath.Join(ResourceChangesMonoVertexOutputPath, "monovertex.yaml"), output)
					if err != nil {
						return
					}
				}
			}
		case <-stopCh:
			return
		}
	}

}

func verifyMonoVertexPaused(namespace string, monoVertexRolloutName string, monoVertexName string) {

	document("Verify that MonoVertex Rollout condition is Pausing/Paused")
	Eventually(func() metav1.ConditionStatus {
		rollout, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
		return getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionMonoVertexPausingOrPaused)
	}, testTimeout).Should(Equal(metav1.ConditionTrue))

	document("Verify that MonoVertex is paused")
	verifyMonoVertexStatusEventually(namespace, monoVertexName,
		func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec, retrievedMonoVertexStatus numaflowv1.MonoVertexStatus) bool {
			return retrievedMonoVertexStatus.Phase == numaflowv1.MonoVertexPhasePaused
		})
}

func verifyMonoVertexStatusEventually(namespace string, monoVertexName string, f func(numaflowv1.MonoVertexSpec, numaflowv1.MonoVertexStatus) bool) {
	Eventually(func() bool {
		_, retrievedMonoVertexSpec, retrievedMonoVertexStatus, err := getMonoVertexFromK8S(namespace, monoVertexName)
		return err == nil && f(retrievedMonoVertexSpec, retrievedMonoVertexStatus)
	}, testTimeout).Should(BeTrue())
}

func getMonoVertexFromK8S(namespace string, monoVertexName string) (*unstructured.Unstructured, numaflowv1.MonoVertexSpec, numaflowv1.MonoVertexStatus, error) {
	var retrievedMonoVertexSpec numaflowv1.MonoVertexSpec
	var retrievedMonoVertexStatus numaflowv1.MonoVertexStatus

	unstruct, err := dynamicClient.Resource(getGVRForMonoVertex()).Namespace(namespace).Get(ctx, monoVertexName, metav1.GetOptions{})
	if err != nil {
		return nil, retrievedMonoVertexSpec, retrievedMonoVertexStatus, err
	}
	retrievedMonoVertexSpec, err = getMonoVertexSpec(unstruct)
	if err != nil {
		return unstruct, retrievedMonoVertexSpec, retrievedMonoVertexStatus, err
	}

	retrievedMonoVertexStatus, err = getMonoVertexStatus(unstruct)

	if err != nil {
		return unstruct, retrievedMonoVertexSpec, retrievedMonoVertexStatus, err
	}
	return unstruct, retrievedMonoVertexSpec, retrievedMonoVertexStatus, nil
}

func getMonoVertexStatus(u *unstructured.Unstructured) (numaflowv1.MonoVertexStatus, error) {
	statusMap := u.Object["status"]
	var status numaflowv1.MonoVertexStatus
	err := util.StructToStruct(&statusMap, &status)
	return status, err
}

func verifyMonoVertexRolloutHealthy(monoVertexRolloutName string) {
	document("Verifying that the MonoVertexRollout Child Condition is Healthy")
	Eventually(func() metav1.ConditionStatus {
		rollout, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
		return getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionChildResourceHealthy)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))
}

func verifyMonoVertexRolloutDeployed(monoVertexRolloutName string) {
	document("Verifying that the MonoVertexRollout is Deployed")
	Eventually(func() bool {
		rollout, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
		return rollout.Status.Phase == apiv1.PhaseDeployed
	}, testTimeout, testPollingInterval).Should(BeTrue())

	Eventually(func() metav1.ConditionStatus {
		rollout, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
		return getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionChildResourceDeployed)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))

}

func startMonoVertexRolloutWatches() {
	wg.Add(1)
	go watchMonoVertexRollout()

	wg.Add(1)
	go watchMonoVertex()
}
