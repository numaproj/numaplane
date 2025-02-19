package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/retry"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func GetMonoVertex(namespace, monoVertexRolloutName string) (*unstructured.Unstructured, error) {
	return getChildResource(GetGVRForMonoVertex(), namespace, monoVertexRolloutName)
}

func GetGVRForMonoVertex() schema.GroupVersionResource {
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

func VerifyMonoVertexSpec(namespace, monoVertexRolloutName string, f func(numaflowv1.MonoVertexSpec) bool) {

	Document("verifying MonoVertex Spec")
	var retrievedMonoVertexSpec numaflowv1.MonoVertexSpec
	Eventually(func() bool {
		unstruct, err := GetMonoVertex(namespace, monoVertexRolloutName)
		if err != nil {
			return false
		}
		if retrievedMonoVertexSpec, err = getMonoVertexSpec(unstruct); err != nil {
			return false
		}
		return f(retrievedMonoVertexSpec)
	}, testTimeout, testPollingInterval).Should(BeTrue())

}

func VerifyMonoVertexRolloutReady(monoVertexRolloutName string) {
	Document("verifying that the MonoVertexRollout is ready")

	Eventually(func() bool {
		rollout, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
		return rollout.Status.Phase == apiv1.PhaseDeployed
	}, testTimeout, testPollingInterval).Should(BeTrue())

	Eventually(func() metav1.ConditionStatus {
		rollout, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceDeployed)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))

	Eventually(func() metav1.ConditionStatus {
		rollout, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceHealthy)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))
}

func VerifyMonoVertexReady(namespace, monoVertexRolloutName string) {

	Document("Verifying that the MonoVertex is running")
	monoVertexName := VerifyMonoVertexStatus(namespace, monoVertexRolloutName,
		func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec, retrievedMonoVertexStatus kubernetes.GenericStatus) bool {
			return retrievedMonoVertexStatus.Phase == string(numaflowv1.MonoVertexPhaseRunning)
		})

	vertexLabelSelector := fmt.Sprintf("%s=%s,%s=%s", numaflowv1.KeyMonoVertexName, monoVertexName, numaflowv1.KeyComponent, "mono-vertex")
	daemonLabelSelector := fmt.Sprintf("%s=%s,%s=%s", numaflowv1.KeyMonoVertexName, monoVertexName, numaflowv1.KeyComponent, "mono-vertex-daemon")

	Document("Verifying that the MonoVertex is ready")
	verifyPodsRunning(namespace, 1, vertexLabelSelector)
	verifyPodsRunning(namespace, 1, daemonLabelSelector)

}

func VerifyMonoVertexStatus(namespace, monoVertexRolloutName string, f func(numaflowv1.MonoVertexSpec, kubernetes.GenericStatus) bool) string {

	Document("verifying MonoVertexStatus")
	var retrievedMonoVertexSpec numaflowv1.MonoVertexSpec
	var retrievedMonoVertexStatus kubernetes.GenericStatus
	var monoVertexName string
	Eventually(func() bool {
		unstruct, err := GetMonoVertex(namespace, monoVertexRolloutName)
		if err != nil {
			return false
		}
		if retrievedMonoVertexSpec, err = getMonoVertexSpec(unstruct); err != nil {
			return false
		}
		if retrievedMonoVertexStatus, err = getNumaflowResourceStatus(unstruct); err != nil {
			return false
		}
		monoVertexName = unstruct.GetName()
		return f(retrievedMonoVertexSpec, retrievedMonoVertexStatus)
	}, testTimeout, testPollingInterval).Should(BeTrue())

	return monoVertexName
}

func UpdateMonoVertexRolloutInK8S(name string, f func(apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error)) {

	Document("updating MonoVertexRollout")
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

	watchResourceType(func() (watch.Interface, error) {
		watcher, err := monoVertexRolloutClient.Watch(context.Background(), metav1.ListOptions{})
		return watcher, err
	}, func(o runtime.Object) Output {
		if rollout, ok := o.(*apiv1.MonoVertexRollout); ok {
			rollout.ManagedFields = nil
			return Output{
				APIVersion: NumaplaneAPIVersion,
				Kind:       "MonoVertexRollout",
				Metadata:   rollout.ObjectMeta,
				Spec:       rollout.Spec,
				Status:     rollout.Status,
			}
		}
		return Output{}
	})

}

func watchMonoVertex() {

	watchResourceType(func() (watch.Interface, error) {
		watcher, err := dynamicClient.Resource(GetGVRForMonoVertex()).Namespace(Namespace).Watch(context.Background(), metav1.ListOptions{})
		return watcher, err
	}, func(o runtime.Object) Output {
		if obj, ok := o.(*unstructured.Unstructured); ok {
			mvtx := numaflowv1.MonoVertex{}
			err := util.StructToStruct(&obj, &mvtx)
			if err != nil {
				fmt.Printf("Failed to convert unstruct: %v\n", err)
				return Output{}
			}
			mvtx.ManagedFields = nil
			return Output{
				APIVersion: NumaflowAPIVersion,
				Kind:       "MonoVertex",
				Metadata:   mvtx.ObjectMeta,
				Spec:       mvtx.Spec,
				Status:     mvtx.Status,
			}
		}
		return Output{}
	})

}

func VerifyMonoVertexPaused(namespace string, monoVertexRolloutName string) {

	Document("Verify that MonoVertex Rollout condition is Pausing/Paused")
	Eventually(func() metav1.ConditionStatus {
		rollout, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionMonoVertexPausingOrPaused)
	}, testTimeout).Should(Equal(metav1.ConditionTrue))

	Document("Verify that MonoVertex is paused")
	VerifyMonoVertexStatusEventually(namespace, monoVertexRolloutName,
		func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec, retrievedMonoVertexStatus numaflowv1.MonoVertexStatus) bool {
			return retrievedMonoVertexStatus.Phase == numaflowv1.MonoVertexPhasePaused
		})
}

func VerifyMonoVertexStatusEventually(namespace string, monoVertexRolloutName string, f func(numaflowv1.MonoVertexSpec, numaflowv1.MonoVertexStatus) bool) {
	Eventually(func() bool {
		_, retrievedMonoVertexSpec, retrievedMonoVertexStatus, err := GetMonoVertexFromK8S(namespace, monoVertexRolloutName)
		return err == nil && f(retrievedMonoVertexSpec, retrievedMonoVertexStatus)
	}, testTimeout).Should(BeTrue())
}

func GetMonoVertexFromK8S(namespace string, monoVertexRolloutName string) (*unstructured.Unstructured, numaflowv1.MonoVertexSpec, numaflowv1.MonoVertexStatus, error) {
	var retrievedMonoVertexSpec numaflowv1.MonoVertexSpec
	var retrievedMonoVertexStatus numaflowv1.MonoVertexStatus

	unstruct, err := GetMonoVertex(namespace, monoVertexRolloutName)
	if err != nil {
		return nil, retrievedMonoVertexSpec, retrievedMonoVertexStatus, err
	}

	retrievedMonoVertexSpec, err = getMonoVertexSpec(unstruct)
	if err != nil {
		return unstruct, retrievedMonoVertexSpec, retrievedMonoVertexStatus, err
	}

	retrievedMonoVertexStatus, err = GetMonoVertexStatus(unstruct)

	if err != nil {
		return unstruct, retrievedMonoVertexSpec, retrievedMonoVertexStatus, err
	}
	return unstruct, retrievedMonoVertexSpec, retrievedMonoVertexStatus, nil
}

func GetMonoVertexStatus(u *unstructured.Unstructured) (numaflowv1.MonoVertexStatus, error) {
	statusMap := u.Object["status"]
	var status numaflowv1.MonoVertexStatus
	err := util.StructToStruct(&statusMap, &status)
	return status, err
}

func VerifyMonoVertexRolloutHealthy(monoVertexRolloutName string) {
	Document("Verifying that the MonoVertexRollout Child Condition is Healthy")
	Eventually(func() metav1.ConditionStatus {
		rollout, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceHealthy)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))
}

func VerifyMonoVertexRolloutDeployed(monoVertexRolloutName string) {
	Document("Verifying that the MonoVertexRollout is Deployed")
	Eventually(func() bool {
		rollout, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
		return rollout.Status.Phase == apiv1.PhaseDeployed
	}, testTimeout, testPollingInterval).Should(BeTrue())

	Eventually(func() metav1.ConditionStatus {
		rollout, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceDeployed)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))

}

func startMonoVertexRolloutWatches() {
	wg.Add(1)
	go watchMonoVertexRollout()

	wg.Add(1)
	go watchMonoVertex()
}

// shared functions

// creates MonoVertexRollout of a given spec/name and makes sure it's running
func CreateMonoVertexRollout(name, namespace string, spec numaflowv1.MonoVertexSpec) {

	monoVertexRolloutSpec := createMonoVertexRolloutSpec(name, namespace, spec)
	_, err := monoVertexRolloutClient.Create(ctx, monoVertexRolloutSpec, metav1.CreateOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	Document("Verifying that the MonoVertexRollout was created")
	Eventually(func() error {
		_, err := monoVertexRolloutClient.Get(ctx, name, metav1.GetOptions{})
		return err
	}, testTimeout, testPollingInterval).Should(Succeed())

	Document("Verifying that the MonoVertex was created")
	VerifyMonoVertexSpec(Namespace, name, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
		return spec.Source != nil
	})

	VerifyMonoVertexRolloutReady(name)

	VerifyMonoVertexReady(namespace, name)

}

func createMonoVertexRolloutSpec(name, namespace string, spec numaflowv1.MonoVertexSpec) *apiv1.MonoVertexRollout {

	rawSpec, err := json.Marshal(spec)
	Expect(err).ShouldNot(HaveOccurred())

	monoVertexRollout := &apiv1.MonoVertexRollout{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "numaplane.numaproj.io/v1alpha1",
			Kind:       "MonoVertexRollout",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: apiv1.MonoVertexRolloutSpec{
			MonoVertex: apiv1.MonoVertex{
				Spec: runtime.RawExtension{
					Raw: rawSpec,
				},
			},
		},
	}

	return monoVertexRollout
}

// delete MonoVertexRollout and verify deletion
func DeleteMonoVertexRollout(name string) {
	Document("Deleting MonoVertexRollout")
	err := monoVertexRolloutClient.Delete(ctx, name, metav1.DeleteOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	Document("Verifying MonoVertexRollout deletion")
	Eventually(func() bool {
		_, err := monoVertexRolloutClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if !errors.IsNotFound(err) {
				Fail("An unexpected error occurred when fetching the MonoVertexRollout: " + err.Error())
			}
			return false
		}
		return true
	}).WithTimeout(testTimeout).Should(BeFalse(), "The MonoVertexRollout should have been deleted but it was found.")

	Document("Verifying MonoVertex deletion")
	Eventually(func() bool {
		list, err := dynamicClient.Resource(GetGVRForMonoVertex()).Namespace(Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false
		}
		if len(list.Items) == 0 {
			return true
		}
		return false
	}).WithTimeout(testTimeout).Should(BeTrue(), "The MonoVertex should have been deleted but it was found.")
}

func UpdateMonoVertexRollout(name string, newSpec numaflowv1.MonoVertexSpec, expectedFinalPhase numaflowv1.MonoVertexPhase, verifySpecFunc func(numaflowv1.MonoVertexSpec) bool) {

	rawSpec, err := json.Marshal(newSpec)
	Expect(err).ShouldNot(HaveOccurred())

	// update the MonoVertexRollout
	UpdateMonoVertexRolloutInK8S(name, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
		rollout.Spec.MonoVertex.Spec.Raw = rawSpec
		return rollout, nil
	})
	Document("Verifying MonoVertex spec got updated")
	// get Pipeline to check that spec has been updated to correct spec
	VerifyMonoVertexSpec(Namespace, name, verifySpecFunc)

	Document("verifying MonoVertexRollout Phase=Deployed")
  VerifyMonoVertexRolloutDeployed(name)
	if expectedFinalPhase == numaflowv1.MonoVertexPhaseRunning {
		VerifyMonoVertexRolloutHealthy(name)
	}

	VerifyInProgressStrategy(name, apiv1.UpgradeStrategyNoOp)

	if expectedFinalPhase == numaflowv1.MonoVertexPhasePaused {
		VerifyMonoVertexPaused(Namespace, name)
	} else {
		VerifyMonoVertexReady(Namespace, name)
	}

}

func VerifyMonoVertexStaysPaused(name string) {
	Document("verifying MonoVertex stays in paused or otherwise pausing")
	Consistently(func() bool {
		rollout, _ := monoVertexRolloutClient.Get(ctx, name, metav1.GetOptions{})
		_, _, retrievedMonoVertexStatus, err := GetMonoVertexFromK8S(Namespace, name)
		if err != nil {
			return false
		}
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionMonoVertexPausingOrPaused) == metav1.ConditionTrue &&
			(retrievedMonoVertexStatus.Phase == numaflowv1.MonoVertexPhasePaused)
	}, 15*time.Second, testPollingInterval).Should(BeTrue())

	VerifyInProgressStrategy(name, apiv1.UpgradeStrategyNoOp)

	verifyPodsRunning(Namespace, 0, getVertexLabelSelector(name))
}
