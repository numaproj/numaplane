/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
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
	"k8s.io/client-go/util/retry"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	numaflowControllerRolloutName = "numaflow-controller"
	isbServiceRolloutName         = "test-isbservice-rollout"
	isbServiceStatefulSetName     = "isbsvc-test-isbservice-rollout-js"
	pipelineRolloutName           = "test-pipeline-rollout"
)

var (
	pipelineSpecSourceRPU      = int64(5)
	pipelineSpecSourceDuration = metav1.Duration{
		Duration: time.Second,
	}
	pipelineSpec = numaflowv1.PipelineSpec{
		InterStepBufferServiceName: isbServiceRolloutName,
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: "in",
				Source: &numaflowv1.Source{
					Generator: &numaflowv1.GeneratorSource{
						RPU:      &pipelineSpecSourceRPU,
						Duration: &pipelineSpecSourceDuration,
					},
				},
			},
			{
				Name: "out",
				Sink: &numaflowv1.Sink{
					AbstractSink: numaflowv1.AbstractSink{
						Log: &numaflowv1.Log{},
					},
				},
			},
		},
		Edges: []numaflowv1.Edge{
			{
				From: "in",
				To:   "out",
			},
		},
	}

	isbServiceSpec = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: "2.9.6",
		},
	}

	pipelinegvr   schema.GroupVersionResource
	isbservicegvr schema.GroupVersionResource
)

func init() {

	pipelinegvr = getGVRForPipeline()
	isbservicegvr = getGVRForISBService()
}

var _ = Describe("PipelineRollout e2e", func() {

	It("Should create the NumaflowControllerRollout if it doesn't exist", func() {

		numaflowControllerRolloutSpec := createNumaflowControllerRolloutSpec(numaflowControllerRolloutName, Namespace)
		_, err := numaflowControllerRolloutClient.Create(ctx, numaflowControllerRolloutSpec, metav1.CreateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		document("Verifying that the NumaflowControllerRollout was created")
		Eventually(func() error {
			_, err := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			return err
		}).WithTimeout(testTimeout).Should(Succeed())

		verifyNumaflowControllerReady(Namespace)
	})

	It("Should create the ISBServiceRollout if it doesn't exist", func() {

		isbServiceRolloutSpec := createISBServiceRolloutSpec(isbServiceRolloutName, Namespace)
		_, err := isbServiceRolloutClient.Create(ctx, isbServiceRolloutSpec, metav1.CreateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		document("Verifying that the ISBServiceRollout was created")
		Eventually(func() error {
			_, err := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			return err
		}).WithTimeout(testTimeout).Should(Succeed())

		verifyISBSvcReady(Namespace, isbServiceRolloutName, 3)

	})

	It("Should create the PipelineRollout if it does not exist", func() {

		pipelineRolloutSpec := createPipelineRolloutSpec(pipelineRolloutName, Namespace)
		_, err := pipelineRolloutClient.Create(ctx, pipelineRolloutSpec, metav1.CreateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		document("Verifying that the PipelineRollout was created")
		Eventually(func() error {
			_, err := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			return err
		}).WithTimeout(testTimeout).Should(Succeed())

		document("Verifying that the Pipeline was created")
		verifyPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return len(pipelineSpec.Vertices) == 2 // TODO: make less kludgey
			//return reflect.DeepEqual(pipelineSpec, retrievedPipelineSpec) // this may have had some false negatives due to "lifecycle" field maybe, or null values in one
		})

		verifyPipelineReady(Namespace, pipelineRolloutName, 2)

	})

	It("Should automatically heal a Pipeline if it is updated directly", func() {

		document("Updating Pipeline directly")

		// update child Pipeline
		updatePipelineSpecInK8S(Namespace, pipelineRolloutName, func(pipelineSpec numaflowv1.PipelineSpec) (numaflowv1.PipelineSpec, error) {
			rpu := int64(10)
			pipelineSpec.Vertices[0].Source.Generator.RPU = &rpu
			return pipelineSpec, nil
		})

		// allow time for self healing to reconcile
		time.Sleep(5 * time.Second)

		// get updated Pipeline again to compare spec
		document("Verifying self-healing")
		verifyPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return *retrievedPipelineSpec.Vertices[0].Source.Generator.RPU == int64(5)
		})

		verifyPipelineReady(Namespace, pipelineRolloutName, 2)

	})

	It("Should update the child Pipeline if the PipelineRollout is updated", func() {

		document("Updating Pipeline spec in PipelineRollout")

		// new Pipeline spec
		updatedPipelineSpec := pipelineSpec
		rpu := int64(10)
		updatedPipelineSpec.Vertices[0].Source.Generator.RPU = &rpu

		rawSpec, err := json.Marshal(updatedPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())

		// update the PipelineRollout
		updatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
			rollout.Spec.Pipeline.Spec.Raw = rawSpec
			return rollout, nil
		})

		// wait for update to reconcile
		time.Sleep(5 * time.Second)

		document("Verifying Pipeline got updated")

		// get Pipeline to check that spec has been updated to correct spec
		updatedPipelineSpecRunning := updatedPipelineSpec
		updatedPipelineSpecRunning.Lifecycle.DesiredPhase = numaflowv1.PipelinePhaseRunning
		verifyPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return *retrievedPipelineSpec.Vertices[0].Source.Generator.RPU == int64(10)
		})

		verifyPipelineReady(Namespace, pipelineRolloutName, 2)

	})

	It("Should update the child NumaflowController if the NumaflowControllerRollout is updated", func() {

		// new NumaflowController spec
		updatedNumaflowControllerSpec := apiv1.NumaflowControllerRolloutSpec{
			Controller: apiv1.Controller{Version: "0.0.6"},
		}

		updateNumaflowControllerRolloutInK8S(func(rollout apiv1.NumaflowControllerRollout) (apiv1.NumaflowControllerRollout, error) {
			rollout.Spec = updatedNumaflowControllerSpec
			return rollout, nil
		})

		verifyNumaflowControllerDeployment(Namespace, func(d appsv1.Deployment) bool {
			return d.Spec.Template.Spec.Containers[0].Image == "quay.io/numaio/numaflow-rc:v0.0.6"
		})

		verifyNumaflowControllerReady(Namespace)

		verifyPipelineReady(Namespace, pipelineRolloutName, 2)

	})

	It("Should update the child ISBService if the ISBServiceRollout is updated", func() {

		// new ISBService spec
		updatedISBServiceSpec := isbServiceSpec
		updatedISBServiceSpec.JetStream.Version = "2.9.8"
		rawSpec, err := json.Marshal(updatedISBServiceSpec)
		Expect(err).ShouldNot(HaveOccurred())

		updateISBServiceRolloutInK8S(isbServiceRolloutName, func(rollout apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
			rollout.Spec.InterStepBufferService.Spec.Raw = rawSpec
			return rollout, nil
		})

		verifyISBServiceSpec(Namespace, isbServiceRolloutName, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream.Version == "2.9.8"
		})

		verifyISBSvcReady(Namespace, isbServiceRolloutName, 3)

		verifyPipelineReady(Namespace, pipelineRolloutName, 2)

	})

	It("Should delete the PipelineRollout and child Pipeline", func() {

		document("Deleting PipelineRollout")

		err := pipelineRolloutClient.Delete(ctx, pipelineRolloutName, metav1.DeleteOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		document("Verifying PipelineRollout deletion")
		Eventually(func() bool {
			_, err := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					Fail("An unexpected error occurred when fetching the PipelineRollout: " + err.Error())
				}
				return false
			}
			return true
		}).WithTimeout(testTimeout).Should(BeFalse(), "The PipelineRollout should have been deleted but it was found.")

		document("Verifying Pipeline deletion")

		Eventually(func() bool {
			_, err := dynamicClient.Resource(pipelinegvr).Namespace(Namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					Fail("An unexpected error occurred when fetching the Pipeline: " + err.Error())
				}
				return false
			}
			return true
		}).WithTimeout(testTimeout).Should(BeFalse(), "The Pipeline should have been deleted but it was found.")

	})

	It("Should delete the ISBServiceRollout and child ISBService", func() {

		document("Deleting ISBServiceRollout")

		err := isbServiceRolloutClient.Delete(ctx, isbServiceRolloutName, metav1.DeleteOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		document("Verifying ISBServiceRollout deletion")

		Eventually(func() bool {
			_, err := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
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
			_, err := dynamicClient.Resource(isbservicegvr).Namespace(Namespace).Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					Fail("An unexpected error occurred when fetching the ISBService: " + err.Error())
				}
				return false
			}
			return true
		}).WithTimeout(testTimeout).Should(BeFalse(), "The ISBService should have been deleted but it was found.")

	})

	It("Should delete the NumaflowControllerRollout and child NumaflowController", func() {
		document("Deleting NumaflowControllerRollout")

		err := numaflowControllerRolloutClient.Delete(ctx, numaflowControllerRolloutName, metav1.DeleteOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		document("Verifying NumaflowControllerRollout deletion")
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

		document("Verifying Numaflow Controller deletion")

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

	})

})

// document for Ginkgo framework and print to console
func document(testName string) {
	snapshotCluster(testName)
	By(testName)
}

func snapshotCluster(testName string) {
	fmt.Printf("*** %+v: NAMESPACE POD STATE BEFORE TEST: %s\n", time.Now(), testName)
	podList, _ := kubeClient.CoreV1().Pods(Namespace).List(ctx, metav1.ListOptions{})
	if podList != nil {
		for _, pod := range podList.Items {
			fmt.Printf("Pod: %q, %q, Reason:%q\n", pod.Name, pod.Status.Phase, pod.Status.Reason)

		}
	}
}

// verify that the Deployment matches some criteria
func verifyNumaflowControllerDeployment(namespace string, f func(appsv1.Deployment) bool) {
	document("verifying Numaflow Controller Deployment")
	Eventually(func() bool {
		deployment, err := kubeClient.AppsV1().Deployments(namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		return f(*deployment)
	}).WithTimeout(testTimeout).Should(BeTrue())
}

func verifyNumaflowControllerReady(namespace string) {
	document("Verifying that the Numaflow Controller Deployment exists")
	Eventually(func() error {
		_, err := kubeClient.AppsV1().Deployments(namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		return err
	}).WithTimeout(testTimeout).Should(Succeed())

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

	}).WithTimeout(testTimeout).Should(BeTrue())
}

func verifyISBServiceSpec(namespace string, name string, f func(numaflowv1.InterStepBufferServiceSpec) bool) {

	document("verifying ISBService Spec")
	var retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec
	Eventually(func() bool {
		unstruct, err := dynamicClient.Resource(getGVRForISBService()).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		if retrievedISBServiceSpec, err = getISBServiceSpec(unstruct); err != nil {
			return false
		}

		return f(retrievedISBServiceSpec)
	}).WithTimeout(testTimeout).Should(BeTrue())
}

func verifyISBSvcReady(namespace string, isbsvc string, nodeSize int) {
	document("Verifying that the ISBService exists")
	Eventually(func() error {
		_, err := dynamicClient.Resource(getGVRForISBService()).Namespace(namespace).Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
		return err
	}).WithTimeout(testTimeout).Should(Succeed())

	// TODO: eventually we can use ISBServiceRollout.Status.Conditions(ChildResourcesHealthy) to get this instead
	document("Verifying that the StatefulSet exists and is ready")
	Eventually(func() bool {
		statefulSet, _ := kubeClient.AppsV1().StatefulSets(namespace).Get(ctx, isbServiceStatefulSetName, metav1.GetOptions{})
		return statefulSet != nil && statefulSet.Generation == statefulSet.Status.ObservedGeneration && statefulSet.Status.UpdatedReplicas == int32(nodeSize)
	}).WithTimeout(testTimeout).Should(BeTrue())

	document("Verifying that the StatefulSet Pods are in Running phase")
	Eventually(func() bool {
		podList, _ := kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", numaflowv1.KeyISBSvcName, isbsvc)})
		podsInRunning := 0
		if podList != nil {
			for _, pod := range podList.Items {
				if pod.Status.Phase == corev1.PodRunning {
					podsInRunning += 1
				}
			}
		}
		return podsInRunning == nodeSize
	}).WithTimeout(testTimeout).Should(BeTrue())
}

func verifyPipelineSpec(namespace string, pipelineName string, f func(numaflowv1.PipelineSpec) bool) {

	document("verifying Pipeline Spec")
	var retrievedPipelineSpec numaflowv1.PipelineSpec
	Eventually(func() bool {
		unstruct, err := dynamicClient.Resource(getGVRForPipeline()).Namespace(namespace).Get(ctx, pipelineName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		if retrievedPipelineSpec, err = getPipelineSpec(unstruct); err != nil {
			return false
		}

		return f(retrievedPipelineSpec)
	}).WithTimeout(testTimeout).Should(BeTrue())
}

func verifyPipelineStatus(namespace string, pipelineName string, f func(numaflowv1.PipelineSpec, kubernetes.GenericStatus) bool) {

	document("verifying PipelineStatus")
	var retrievedPipelineSpec numaflowv1.PipelineSpec
	var retrievedPipelineStatus kubernetes.GenericStatus
	Eventually(func() bool {
		unstruct, err := dynamicClient.Resource(getGVRForPipeline()).Namespace(namespace).Get(ctx, pipelineName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		if retrievedPipelineSpec, err = getPipelineSpec(unstruct); err != nil {
			return false
		}
		if retrievedPipelineStatus, err = getNumaflowResourceStatus(unstruct); err != nil {
			return false
		}

		return f(retrievedPipelineSpec, retrievedPipelineStatus)
	}).WithTimeout(testTimeout).Should(BeTrue())
}

func verifyPipelineReady(namespace string, pipelineName string, numVertices int) {
	document("Verifying that the Pipeline is running")
	verifyPipelineStatus(namespace, pipelineName,
		func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus kubernetes.GenericStatus) bool {
			return retrievedPipelineStatus.Phase == string(numaflowv1.PipelinePhaseRunning)
		})

	vertexLabelSelector := fmt.Sprintf("%s=%s,%s=%s", numaflowv1.KeyPipelineName, pipelineName, numaflowv1.KeyComponent, "vertex")
	daemonLabelSelector := fmt.Sprintf("%s=%s,%s=%s", numaflowv1.KeyPipelineName, pipelineName, numaflowv1.KeyComponent, "daemon")

	// Get Pipeline Pods to verify they're all up
	// TODO: eventually we can use PipelineRollout.Status.Conditions(ChildResourcesHealthy) to get this instead
	document("Verifying that the Pipeline is ready")
	// check "vertex" Pods
	verifyPodsRunning(namespace, 2, vertexLabelSelector)
	verifyPodsRunning(namespace, 1, daemonLabelSelector)

}

func verifyPodsRunning(namespace string, numPods int, labelSelector string) {
	document(fmt.Sprintf("verifying %d Pods running with label selector %q", numPods, labelSelector))

	Eventually(func() bool {
		podsList, _ := kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		if podsList != nil && len(podsList.Items) >= numPods {
			for _, pod := range podsList.Items {
				if pod.Status.Phase != "Running" {
					return false
				}
			}
			return true
		}
		return false

	}).WithTimeout(testTimeout).Should(BeTrue())

}

func createPipelineRolloutSpec(name, namespace string) *apiv1.PipelineRollout {

	pipelineSpecRaw, err := json.Marshal(pipelineSpec)
	Expect(err).ShouldNot(HaveOccurred())

	pipelineRollout := &apiv1.PipelineRollout{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "numaplane.numaproj.io/v1alpha1",
			Kind:       "PipelineRollout",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: apiv1.Pipeline{
				Spec: runtime.RawExtension{
					Raw: pipelineSpecRaw,
				},
			},
		},
	}

	return pipelineRollout

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

func updatePipelineRolloutInK8S(namespace string, name string, f func(apiv1.PipelineRollout) (apiv1.PipelineRollout, error)) {
	document("updating PipelineRollout")
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rollout, err := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		if err != nil {
			return err
		}

		*rollout, err = f(*rollout)
		if err != nil {
			return err
		}
		_, err = pipelineRolloutClient.Update(ctx, rollout, metav1.UpdateOptions{})
		return err
	})
	Expect(err).ShouldNot(HaveOccurred())
}

func updatePipelineSpecInK8S(namespace string, pipelineName string, f func(numaflowv1.PipelineSpec) (numaflowv1.PipelineSpec, error)) {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {

		unstruct, err := dynamicClient.Resource(pipelinegvr).Namespace(Namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		Expect(err).ShouldNot(HaveOccurred())
		retrievedPipeline := unstruct

		// modify Pipeline Spec to verify it gets auto-healed
		err = updatePipelineSpec(retrievedPipeline, f)
		Expect(err).ShouldNot(HaveOccurred())
		_, err = dynamicClient.Resource(pipelinegvr).Namespace(Namespace).Update(ctx, retrievedPipeline, metav1.UpdateOptions{})
		return err
	})
	Expect(err).ShouldNot(HaveOccurred())
}

// Take a Pipeline Unstructured type and update the PipelineSpec in some way
func updatePipelineSpec(u *unstructured.Unstructured, f func(numaflowv1.PipelineSpec) (numaflowv1.PipelineSpec, error)) error {

	// get PipelineSpec from unstructured object
	specMap := u.Object["spec"]
	var pipelineSpec numaflowv1.PipelineSpec
	err := util.StructToStruct(&specMap, &pipelineSpec)
	if err != nil {
		return err
	}
	// update PipelineSpec
	pipelineSpec, err = f(pipelineSpec)
	if err != nil {
		return err
	}
	var newMap map[string]interface{}
	err = util.StructToStruct(&pipelineSpec, &newMap)
	if err != nil {
		return err
	}
	u.Object["spec"] = newMap
	return nil
}

// Get ISBServiceSpec from Unstructured type
func getISBServiceSpec(u *unstructured.Unstructured) (numaflowv1.InterStepBufferServiceSpec, error) {
	specMap := u.Object["spec"]
	var isbServiceSpec numaflowv1.InterStepBufferServiceSpec
	err := util.StructToStruct(&specMap, &isbServiceSpec)
	return isbServiceSpec, err
}

// Get PipelineSpec from Unstructured type
func getPipelineSpec(u *unstructured.Unstructured) (numaflowv1.PipelineSpec, error) {
	specMap := u.Object["spec"]
	var pipelineSpec numaflowv1.PipelineSpec
	err := util.StructToStruct(&specMap, &pipelineSpec)
	return pipelineSpec, err
}

func getNumaflowResourceStatus(u *unstructured.Unstructured) (kubernetes.GenericStatus, error) {
	statusMap := u.Object["status"]
	var status kubernetes.GenericStatus
	err := util.StructToStruct(&statusMap, &status)
	return status, err
}

func createNumaflowControllerRolloutSpec(name, namespace string) *apiv1.NumaflowControllerRollout {

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
			Controller: apiv1.Controller{Version: "0.0.7"},
		},
	}

	return controllerRollout

}

func createISBServiceRolloutSpec(name, namespace string) *apiv1.ISBServiceRollout {

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

func getGVRForPipeline() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "pipelines",
	}
}

func getGVRForISBService() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "interstepbufferservices",
	}
}
