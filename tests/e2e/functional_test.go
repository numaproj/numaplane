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

var pipelineSpecSourceRPU = int64(5)
var pipelineSpecSourceDuration = metav1.Duration{
	Duration: time.Second,
}
var pipelineSpec = numaflowv1.PipelineSpec{
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

var isbServiceSpec = numaflowv1.InterStepBufferServiceSpec{
	Redis: nil,
	JetStream: &numaflowv1.JetStreamBufferService{
		Version: "latest",
		Persistence: &numaflowv1.PersistenceStrategy{
			VolumeSize: &numaflowv1.DefaultVolumeSize,
		},
	},
}

var _ = Describe("PipelineRollout e2e", func() {

	pipelinegvr := getGVRForPipeline()
	isbservicegvr := getGVRForISBService()

	It("Should create the NumaflowControllerRollout if it doesn't exist", func() {

		numaflowControllerRolloutSpec := CreateNumaflowControllerRolloutSpec(numaflowControllerRolloutName, Namespace)
		_, err := numaflowControllerRolloutClient.Create(ctx, numaflowControllerRolloutSpec, metav1.CreateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		document("Verifying that the NumaflowControllerRollout was created")
		Eventually(func() error {
			_, err := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			return err
		}).WithTimeout(testTimeout).Should(Succeed())

		document("Verifying that the Numaflow Controller was created")
		Eventually(func() error {
			_, err := dynamicClient.Resource(getGVRForDeployment()).Namespace(Namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
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

	})

	It("Should create the ISBServiceRollout if it doesn't exist", func() {

		isbServiceRolloutSpec := CreateISBServiceRolloutSpec(isbServiceRolloutName, Namespace)
		_, err := isbServiceRolloutClient.Create(ctx, isbServiceRolloutSpec, metav1.CreateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		document("Verifying that the ISBServiceRollout was created")
		Eventually(func() error {
			_, err := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			return err
		}).WithTimeout(testTimeout).Should(Succeed())

		document("Verifying that the ISBService was created")
		Eventually(func() error {
			_, err := dynamicClient.Resource(isbservicegvr).Namespace(Namespace).Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
			return err
		}).WithTimeout(testTimeout).Should(Succeed())

		// TODO: eventually we can use ISBServiceRollout.Status.Conditions(ChildResourcesHealthy) to get this instead
		document("Verifying that the StatefulSet was created and is ready")
		Eventually(func() bool {
			statefulSet, _ := kubeClient.AppsV1().StatefulSets(Namespace).Get(ctx, isbServiceStatefulSetName, metav1.GetOptions{})
			return statefulSet != nil && statefulSet.Generation == statefulSet.Status.ObservedGeneration && statefulSet.Status.UpdatedReplicas == 3
		}).WithTimeout(testTimeout).Should(BeTrue())

	})

	It("Should create the PipelineRollout if it does not exist", func() {

		pipelineRolloutSpec := CreatePipelineRolloutSpec(pipelineRolloutName, Namespace)
		_, err := pipelineRolloutClient.Create(ctx, pipelineRolloutSpec, metav1.CreateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		document("Verifying that the PipelineRollout was created")
		Eventually(func() error {
			_, err := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			return err
		}).WithTimeout(testTimeout).Should(Succeed())

		document("Verifying that the Pipeline was created")
		EventuallyPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return len(pipelineSpec.Vertices) == 2 // TODO: make less kludgey
			//return reflect.DeepEqual(pipelineSpec, retrievedPipelineSpec)
		})

		document("Verifying that the Pipeline is running")
		EventuallyPipelineStatus(Namespace, pipelineRolloutName,
			func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus kubernetes.GenericStatus) bool {
				return retrievedPipelineStatus.Phase == string(numaflowv1.PipelinePhaseRunning)
			})

		// Get Pipeline Pods to verify they're all up
		// TODO: eventually we can use PipelineRollout.Status.Conditions(ChildResourcesHealthy) to get this instead
		document("Verifying that the Pipeline is ready")
		Eventually(func() bool {
			podsList, _ := kubeClient.CoreV1().Pods(Namespace).List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", numaflowv1.KeyPipelineName, pipelineRolloutName)})
			return podsList != nil && len(podsList.Items) == 3 // 3 = 2 Vertices + daemon

		}).WithTimeout(testTimeout).Should(BeTrue())

		podsList, _ := kubeClient.CoreV1().Pods(Namespace).List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", numaflowv1.KeyPipelineName, pipelineRolloutName)})
		if len(podsList.Items) < 3 {
			fmt.Printf("\ndebug info: len(podsList)=%d, podsList=%+v\n\n", len(podsList.Items), podsList.Items)
			unstruct, err := dynamicClient.Resource(pipelinegvr).Namespace(Namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			if err != nil {
				fmt.Println("debug info: error getting pipeline")
			} else {
				fmt.Printf("debug info: unstruct=%+v\n\n", unstruct)
			}
		}

	})

	It("Should automatically heal a Pipeline if it is updated directly", func() {

		// get child Pipeline
		document("Retrieving PipelineRollout")
		createdPipeline := &unstructured.Unstructured{}
		Eventually(func() bool {
			unstruct, err := dynamicClient.Resource(pipelinegvr).Namespace(Namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			if err != nil {
				return false
			}
			createdPipeline = unstruct
			return true
		}).WithTimeout(testTimeout).Should(BeTrue())

		// modify Pipeline Spec to verify it gets auto-healed
		err := updatePipelineSpec(createdPipeline, func(pipelineSpec numaflowv1.PipelineSpec) (numaflowv1.PipelineSpec, error) {
			rpu := int64(10)
			pipelineSpec.Vertices[0].Source.Generator.RPU = &rpu
			return pipelineSpec, nil
		})
		Expect(err).ShouldNot(HaveOccurred())

		document("Updating Pipeline directly")

		// update child Pipeline
		_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			_, err := dynamicClient.Resource(pipelinegvr).Namespace(Namespace).Update(ctx, createdPipeline, metav1.UpdateOptions{})
			return err
		})
		Expect(err).ShouldNot(HaveOccurred())

		// allow time for self healing to reconcile
		time.Sleep(5 * time.Second)

		// get updated Pipeline again to compare spec
		document("Verifying self-healing")
		EventuallyPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return *retrievedPipelineSpec.Vertices[0].Source.Generator.RPU == int64(5)
		})

	})

	It("Should update the child Pipeline if the PipelineRollout is updated", func() {

		document("Updating Pipeline spec in PipelineRollout")

		// new Pipeline spec
		updatedPipelineSpec := pipelineSpec
		rpu := int64(10)
		updatedPipelineSpec.Vertices[0].Source.Generator.RPU = &rpu

		rawSpec, err := json.Marshal(updatedPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())

		// get current PipelineRollout
		document("Getting current PipelineRollout")
		rollout := &apiv1.PipelineRollout{}
		Eventually(func() bool {
			rollout, err = pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			return err == nil
		}).WithTimeout(testTimeout).Should(BeTrue())

		document("Updating PipelineRollout")

		// update the PipelineRollout
		rollout.Spec.Pipeline.Spec.Raw = rawSpec
		_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			_, err = pipelineRolloutClient.Update(ctx, rollout, metav1.UpdateOptions{}) // TODO: should use RetryOnConflict for all Updates
			return err
		})
		Expect(err).ShouldNot(HaveOccurred())

		// wait for update to reconcile
		time.Sleep(5 * time.Second)

		document("Verifying Pipeline got updated")

		// get Pipeline to check that spec has been updated to correct spec
		updatedPipelineSpecRunning := updatedPipelineSpec
		updatedPipelineSpecRunning.Lifecycle.DesiredPhase = numaflowv1.PipelinePhaseRunning
		EventuallyPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return *retrievedPipelineSpec.Vertices[0].Source.Generator.RPU == int64(10)
		})

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
			_, err := dynamicClient.Resource(getGVRForDeployment()).Namespace(Namespace).Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
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

func document(testName string) {
	snapshotCluster(testName)
	By(testName)
}

func snapshotCluster(testName string) {
	fmt.Printf("*** PODS IN NAMESPACE *** Test: %s\n", testName)
	podList, _ := kubeClient.CoreV1().Pods(Namespace).List(ctx, metav1.ListOptions{})
	if podList != nil {
		for _, pod := range podList.Items {
			fmt.Printf("Pod: %q, %q\n", pod.Name, pod.Status.Phase)
		}
	}
}

func EventuallyPipelineSpec(namespace string, pipelineName string, f func(numaflowv1.PipelineSpec) bool) {

	var retrievedPipelineSpec numaflowv1.PipelineSpec
	Eventually(func() bool {
		unstruct, err := dynamicClient.Resource(getGVRForPipeline()).Namespace(Namespace).Get(ctx, pipelineName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		if retrievedPipelineSpec, err = getPipelineSpec(unstruct); err != nil {
			return false
		}

		return f(retrievedPipelineSpec)
	}).WithPolling(5 * time.Second).WithTimeout(testTimeout).Should(BeTrue())
}

func EventuallyPipelineStatus(namespace string, pipelineName string, f func(numaflowv1.PipelineSpec, kubernetes.GenericStatus) bool) {

	var retrievedPipelineSpec numaflowv1.PipelineSpec
	var retrievedPipelineStatus kubernetes.GenericStatus
	Eventually(func() bool {
		unstruct, err := dynamicClient.Resource(getGVRForPipeline()).Namespace(Namespace).Get(ctx, pipelineName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		if retrievedPipelineSpec, err = getPipelineSpec(unstruct); err != nil {
			return false
		}
		if retrievedPipelineStatus, err = getPipelineStatus(unstruct); err != nil {
			return false
		}

		return f(retrievedPipelineSpec, retrievedPipelineStatus)
	}).WithPolling(5 * time.Second).WithTimeout(testTimeout).Should(BeTrue())
}

func CreatePipelineRolloutSpec(name, namespace string) *apiv1.PipelineRollout {

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

// Get PipelineSpec from Unstructured type
func getPipelineSpec(u *unstructured.Unstructured) (numaflowv1.PipelineSpec, error) {
	specMap := u.Object["spec"]
	var pipelineSpec numaflowv1.PipelineSpec
	err := util.StructToStruct(&specMap, &pipelineSpec)
	return pipelineSpec, err
}

func getPipelineStatus(u *unstructured.Unstructured) (kubernetes.GenericStatus, error) {
	statusMap := u.Object["status"]
	var pipelineStatus kubernetes.GenericStatus
	err := util.StructToStruct(&statusMap, &pipelineStatus)
	return pipelineStatus, err
}

func CreateNumaflowControllerRolloutSpec(name, namespace string) *apiv1.NumaflowControllerRollout {

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
			Controller: apiv1.Controller{Version: "0.0.2"},
		},
	}

	return controllerRollout

}

func CreateISBServiceRolloutSpec(name, namespace string) *apiv1.ISBServiceRollout {

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

func getGVRForDeployment() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "apps",
		Version:  "v1",
		Resource: "deployments",
	}
}

func getGVRForISBService() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "interstepbufferservices",
	}
}