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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	isbServiceRolloutName = "test-isbservice-rollout"
	pipelineRolloutName   = "test-pipeline-rollout"
	pipelineName          = "test-pipeline-rollout-0"
	monoVertexRolloutName = "test-monovertex-rollout"
)

var (
	pipelineSpecSourceRPU      = int64(5)
	pipelineSpecSourceDuration = metav1.Duration{
		Duration: time.Second,
	}
	numVertices         = int32(1)
	zeroReplicaSleepSec = uint32(15) // if for some reason the Vertex has 0 replicas, this will cause Numaflow to scale it back up
	currentPipelineSpec numaflowv1.PipelineSpec
	pipelineSpec        = numaflowv1.PipelineSpec{
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
				Scale: numaflowv1.Scale{Min: &numVertices, Max: &numVertices, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name: "out",
				Sink: &numaflowv1.Sink{
					AbstractSink: numaflowv1.AbstractSink{
						Log: &numaflowv1.Log{},
					},
				},
				Scale: numaflowv1.Scale{Min: &numVertices, Max: &numVertices, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
		},
		Edges: []numaflowv1.Edge{
			{
				From: "in",
				To:   "out",
			},
		},
	}

	updatedPipelineSpec = numaflowv1.PipelineSpec{
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
				Scale: numaflowv1.Scale{Min: &numVertices, Max: &numVertices, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name: "cat",
				UDF: &numaflowv1.UDF{
					Builtin: &numaflowv1.Function{
						Name: "cat",
					},
				},
				Scale: numaflowv1.Scale{Min: &numVertices, Max: &numVertices, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name: "out",
				Sink: &numaflowv1.Sink{
					AbstractSink: numaflowv1.AbstractSink{
						Log: &numaflowv1.Log{},
					},
				},
				Scale: numaflowv1.Scale{Min: &numVertices, Max: &numVertices, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
		},
		Edges: []numaflowv1.Edge{
			{
				From: "in",
				To:   "cat",
			},
			{
				From: "cat",
				To:   "out",
			},
		},
	}

	volSize, _     = apiresource.ParseQuantity("10Mi")
	memLimit, _    = apiresource.ParseQuantity("10Gi")
	isbServiceSpec = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: "2.9.6",
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &volSize,
			},
		},
	}
	ISBServiceSpecExcludedField = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: "2.9.8",
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &volSize,
			},
			ContainerTemplate: &numaflowv1.ContainerTemplate{
				Resources: v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceMemory: memLimit},
				},
			},
		},
	}

	monoVertexSpec = numaflowv1.MonoVertexSpec{
		Replicas: ptr.To(int32(1)),
		Source: &numaflowv1.Source{
			UDSource: &numaflowv1.UDSource{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-java/source-simple-source:stable",
				},
			},
			UDTransformer: &numaflowv1.UDTransformer{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-rs/source-transformer-now:stable",
				},
			},
		},
		Sink: &numaflowv1.Sink{
			AbstractSink: numaflowv1.AbstractSink{
				UDSink: &numaflowv1.UDSink{
					Container: &numaflowv1.Container{
						Image: "quay.io/numaio/numaflow-java/simple-sink:stable",
					},
				},
			},
		},
	}

	pipelinegvr   schema.GroupVersionResource
	isbservicegvr schema.GroupVersionResource
	monovertexgvr schema.GroupVersionResource
)

func init() {

	pipelinegvr = getGVRForPipeline()
	isbservicegvr = getGVRForISBService()
	monovertexgvr = getGVRForMonoVertex()

}

var _ = Describe("Functional e2e", Serial, func() {

	It("Should create the NumaflowControllerRollout if it doesn't exist", func() {

		numaflowControllerRolloutSpec := createNumaflowControllerRolloutSpec(numaflowControllerRolloutName, Namespace)
		_, err := numaflowControllerRolloutClient.Create(ctx, numaflowControllerRolloutSpec, metav1.CreateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		document("Verifying that the NumaflowControllerRollout was created")
		Eventually(func() error {
			_, err := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			return err
		}, testTimeout, testPollingInterval).Should(Succeed())

		if disableTestArtifacts != "true" {
			wg.Add(1)
			go watchNumaflowControllerRollout()
		}

		verifyNumaflowControllerRolloutReady()

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
		}, testTimeout, testPollingInterval).Should(Succeed())

		if disableTestArtifacts != "true" {
			wg.Add(1)
			go watchISBServiceRollout()

			wg.Add(1)
			go watchISBService()
		}

		verifyISBSvcRolloutReady(isbServiceRolloutName)

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
		}, testTimeout, testPollingInterval).Should(Succeed())

		if disableTestArtifacts != "true" {
			wg.Add(1)
			go watchPipelineRollout()

			wg.Add(1)
			go watchPipeline()
		}

		document("Verifying that the Pipeline was created")
		verifyPipelineSpec(Namespace, pipelineName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return len(pipelineSpec.Vertices) == 2 // TODO: make less kludgey
			//return reflect.DeepEqual(pipelineSpec, retrievedPipelineSpec) // this may have had some false negatives due to "lifecycle" field maybe, or null values in one
		})

		verifyPipelineRolloutDeployed(pipelineRolloutName)
		verifyPipelineRolloutHealthy(pipelineRolloutName)
		verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

		verifyPipelineRunning(Namespace, pipelineName, 2)

	})

	currentPipelineSpec = pipelineSpec

	It("Should create the MonoVertexRollout if it does not exist", func() {

		monoVertexRolloutSpec := createMonoVertexRolloutSpec(monoVertexRolloutName, Namespace)
		_, err := monoVertexRolloutClient.Create(ctx, monoVertexRolloutSpec, metav1.CreateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		document("Verifying that the MonoVertexRollout was created")
		Eventually(func() error {
			_, err := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
			return err
		}, testTimeout, testPollingInterval).Should(Succeed())

		document("Verifying that the MonoVertex was created")
		verifyMonoVertexSpec(Namespace, monoVertexRolloutName, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
			return monoVertexSpec.Source != nil
		})

		if disableTestArtifacts != "true" {
			wg.Add(1)
			go watchMonoVertexRollout()

			wg.Add(1)
			go watchMonoVertex()
		}

		verifyMonoVertexRolloutReady(monoVertexRolloutName)

		verifyMonoVertexReady(Namespace, monoVertexRolloutName)

	})

	It("Should automatically heal a Pipeline if it is updated directly", func() {

		document("Updating Pipeline directly")

		// update child Pipeline
		updatePipelineSpecInK8S(Namespace, pipelineName, func(pipelineSpec numaflowv1.PipelineSpec) (numaflowv1.PipelineSpec, error) {
			pipelineSpec.Watermark.Disabled = true
			return pipelineSpec, nil
		})

		if dataLossPrevention == "true" {
			document("Verify that child Pipeline is not paused when an update not requiring pause is made")
			verifyPipelineStatusConsistently(Namespace, pipelineName, func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
				return retrievedPipelineStatus.Phase != numaflowv1.PipelinePhasePaused
			})
		}

		// allow time for self healing to reconcile
		time.Sleep(5 * time.Second)

		// get updated Pipeline again to compare spec
		document("Verifying self-healing")
		verifyPipelineSpec(Namespace, pipelineName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return !retrievedPipelineSpec.Watermark.Disabled
		})

		verifyPipelineRolloutDeployed(pipelineRolloutName)
		verifyPipelineRolloutHealthy(pipelineRolloutName)

		verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

		verifyPipelineRunning(Namespace, pipelineName, 2)

	})

	time.Sleep(2 * time.Second)

	It("Should update the child Pipeline if the PipelineRollout is updated", func() {

		document("Updating Pipeline spec in PipelineRollout")
		rawSpec, err := json.Marshal(updatedPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())

		// update the PipelineRollout
		updatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
			rollout.Spec.Pipeline.Spec.Raw = rawSpec
			return rollout, nil
		})

		if dataLossPrevention == "true" {

			document("Verify that in-progress-strategy gets set to PPND")
			verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyPPND)

			verifyPipelinePaused(Namespace, pipelineRolloutName, pipelineName)

		}

		// wait for update to reconcile
		time.Sleep(5 * time.Second)

		document("Verifying Pipeline got updated")

		// get Pipeline to check that spec has been updated to correct spec
		verifyPipelineSpec(Namespace, pipelineName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return len(retrievedPipelineSpec.Vertices) == 3
		})

		verifyPipelineRolloutDeployed(pipelineRolloutName)
		verifyPipelineRolloutHealthy(pipelineRolloutName)

		verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

		verifyPipelineRunning(Namespace, pipelineName, 3)

	})

	currentPipelineSpec = updatedPipelineSpec

	time.Sleep(2 * time.Second)

	It("Should pause the Pipeline if user requests it", func() {

		currentPipelineSpec.Lifecycle.DesiredPhase = numaflowv1.PipelinePhasePaused

		document("setting desiredPhase=Paused")

		rawSpec, err := json.Marshal(currentPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())

		// update the PipelineRollout
		updatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
			rollout.Spec.Pipeline.Spec.Raw = rawSpec
			return rollout, nil
		})
		document("verifying PipelineRollout spec deployed")
		verifyPipelineRolloutDeployed(pipelineRolloutName)

		// Give it a little while to get to Paused and then verify that it stays in Paused (or otherwise Pausing)
		verifyPipelinePaused(Namespace, pipelineRolloutName, pipelineName)
		document("verifying Pipeline stays in paused or otherwise pausing")
		Consistently(func() bool {
			rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			_, _, retrievedPipelineStatus, err := getPipelineFromK8S(Namespace, pipelineName)
			if err != nil {
				return false
			}
			return getRolloutCondition(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused) == metav1.ConditionTrue &&
				(retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePaused || retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePausing)
		}, 1*time.Minute, testPollingInterval).Should(BeTrue())

		verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

		verifyPodsRunning(Namespace, 0, getVertexLabelSelector(pipelineName))
	})

	time.Sleep(2 * time.Second)

	It("Should resume the Pipeline if user requests it", func() {

		currentPipelineSpec.Lifecycle.DesiredPhase = numaflowv1.PipelinePhaseRunning

		document("setting desiredPhase=Running")

		rawSpec, err := json.Marshal(currentPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())

		// update the PipelineRollout
		updatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
			rollout.Spec.Pipeline.Spec.Raw = rawSpec
			return rollout, nil
		})
		document("verifying PipelineRollout spec deployed")

		verifyPipelineRolloutDeployed(pipelineRolloutName)
		verifyPipelineRolloutHealthy(pipelineRolloutName)

		verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		verifyPipelineRunning(Namespace, pipelineName, 3)
	})

	time.Sleep(2 * time.Second)

	It("Should update the child NumaflowController if the NumaflowControllerRollout is updated", func() {

		// new NumaflowController spec
		updatedNumaflowControllerSpec := apiv1.NumaflowControllerRolloutSpec{
			Controller: apiv1.Controller{Version: "0.0.19"},
		}

		updateNumaflowControllerRolloutInK8S(func(rollout apiv1.NumaflowControllerRollout) (apiv1.NumaflowControllerRollout, error) {
			rollout.Spec = updatedNumaflowControllerSpec
			return rollout, nil
		})

		if dataLossPrevention == "true" {

			document("Verify that in-progress-strategy gets set to PPND")
			verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyPPND)
			verifyPipelinePaused(Namespace, pipelineRolloutName, pipelineName)

			Eventually(func() bool {
				ncRollout, _ := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
				ncCondStatus := getRolloutCondition(ncRollout.Status.Conditions, apiv1.ConditionPausingPipelines)
				plRollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
				plCondStatus := getRolloutCondition(plRollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
				if ncCondStatus != metav1.ConditionTrue || plCondStatus != metav1.ConditionTrue {
					return false
				}
				return true
			}, testTimeout).Should(BeTrue())
		}

		// TODO: update this controller image when Numaflow v1.3.1 is released
		//       versions prior to v1.3.0 do not reconcile MonoVertex
		verifyNumaflowControllerDeployment(Namespace, func(d appsv1.Deployment) bool {
			return d.Spec.Template.Spec.Containers[0].Image == "quay.io/numaio/numaflow-rc:v0.0.19"
		})

		verifyNumaflowControllerRolloutReady()

		verifyNumaflowControllerReady(Namespace)

		verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		verifyPipelineRunning(Namespace, pipelineName, 3)

	})

	time.Sleep(2 * time.Second)

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

		if dataLossPrevention == "true" {

			document("Verify that in-progress-strategy gets set to PPND")
			verifyInProgressStrategyISBService(Namespace, isbServiceRolloutName, apiv1.UpgradeStrategyPPND)
			verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyPPND)
			verifyPipelinePaused(Namespace, pipelineRolloutName, pipelineName)

			Eventually(func() bool {
				isbRollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
				isbCondStatus := getRolloutCondition(isbRollout.Status.Conditions, apiv1.ConditionPausingPipelines)
				plRollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
				plCondStatus := getRolloutCondition(plRollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
				if isbCondStatus != metav1.ConditionTrue || plCondStatus != metav1.ConditionTrue {
					return false
				}
				return true
			}, testTimeout).Should(BeTrue())
		}

		verifyISBServiceSpec(Namespace, isbServiceRolloutName, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream.Version == "2.9.8"
		})

		verifyISBSvcRolloutReady(isbServiceRolloutName)

		verifyISBSvcReady(Namespace, isbServiceRolloutName, 3)

		verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		verifyPipelineRunning(Namespace, pipelineName, 3)

	})

	It("Should update the child ISBService with an excluded field", func() {

		// new ISBService spec
		rawSpec, err := json.Marshal(ISBServiceSpecExcludedField)
		Expect(err).ShouldNot(HaveOccurred())

		updateISBServiceRolloutInK8S(isbServiceRolloutName, func(rollout apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
			rollout.Spec.InterStepBufferService.Spec.Raw = rawSpec
			return rollout, nil
		})

		if dataLossPrevention == "true" {

			document("Verify that in-progress-strategy gets set to NoOp")
			verifyInProgressStrategyISBService(Namespace, isbServiceRolloutName, apiv1.UpgradeStrategyNoOp)
			verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
			// verifyPipelinePaused(Namespace, pipelineRolloutName, pipelineName)

			Eventually(func() bool {
				isbRollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
				isbCondStatus := getRolloutCondition(isbRollout.Status.Conditions, apiv1.ConditionPausingPipelines)
				plRollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
				plCondStatus := getRolloutCondition(plRollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
				if isbCondStatus == metav1.ConditionTrue || plCondStatus == metav1.ConditionTrue {
					return false
				}
				return true
			}, testTimeout).Should(BeTrue())
		}

		verifyISBServiceSpec(Namespace, isbServiceRolloutName, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			// return retrievedISBServiceSpec.JetStream.Version == "2.9.8"
			return retrievedISBServiceSpec.JetStream.ContainerTemplate.Resources.Limits.Memory() == &memLimit
		})

		verifyISBSvcRolloutReady(isbServiceRolloutName)

		verifyISBSvcReady(Namespace, isbServiceRolloutName, 3)

		verifyInProgressStrategyISBService(Namespace, isbServiceRolloutName, apiv1.UpgradeStrategyNoOp)
		verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		verifyPipelineRunning(Namespace, pipelineName, 3)

	})

	It("Should update child MonoVertex if the MonoVertexRollout is updated", func() {

		// new MonoVertex spec
		updatedMonoVertexSpec := monoVertexSpec
		updatedMonoVertexSpec.Source.UDSource.Container.Image = "quay.io/numaio/numaflow-java/source-simple-source:v0.6.0"
		rawSpec, err := json.Marshal(updatedMonoVertexSpec)
		Expect(err).ShouldNot(HaveOccurred())

		updateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.MonoVertex.Spec.Raw = rawSpec
			return rollout, nil
		})

		verifyMonoVertexSpec(Namespace, monoVertexRolloutName, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
			return retrievedMonoVertexSpec.Source.UDSource.Container.Image == "quay.io/numaio/numaflow-java/source-simple-source:v0.6.0"
		})

		verifyMonoVertexRolloutReady(monoVertexRolloutName)

		verifyMonoVertexReady(Namespace, monoVertexRolloutName)

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
			_, err := dynamicClient.Resource(pipelinegvr).Namespace(Namespace).Get(ctx, pipelineName, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					Fail("An unexpected error occurred when fetching the Pipeline: " + err.Error())
				}
				return false
			}
			return true
		}).WithTimeout(testTimeout).Should(BeFalse(), "The Pipeline should have been deleted but it was found.")

	})

	It("Should delete the MonoVertexRollout and child MonoVertex", func() {

		document("Deleting MonoVertexRollout")

		err := monoVertexRolloutClient.Delete(ctx, monoVertexRolloutName, metav1.DeleteOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		document("Verifying MonoVertexRollout deletion")

		Eventually(func() bool {
			_, err := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					Fail("An unexpected error occurred when fetching the MonoVertexRollout: " + err.Error())
				}
				return false
			}
			return true
		}).WithTimeout(testTimeout).Should(BeFalse(), "The MonoVertexRollout should have been deleted but it was found.")

		document("Verifying MonoVertex deletion")

		Eventually(func() bool {
			_, err := dynamicClient.Resource(monovertexgvr).Namespace(Namespace).Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					Fail("An unexpected error occurred when fetching the MonoVertex: " + err.Error())
				}
				return false
			}
			return true
		}).WithTimeout(testTimeout).Should(BeFalse(), "The MonoVertex should have been deleted but it was found.")
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
			Controller: apiv1.Controller{Version: "0.0.18"},
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

func createMonoVertexRolloutSpec(name, namespace string) *apiv1.MonoVertexRollout {

	rawSpec, err := json.Marshal(monoVertexSpec)
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
