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
	"strings"
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
	"github.com/numaproj/numaplane/internal/controller/config"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	isbServiceRolloutName            = "test-isbservice-rollout"
	pipelineRolloutName              = "test-pipeline-rollout"
	monoVertexRolloutName            = "test-monovertex-rollout"
	initialNumaflowControllerVersion = "1.4.0"
	updatedNumaflowControllerVersion = "1.4.1"
	invalidNumaflowControllerVersion = "99.99.99"
	initialJetstreamVersion          = "2.10.17"
	updatedJetstreamVersion          = "2.10.11"
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
	numPipelineVertices = 2

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
	isbServiceSpec = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: initialJetstreamVersion,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &volSize,
			},
		},
	}
	updatedMemLimit, _            = apiresource.ParseQuantity("2Gi")
	ISBServiceSpecNoDataLossField = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: updatedJetstreamVersion,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &volSize,
			},
			ContainerTemplate: &numaflowv1.ContainerTemplate{
				Resources: v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceMemory: updatedMemLimit},
				},
			},
		},
	}

	currentMonoVertexSpec numaflowv1.MonoVertexSpec
	monoVertexSpec        = numaflowv1.MonoVertexSpec{
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

		verifyNumaflowControllerRolloutReady()

		verifyNumaflowControllerExists(Namespace)

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

		document("Verifying that the Pipeline was created")
		verifyPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return len(pipelineSpec.Vertices) == numPipelineVertices // TODO: make less kludgey
			//return reflect.DeepEqual(pipelineSpec, retrievedPipelineSpec) // this may have had some false negatives due to "lifecycle" field maybe, or null values in one
		})

		verifyPipelineRolloutDeployed(pipelineRolloutName)
		verifyPipelineRolloutHealthy(pipelineRolloutName)
		verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

		verifyPipelineRunning(Namespace, pipelineRolloutName, numPipelineVertices)

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

		verifyMonoVertexRolloutReady(monoVertexRolloutName)

		verifyMonoVertexReady(Namespace, monoVertexRolloutName)

	})

	currentMonoVertexSpec = monoVertexSpec

	time.Sleep(2 * time.Second)

	It("Should automatically heal a Pipeline if it is updated directly", func() {

		document("Updating Pipeline directly")

		// update child Pipeline
		updatePipelineSpecInK8S(Namespace, pipelineRolloutName, func(pipelineSpec numaflowv1.PipelineSpec) (numaflowv1.PipelineSpec, error) {
			pipelineSpec.Watermark.Disabled = true
			return pipelineSpec, nil
		})

		if upgradeStrategy == config.PPNDStrategyID {
			document("Verify that child Pipeline is not paused when an update not requiring pause is made")
			verifyPipelineStatusConsistently(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
				return retrievedPipelineStatus.Phase != numaflowv1.PipelinePhasePaused
			})
		}

		// allow time for self healing to reconcile
		time.Sleep(5 * time.Second)

		// get updated Pipeline again to compare spec
		document("Verifying self-healing")
		verifyPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return !retrievedPipelineSpec.Watermark.Disabled
		})

		verifyPipelineRolloutDeployed(pipelineRolloutName)
		verifyPipelineRolloutHealthy(pipelineRolloutName)

		verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

		verifyPipelineRunning(Namespace, pipelineRolloutName, numPipelineVertices)

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

		if upgradeStrategy == config.PPNDStrategyID {

			document("Verify that in-progress-strategy gets set to PPND")
			verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyPPND)

			verifyPipelinePaused(Namespace, pipelineRolloutName)

		}

		// wait for update to reconcile
		time.Sleep(5 * time.Second)

		document("Verifying Pipeline got updated")
		numPipelineVertices = 3

		// get Pipeline to check that spec has been updated to correct spec
		verifyPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return len(retrievedPipelineSpec.Vertices) == numPipelineVertices
		})

		verifyPipelineRolloutDeployed(pipelineRolloutName)
		verifyPipelineRolloutHealthy(pipelineRolloutName)

		verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

		verifyPipelineRunning(Namespace, pipelineRolloutName, numPipelineVertices)

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
		verifyPipelinePaused(Namespace, pipelineRolloutName)
		document("verifying Pipeline stays in paused or otherwise pausing")
		Consistently(func() bool {
			rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			_, _, retrievedPipelineStatus, err := getPipelineSpecAndStatus(Namespace, pipelineRolloutName)
			if err != nil {
				return false
			}
			return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused) == metav1.ConditionTrue &&
				(retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePaused || retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePausing)
		}, 15*time.Second, testPollingInterval).Should(BeTrue())

		verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

		pipeline, err := getPipeline(Namespace, pipelineRolloutName)
		Expect(err).ShouldNot(HaveOccurred())
		verifyPodsRunning(Namespace, 0, getVertexLabelSelector(pipeline.GetName()))
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
		verifyPipelineRunning(Namespace, pipelineRolloutName, numPipelineVertices)
	})

	It("Should pause the MonoVertex if user requests it", func() {

		currentMonoVertexSpec.Lifecycle.DesiredPhase = numaflowv1.MonoVertexPhasePaused

		document("setting desiredPhase=Paused")

		rawSpec, err := json.Marshal(currentMonoVertexSpec)
		Expect(err).ShouldNot(HaveOccurred())

		// update the MonoVertexRollout
		updateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.MonoVertex.Spec.Raw = rawSpec
			return rollout, nil
		})
		document("verifying MonoVertexRollout spec deployed")
		verifyMonoVertexRolloutDeployed(monoVertexRolloutName)

		// Give it a little while to get to Paused and then verify that it stays in Paused
		verifyMonoVertexPaused(Namespace, monoVertexRolloutName)
		document("verifying MonoVertex stays in paused or otherwise pausing")
		Consistently(func() bool {
			rollout, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
			_, _, retrievedMonoVertexStatus, err := getMonoVertexFromK8S(Namespace, monoVertexRolloutName)
			if err != nil {
				return false
			}
			return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionMonoVertexPausingOrPaused) == metav1.ConditionTrue &&
				(retrievedMonoVertexStatus.Phase == numaflowv1.MonoVertexPhasePaused)
		}, 15*time.Second, testPollingInterval).Should(BeTrue())

		verifyInProgressStrategy(monoVertexRolloutName, apiv1.UpgradeStrategyNoOp)

		verifyPodsRunning(Namespace, 0, getVertexLabelSelector(monoVertexRolloutName))
	})

	time.Sleep(2 * time.Second)

	It("Should resume the MonoVertex if user requests it", func() {

		currentMonoVertexSpec.Lifecycle.DesiredPhase = numaflowv1.MonoVertexPhaseRunning

		document("setting desiredPhase=Running")

		rawSpec, err := json.Marshal(currentMonoVertexSpec)
		Expect(err).ShouldNot(HaveOccurred())

		// update the MonoVertexRollout
		updateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.MonoVertex.Spec.Raw = rawSpec
			return rollout, nil
		})

		document("verifying MonoVertexRollout spec deployed")
		verifyMonoVertexRolloutDeployed(monoVertexRolloutName)
		verifyMonoVertexRolloutHealthy(monoVertexRolloutName)

		verifyInProgressStrategy(monoVertexRolloutName, apiv1.UpgradeStrategyNoOp)
	})

	time.Sleep(2 * time.Second)

	It("Should update the child NumaflowController if the NumaflowControllerRollout is updated", func() {
		updateNumaflowControllerRolloutVersion(initialNumaflowControllerVersion, updatedNumaflowControllerVersion, true)
	})

	time.Sleep(2 * time.Second)

	It("Should fail if the NumaflowControllerRollout is updated with a bad version", func() {
		updateNumaflowControllerRolloutVersion(updatedNumaflowControllerVersion, invalidNumaflowControllerVersion, false)
	})

	time.Sleep(2 * time.Second)

	It("Should update the child NumaflowController if the NumaflowControllerRollout is restored back to previous version", func() {
		updateNumaflowControllerRolloutVersion(invalidNumaflowControllerVersion, updatedNumaflowControllerVersion, true)
	})

	time.Sleep(2 * time.Second)

	It("Should update the child ISBService if the ISBServiceRollout is updated", func() {

		// new ISBService spec
		updatedISBServiceSpec := isbServiceSpec
		updatedISBServiceSpec.JetStream.Version = updatedJetstreamVersion
		rawSpec, err := json.Marshal(updatedISBServiceSpec)
		Expect(err).ShouldNot(HaveOccurred())

		updateISBServiceRolloutInK8S(isbServiceRolloutName, func(rollout apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
			rollout.Spec.InterStepBufferService.Spec.Raw = rawSpec
			return rollout, nil
		})

		if upgradeStrategy == config.PPNDStrategyID {

			document("Verify that in-progress-strategy gets set to PPND")
			verifyInProgressStrategyISBService(Namespace, isbServiceRolloutName, apiv1.UpgradeStrategyPPND)
			verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyPPND)
			verifyPipelinePaused(Namespace, pipelineRolloutName)

			Eventually(func() bool {
				isbRollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
				isbCondStatus := getRolloutConditionStatus(isbRollout.Status.Conditions, apiv1.ConditionPausingPipelines)
				plRollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
				plCondStatus := getRolloutConditionStatus(plRollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
				if isbCondStatus != metav1.ConditionTrue || plCondStatus != metav1.ConditionTrue {
					return false
				}
				return true
			}, testTimeout).Should(BeTrue())

		}

		verifyISBServiceSpec(Namespace, isbServiceRolloutName, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream.Version == updatedJetstreamVersion
		})

		verifyISBSvcRolloutReady(isbServiceRolloutName)

		verifyISBSvcReady(Namespace, isbServiceRolloutName, 3)

		verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		verifyPipelineRunning(Namespace, pipelineRolloutName, numPipelineVertices)

	})

	It("Should update the child ISBService updating a no-data-loss field", func() {

		// new ISBService spec
		rawSpec, err := json.Marshal(ISBServiceSpecNoDataLossField)
		Expect(err).ShouldNot(HaveOccurred())

		updateISBServiceRolloutInK8S(isbServiceRolloutName, func(rollout apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
			rollout.Spec.InterStepBufferService.Spec.Raw = rawSpec
			return rollout, nil
		})

		if upgradeStrategy == config.PPNDStrategyID {
			document("Verify that dependent Pipeline is not paused when an update to ISBService not requiring pause is made")
			verifyNotPausing := func() bool {
				_, _, retrievedPipelineStatus, err := getPipelineSpecAndStatus(Namespace, pipelineRolloutName)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(retrievedPipelineStatus.Phase != numaflowv1.PipelinePhasePaused).To(BeTrue())
				isbRollout, _ := isbServiceRolloutClient.Get(ctx, isbServiceRolloutName, metav1.GetOptions{})
				isbCondStatus := getRolloutConditionStatus(isbRollout.Status.Conditions, apiv1.ConditionPausingPipelines)
				plRollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
				plCondStatus := getRolloutConditionStatus(plRollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
				if isbCondStatus == metav1.ConditionTrue || plCondStatus == metav1.ConditionTrue {
					return false
				}
				if isbRollout.Status.UpgradeInProgress != apiv1.UpgradeStrategyNoOp || plRollout.Status.UpgradeInProgress != apiv1.UpgradeStrategyNoOp {
					return false
				}
				return true
			}

			Consistently(verifyNotPausing, 30*time.Second).Should(BeTrue())
		}

		verifyISBServiceSpec(Namespace, isbServiceRolloutName, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream != nil &&
				retrievedISBServiceSpec.JetStream.ContainerTemplate != nil &&
				retrievedISBServiceSpec.JetStream.ContainerTemplate.Resources.Limits.Memory() != nil &&
				*retrievedISBServiceSpec.JetStream.ContainerTemplate.Resources.Limits.Memory() == updatedMemLimit
		})

		verifyISBSvcRolloutReady(isbServiceRolloutName)

		verifyISBSvcReady(Namespace, isbServiceRolloutName, 3)

		verifyPipelineRunning(Namespace, pipelineRolloutName, numPipelineVertices)

	})

	It("Should update child MonoVertex if the MonoVertexRollout is updated", func() {

		// new MonoVertex spec
		updatedMonoVertexSpec := monoVertexSpec
		updatedMonoVertexSpec.Source.UDSource.Container.Image = "quay.io/numaio/numaflow-python/simple-source:stable"
		rawSpec, err := json.Marshal(updatedMonoVertexSpec)
		Expect(err).ShouldNot(HaveOccurred())

		updateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.MonoVertex.Spec.Raw = rawSpec
			return rollout, nil
		})

		verifyMonoVertexSpec(Namespace, monoVertexRolloutName, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
			return retrievedMonoVertexSpec.Source.UDSource.Container.Image == "quay.io/numaio/numaflow-python/simple-source:stable"
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
			list, err := dynamicClient.Resource(pipelinegvr).Namespace(Namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false
			}
			if len(list.Items) == 0 {
				return true
			}
			return false
		}).WithTimeout(testTimeout).Should(BeTrue(), "The Pipeline should have been deleted but it was found.")

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
			list, err := dynamicClient.Resource(monovertexgvr).Namespace(Namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false
			}
			if len(list.Items) == 0 {
				return true
			}
			return false
		}).WithTimeout(testTimeout).Should(BeTrue(), "The MonoVertex should have been deleted but it was found.")
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
			list, err := dynamicClient.Resource(isbservicegvr).Namespace(Namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false
			}
			if len(list.Items) == 0 {
				return true
			}
			return false
		}).WithTimeout(testTimeout).Should(BeTrue(), "The ISBService should have been deleted but it was found.")

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

func updateNumaflowControllerRolloutVersion(originalVersion, newVersion string, valid bool) {
	// new NumaflowController spec
	updatedNumaflowControllerROSpec := apiv1.NumaflowControllerRolloutSpec{
		Controller: apiv1.Controller{Version: newVersion},
	}

	updateNumaflowControllerRolloutInK8S(func(rollout apiv1.NumaflowControllerRollout) (apiv1.NumaflowControllerRollout, error) {
		rollout.Spec = updatedNumaflowControllerROSpec
		return rollout, nil
	})

	if upgradeStrategy == config.PPNDStrategyID && valid {

		document("Verify that in-progress-strategy gets set to PPND")
		verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyPPND)
		verifyPipelinePaused(Namespace, pipelineRolloutName)

		document("Verify that the pipelines are unpaused by checking the PPND conditions on NumaflowController Rollout and PipelineRollout")
		Eventually(func() bool {
			ncRollout, _ := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
			ncCondStatus := getRolloutConditionStatus(ncRollout.Status.Conditions, apiv1.ConditionPausingPipelines)
			plRollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			plCondStatus := getRolloutConditionStatus(plRollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
			if ncCondStatus != metav1.ConditionTrue || plCondStatus != metav1.ConditionTrue {
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
	verifyNumaflowControllerDeployment(Namespace, func(d appsv1.Deployment) bool {
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

	verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
	verifyPipelineRunning(Namespace, pipelineRolloutName, numPipelineVertices)
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
			Controller: apiv1.Controller{Version: initialNumaflowControllerVersion},
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
