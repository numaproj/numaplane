/*
Copyright 2025.

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
	"reflect"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"

	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"
)

const (
	pipelineRolloutName     = "test-pipeline-rollout"
	isbServiceRolloutName   = "test-isbservice-rollout"
	initialJetstreamVersion = "2.10.17"
	validJetstreamVersion   = "2.10.11"
	invalidJetstreamVersion = "0.0.0"
)

var (
	defaultStrategy = apiv1.PipelineTypeRolloutStrategy{
		PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
			Progressive: apiv1.ProgressiveStrategy{
				AssessmentSchedule: "120,30,10",
			},
		},
	}

	pipelineSpecSourceRPU      = int64(5)
	pipelineSpecSourceDuration = metav1.Duration{
		Duration: time.Second,
	}
	sourceVertexScaleMin = int32(5)
	sourceVertexScaleMax = int32(9)
	numVertices          = int32(1)
	zeroReplicaSleepSec  = uint32(15)
	initialPipelineSpec  = numaflowv1.PipelineSpec{
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
				Scale: numaflowv1.Scale{Min: &sourceVertexScaleMin, Max: &sourceVertexScaleMax, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
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

	volSize, _            = apiresource.ParseQuantity("10Mi")
	initialISBServiceSpec = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: initialJetstreamVersion,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &volSize,
			},
		},
	}
)

func TestProgressiveE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Progressive Pipeline E2E Suite")
}

var _ = Describe("Progressive Pipeline and ISBService E2E", Serial, func() {

	It("Should create initial rollout objects", func() {
		CreateNumaflowControllerRollout(PrimaryNumaflowControllerVersion)
		CreateISBServiceRollout(isbServiceRolloutName, initialISBServiceSpec)
	})

	It("Should validate Pipeline and ISBService upgrade failure followed by success using Progressive strategy", func() {
		By("Creating a PipelineRollout")
		CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false, &defaultStrategy)

		By("Verifying that the Pipeline spec is as expected")
		originalPipelineSpecISBSvcName := initialPipelineSpec.InterStepBufferServiceName
		initialPipelineSpec.InterStepBufferServiceName = GetInstanceName(isbServiceRolloutName, 0)
		VerifyPromotedPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return reflect.DeepEqual(retrievedPipelineSpec, initialPipelineSpec)
		})
		initialPipelineSpec.InterStepBufferServiceName = originalPipelineSpecISBSvcName
		VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyPipelineRolloutHealthy(pipelineRolloutName)

		By("Updating the Pipeline Topology to cause a Progressive change - Failure case")
		updatedPipelineSpec := initialPipelineSpec.DeepCopy()
		updatedPipelineSpec.Vertices[1].UDF = &numaflowv1.UDF{Builtin: &numaflowv1.Function{
			Name: "badcat",
		}}
		rawSpec, err := json.Marshal(updatedPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())
		UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(pipelineRollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
			pipelineRollout.Spec.Pipeline.Spec.Raw = rawSpec
			return pipelineRollout, nil
		})

		By("Updating the ISBService to cause a Progressive change - Failure case")
		updatedISBServiceSpec := initialISBServiceSpec.DeepCopy()
		updatedISBServiceSpec.JetStream.Version = invalidJetstreamVersion
		rawSpec, err = json.Marshal(updatedISBServiceSpec)
		Expect(err).ShouldNot(HaveOccurred())
		UpdateISBServiceRolloutInK8S(isbServiceRolloutName, func(isbSvcRollout apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
			isbSvcRollout.Spec.InterStepBufferService.Spec.Raw = rawSpec
			return isbSvcRollout, nil
		})

		VerifyPromotedPipelineScaledDownForProgressive(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0))
		VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 1), true, apiv1.AssessmentResultFailure, defaultStrategy.Progressive.ForcePromote)

		// Verify that when the "upgrading" Pipeline fails, it scales down to 0 Pods, and the "promoted" Pipeline scales back up
		initialPipelineSpecVertices := []numaflowv1.AbstractVertex{}
		for _, vertex := range initialPipelineSpec.Vertices {
			initialPipelineSpecVertices = append(initialPipelineSpecVertices, numaflowv1.AbstractVertex{Name: vertex.Name, Scale: vertex.Scale})
		}
		VerifyVerticesPodsRunning(Namespace, GetInstanceName(pipelineRolloutName, 0), initialPipelineSpecVertices, ComponentVertex)
		initialPipelineSpecVerticesZero := []numaflowv1.AbstractVertex{}
		for _, vertex := range initialPipelineSpec.Vertices {
			initialPipelineSpecVerticesZero = append(initialPipelineSpecVerticesZero, numaflowv1.AbstractVertex{Name: vertex.Name, Scale: numaflowv1.Scale{Min: ptr.To(int32(0)), Max: ptr.To(int32(0))}})
		}
		VerifyVerticesPodsRunning(Namespace, GetInstanceName(pipelineRolloutName, 1), initialPipelineSpecVerticesZero, ComponentVertex)

		By("Updating the Pipeline Topology to cause a Progressive change - Successful case")
		updatedPipelineSpec = initialPipelineSpec.DeepCopy()
		rawSpec, err = json.Marshal(updatedPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())
		UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(pipelineRollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
			pipelineRollout.Spec.Pipeline.Spec.Raw = rawSpec
			return pipelineRollout, nil
		})

		By("Updating the ISBService to cause a Progressive change - Successful case")
		updatedISBServiceSpec.JetStream.Version = validJetstreamVersion
		rawSpec, err = json.Marshal(updatedISBServiceSpec)
		Expect(err).ShouldNot(HaveOccurred())
		UpdateISBServiceRolloutInK8S(isbServiceRolloutName, func(isbSvcRollout apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
			isbSvcRollout.Spec.InterStepBufferService.Spec.Raw = rawSpec
			return isbSvcRollout, nil
		})

		VerifyPromotedPipelineScaledDownForProgressive(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0))
		VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 2), false, apiv1.AssessmentResultSuccess, defaultStrategy.Progressive.ForcePromote)

		VerifyVerticesPodsRunning(Namespace, GetInstanceName(pipelineRolloutName, 2), initialPipelineSpecVertices, ComponentVertex)

		// Verify the previously promoted pipeline was deleted
		VerifyVerticesPodsRunning(Namespace, GetInstanceName(pipelineRolloutName, 1), initialPipelineSpecVerticesZero, ComponentVertex)
		VerifyPipelineDeletion(GetInstanceName(pipelineRolloutName, 1))

		// Verify ISBServiceRollout Progressive Status
		VerifyISBServiceRolloutProgressiveStatus(isbServiceRolloutName, GetInstanceName(isbServiceRolloutName, 0), GetInstanceName(isbServiceRolloutName, 2), apiv1.AssessmentResultSuccess)

		DeletePipelineRollout(pipelineRolloutName)
	})

	It("Should validate ISBService as failure when Pipeline upgrade assesses in failure", func() {
		By("Creating a PipelineRollout")
		CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false, &defaultStrategy)

		By("Verifying that the Pipeline spec is as expected")
		originalPipelineSpecISBSvcName := initialPipelineSpec.InterStepBufferServiceName
		initialPipelineSpec.InterStepBufferServiceName = GetInstanceName(isbServiceRolloutName, 2)
		VerifyPromotedPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return reflect.DeepEqual(retrievedPipelineSpec, initialPipelineSpec)
		})
		initialPipelineSpec.InterStepBufferServiceName = originalPipelineSpecISBSvcName
		VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyPipelineRolloutHealthy(pipelineRolloutName)

		By("Updating the Pipeline Topology to cause a Progressive change - Invalid change causing failure")
		updatedPipelineSpec := initialPipelineSpec.DeepCopy()
		updatedPipelineSpec.Vertices[1].UDF = &numaflowv1.UDF{Builtin: &numaflowv1.Function{
			Name: "badcat",
		}}
		rawSpec, err := json.Marshal(updatedPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())
		UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(pipelineRollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
			pipelineRollout.Spec.Pipeline.Spec.Raw = rawSpec
			return pipelineRollout, nil
		})

		By("Updating the ISBService to cause a Progressive change - Valid change")
		updatedISBServiceSpec := initialISBServiceSpec.DeepCopy()
		updatedISBServiceSpec.JetStream.Version = initialJetstreamVersion
		rawSpec, err = json.Marshal(updatedISBServiceSpec)
		Expect(err).ShouldNot(HaveOccurred())
		UpdateISBServiceRolloutInK8S(isbServiceRolloutName, func(isbSvcRollout apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
			isbSvcRollout.Spec.InterStepBufferService.Spec.Raw = rawSpec
			return isbSvcRollout, nil
		})

		VerifyPromotedPipelineScaledDownForProgressive(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0))
		VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 1), true, apiv1.AssessmentResultFailure, defaultStrategy.Progressive.ForcePromote)

		// Verify that when the "upgrading" Pipeline fails, it scales down to 0 Pods, and the "promoted" Pipeline scales back up
		initialPipelineSpecVertices := []numaflowv1.AbstractVertex{}
		for _, vertex := range initialPipelineSpec.Vertices {
			initialPipelineSpecVertices = append(initialPipelineSpecVertices, numaflowv1.AbstractVertex{Name: vertex.Name, Scale: vertex.Scale})
		}
		VerifyVerticesPodsRunning(Namespace, GetInstanceName(pipelineRolloutName, 0), initialPipelineSpecVertices, ComponentVertex)
		initialPipelineSpecVerticesZero := []numaflowv1.AbstractVertex{}
		for _, vertex := range initialPipelineSpec.Vertices {
			initialPipelineSpecVerticesZero = append(initialPipelineSpecVerticesZero, numaflowv1.AbstractVertex{Name: vertex.Name, Scale: numaflowv1.Scale{Min: ptr.To(int32(0)), Max: ptr.To(int32(0))}})
		}
		VerifyVerticesPodsRunning(Namespace, GetInstanceName(pipelineRolloutName, 1), initialPipelineSpecVerticesZero, ComponentVertex)

		// Verify ISBServiceRollout Progressive Status
		VerifyISBServiceRolloutProgressiveStatus(isbServiceRolloutName, GetInstanceName(isbServiceRolloutName, 2), GetInstanceName(isbServiceRolloutName, 3), apiv1.AssessmentResultFailure)

		By("Updating the Pipeline back to original healthy state")
		rawSpec, err = json.Marshal(initialPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())
		UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(pipelineRollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
			pipelineRollout.Spec.Pipeline.Spec.Raw = rawSpec
			return pipelineRollout, nil
		})

		VerifyPromotedPipelineScaledDownForProgressive(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0))
		VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 2), true, apiv1.AssessmentResultSuccess, defaultStrategy.Progressive.ForcePromote)

		VerifyVerticesPodsRunning(Namespace, GetInstanceName(pipelineRolloutName, 2), initialPipelineSpecVertices, ComponentVertex)

		// Verify the previously promoted pipeline was deleted
		VerifyVerticesPodsRunning(Namespace, GetInstanceName(pipelineRolloutName, 0), initialPipelineSpecVerticesZero, ComponentVertex)
		VerifyPipelineDeletion(GetInstanceName(pipelineRolloutName, 0))

		By("Updating the ISBService to set the 'force promote' Label")
		UpdateISBServiceInK8S(GetInstanceName(isbServiceRolloutName, 3), func(isbservice *unstructured.Unstructured) (*unstructured.Unstructured, error) {
			labels := isbservice.GetLabels()
			if labels == nil {
				labels = make(map[string]string)
			}
			labels[common.LabelKeyForcePromote] = "true"
			isbservice.SetLabels(labels)
			return isbservice, nil
		})

		VerifyISBServiceRolloutProgressiveStatus(isbServiceRolloutName, GetInstanceName(isbServiceRolloutName, 2), GetInstanceName(isbServiceRolloutName, 3), apiv1.AssessmentResultSuccess)
		VerifyISBServiceDeletion(GetInstanceName(isbServiceRolloutName, 2))

		DeletePipelineRollout(pipelineRolloutName)
	})

	It("Should validate Pipeline as failure when ISBService upgrade assesses in failure", func() {
		By("Creating a PipelineRollout")
		CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false, &defaultStrategy)

		By("Verifying that the Pipeline spec is as expected")
		originalPipelineSpecISBSvcName := initialPipelineSpec.InterStepBufferServiceName
		initialPipelineSpec.InterStepBufferServiceName = GetInstanceName(isbServiceRolloutName, 4)
		VerifyPromotedPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return reflect.DeepEqual(retrievedPipelineSpec, initialPipelineSpec)
		})
		initialPipelineSpec.InterStepBufferServiceName = originalPipelineSpecISBSvcName
		VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyPipelineRolloutHealthy(pipelineRolloutName)

		By("Updating the ISBService to cause a Progressive change - Invalid change causing failure")
		updatedISBServiceSpec := initialISBServiceSpec.DeepCopy()
		updatedISBServiceSpec.JetStream.Version = invalidJetstreamVersion
		rawSpec, err := json.Marshal(updatedISBServiceSpec)
		Expect(err).ShouldNot(HaveOccurred())
		UpdateISBServiceRolloutInK8S(isbServiceRolloutName, func(isbSvcRollout apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
			isbSvcRollout.Spec.InterStepBufferService.Spec.Raw = rawSpec
			return isbSvcRollout, nil
		})

		VerifyPromotedPipelineScaledDownForProgressive(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0))
		VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 1), true, apiv1.AssessmentResultFailure, defaultStrategy.Progressive.ForcePromote)

		// Verify that when the "upgrading" Pipeline fails, it scales down to 0 Pods, and the "promoted" Pipeline scales back up
		initialPipelineSpecVertices := []numaflowv1.AbstractVertex{}
		for _, vertex := range initialPipelineSpec.Vertices {
			initialPipelineSpecVertices = append(initialPipelineSpecVertices, numaflowv1.AbstractVertex{Name: vertex.Name, Scale: vertex.Scale})
		}
		VerifyVerticesPodsRunning(Namespace, GetInstanceName(pipelineRolloutName, 0), initialPipelineSpecVertices, ComponentVertex)
		initialPipelineSpecVerticesZero := []numaflowv1.AbstractVertex{}
		for _, vertex := range initialPipelineSpec.Vertices {
			initialPipelineSpecVerticesZero = append(initialPipelineSpecVerticesZero, numaflowv1.AbstractVertex{Name: vertex.Name, Scale: numaflowv1.Scale{Min: ptr.To(int32(0)), Max: ptr.To(int32(0))}})
		}
		VerifyVerticesPodsRunning(Namespace, GetInstanceName(pipelineRolloutName, 1), initialPipelineSpecVerticesZero, ComponentVertex)

		// Verify ISBServiceRollout Progressive Status
		VerifyISBServiceRolloutProgressiveStatus(isbServiceRolloutName, GetInstanceName(isbServiceRolloutName, 3), GetInstanceName(isbServiceRolloutName, 4), apiv1.AssessmentResultFailure)

		DeletePipelineRollout(pipelineRolloutName)
	})

	It("Should delete all remaining rollout objects", func() {
		DeleteISBServiceRollout(isbServiceRolloutName)
		DeleteNumaflowControllerRollout()
	})
})
