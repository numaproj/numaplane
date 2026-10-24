/*
Copyright 2026.

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
	"strconv"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	pipelineRolloutName   = "test-pipeline-rollout"
	isbServiceRolloutName = "test-isbservice-rollout"
	monoVertexRolloutName = "test-monovertex-rollout"
)

func TestProgressiveNCCombinedE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	BeforeSuite(func() {
		BeforeSuiteSetup()
	})
	RunSpecs(t, "Progressive NumaflowController combined changes E2E Suite")
}

var _ = Describe("Controller changes applied together with a dependent", Ordered, Serial, func() {
	var (
		controllerVersion = InitialNumaflowControllerVersion
		isbVersion        = InitialJetstreamVersion
		revision          = 0
	)

	nextControllerVersion := func() string {
		if controllerVersion == InitialNumaflowControllerVersion {
			controllerVersion = UpdatedNumaflowControllerVersion
		} else {
			controllerVersion = InitialNumaflowControllerVersion
		}
		return controllerVersion
	}

	It("creates the controller and its dependents", func() {
		SetupProgressiveControllerStack(controllerVersion, isbServiceRolloutName, pipelineRolloutName, monoVertexRolloutName, FastWorkloadSchedule)
	})

	It("applies a controller version change and an ISBService change together", func() {
		version := nextControllerVersion()
		if isbVersion == InitialJetstreamVersion {
			isbVersion = UpdatedJetstreamVersion
		} else {
			isbVersion = InitialJetstreamVersion
		}

		SetNumaflowControllerVersion(version, ControllerSuccessSchedule)
		UpdateISBService(isbServiceRolloutName, ProgressiveISBSpec(isbVersion))

		VerifySuccessfulControllerUpgrade(version, pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName)
		VerifyPromotedISBVersion(isbServiceRolloutName, isbVersion)
	})

	It("applies a controller version change and a Pipeline change together", func() {
		version := nextControllerVersion()
		revision++
		envValue := strconv.Itoa(revision)

		SetNumaflowControllerVersion(version, ControllerSuccessSchedule)
		UpdatePipeline(pipelineRolloutName, ProgressivePipelineSpecWithEnv(isbServiceRolloutName, envValue))

		VerifySuccessfulControllerUpgrade(version, pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName)
		VerifyPromotedPipelineEnv(pipelineRolloutName, envValue)
	})

	It("applies a controller version change and a MonoVertex change together", func() {
		version := nextControllerVersion()
		revision++
		envValue := strconv.Itoa(revision)

		SetNumaflowControllerVersion(version, ControllerSuccessSchedule)
		UpdateMonoVertexSpec(monoVertexRolloutName, ProgressiveMonoVertexSpecWithEnv(envValue))

		VerifySuccessfulControllerUpgrade(version, pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName)
		VerifyPromotedMonoVertexEnv(monoVertexRolloutName, envValue)
	})

	It("deletes the remaining rollouts", func() {
		DeleteProgressiveControllerStack(
			[]string{pipelineRolloutName},
			[]string{monoVertexRolloutName},
			[]string{isbServiceRolloutName},
		)
	})
})
