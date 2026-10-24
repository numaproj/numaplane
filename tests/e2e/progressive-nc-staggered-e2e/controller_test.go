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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	pipelineRolloutName   = "test-pipeline-rollout"
	isbServiceRolloutName = "test-isbservice-rollout"
	monoVertexRolloutName = "test-monovertex-rollout"

	stagger = 30 * time.Second
)

func TestProgressiveNCStaggeredE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	BeforeSuite(func() {
		BeforeSuiteSetup()
	})
	RunSpecs(t, "Progressive NumaflowController staggered changes E2E Suite")
}

var _ = Describe("Controller changes applied about 30 seconds apart from a dependent", Ordered, Serial, func() {
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

	nextEnv := func() string {
		revision++
		return strconv.Itoa(revision)
	}

	toggleISB := func() string {
		if isbVersion == InitialJetstreamVersion {
			isbVersion = UpdatedJetstreamVersion
		} else {
			isbVersion = InitialJetstreamVersion
		}
		return isbVersion
	}

	finish := func(version, pipelineEnv, monoVertexEnv, isb string) {
		VerifySuccessfulControllerUpgrade(version, pipelineRolloutName, isbServiceRolloutName, monoVertexRolloutName)
		if pipelineEnv != "" {
			VerifyPromotedPipelineEnv(pipelineRolloutName, pipelineEnv)
		}
		if monoVertexEnv != "" {
			VerifyPromotedMonoVertexEnv(monoVertexRolloutName, monoVertexEnv)
		}
		if isb != "" {
			VerifyPromotedISBVersion(isbServiceRolloutName, isb)
		}
	}

	It("creates the controller and its dependents", func() {
		SetupProgressiveControllerStack(controllerVersion, isbServiceRolloutName, pipelineRolloutName, monoVertexRolloutName, FastWorkloadSchedule)
	})

	It("changes the controller, then the ISBService", func() {
		version := nextControllerVersion()
		isb := toggleISB()
		SetNumaflowControllerVersion(version, ControllerSuccessSchedule)
		time.Sleep(stagger)
		UpdateISBService(isbServiceRolloutName, ProgressiveISBSpec(isb))
		finish(version, "", "", isb)
	})

	It("changes the ISBService, then the controller", func() {
		version := nextControllerVersion()
		isb := toggleISB()
		UpdateISBService(isbServiceRolloutName, ProgressiveISBSpec(isb))
		time.Sleep(stagger)
		SetNumaflowControllerVersion(version, ControllerSuccessSchedule)
		finish(version, "", "", isb)
	})

	It("changes the controller, then the Pipeline", func() {
		version := nextControllerVersion()
		envValue := nextEnv()
		SetNumaflowControllerVersion(version, ControllerSuccessSchedule)
		time.Sleep(stagger)
		UpdatePipeline(pipelineRolloutName, ProgressivePipelineSpecWithEnv(isbServiceRolloutName, envValue))
		finish(version, envValue, "", "")
	})

	It("changes the Pipeline, then the controller", func() {
		version := nextControllerVersion()
		envValue := nextEnv()
		UpdatePipeline(pipelineRolloutName, ProgressivePipelineSpecWithEnv(isbServiceRolloutName, envValue))
		time.Sleep(stagger)
		SetNumaflowControllerVersion(version, ControllerSuccessSchedule)
		finish(version, envValue, "", "")
	})

	It("changes the controller, then the MonoVertex", func() {
		version := nextControllerVersion()
		envValue := nextEnv()
		SetNumaflowControllerVersion(version, ControllerSuccessSchedule)
		time.Sleep(stagger)
		UpdateMonoVertexSpec(monoVertexRolloutName, ProgressiveMonoVertexSpecWithEnv(envValue))
		finish(version, "", envValue, "")
	})

	It("changes the MonoVertex, then the controller", func() {
		version := nextControllerVersion()
		envValue := nextEnv()
		UpdateMonoVertexSpec(monoVertexRolloutName, ProgressiveMonoVertexSpecWithEnv(envValue))
		time.Sleep(stagger)
		SetNumaflowControllerVersion(version, ControllerSuccessSchedule)
		finish(version, "", envValue, "")
	})

	It("deletes the remaining rollouts", func() {
		DeleteProgressiveControllerStack(
			[]string{pipelineRolloutName},
			[]string{monoVertexRolloutName},
			[]string{isbServiceRolloutName},
		)
	})
})
