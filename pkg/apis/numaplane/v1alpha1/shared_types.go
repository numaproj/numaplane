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

// Important: Run "make" to regenerate code after modifying this file

package v1alpha1

// ControllerDefinitions stores the Numaflow controller definitions
// for different versions.
type ControllerDefinitions struct {
	Version  string `json:"version" yaml:"version"`
	FullSpec string `json:"fullSpec" yaml:"fullSpec"`
}

type Metadata struct {
	Annotations map[string]string `json:"annotations,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}

type UpgradeStrategy string

const (
	UpgradeStrategyNoOp        UpgradeStrategy = ""
	UpgradeStrategyError       UpgradeStrategy = "Error"
	UpgradeStrategyApply       UpgradeStrategy = "DirectApply"
	UpgradeStrategyPPND        UpgradeStrategy = "PipelinePauseAndDrain"
	UpgradeStrategyProgressive UpgradeStrategy = "Progressive"
)

type RolloutStrategy struct {
	Progressive ProgressiveStrategy `json:"progressiveStrategy"`
}

// PipelineTypeRolloutStrategy specifies the RolloutStrategy for fields shared by Pipeline and MonoVertex
type PipelineTypeRolloutStrategy struct {
	RolloutStrategy `json:",inline"`
}

type ProgressiveStrategy struct {
	// optional string: comma-separated list consisting of:
	// assessmentDelay, assessmentPeriod, assessmentInterval
	AssessmentSchedule string `json:"assessmentSchedule"`
}
