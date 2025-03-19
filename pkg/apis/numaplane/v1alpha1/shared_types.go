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

import (
	argorolloutsv1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
)

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

// PipelineTypeRolloutStrategy specifies the Rollout Strategy for fields shared by Pipeline and MonoVertex
type PipelineTypeRolloutStrategy struct {
	PipelineTypeProgressiveStrategy `json:",inline"`
}

// PipelineTypeProgressiveStrategy specifies the Progressive Rollout Strategy for fields shared by Pipeline and MonoVertex
type PipelineTypeProgressiveStrategy struct {
	Progressive ProgressiveStrategy `json:"progressive,omitempty"`

	Analysis Analysis `json:"analysis,omitempty"`
}

// Analysis defines how to perform analysis of health, outside of basic resource checking
type Analysis struct {
	// Arguments can be passed to templates to evaluate any parameterization
	Args []argorolloutsv1.AnalysisRunArgument `json:"args,omitempty"`

	// Templates are used to analyze the AnalysisRun
	Templates []argorolloutsv1.AnalysisTemplateRef `json:"templates,omitempty"`
}

type ProgressiveStrategy struct {
	// optional string: comma-separated list consisting of:
	// assessmentDelay, assessmentPeriod, assessmentInterval
	AssessmentSchedule string `json:"assessmentSchedule,omitempty"`

	// if ForcePromote is set, assessment will be skipped and Progressive upgrade will succeed
	ForcePromote bool `json:"forcePromote,omitempty"`
}
