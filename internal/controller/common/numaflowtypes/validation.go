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

package numaflowtypes

import (
	"encoding/json"
	"fmt"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ValidatePipelineSpec validates a raw Pipeline spec by attempting to unmarshal it
// into the typed numaflow PipelineSpec struct. This catches type mismatches (e.g.,
// invalid duration strings) that would otherwise cause the Numaflow controller to crash.
func ValidatePipelineSpec(rawSpec runtime.RawExtension) error {
	var pipelineSpec numaflowv1.PipelineSpec
	if err := json.Unmarshal(rawSpec.Raw, &pipelineSpec); err != nil {
		return fmt.Errorf("invalid Pipeline spec: %w", err)
	}
	return nil
}

// ValidateMonoVertexSpec validates a raw MonoVertex spec by attempting to unmarshal it
// into the typed numaflow MonoVertexSpec struct.
func ValidateMonoVertexSpec(rawSpec runtime.RawExtension) error {
	var monoVertexSpec numaflowv1.MonoVertexSpec
	if err := json.Unmarshal(rawSpec.Raw, &monoVertexSpec); err != nil {
		return fmt.Errorf("invalid MonoVertex spec: %w", err)
	}
	return nil
}

// ValidateISBServiceSpec validates a raw InterStepBufferService spec by attempting
// to unmarshal it into the typed numaflow InterStepBufferServiceSpec struct.
func ValidateISBServiceSpec(rawSpec runtime.RawExtension) error {
	var isbSpec numaflowv1.InterStepBufferServiceSpec
	if err := json.Unmarshal(rawSpec.Raw, &isbSpec); err != nil {
		return fmt.Errorf("invalid ISBService spec: %w", err)
	}
	return nil
}
