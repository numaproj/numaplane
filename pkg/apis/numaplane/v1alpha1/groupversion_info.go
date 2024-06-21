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

// Package v1alpha1 contains API Schema definitions for the numaplane.numaproj.io v1alpha1 API group
// +kubebuilder:object:generate=true
// +groupName=numaplane.numaproj.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// SchemeGroupVersion is group version used to register these objects
	SchemeGroupVersion = schema.GroupVersion{Group: "numaplane.numaproj.io", Version: "v1alpha1"}

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme
	SchemeBuilder = &scheme.Builder{GroupVersion: SchemeGroupVersion}

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme

	ISBServiceRolloutGroupVersionKind     = SchemeGroupVersion.WithKind("ISBServiceRollout")
	ISBServiceRolloutGroupVersionResource = SchemeGroupVersion.WithResource("isbservicerollouts")

	PipelineRolloutGroupVersionKind     = SchemeGroupVersion.WithKind("PipelineRollout")
	PipelineRolloutGroupVersionResource = SchemeGroupVersion.WithResource("pipelinerollouts")

	NumaflowControllerRolloutGroupVersionKind     = SchemeGroupVersion.WithKind("NumaflowControllerRollout")
	NumaflowControllerRolloutGroupVersionResource = SchemeGroupVersion.WithResource("numaflowcontrollerrollouts")
)

// Resource takes an unqualified resource and returns a Group qualified GroupResource
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}
