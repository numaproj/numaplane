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
	"github.com/numaproj/numaplane/internal/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/numaproj/numaplane/internal/util/kubernetes"
)

type MonoVertexStatus = kubernetes.GenericStatus

func ParseMonoVertexStatus(monoVertex *unstructured.Unstructured) (MonoVertexStatus, error) {
	if monoVertex == nil || len(monoVertex.Object) == 0 {
		return MonoVertexStatus{}, nil
	}

	var status MonoVertexStatus
	err := util.StructToStruct(monoVertex.Object["status"], &status)
	if err != nil {
		return MonoVertexStatus{}, err
	}

	return status, nil
}
func MonoVertexWithoutReplicas(monoVertex *unstructured.Unstructured) (map[string]interface{}, error) {
	var specAsMap map[string]any
	if err := util.StructToStruct(monoVertex.Object["spec"], &specAsMap); err != nil {
		return nil, err
	}
	// remove "replicas" field
	comparisonExcludedPaths := []string{"replicas"}
	util.RemovePaths(specAsMap, comparisonExcludedPaths, ".")
	return specAsMap, nil
}
