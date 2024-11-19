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

package common

import (
	"fmt"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

type RolloutObject interface {
	GetRolloutGVR() metav1.GroupVersionResource

	GetRolloutGVK() schema.GroupVersionKind

	GetChildGVR() metav1.GroupVersionResource

	GetChildGVK() schema.GroupVersionKind

	//GetTypeMeta() *metav1.TypeMeta

	GetRolloutObjectMeta() *metav1.ObjectMeta

	GetRolloutStatus() *apiv1.Status
}

// assume child name is "<rolloutname>-<number>"
func GetRolloutParentName(childName string) (string, error) {

	index := strings.LastIndex(childName, "-")
	if index > 0 && index < len(childName)-1 {
		_, err := strconv.Atoi(childName[index+1:])
		if err == nil {
			return childName[:index], nil
		}
	}
	return "", fmt.Errorf("unexpected child name %q doesn't end with '-<number>'", childName)
}
