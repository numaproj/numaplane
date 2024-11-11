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
	"context"
	"fmt"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Each ISBService has one underlying StatefulSet
func GetISBSvcStatefulSetFromK8s(ctx context.Context, c client.Client, isbsvc *unstructured.Unstructured) (*appsv1.StatefulSet, error) {
	statefulSetSelector := labels.NewSelector()
	requirement, err := labels.NewRequirement(numaflowv1.KeyISBSvcName, selection.Equals, []string{isbsvc.GetName()})
	if err != nil {
		return nil, err
	}
	statefulSetSelector = statefulSetSelector.Add(*requirement)

	var statefulSetList appsv1.StatefulSetList
	err = c.List(ctx, &statefulSetList, &client.ListOptions{Namespace: isbsvc.GetNamespace(), LabelSelector: statefulSetSelector}) //TODO: add Watch to StatefulSet (unless we decide to use isbsvc to get all the info directly)
	if err != nil {
		return nil, err
	}
	if len(statefulSetList.Items) > 1 {
		return nil, fmt.Errorf("unexpected: isbsvc %s/%s has multiple StatefulSets: %+v", isbsvc.GetNamespace(), isbsvc.GetName(), statefulSetList.Items)
	} else if len(statefulSetList.Items) == 0 {
		return nil, nil
	} else {
		return &(statefulSetList.Items[0]), nil
	}
}

func GetISBServiceChildResourceHealth(conditions []metav1.Condition) (metav1.ConditionStatus, string) {
	for _, cond := range conditions {
		if cond.Type == "ChildrenResourcesHealthy" && cond.Status != "True" {
			return cond.Status, cond.Reason
		}
	}
	return "True", ""
}
