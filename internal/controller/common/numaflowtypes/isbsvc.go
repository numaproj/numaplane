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
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Each ISBService has one underlying StatefulSet
// Find it
// Depending on value "checkLive", either check K8S API directly or go to informer cache
func GetISBSvcStatefulSetFromK8s(ctx context.Context, c client.Client, isbsvc *unstructured.Unstructured, checkLive bool) (*appsv1.StatefulSet, error) {
	statefulSetSelector := labels.NewSelector()
	requirement, err := labels.NewRequirement(numaflowv1.KeyISBSvcName, selection.Equals, []string{isbsvc.GetName()})
	if err != nil {
		return nil, fmt.Errorf("Error creating label requirement: %v", err)
	}
	statefulSetSelector = statefulSetSelector.Add(*requirement)

	var statefulSetList appsv1.StatefulSetList
	if checkLive {
		statefulSets, err := kubernetes.KubernetesClient.AppsV1().StatefulSets(isbsvc.GetNamespace()).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("%s=%s", numaflowv1.KeyISBSvcName, isbsvc.GetName())})
		if err != nil {
			return nil, fmt.Errorf("Error listing live StatefulSets: %v", err)
		}
		statefulSetList = *statefulSets
	} else {
		//TODO: this is currently making a Live K8S call, not a call to cache as it's supposed to
		// Option 1: figure out how we can watch a StatefulSet and have its owner's owner reconcile it
		// Option 2: update InterstepBufferService code in Numaflow to provide all the info we need so we don't need to access StatefulSet directly
		err = c.List(ctx, &statefulSetList, &client.ListOptions{Namespace: isbsvc.GetNamespace(), LabelSelector: statefulSetSelector})
		if err != nil {
			return nil, fmt.Errorf("Error listing StatefulSets: %v", err)
		}
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
		if cond.Type == "ChildrenResourcesHealthy" && cond.Status != metav1.ConditionTrue {
			return cond.Status, cond.Reason
		}
	}
	return metav1.ConditionTrue, ""
}
