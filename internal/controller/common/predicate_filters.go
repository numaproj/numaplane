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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

/*
	Reference: https://github.com/kubernetes-sigs/controller-runtime/issues/2355#issuecomment-2477548322
	There's not an explicit attribute that holds this info about sync trigger. However, the "update" events that come in
	when these batches of resync-triggered events happen, the resource version changes for every real update,
	but doesn't for the resync-triggered events. So, to detect these cases while also making use of the generation change check,
	added a custom predicate that is a logical OR of checking
		1. If the generation changed between the old and new object.
		2. If the resource version remained the same between the old and new object.
	Assuming there are no other edge cases where the resource version remains the same across an update event
*/

type TypedGenerationChangedPredicate[object metav1.Object] struct {
	// Returns true by default for all events; see overrides below.
	predicate.TypedFuncs[object]
}

func (p TypedGenerationChangedPredicate[object]) Update(e event.TypedUpdateEvent[object]) bool {
	if e.ObjectOld.GetName() == "" || e.ObjectNew.GetName() == "" {
		return false
	}

	// Process the event if the generation changed (which happens e.g. if the spec
	// was updated, but not if the status or metadata fields were updated).
	if e.ObjectNew.GetGeneration() != e.ObjectOld.GetGeneration() {
		return true
	}

	// If the generation is unchanged, we generally don't want to process the event,
	// except events triggered by periodic resyncs (which are, for some
	// reason, categorized as updates). We identify such events by checking if the old
	// and new resource versions are equal.
	if e.ObjectNew.GetResourceVersion() == e.ObjectOld.GetResourceVersion() {
		return true
	}

	return false
}
