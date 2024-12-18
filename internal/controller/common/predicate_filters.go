package common

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

type TypedGenerationChangedPredicate[object metav1.Object] struct {
	predicate.TypedFuncs[object]
}

func (p TypedGenerationChangedPredicate[object]) Update(e event.TypedUpdateEvent[object]) bool {
	if e.ObjectOld.GetName() == "" || e.ObjectNew.GetName() == "" {
		return false
	}

	if e.ObjectNew.GetGeneration() != e.ObjectOld.GetGeneration() {
		return true
	}

	if e.ObjectNew.GetResourceVersion() == e.ObjectOld.GetResourceVersion() {
		return true
	}

	return false
}
