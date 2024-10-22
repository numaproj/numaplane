package controller

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

type RolloutObject interface {
	GetChildPluralName() string

	GetTypeMeta() *metav1.TypeMeta

	GetObjectMeta() *metav1.ObjectMeta

	GetStatus() *apiv1.Status
}
