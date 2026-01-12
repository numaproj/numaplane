package kubernetes

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8sClient "sigs.k8s.io/controller-runtime/pkg/client"
)

// this file contains utility functions for working with standard Kubernetes types
// (using their typed structs as opposed to Unstructured type)

// GetSecret gets secret using the kubernetes client
func GetSecret(ctx context.Context, client k8sClient.Client, namespace, secretName string) (*corev1.Secret, error) {
	if namespace == "" {
		return nil, fmt.Errorf("namespace cannot be empty")
	}
	if secretName == "" {
		return nil, fmt.Errorf("secretName cannot be empty")
	}
	secret := &corev1.Secret{}
	key := k8sClient.ObjectKey{
		Namespace: namespace,
		Name:      secretName,
	}
	if err := client.Get(ctx, key, secret); err != nil {
		return nil, err
	}
	return secret, nil
}

func NewPodDisruptionBudget(name, namespace string, maxUnavailable int32, ownerReference []metav1.OwnerReference) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			OwnerReferences: ownerReference,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &intstr.IntOrString{Type: intstr.Int, IntVal: maxUnavailable},
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/component":      "isbsvc",
					"numaflow.numaproj.io/isbsvc-name": name,
				},
			},
		},
	}
}

func ListPodsMetadataOnly(ctx context.Context, c k8sClient.Client, namespace, labels string) (*metav1.PartialObjectMetadataList, error) {
	podsMeta := &metav1.PartialObjectMetadataList{}

	err := KubernetesClient.CoreV1().RESTClient().
		Get().
		Namespace(namespace).
		Resource("pods").
		Param("labelSelector", labels).
		SetHeader("Accept", "application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1").
		Do(ctx).
		Into(podsMeta)

	if err != nil {
		return nil, err
	}

	return podsMeta, nil
}

// isHPA checks if a GVK represents a HorizontalPodAutoscaler
func IsHPA(gvk schema.GroupVersionKind) bool {
	return gvk.Group == "autoscaling" && gvk.Kind == "HorizontalPodAutoscaler"
}
