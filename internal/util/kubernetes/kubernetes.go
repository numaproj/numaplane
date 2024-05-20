package kubernetes

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/argoproj/gitops-engine/pkg/utils/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"
	k8sClient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/util/logger"
)

// validManifestExtensions contains the supported extension for raw file.
var validManifestExtensions = map[string]struct{}{"yaml": {}, "yml": {}, "json": {}}

func IsValidKubernetesNamespace(name string) bool {
	// All namespace names must be valid RFC 1123 DNS labels.
	errs := validation.IsDNS1123Label(name)
	reservedNamesRegex := regexp.MustCompile(`^(kubernetes-|kube-)`)
	if len(errs) == 0 && !reservedNamesRegex.MatchString(name) {
		return true
	}
	return false
}

// GetGitSyncInstanceLabel returns the application instance name from label
func GetGitSyncInstanceLabel(un *unstructured.Unstructured, key string) (string, error) {
	labels, err := nestedNullableStringMap(un.Object, "metadata", "labels")
	if err != nil {
		return "", fmt.Errorf("failed to get labels from target object %s %s/%s: %w", un.GroupVersionKind().String(), un.GetNamespace(), un.GetName(), err)
	}
	if labels != nil {
		return labels[key], nil
	}
	return "", nil
}

// SetGitSyncInstanceLabel sets the recommended app.kubernetes.io/instance label against an unstructured object
func SetGitSyncInstanceLabel(target *unstructured.Unstructured, key, val string) error {

	labels, err := nestedNullableStringMap(target.Object, "metadata", "labels")
	if err != nil {
		return fmt.Errorf("failed to get labels from target object %s %s/%s: %w", target.GroupVersionKind().String(), target.GetNamespace(), target.GetName(), err)
	}
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[key] = val
	target.SetLabels(labels)

	return nil
}

// nestedNullableStringMap returns a copy of map[string]string value of a nested field.
// Returns an error if not one of map[string]interface{} or nil, or contains non-string values in the map.
func nestedNullableStringMap(obj map[string]interface{}, fields ...string) (map[string]string, error) {
	var m map[string]string
	val, found, err := unstructured.NestedFieldNoCopy(obj, fields...)
	if err != nil {
		return nil, err
	}
	if found && val != nil {
		val, _, err := unstructured.NestedStringMap(obj, fields...)
		return val, err
	}
	return m, err
}

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

func DeleteKubernetesResource(ctx context.Context, client k8sClient.Client, item k8sClient.Object) error {
	numaLogger := logger.FromContext(ctx)
	if err := client.Delete(ctx, item); err != nil {
		if apierrors.IsNotFound(err) {
			numaLogger.Info("Object not found", item)
			return nil
		}
		return fmt.Errorf("error deleting resource %s/%s: %v", item.GetNamespace(), item.GetName(), err)
	}
	return nil
}

func IsValidKubernetesManifestFile(fileName string) bool {
	fileExt := strings.Split(fileName, ".")
	if _, ok := validManifestExtensions[fileExt[len(fileExt)-1]]; ok {
		return true
	}
	return false
}

// DeleteManagedObjects deletes Kubernetes resources from a map sequentially, returning an error if any deletion fails.
func DeleteManagedObjects(ctx context.Context, client k8sClient.Client, objs map[kube.ResourceKey]*unstructured.Unstructured) error {
	for _, obj := range objs {
		if err := DeleteKubernetesResource(ctx, client, obj); err != nil {
			return err
		}
	}
	return nil
}
