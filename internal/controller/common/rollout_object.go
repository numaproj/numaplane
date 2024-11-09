package common

import (
	"fmt"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

type RolloutObject interface {
	GetChildPluralName() string

	GetTypeMeta() *metav1.TypeMeta

	GetObjectMeta() *metav1.ObjectMeta

	GetStatus() *apiv1.Status
}

// assume child name is "<rolloutname>-<number>"
func GetRolloutParentName(childName string) (string, error) {

	index := strings.LastIndex(childName, "-")
	if index > 0 && index < len(childName)-1 {
		_, err := strconv.Atoi(childName[index+1:])
		if err != nil {
			return childName[:index], nil
		}
	}
	return "", fmt.Errorf("unexpected child name %q doesn't end with '-<number>'", childName)
}
