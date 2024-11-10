package common

import (
	"encoding/json"

	"github.com/numaproj/numaplane/internal/util/kubernetes"
)

type MonoVertexStatus = kubernetes.GenericStatus

func ParseMonoVertexStatus(obj *kubernetes.GenericObject) (MonoVertexStatus, error) {
	if obj == nil || len(obj.Status.Raw) == 0 {
		return MonoVertexStatus{}, nil
	}

	var status MonoVertexStatus
	err := json.Unmarshal(obj.Status.Raw, &status)
	if err != nil {
		return MonoVertexStatus{}, err
	}

	return status, nil
}
