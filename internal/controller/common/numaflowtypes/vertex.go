package numaflowtypes

import (
	"fmt"

	"github.com/numaproj/numaplane/internal/util"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// AbstractVertex keeps track of minimum number of fields we need to know about in Numaflow's AbstractVertex, which are presumed not to change from version to version
type AbstractVertex struct {
	Name  string `json:"name"`
	Scale Scale  `json:"scale,omitempty"`
}

// Scale keeps track of minimum number of fields we need to know about in Numaflow's Scale struct, which are presumed not to change from version to version
type Scale struct {
	// Minimum replicas.
	Min *int32 `json:"min,omitempty"`
	// Maximum replicas.
	Max *int32 `json:"max,omitempty"`
}

func ExtractScaleMinMax(object map[string]any, pathToScale []string) (*apiv1.ScaleDefinition, error) {

	scaleDef, foundScale, err := unstructured.NestedMap(object, pathToScale...)
	if err != nil {
		return nil, err
	}

	if !foundScale {
		return nil, nil
	}
	scaleMinMax := apiv1.ScaleDefinition{}
	minInterface := scaleDef["min"]
	maxInterface := scaleDef["max"]
	if minInterface != nil {
		min, valid := util.ToInt64(minInterface)
		if !valid {
			return nil, fmt.Errorf("scale min %+v of unexpected type", minInterface)
		}
		scaleMinMax.Min = &min
	}
	if maxInterface != nil {
		max, valid := util.ToInt64(maxInterface)
		if !valid {
			return nil, fmt.Errorf("scale max %+v of unexpected type", maxInterface)
		}
		scaleMinMax.Max = &max
	}

	return &scaleMinMax, nil
}
