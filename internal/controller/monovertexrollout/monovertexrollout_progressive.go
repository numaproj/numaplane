package monovertexrollout

import (
	"context"

	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TODO: can move other progressive interface functions here as well? (same with PipelineRollout)

// TODO: fix this assessment not to return an immediate result as soon as things are healthy or unhealthy
func (r *MonoVertexRolloutReconciler) AssessUpgradingChild(ctx context.Context, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, error) {
	numaLogger := logger.FromContext(ctx)
	upgradingObjectStatus, err := kubernetes.ParseStatus(existingUpgradingChildDef)
	if err != nil {
		return apiv1.AssessmentResultUnknown, err
	}

	numaLogger.
		WithValues("namespace", existingUpgradingChildDef.GetNamespace(), "name", existingUpgradingChildDef.GetName()).
		Debugf("Upgrading child is in phase %s", upgradingObjectStatus.Phase)

	if upgradingObjectStatus.Phase == "Running" && progressive.IsNumaflowChildReady(&upgradingObjectStatus) {
		return apiv1.AssessmentResultSuccess, nil
	}
	if upgradingObjectStatus.Phase == "Failed" {
		return apiv1.AssessmentResultFailure, nil
	}
	return apiv1.AssessmentResultUnknown, nil
}
