package pipelinerollout

import (
	"context"
	"fmt"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Determine if we need to start the Progressive rollout process for this PipelineRollout
// (Either the pipeline requires it or there's an "upgrading" isbsvc)
/*func (r *PipelineRolloutReconciler) needProgressive(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, pipelineUpdateRequiringProgressive bool) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	// is there an "upgrading" isbsvc associated with our ISBServiceRollout, which we're not already using?

	upgradingISBSvc, err := r.getISBSvc(ctx, pipelineRollout, common.LabelValueUpgradeInProgress)
	if err != nil {
		return false, err
	}
	// check if pipeline is already using the "upgrading" isbsvc
	upgradingISBSvcUnused := false
	if upgradingISBSvc != nil {

	}

	needProgressive := pipelineUpdateRequiringProgressive || upgradingISBSvc != nil
	numaLogger.Debugf("needProgressive=%t, pipelineUpdateRequiringProgressive=%t, upgrading isbsvc=%t", needProgressive, pipelineUpdateRequiringProgressive, upgradingISBSvc != nil)

	return needProgressive, nil
}*/

// get the isbsvc child of ISBServiceRollout with the given upgrading state label
func (r *PipelineRolloutReconciler) getISBSvc(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, upgradeState common.UpgradeState) (*unstructured.Unstructured, error) {
	isbsvcRollout, err := r.getISBSvcRollout(ctx, pipelineRollout)
	if err != nil || isbsvcRollout == nil {
		return nil, fmt.Errorf("unable to find ISBServiceRollout, err=%v", err)
	}

	isbsvc, err := progressive.FindMostCurrentChildOfUpgradeState(ctx, isbsvcRollout, upgradeState, false, r.client)
	if err != nil {
		return nil, err
	}
	return isbsvc, nil
}

func (r *PipelineRolloutReconciler) AssessUpgradingChild(ctx context.Context, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, error) {

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