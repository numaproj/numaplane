package pipelinerollout

import (
	"context"
	"fmt"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Determine if we need to start the Progressive rollout process for this PipelineRollout
// (Either the pipeline requires it or there's an "upgrading" isbsvc)
func (r *PipelineRolloutReconciler) needProgressive(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, newPipelineDef *unstructured.Unstructured, pipelineUpdateRequiringProgressive bool) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	// is there an "upgrading" isbsvc?

	// determine name of the InterstepBufferService by finding the "promoted" isbsvc for the ISBServiceRollout
	isbsvcRollout, err := r.getISBSvcRollout(ctx, pipelineRollout)
	if err != nil || isbsvcRollout == nil {
		return false, fmt.Errorf("unable to find ISBServiceRollout, err=%v", err)
	}

	upgradingISBSvc, err := progressive.FindMostCurrentChildOfUpgradeState(ctx, isbsvcRollout, common.LabelValueUpgradeInProgress, false, r.client)
	if err != nil {
		return false, err
	}

	needProgressive := pipelineUpdateRequiringProgressive || upgradingISBSvc != nil
	numaLogger.Debugf("needProgressive=%t, pipelineUpdateRequiringProgressive=%t, upgrading isbsvc=%t", needProgressive, pipelineUpdateRequiringProgressive, upgradingISBSvc != nil)

	return needProgressive, nil
}
