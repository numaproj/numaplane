package isbservicerollout

import (
	"context"
	"fmt"
	"strconv"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func (r *ISBServiceRolloutReconciler) AssessUpgradingChild(ctx context.Context, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, error) {
	numaLogger := logger.FromContext(ctx).WithValues("upgrading child", fmt.Sprintf("%s/%s", existingUpgradingChildDef.GetNamespace(), existingUpgradingChildDef.GetName()))

	// TODO: we need to assess the isbsvc's health, but like with PipelineRollout and MonoVertexRollout,
	// we need to do it over time and not all at once

	// Get the Pipelines using this "upgrading" isbsvc to determine if they're healthy
	// First get all PipelineRollouts using this ISBServiceRollout - need to make sure all have created a Pipeline using this isbsvc
	isbsvcRolloutName, found := existingUpgradingChildDef.GetLabels()[common.LabelKeyParentRollout]
	if !found {
		return apiv1.AssessmentResultUnknown, fmt.Errorf("There is no Label named %q for isbsvc %s/%s; can't make assessment for progressive",
			common.LabelKeyParentRollout, existingUpgradingChildDef.GetNamespace(), existingUpgradingChildDef.GetName())
	}
	// get all PipelineRollouts using this ISBServiceRollout
	pipelineRollouts, err := r.GetPipelineRolloutList(ctx, existingUpgradingChildDef.GetNamespace(), isbsvcRolloutName)
	if err != nil {
		return apiv1.AssessmentResultUnknown, fmt.Errorf("Error getting PipelineRollouts: %s", err.Error())
	}
	if len(pipelineRollouts) == 0 {
		numaLogger.Warn("Found no PipelineRollouts using ISBServiceRollout: so isbsvc is deemed Successful") // not typical but could happen
		return apiv1.AssessmentResultSuccess, nil
	}

	// Get all Pipelines using this "upgrading" isbsvc
	pipelines, err := r.getPipelineListForChildISBSvc(ctx, existingUpgradingChildDef.GetNamespace(), existingUpgradingChildDef.GetName())
	if err != nil {
		return apiv1.AssessmentResultUnknown, fmt.Errorf("Error retrieving pipelines for isbsvc %s/%s; can't make assessment for progressive: %s",
			existingUpgradingChildDef.GetNamespace(), existingUpgradingChildDef.GetName(), err.Error())
	}
	if pipelines == nil {
		numaLogger.Debugf("Can't assess isbsvc; didn't find any pipelines yet using this isbsvc")
		return apiv1.AssessmentResultUnknown, nil
	}
	// map each PipelineRollout to its Pipeline - if we don't have a Pipeline for any of them, then we return "Unknown"
	rolloutToPipeline := make(map[*apiv1.PipelineRollout]*unstructured.Unstructured)
	for _, pipelineRollout := range pipelineRollouts {
		foundPipeline := false
		for _, pipeline := range pipelines.Items {
			if isPipelineChildOfPipelineRollout(pipeline.GetName(), pipelineRollout.Name) {
				rolloutToPipeline[&pipelineRollout] = &pipeline
				foundPipeline = true
				break
			}
		}
		if !foundPipeline {
			numaLogger.Debugf("Can't assess isbsvc; didn't find a Pipeline associated with PipelineRollout %s/%s using this isbsvc",
				pipelineRollout.GetNamespace(), pipelineRollout.GetName())
			return apiv1.AssessmentResultUnknown, nil
		}
	}
	numaLogger.Debugf("found these PipelineRollout/Pipeline pairs for this isbsvc: %+v", rolloutToPipeline)

	// Assess the health of all of the Pipelines
	// if all Pipelines passed the assessment, return Success
	// if any Pipelines failed the assessment, return Failed
	// if any Pipelines are still being assessed, return Unknown
	for pipelineRollout, pipeline := range rolloutToPipeline {

		// Look for this Pipeline in the PipelineRollout's ProgressiveStatus
		if pipelineRollout.Status.ProgressiveStatus.UpgradingChildStatus.Name == pipeline.GetName() {
			switch pipelineRollout.Status.ProgressiveStatus.UpgradingChildStatus.AssessmentResult {
			case apiv1.AssessmentResultFailure:
				return apiv1.AssessmentResultFailure, nil
			case apiv1.AssessmentResultUnknown:
				return apiv1.AssessmentResultUnknown, nil
			}
		} else {
			return apiv1.AssessmentResultUnknown, nil
		}
	}
	return apiv1.AssessmentResultSuccess, nil
}

func isPipelineChildOfPipelineRollout(pipelineName string, pipelineRolloutName string) bool {
	if len(pipelineRolloutName) <= len(pipelineName) {
		return false
	}

	pipelineNamePrefix := pipelineName[:len(pipelineRolloutName)]
	if pipelineNamePrefix != pipelineRolloutName {
		return false
	}

	if pipelineName[len(pipelineRolloutName)] != '-' {
		return false
	}
	pipelineNameSuffix := pipelineName[len(pipelineRolloutName)+1:]
	_, err := strconv.Atoi(pipelineNameSuffix)
	return err == nil
}
