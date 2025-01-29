package pipelinerollout

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
)

// CreateUpgradingChildDefinition creates a definition for an "upgrading" pipeline
// This implements a function of the progressiveController interface
func (r *PipelineRolloutReconciler) CreateUpgradingChildDefinition(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, name string) (*unstructured.Unstructured, error) {
	numaLogger := logger.FromContext(ctx)

	pipelineRollout := rolloutObject.(*apiv1.PipelineRollout)
	metadata, err := getBasePipelineMetadata(pipelineRollout)
	if err != nil {
		return nil, err
	}

	// which InterstepBufferServiceName should we use?
	// If there is an upgrading isbsvc, use that
	// Otherwise, use the promoted one
	isbsvc, err := r.getISBSvc(ctx, pipelineRollout, common.LabelValueUpgradeInProgress)
	if err != nil {
		return nil, err
	}
	if isbsvc == nil {
		numaLogger.Debugf("no Upgrading isbsvc found for Pipeline, will find promoted one")
		isbsvc, err = r.getISBSvc(ctx, pipelineRollout, common.LabelValueUpgradePromoted)
		if err != nil || isbsvc == nil {
			return nil, fmt.Errorf("failed to find isbsvc that's 'promoted': won't be able to reconcile PipelineRollout, err=%v", err)
		}
	}

	pipeline, err := r.makePipelineDefinition(pipelineRollout, name, isbsvc.GetName(), metadata)
	if err != nil {
		return nil, err
	}

	labels := pipeline.GetLabels()
	labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeInProgress)
	labels[common.LabelKeyISBServiceChildNameForPipeline] = isbsvc.GetName()
	pipeline.SetLabels(labels)

	return pipeline, nil
}

// AssessUpgradingChild makes an assessment of the upgrading child to determine if it was successful, failed, or still not known
// Assessment:
// Success: phase must be "Running" and all conditions must be True
// Failure: phase is "Failed" or any condition is False
// Unknown: neither of the above if met
// This implements a function of the progressiveController interface
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

	if upgradingObjectStatus.Phase == "Failed" || !progressive.IsNumaflowChildReady(&upgradingObjectStatus) {
		return apiv1.AssessmentResultFailure, nil
	}

	return apiv1.AssessmentResultUnknown, nil
}

func scaleDownPromotedPipelineSourceVertices(ctx context.Context, pipelineRolloutObject ctlrcommon.RolloutObject, promotedPipelineDef *unstructured.Unstructured, c client.Client) error {
	numaLogger := logger.FromContext(ctx).WithName("scaleDownPromotedPipelineSourceVertices")

	numaLogger.Debugf("started promoted pipeline source vertices scaling down process")

	promotedPipelines, err := progressive.FindChildrenOfUpgradeState(ctx, pipelineRolloutObject, common.LabelValueUpgradePromoted, true, c)
	if err != nil {
		return fmt.Errorf("error while looking for promoted pipeline: %w", err)
	}

	if len(promotedPipelines.Items) > 1 {
		return errors.New("there should only be one promoted pipeline per PipelineRollout")
	}

	promotedPipeline := promotedPipelines.Items[0]

	vertices, _, err := unstructured.NestedSlice(promotedPipelineDef.Object, "spec", "vertices")
	if err != nil {
		return fmt.Errorf("error while getting vertices of promoted pipeline: %w", err)
	}

	numaLogger.WithValues("promotedPipelineName", promotedPipeline.GetName(), "vertices", vertices).Debugf("found vertices for the promoted pipeline: %d", len(vertices))

	for _, vertex := range vertices {
		if vertexAsMap, ok := vertex.(map[string]any); ok {
			_, found, err := unstructured.NestedMap(vertexAsMap, "source")
			if err != nil {
				return err
			}
			if !found {
				continue
			}

			vertexName, found, err := unstructured.NestedString(vertexAsMap, "name")
			if err != nil {
				return err
			}
			if !found {
				return errors.New("a vertex must have a name")
			}

			pods, err := kubernetes.KubernetesClient.CoreV1().Pods(promotedPipeline.GetNamespace()).List(ctx, metav1.ListOptions{
				LabelSelector: fmt.Sprintf("%s=%s, %s=%s", common.LabelKeyNumaflowPipelineName, promotedPipeline.GetName(), common.LabelKeyNumaflowVertexName, vertexName),
			})
			if err != nil {
				return err
			}

			scaleValue := math.Floor(float64(len(pods.Items)) / float64(2))

			numaLogger.WithValues("promotedPipelineName", promotedPipeline.GetName(), "vertexName", vertexName).Debugf("found %d pod(s) for the source vertex, scaling down to %.0f", len(pods.Items), scaleValue)

			if err := unstructured.SetNestedField(vertexAsMap, scaleValue, "scale", "max"); err != nil {
				return err
			}

			// If scale.min exceeds the new scale.max (scaleValue), reduce also scale.min to scaleValue
			currMin, found, err := unstructured.NestedInt64(vertexAsMap, "scale", "min")
			if err != nil {
				return err
			}
			if found && float64(currMin) > scaleValue {
				if err := unstructured.SetNestedField(vertexAsMap, scaleValue, "scale", "min"); err != nil {
					return err
				}
			}
		}
	}

	err = unstructured.SetNestedSlice(promotedPipelineDef.Object, vertices, "spec", "vertices")
	if err != nil {
		return err
	}

	numaLogger.WithValues("vertices", vertices).Debug("applied updated vertices to promoted pipeline definition")

	return nil
}
