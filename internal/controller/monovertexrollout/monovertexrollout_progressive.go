package monovertexrollout

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"

	argorolloutsv1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
)

// CreateUpgradingChildDefinition creates a definition for an "upgrading" monovertex
// This implements a function of the progressiveController interface
func (r *MonoVertexRolloutReconciler) CreateUpgradingChildDefinition(ctx context.Context, rolloutObject progressive.ProgressiveRolloutObject, name string) (*unstructured.Unstructured, error) {
	monoVertexRollout := rolloutObject.(*apiv1.MonoVertexRollout)
	metadata, err := getBaseMonoVertexMetadata(monoVertexRollout)
	if err != nil {
		return nil, err
	}
	monoVertex, err := r.makeMonoVertexDefinition(monoVertexRollout, name, metadata)
	if err != nil {
		return nil, err
	}

	labels := monoVertex.GetLabels()
	labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeInProgress)
	monoVertex.SetLabels(labels)

	return monoVertex, nil
}

// AssessUpgradingChild makes an assessment of the upgrading child to determine if it was successful, failed, or still not known
// This implements a function of the progressiveController interface
func (r *MonoVertexRolloutReconciler) AssessUpgradingChild(ctx context.Context, rolloutObject progressive.ProgressiveRolloutObject, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, string, error) {
	mvtxRollout := rolloutObject.(*apiv1.MonoVertexRollout)
	analysis := mvtxRollout.GetAnalysis()
	// only check for and create AnalysisRuns if templates are specified
	if len(analysis.Templates) > 0 {
		analysisRun := &argorolloutsv1.AnalysisRun{}
		// check if analysisRun has already been created
		if err := r.client.Get(ctx, client.ObjectKey{Name: existingUpgradingChildDef.GetName(), Namespace: existingUpgradingChildDef.GetNamespace()}, analysisRun); err != nil {
			if apierrors.IsNotFound(err) {
				// analysisRun is created the first time the upgrading child is assessed
				err := progressive.CreateAnalysisRun(ctx, analysis, existingUpgradingChildDef, r.client)
				if err != nil {
					return apiv1.AssessmentResultUnknown, "", err
				}
				analysisStatus := mvtxRollout.GetAnalysisStatus()
				if analysisStatus == nil {
					return apiv1.AssessmentResultUnknown, "", errors.New("analysisStatus not set")
				}
				// analysisStatus is updated with name of AnalysisRun (which is the same name as the upgrading child)
				// and start time for its assessment
				analysisStatus.AnalysisRunName = existingUpgradingChildDef.GetName()
				timeNow := metav1.NewTime(time.Now())
				analysisStatus.StartTime = &timeNow
				mvtxRollout.SetAnalysisStatus(analysisStatus)
			} else {
				return apiv1.AssessmentResultUnknown, "", err
			}
		}

		analysisStatus := mvtxRollout.GetAnalysisStatus()
		if analysisStatus != nil {
			// assess analysisRun status and set endTime if completed
			if analysisRun.Status.Phase.Completed() && analysisStatus.EndTime == nil {
				analysisStatus.EndTime = analysisRun.Status.CompletedAt
			}
			analysisStatus.AnalysisRunName = existingUpgradingChildDef.GetName()
			analysisStatus.Phase = analysisRun.Status.Phase
			mvtxRollout.SetAnalysisStatus(analysisStatus)
		}

	}

	return progressive.AssessUpgradingPipelineType(ctx, mvtxRollout.GetAnalysisStatus(), existingUpgradingChildDef, progressive.AreVertexReplicasReady)

}

/*
ProcessPromotedChildPreUpgrade handles the pre-upgrade processing of a promoted monovertex.
It performs the following pre-upgrade operations:
- it ensures that the promoted monovertex is scaled down before proceeding with a progressive upgrade.

Parameters:
  - ctx: the context for managing request-scoped values.
  - rolloutObject: the MonoVertexRollout instance
  - promotedMonoVertexDef: the definition of the promoted monovertex as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *MonoVertexRolloutReconciler) ProcessPromotedChildPreUpgrade(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	promotedMonoVertexDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessPromotedChildPreUpgrade").WithName("MonoVertexRollout").
		WithValues("promotedMonoVertexNamespace", promotedMonoVertexDef.GetNamespace(), "promotedMonoVertexName", promotedMonoVertexDef.GetName())

	numaLogger.Debug("started pre-upgrade processing of promoted monovertex")
	monoVertexRollout, ok := rolloutObject.(*apiv1.MonoVertexRollout)
	if !ok {
		return true, fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process promoted monovertex pre-upgrade", rolloutObject)
	}

	if monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus == nil {
		return true, errors.New("unable to perform pre-upgrade operations because the rollout does not have promotedChildStatus set")
	}

	// scaleDownMonoVertex either updates the promoted monovertex to scale down the pods or
	// retrieves the currently running pods to update the PromotedMonoVertexStatus scaleValues.
	// This serves to make sure that the pods have been really scaled down before proceeding
	// with the progressive upgrade.
	requeue, err := scaleDownPromotedMonoVertex(ctx, monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus, promotedMonoVertexDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed pre-upgrade processing of promoted monovertex")

	return requeue, nil
}

/*
ProcessUpgradingChildPostFailure handles the failure of an upgrading monovertex (anything specific to MonoVertex)
It performs the following post-failure operations:
- it scales down the upgrading monovertex to 0 pods if it's not already

Parameters:
  - ctx: the context for managing request-scoped values.
  - rolloutObject: the MonoVertexRollout instance
  - upgradingMonoVertexDef: the definition of the existing upgrading monovertex from the beginning of reconciliation
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *MonoVertexRolloutReconciler) ProcessUpgradingChildPostFailure(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingMonoVertexDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessUpgradingChildPostFailure").WithName("MonoVertexRollout").
		WithValues("upgradingMonoVertexNamespace", upgradingMonoVertexDef.GetNamespace(), "upgradingMonoVertexName", upgradingMonoVertexDef.GetName())

	numaLogger.Debug("started post-failure processing of upgrading monovertex")

	// scale down monovertex to 0 Pods
	// need to check to see if it's already scaled down before we do this
	existingSpec := upgradingMonoVertexDef.Object["spec"].(map[string]interface{})

	existingScale, err := getScaleValuesFromMonoVertexSpec(existingSpec)
	if err != nil {
		return true, err
	}

	if existingScale != nil && existingScale.Min != nil && *existingScale.Min == 0 && existingScale.Max != nil && *existingScale.Max == 0 {
		numaLogger.Debug("already scaled down upgrading monovertex to 0, so no need to repeat")
		return false, nil
	}

	// scale the Pods down to 0
	min := int64(0)
	max := int64(0)
	err = scaleMonoVertex(ctx, upgradingMonoVertexDef, &apiv1.ScaleDefinition{Min: &min, Max: &max}, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("scaled down upgrading monovertex to 0, completed post-failure processing of upgrading monovertex")

	return false, nil
}

/*
ProcessUpgradingChildPostSuccess handles an upgrading monovertex that has been deemed successful to be promoted.
It performs the following post-success operations:
- it scales up the monovertex back to the original scale.min and scale.max

Parameters:
  - ctx: the context for managing request-scoped values.
  - rolloutObject: the MonoVertexRollout instance
  - upgradingMonoVertexDef: the definition of the existing upgrading monovertex from the beginning of reconciliation
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - An error if any issues occur during processing.
*/
func (r *MonoVertexRolloutReconciler) ProcessUpgradingChildPostSuccess(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingMonoVertexDef *unstructured.Unstructured,
	c client.Client,
) error {

	numaLogger := logger.FromContext(ctx).WithName("ProcessUpgradingChildPostSuccess").WithName("MonoVertexRollout").
		WithValues("upgradingMonoVertexNamespace", upgradingMonoVertexDef.GetNamespace(), "upgradingMonoVertexName", upgradingMonoVertexDef.GetName())

	numaLogger.Debug("started post-success processing of upgrading monovertex")

	monoVertexRollout, ok := rolloutObject.(*apiv1.MonoVertexRollout)
	if !ok {
		return fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process upgrading monovertex post-success", rolloutObject)
	}

	// Scale the Upgrading MonoVertex back to its original min and max values
	originalScaleMinMax := "null"
	upgradingMonoVertexStatus := monoVertexRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus
	if upgradingMonoVertexStatus == nil {
		numaLogger.Error(errors.New("UpgradingMonoVertexStatus field nil"), "UpgradingMonoVertexStatus is nil; will default scale to null")
	} else {
		originalScaleMinMax = upgradingMonoVertexStatus.OriginalScaleMinMax
		if originalScaleMinMax == "" {
			numaLogger.Error(errors.New("OriginalScaleMinMax unset"), "OriginalScaleMinMax is not set; will default scale to null")
			originalScaleMinMax = "null"
		}
	}

	patchJson := fmt.Sprintf(`{"spec": {"scale": %s}}`, originalScaleMinMax)
	if err := kubernetes.PatchResource(ctx, c, upgradingMonoVertexDef, patchJson, k8stypes.MergePatchType); err != nil {
		return fmt.Errorf("error scaling the existing upgrading monovertex to original values: %w", err)
	}

	numaLogger.WithValues("originalScaleMinMax", originalScaleMinMax).Debug("updated scale values for upgrading monovertex to desired scale values, completed post-success processing of upgrading monovertex")

	return nil
}

/*
ProcessUpgradingChildPreUpgrade handles the processing of an upgrading monovertex definition before it's been created
It performs the following pre-upgrade operations:
- it uses the promoted rollout status scale values to calculate the upgrading monovertex scale min and max.

Parameters:
  - ctx: the context for managing request-scoped values.
  - rolloutObject: the MonoVertexRollout instance
  - upgradingMonoVertexDef: the definition of the upgrading monovertex as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *MonoVertexRolloutReconciler) ProcessUpgradingChildPreUpgrade(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingMonoVertexDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessUpgradingChildPreUpgrade").WithName("MonoVertexRollout").
		WithValues("upgradingMonoVertexNamespace", upgradingMonoVertexDef.GetNamespace(), "upgradingMonoVertexName", upgradingMonoVertexDef.GetName())

	numaLogger.Debug("started pre-upgrade processing of upgrading monovertex")
	monoVertexRollout, ok := rolloutObject.(*apiv1.MonoVertexRollout)
	if !ok {
		return true, fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process upgrading monovertex pre-upgrade", rolloutObject)
	}

	err := scaleDownUpgradingMonoVertex(monoVertexRollout, upgradingMonoVertexDef)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed pre-upgrade processing of upgrading monovertex")

	return false, nil
}

// scaleDownUpgradingMonoVertex sets the upgrading MonoVertex's scale definition to the number of Pods
// that were removed from the promoted MonoVertex
func scaleDownUpgradingMonoVertex(
	monoVertexRollout *apiv1.MonoVertexRollout,
	upgradingMonoVertexDef *unstructured.Unstructured,
) error {
	// Update the scale values of the Upgrading Child, but first save the original scale values
	originalScaleMinMax, err := progressive.ExtractScaleMinMaxAsJSONString(upgradingMonoVertexDef.Object, []string{"spec", "scale"})
	if err != nil {
		return fmt.Errorf("cannot extract the scale min and max values from the upgrading monovertex: %w", err)
	}
	monoVertexRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus.OriginalScaleMinMax = originalScaleMinMax

	if monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus == nil {
		return errors.New("unable to perform pre-upgrade operations because the rollout does not have promotedChildStatus set")
	}

	// There is only one key-value on this map, so we can just iterate over it instead of having to pass the promotedChild name to this func
	for _, scaleValue := range monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus.ScaleValues {
		// Set the upgrading MonoVertex scale.min and scale.max to the number of Pods that were removed
		// from the promoted MonoVertex.
		// This ensures:
		// 1. that the total number of running pods on the "upgrading" monovertex does not change during the upgrade process,
		// which is necessary when performing the health check for "ready replicas >= desired replicas"
		// 2. that the total number of Pods (between the 2 monovertices) before and during upgrade remains the same
		upgradingChildScaleTo := scaleValue.Initial - scaleValue.ScaleTo

		err := unstructured.SetNestedField(upgradingMonoVertexDef.Object, upgradingChildScaleTo, "spec", "scale", "min")
		if err != nil {
			return err
		}

		err = unstructured.SetNestedField(upgradingMonoVertexDef.Object, upgradingChildScaleTo, "spec", "scale", "max")
		if err != nil {
			return err
		}
	}
	return nil
}

/*
ProcessUpgradingChildPostUpgrade handles the processing of an upgrading monovertex definition after it's been created

Parameters:
  - ctx: the context for managing request-scoped values.
  - rolloutObject: the MonoVertexRollout instance
  - upgradingMonoVertexDef: the definition of the upgrading monovertex as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *MonoVertexRolloutReconciler) ProcessUpgradingChildPostUpgrade(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingMonoVertexDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	return false, nil
}

func getScaleValuesFromMonoVertexSpec(monovertexSpec map[string]interface{}) (*apiv1.ScaleDefinition, error) {
	return progressive.ExtractScaleMinMax(monovertexSpec, []string{"scale"})
}

/*
ProcessPromotedChildPostFailure andles the post-upgrade processing of the promoted monovertex after the "upgrading" child has failed.
It performs the following post-upgrade operations:
- it restores the promoted monovertex scale values to the original values retrieved from the rollout status.

Parameters:
  - ctx: the context for managing request-scoped values.
  - rolloutObject: the MonoVertexRollout instance
  - promotedMonoVertexDef: the definition of the promoted monovertex as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *MonoVertexRolloutReconciler) ProcessPromotedChildPostFailure(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	promotedMonoVertexDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessPromotedChildPostFailure").WithName("MonoVertexRollout").
		WithValues("promotedMonoVertexNamespace", promotedMonoVertexDef.GetNamespace(), "promotedMonoVertexName", promotedMonoVertexDef.GetName())

	numaLogger.Debug("started post-failure processing of promoted monovertex")

	monoVertexRollout, ok := rolloutObject.(*apiv1.MonoVertexRollout)
	if !ok {
		return true, fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process promoted monovertex post-upgrade", rolloutObject)
	}

	if monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus == nil {
		return true, errors.New("unable to perform post-upgrade operations because the rollout does not have promotedChildStatus set")
	}

	requeue, err := scalePromotedMonoVertexToOriginalValues(ctx, monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus, promotedMonoVertexDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed post-upgrade processing of promoted monovertex")

	return requeue, nil
}

/*
scaleDownPromotedMonoVertex scales down the pods of a monovertex to half of the current count if not already scaled down.
It checks if the monovertex was already scaled down and skips the operation if true.
The function updates the scale values in the rollout status and adjusts the scale configuration
of the promoted monovertex definition. It ensures that the scale.min does not exceed the new scale.max.
Returns a boolean indicating if scaling was performed and an error if any operation fails.

Parameters:
- ctx: the context for managing request-scoped values.
- promotedMVStatus: the status of the promoted monovertex in the rollout.
- promotedMonoVertexDef: the unstructured object representing the promoted monovertex definition.
- c: the Kubernetes client for resource operations.

Returns:
- bool: true if should requeue, false otherwise. Should requeue in case of error, or if the pods count has changed, or if the monovertex has not been scaled down yet.
- error: an error if any operation fails during the scaling process.
*/
func scaleDownPromotedMonoVertex(
	ctx context.Context,
	promotedMVStatus *apiv1.PromotedMonoVertexStatus,
	promotedMonoVertexDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("scaleDownPromotedMonoVertex").
		WithValues("promotedMonoVertexNamespace", promotedMonoVertexDef.GetNamespace(), "promotedMonoVertexName", promotedMonoVertexDef.GetName())

	// If the monovertex has been scaled down already, do not perform scaling down operations
	if promotedMVStatus.AreAllVerticesScaledDown(promotedMonoVertexDef.GetName()) {
		return false, nil
	}

	scaleValuesMap := map[string]apiv1.ScaleValues{}
	if promotedMVStatus.ScaleValues != nil {
		scaleValuesMap = promotedMVStatus.ScaleValues
	}

	podsList, err := kubernetes.ListPodsMetadataOnly(ctx, c, promotedMonoVertexDef.GetNamespace(), fmt.Sprintf(
		"%s=%s, %s=%s",
		common.LabelKeyNumaflowPodMonoVertexName, promotedMonoVertexDef.GetName(),
		// the vertex name for a monovertex is the same as the monovertex name
		common.LabelKeyNumaflowPodMonoVertexVertexName, promotedMonoVertexDef.GetName(),
	))
	if err != nil {
		return true, err
	}

	currentPodsCount := int64(len(podsList.Items))

	// If for the vertex we already set a ScaleTo value, we only need to update the current pods count
	// to later verify that the pods were actually scaled down.
	// We want to skip scaling down again.
	if vertexScaleValues, exist := scaleValuesMap[promotedMonoVertexDef.GetName()]; exist {
		vertexScaleValues.Current = currentPodsCount
		scaleValuesMap[promotedMonoVertexDef.GetName()] = vertexScaleValues

		promotedMVStatus.ScaleValues = scaleValuesMap
		promotedMVStatus.MarkAllVerticesScaledDown()

		numaLogger.WithValues("scaleValuesMap", scaleValuesMap).Debug("updated scaleValues map with running pods count, skipping scaling down since it has already been done")
		return true, nil
	}

	originalScaleMinMax, err := progressive.ExtractScaleMinMaxAsJSONString(promotedMonoVertexDef.Object, []string{"spec", "scale"})
	if err != nil {
		return true, fmt.Errorf("cannot extract the scale min and max values from the promoted monovertex: %w", err)
	}

	newMin, newMax, err := progressive.CalculateScaleMinMaxValues(promotedMonoVertexDef.Object, int(currentPodsCount), []string{"spec", "scale", "min"})
	if err != nil {
		return true, fmt.Errorf("cannot calculate the scale min and max values: %+w", err)
	}

	numaLogger.WithValues(
		"promotedChildName", promotedMonoVertexDef.GetName(),
		"currentPodsCount", currentPodsCount,
		"newMin", newMin,
		"newMax", newMax,
		"originalScaleMinMax", originalScaleMinMax,
	).Debugf("found %d pod(s) for the monovertex, scaling down to %d", currentPodsCount, newMax)

	scaleValuesMap[promotedMonoVertexDef.GetName()] = apiv1.ScaleValues{
		OriginalScaleMinMax: originalScaleMinMax,
		ScaleTo:             newMax,
		Current:             currentPodsCount,
		Initial:             currentPodsCount,
	}

	if err := scaleMonoVertex(ctx, promotedMonoVertexDef, &apiv1.ScaleDefinition{Min: &newMin, Max: &newMax}, c); err != nil {
		return true, fmt.Errorf("error scaling the existing promoted monovertex to the original scale values: %w", err)
	}

	numaLogger.WithValues("promotedMonoVertexDef", promotedMonoVertexDef, "scaleValuesMap", scaleValuesMap).Debug("patched the promoted monovertex with the new scale configuration")

	promotedMVStatus.ScaleValues = scaleValuesMap
	promotedMVStatus.MarkAllVerticesScaledDown()

	// Set ScaleValuesRestoredToOriginal to false in case previously set to true and now scaling back down to recover from a previous failure
	promotedMVStatus.ScaleValuesRestoredToOriginal = false

	return !promotedMVStatus.AreAllVerticesScaledDown(promotedMonoVertexDef.GetName()), nil
}

/*
scalePromotedMonoVertexToOriginalValues scales a monovertex to its original values based on the rollout status.
This function checks if the monovertex has already been scaled to the original values. If not, it restores the scale values
from the rollout's promoted monovertex status and updates the Kubernetes resource accordingly.

Parameters:
- ctx: the context for managing request-scoped values.
- promotedMVStatus: the status of the promoted monovertex in the rollout, containing scale values.
- promotedMonoVertexDef: the unstructured definition of the promoted monovertex resource.
- c: the Kubernetes client for resource operations.

Returns:
- bool: true if should requeue, false otherwise. Should requeue in case of error or if the monovertex has not been scaled back to original values.
- An error if any issues occur during the scaling process.
*/
func scalePromotedMonoVertexToOriginalValues(
	ctx context.Context,
	promotedMVStatus *apiv1.PromotedMonoVertexStatus,
	promotedMonoVertexDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("scalePromotedMonoVertexToOriginalValues").
		WithValues("promotedMonoVertexNamespace", promotedMonoVertexDef.GetNamespace(), "promotedMonoVertexName", promotedMonoVertexDef.GetName())

	// If the monovertex has been scaled back to the original values already, do not restore scaling values again
	if promotedMVStatus.AreScaleValuesRestoredToOriginal(promotedMonoVertexDef.GetName()) {
		return false, nil
	}

	if promotedMVStatus.ScaleValues == nil {
		return true, errors.New("unable to restore scale values for the promoted monovertex because the rollout does not have promotedChildStatus scaleValues set")
	}

	patchJson := fmt.Sprintf(`{"spec": {"scale": %s}}`, promotedMVStatus.ScaleValues[promotedMonoVertexDef.GetName()].OriginalScaleMinMax)

	if err := kubernetes.PatchResource(ctx, c, promotedMonoVertexDef, patchJson, k8stypes.MergePatchType); err != nil {
		return true, fmt.Errorf("error scaling the existing promoted monovertex to original values: %w", err)
	}

	numaLogger.WithValues("promotedMonoVertexDef", promotedMonoVertexDef).Debug("patched the promoted monovertex with the original scale configuration")

	promotedMVStatus.ScaleValuesRestoredToOriginal = true
	promotedMVStatus.AllVerticesScaledDown = false
	promotedMVStatus.ScaleValues = nil

	return false, nil
}

/*
scaleMonoVertex scales a monovertex to the specified min and max if defined
If either is not defined, it sets the scale definition based on whatever is defined, or null otherwise

Parameters:
- ctx: the context for managing request-scoped values.
- monovertex: the existing monovertex definition
- min: minimum value, or if null, then not defined
- max: maximum value, or if null, then not defined
- c: the Kubernetes client for resource operations.

Returns:
- An error if any issues occur during the scaling process.
*/
func scaleMonoVertex(
	ctx context.Context,
	monovertex *unstructured.Unstructured,
	scaleDefinition *apiv1.ScaleDefinition,
	c client.Client) error {

	scaleValue := scaleDefinitionToPatchString(scaleDefinition)
	patchJson := fmt.Sprintf(`{"spec": {"scale": %s}}`, scaleValue)
	return kubernetes.PatchResource(ctx, c, monovertex, patchJson, k8stypes.MergePatchType)
}

func scaleDefinitionToPatchString(scaleDefinition *apiv1.ScaleDefinition) string {
	var scaleValue string
	if scaleDefinition == nil {
		scaleValue = "null"
	} else if scaleDefinition.Min != nil && scaleDefinition.Max != nil {
		scaleValue = fmt.Sprintf(`{"min": %d, "max": %d}`, *scaleDefinition.Min, *scaleDefinition.Max)
	} else if scaleDefinition.Min != nil {
		scaleValue = fmt.Sprintf(`{"min": %d, "max": null}`, *scaleDefinition.Min)
	} else if scaleDefinition.Max != nil {
		scaleValue = fmt.Sprintf(`{"min": null, "max": %d}`, *scaleDefinition.Max)
	} else {
		scaleValue = `{"min": null, "max": null}`
	}
	return scaleValue
}
