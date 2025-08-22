package monovertexrollout

import (
	"context"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/usde"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"

	"github.com/numaproj/numaplane/internal/common"
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
func (r *MonoVertexRolloutReconciler) AssessUpgradingChild(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	existingUpgradingChildDef *unstructured.Unstructured,
	assessmentSchedule config.AssessmentSchedule) (apiv1.AssessmentResult, string, error) {

	numaLogger := logger.FromContext(ctx)
	mvtxRollout := rolloutObject.(*apiv1.MonoVertexRollout)
	childStatus := mvtxRollout.GetUpgradingChildStatus()
	currentTime := time.Now()

	// Check if endTime has arrived and basic assessment is not complete yet, in which case we should declare failure
	if currentTime.Sub(childStatus.BasicAssessmentStartTime.Time) > assessmentSchedule.End && !childStatus.IsBasicAssessmentResultSet() {
		numaLogger.Debugf("Assessment window ended for upgrading child %s", existingUpgradingChildDef.GetName())
		_ = progressive.UpdateUpgradingChildStatus(mvtxRollout, func(status *apiv1.UpgradingChildStatus) {
			status.AssessmentResult = apiv1.AssessmentResultFailure
			status.BasicAssessmentEndTime = &metav1.Time{Time: currentTime}
		})
		return apiv1.AssessmentResultFailure, "Assessment window ended", nil
	}

	// First perform basic resource health check
	assessment, reasonFailure, err := progressive.PerformResourceHealthCheckForPipelineType(ctx, existingUpgradingChildDef, progressive.AreVertexReplicasReady)
	if err != nil {
		return assessment, reasonFailure, err
	}
	// if we fail once, it's okay: we'll check again later
	if assessment == apiv1.AssessmentResultFailure {
		numaLogger.Debugf("Assessment failed for upgrading child %s, checking again...", existingUpgradingChildDef.GetName())
		_ = progressive.UpdateUpgradingChildStatus(mvtxRollout, func(status *apiv1.UpgradingChildStatus) {
			status.TrialWindowStartTime = nil
			status.AssessmentResult = apiv1.AssessmentResultUnknown
		})

		return assessment, reasonFailure, nil
	}
	// if we succeed, we must continue to succeed for a prescribed period of time to consider the resource health
	// check "successful"
	if assessment == apiv1.AssessmentResultSuccess {
		if !childStatus.IsTrialWindowStartTimeSet() {
			_ = progressive.UpdateUpgradingChildStatus(mvtxRollout, func(status *apiv1.UpgradingChildStatus) {
				status.TrialWindowStartTime = &metav1.Time{Time: currentTime}
				status.AssessmentResult = apiv1.AssessmentResultUnknown
			})
			numaLogger.Debugf("Assessment succeeded for upgrading child %s, setting TrialWindowStartTime to %s", existingUpgradingChildDef.GetName(), currentTime)
		}

		// if end time has arrived (i.e., we continually determined "Success" for the entire prescribed period of time),
		// if we need to launch an AnalysisRun, we can do it now;
		// otherwise, we can declare success
		if childStatus.IsTrialWindowStartTimeSet() && currentTime.Sub(childStatus.TrialWindowStartTime.Time) >= assessmentSchedule.Period {
			// Success window passed, launch AnalysisRun or declared success
			_ = progressive.UpdateUpgradingChildStatus(mvtxRollout, func(status *apiv1.UpgradingChildStatus) {
				status.BasicAssessmentEndTime = &metav1.Time{Time: currentTime}
				status.BasicAssessmentResult = apiv1.AssessmentResultSuccess
			})
			analysis := mvtxRollout.GetAnalysis()
			// only check for and create AnalysisRun if templates are specified
			if len(analysis.Templates) > 0 {
				// this will create an AnalysisRun if it doesn't exist yet; or otherwise it will check if it's finished running
				analysisStatus, err := progressive.PerformAnalysis(ctx, existingUpgradingChildDef, mvtxRollout, mvtxRollout.GetAnalysis(), mvtxRollout.GetAnalysisStatus(), r.client)
				if err != nil {
					return apiv1.AssessmentResultUnknown, "", err
				}
				return progressive.AssessAnalysisStatus(ctx, existingUpgradingChildDef, analysisStatus)
			} else {
				return apiv1.AssessmentResultSuccess, "", nil
			}
		}
		numaLogger.Debugf("Assessment succeeded for upgrading child %s, but success window has not passed yet", existingUpgradingChildDef.GetName())
		// Still waiting for a success window to pass
		return apiv1.AssessmentResultUnknown, "", nil
	}

	return apiv1.AssessmentResultUnknown, "", nil

}

// CheckForDifferences tests for essential equality.
// This implements a function of the progressiveController interface, used to determine if a previously Upgrading MonoVertex
// should be replaced with a new one.
// What should a user be able to update to cause this?: Ideally, they should be able to change any field if they need to and not just those that are
// configured as "progressive", in the off chance that changing one of those fixes a problem.
// However, we need to exclude any field that Numaplane or another platform changes, or it will confuse things.
func (r *MonoVertexRolloutReconciler) CheckForDifferences(ctx context.Context, from, to *unstructured.Unstructured) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	// remove certain fields (which numaplane needs to set) from comparison to test for equality
	removeFunc := func(monoVertex *unstructured.Unstructured) (map[string]interface{}, error) {
		var specAsMap map[string]any

		if err := util.StructToStruct(monoVertex.Object["spec"], &specAsMap); err != nil {
			return nil, err
		}

		excludedPaths := []string{"replicas", "scale.min", "scale.max"}
		util.RemovePaths(specAsMap, excludedPaths, ".")

		// if "scale" is there and empty, remove it
		// (this enables accurate comparison between one monovertex with "scale" empty and one with "scale" not present)
		scaleMap, found := specAsMap["scale"].(map[string]interface{})
		if found && len(scaleMap) == 0 {
			unstructured.RemoveNestedField(specAsMap, "scale")
		}

		return specAsMap, nil
	}

	fromNew, err := removeFunc(from)
	if err != nil {
		return false, err
	}
	toNew, err := removeFunc(to)
	if err != nil {
		return false, err
	}

	specsEqual := util.CompareStructNumTypeAgnostic(fromNew, toNew)
	// just look specifically for metadata fields that can result in Progressive
	// anything else could be updated by some platform and not by the user, which would cause an issue
	metadataRisk := usde.ResourceMetadataHasDataLossRisk(ctx, from, to)
	numaLogger.Debugf("specsEqual: %t, metadataRisk=%t, from=%v, to=%v\n",
		specsEqual, metadataRisk, fromNew, toNew)

	return !specsEqual || metadataRisk, nil

}

/*
ProcessPromotedChildPreUpgrade handles the pre-upgrade processing of a promoted monovertex.
It performs the following pre-upgrade operations:
- it calculates how to scale down the promoted monovertex before proceeding with a progressive upgrade.

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

	requeue, err := computePromotedMonoVertexScaleValues(ctx, monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus, promotedMonoVertexDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed pre-upgrade processing of promoted monovertex")

	return requeue, nil
}

func (r *MonoVertexRolloutReconciler) ProcessPromotedChildPostUpgrade(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	promotedMonoVertexDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessPromotedChildPostUpgrade").WithName("MonoVertexRollout").
		WithValues("promotedMonoVertexNamespace", promotedMonoVertexDef.GetNamespace(), "promotedMonoVertexName", promotedMonoVertexDef.GetName())

	numaLogger.Debug("started post-upgrade processing of promoted monovertex")

	monoVertexRollout, ok := rolloutObject.(*apiv1.MonoVertexRollout)
	if !ok {
		return true, fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process promoted monovertex post-upgrade", rolloutObject)
	}

	if monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus == nil {
		return true, errors.New("unable to perform post-upgrade operations because the rollout does not have promotedChildStatus set")
	}

	// There is only one key-value on this map, so we can just iterate over it instead of having to pass the promotedChild name to this func
	for _, scaleValue := range monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus.ScaleValues {
		if err := scaleMonoVertex(ctx, promotedMonoVertexDef, &apiv1.ScaleDefinition{Min: &scaleValue.ScaleTo, Max: &scaleValue.ScaleTo}, c); err != nil {
			return true, fmt.Errorf("error scaling the existing promoted monovertex to the desired scale values: %w", err)
		}

		numaLogger.WithValues("promotedMonoVertexDef", promotedMonoVertexDef, "scaleTo", scaleValue.ScaleTo).Debug("patched the promoted monovertex with the new scale configuration")
	}

	numaLogger.Debug("completed post-upgrade processing of promoted monovertex")

	return false, nil
}

// ProcessPromotedChildPreRecycle processes the Promoted child directly prior to it being recycled
// (due to being replaced by a new Promoted child)
func (r *MonoVertexRolloutReconciler) ProcessPromotedChildPreRecycle(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	promotedMonoVertexDef *unstructured.Unstructured,
	c client.Client,
) error {
	return nil
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
	originalScaleMinMaxString, err := progressive.ExtractScaleMinMaxAsJSONString(upgradingMonoVertexDef.Object, []string{"spec", "scale"})
	if err != nil {
		return fmt.Errorf("cannot extract the scale min and max values from the upgrading monovertex as string: %w", err)
	}
	monoVertexRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus.OriginalScaleMinMax = originalScaleMinMaxString
	originalScaleMinMax, err := progressive.ExtractScaleMinMax(upgradingMonoVertexDef.Object, []string{"spec", "scale"})
	if err != nil {
		return fmt.Errorf("cannot extract the scale min and max values from the upgrading monovertex: %w", err)
	}

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
		// if for some reason there were no Pods running in the promoted MonoVertex at the time (i.e. maybe some failure) and the Max was not set to 0 explicitly,
		// then we don't want to set our Pods to 0 so set to 1 at least
		maxZero := originalScaleMinMax != nil && originalScaleMinMax.Max != nil && *originalScaleMinMax.Max == 0
		if upgradingChildScaleTo <= 0 && !maxZero {
			upgradingChildScaleTo = 1
		}

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

	err := scalePromotedMonoVertexToOriginalValues(ctx, monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus, promotedMonoVertexDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed post-upgrade processing of promoted monovertex")

	return false, nil
}

// ProcessUpgradingChildPreRecycle processes the Upgrading child directly prior to it being recycled
// (due to being replaced by a new Upgrading child)
func (r *MonoVertexRolloutReconciler) ProcessUpgradingChildPreRecycle(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingMonoVertexDef *unstructured.Unstructured,
	c client.Client,
) error {
	return nil
}

/*
computePromotedMonoVertexScaleValues creates the apiv1.ScaleValues to be stored in the MonoVertexRollout
before performing the actually scaling down of the promoted monovertex.
It checks if the ScaleValues have been already stored and skips the operation if true.

Parameters:
- ctx: the context for managing request-scoped values.
- promotedMVStatus: the status of the promoted monovertex in the rollout.
- promotedMonoVertexDef: the unstructured object representing the promoted monovertex definition.
- c: the Kubernetes client for resource operations.

Returns:
- bool: true if should requeue, false otherwise. Should requeue in case of error or to store the computed ScaleValues.
- error: an error if any operation fails during the scaling process.
*/
func computePromotedMonoVertexScaleValues(
	ctx context.Context,
	promotedMVStatus *apiv1.PromotedMonoVertexStatus,
	promotedMonoVertexDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("computePromotedMonoVertexScaleValues").
		WithValues("promotedMonoVertexNamespace", promotedMonoVertexDef.GetNamespace(), "promotedMonoVertexName", promotedMonoVertexDef.GetName())

	if promotedMVStatus.ScaleValues != nil {
		return false, nil
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

	originalScaleMinMax, err := progressive.ExtractScaleMinMaxAsJSONString(promotedMonoVertexDef.Object, []string{"spec", "scale"})
	if err != nil {
		return true, fmt.Errorf("cannot extract the scale min and max values from the promoted monovertex: %w", err)
	}

	scaleTo := progressive.CalculateScaleMinMaxValues(int(currentPodsCount))
	newMin := scaleTo
	newMax := scaleTo

	numaLogger.WithValues(
		"promotedChildName", promotedMonoVertexDef.GetName(),
		"newMin", newMin,
		"newMax", newMax,
		"originalScaleMinMax", originalScaleMinMax,
	).Debugf("found %d pod(s) for the monovertex, scaling down to %d", currentPodsCount, newMax)

	scaleValuesMap := map[string]apiv1.ScaleValues{}
	scaleValuesMap[promotedMonoVertexDef.GetName()] = apiv1.ScaleValues{
		OriginalScaleMinMax: originalScaleMinMax,
		ScaleTo:             scaleTo,
		Initial:             currentPodsCount,
	}

	promotedMVStatus.ScaleValues = scaleValuesMap

	// Set ScaleValuesRestoredToOriginal to false in case previously set to true and now scaling back down to recover from a previous failure
	promotedMVStatus.ScaleValuesRestoredToOriginal = false

	// Requeue if it is the first time that ScaleValues is set so that the reconciliation process will store these
	// values in the rollout status in case of failure with the rest of the progressive operations.
	// This will ensure to always calculate the scaleTo value based on the correct number of pods before actually scaling down.
	return true, nil
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
) error {

	numaLogger := logger.FromContext(ctx).WithName("scalePromotedMonoVertexToOriginalValues").
		WithValues("promotedMonoVertexNamespace", promotedMonoVertexDef.GetNamespace(), "promotedMonoVertexName", promotedMonoVertexDef.GetName())

	// If the monovertex has been scaled back to the original values already, do not restore scaling values again
	if promotedMVStatus.AreScaleValuesRestoredToOriginal(promotedMonoVertexDef.GetName()) {
		return nil
	}

	if promotedMVStatus.ScaleValues == nil {
		return errors.New("unable to restore scale values for the promoted monovertex because the rollout does not have promotedChildStatus scaleValues set")
	}

	patchJson := fmt.Sprintf(`{"spec": {"scale": %s}}`, promotedMVStatus.ScaleValues[promotedMonoVertexDef.GetName()].OriginalScaleMinMax)

	if err := kubernetes.PatchResource(ctx, c, promotedMonoVertexDef, patchJson, k8stypes.MergePatchType); err != nil {
		return fmt.Errorf("error scaling the existing promoted monovertex to original values: %w", err)
	}

	numaLogger.WithValues("promotedMonoVertexDef", promotedMonoVertexDef).Debug("patched the promoted monovertex with the original scale configuration")

	promotedMVStatus.ScaleValuesRestoredToOriginal = true
	promotedMVStatus.ScaleValues = nil

	return nil
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
