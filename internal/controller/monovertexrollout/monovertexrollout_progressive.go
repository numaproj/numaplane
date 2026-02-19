package monovertexrollout

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/controller/progressive"
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
	labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeTrial)
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
	if childStatus == nil {
		err := mvtxRollout.ResetUpgradingChildStatus(existingUpgradingChildDef)
		if err != nil {
			return "", "", err
		}
	}
	currentTime := time.Now()

	// If a basic assessment result is not yet set, we need to perform basic assessment first
	// (if it is set, it means we already performed basic assessment and can move on to AnalysisRun if needed)
	if !childStatus.IsBasicAssessmentResultSet() {
		// Check if endTime has arrived and basic assessment is not complete yet, in which case we should declare failure
		if currentTime.Sub(childStatus.BasicAssessmentStartTime.Time) > assessmentSchedule.End {
			numaLogger.Debugf("Assessment window ended for upgrading child %s", existingUpgradingChildDef.GetName())
			childStatus.AssessmentResult = apiv1.AssessmentResultFailure
			childStatus.BasicAssessmentEndTime = &metav1.Time{Time: currentTime}
			childStatus.BasicAssessmentResult = apiv1.AssessmentResultFailure
			return apiv1.AssessmentResultFailure, "Basic Resource Health Check failed", nil
		}

		// perform basic resource health check
		assessment, reasonFailure, err := progressive.PerformResourceHealthCheckForPipelineType(ctx, existingUpgradingChildDef, progressive.AreVertexReplicasReady)
		if err != nil {
			return assessment, reasonFailure, err
		}

		// if we fail once, it's okay: we'll check again later
		if assessment == apiv1.AssessmentResultFailure {
			monoVertexChildStatus, err := json.Marshal(existingUpgradingChildDef.Object["status"])
			if err != nil {
				return assessment, reasonFailure, err
			}
			numaLogger.Debugf("Assessment failed for upgrading child %s, checking again...", existingUpgradingChildDef.GetName())
			childStatus.TrialWindowStartTime = nil
			childStatus.AssessmentResult = apiv1.AssessmentResultUnknown
			childStatus.FailureReason = reasonFailure
			childStatus.ChildStatus.Raw = monoVertexChildStatus
			return apiv1.AssessmentResultUnknown, "", nil
		}

		// if we succeed, we must continue to succeed for a prescribed period of time to consider the resource health
		// check "successful".
		if assessment == apiv1.AssessmentResultSuccess {
			if !childStatus.IsTrialWindowStartTimeSet() {
				childStatus.TrialWindowStartTime = &metav1.Time{Time: currentTime}
				childStatus.AssessmentResult = apiv1.AssessmentResultUnknown
				numaLogger.Debugf("Assessment succeeded for upgrading child %s, setting TrialWindowStartTime to %s", existingUpgradingChildDef.GetName(), currentTime)
			}

			// Check if the trial window is set and if the success window has passed.
			if childStatus.IsTrialWindowStartTimeSet() && currentTime.Sub(childStatus.TrialWindowStartTime.Time) >= assessmentSchedule.Period {
				// Success window passed, launch AnalysisRuns or declare success
				childStatus.BasicAssessmentEndTime = &metav1.Time{Time: currentTime}
				childStatus.BasicAssessmentResult = apiv1.AssessmentResultSuccess
				return r.checkAnalysisTemplates(ctx, mvtxRollout, existingUpgradingChildDef)
			}

			numaLogger.Debugf("Assessment succeeded for upgrading child %s, but success window has not passed yet", existingUpgradingChildDef.GetName())
			// Still waiting for a success window to pass
			return apiv1.AssessmentResultUnknown, "", nil
		}
	} else {
		if childStatus.BasicAssessmentResult == apiv1.AssessmentResultSuccess {
			childStatus.ChildStatus.Raw = nil
			childStatus.FailureReason = ""
			return r.checkAnalysisTemplates(ctx, mvtxRollout, existingUpgradingChildDef)
		}
		return childStatus.BasicAssessmentResult, "Basic assessment failed", nil
	}

	return apiv1.AssessmentResultUnknown, "", nil
}

// checkAnalysisTemplates checks if there are any analysis templates to run and runs them if so.
// otherwise it returns success.
func (r *MonoVertexRolloutReconciler) checkAnalysisTemplates(ctx context.Context,
	mvtxRollout *apiv1.MonoVertexRollout,
	existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, string, error) {

	numaLogger := logger.FromContext(ctx)
	analysis := mvtxRollout.GetAnalysis()
	// only check for and create AnalysisRun if templates are specified
	if len(analysis.Templates) > 0 {
		// this will create an AnalysisRun if it doesn't exist yet; or otherwise it will check if it's finished running
		numaLogger.Debugf("Performing analysis for upgrading child %s", existingUpgradingChildDef.GetName())
		analysisStatus, err := progressive.PerformAnalysis(ctx, existingUpgradingChildDef, mvtxRollout, mvtxRollout.GetAnalysis(), mvtxRollout.GetAnalysisStatus(), r.client)
		if err != nil {
			return apiv1.AssessmentResultUnknown, "", err
		}
		assessment, err := progressive.AssessAnalysisStatus(ctx, existingUpgradingChildDef, analysisStatus)
		if err != nil {
			return apiv1.AssessmentResultUnknown, "", err
		}
		return assessment, "", nil
	}
	return apiv1.AssessmentResultSuccess, "", nil
}

// CheckForDifferences checks to see if the monovertex definition matches the spec and the required metadata
func (r *MonoVertexRolloutReconciler) CheckForDifferences(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	monoVertexDef *unstructured.Unstructured,
	requiredSpec map[string]interface{},
	requiredMetadata map[string]interface{},
	existingChildUpgradeState common.UpgradeState) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	var from, to map[string]interface{}
	if err := util.StructToStruct(monoVertexDef.Object["spec"], &from); err != nil {
		return false, err
	}

	if err := util.StructToStruct(requiredSpec["spec"], &to); err != nil {
		return false, err
	}

	// first remove the "replicas" field from the spec since that's a field which Numaflow Controller adds and we don't want to compare it
	removeReplicasFieldFunc := func(spec map[string]interface{}) error {
		excludedPaths := []string{"replicas"}
		util.RemovePaths(spec, excludedPaths, ".")
		return nil
	}
	err := removeReplicasFieldFunc(from)
	if err != nil {
		return false, err
	}
	err = removeReplicasFieldFunc(to)
	if err != nil {
		return false, err
	}

	// During a Progressive Upgrade, we need to be aware of the fact that our promoted and upgrading monovertices have been scaled down,
	// so we need to be careful about how we compare to the target definition

	// If we are comparing to an existing "upgrading" monovertex, we need to re-form its definition from prior to when we
	// rescaled it for Progressive, in order to effectively compare it to the new desired spec
	switch existingChildUpgradeState {
	case common.LabelValueUpgradeTrial:
		monoVertexRollout := rolloutObject.(*apiv1.MonoVertexRollout)
		upgradingMonoVertexStatus := monoVertexRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus
		if upgradingMonoVertexStatus == nil {
			return false, fmt.Errorf("can't CheckForDifferences for MonoVertexRollout %s/%s: upgradingMonoVertexStatus is nil",
				monoVertexRollout.Namespace, monoVertexRollout.Name)
		}
		if upgradingMonoVertexStatus.Name != monoVertexDef.GetName() {
			return false, fmt.Errorf("can't CheckForDifferences for MonoVertexRollout %s/%s: upgradingMonoVertexStatus.Name %s != existing monovertex name %s",
				monoVertexRollout.Namespace, monoVertexRollout.Name, upgradingMonoVertexStatus.Name, monoVertexDef.GetName())
		}

		originalScaleDefinition := upgradingMonoVertexStatus.OriginalScaleDefinition
		// Temporary code for backward compatibility: if OriginalScaleDefinition wasn't set yet (because we just rolled out this change), then we set it to what the Rollout says initially
		// TODO: remove later
		if originalScaleDefinition == "" {
			if to["scale"] == nil {
				originalScaleDefinition = "null"
			} else {
				jsonBytes, err := json.Marshal(to["scale"])
				if err != nil {
					return false, fmt.Errorf("can't CheckForDifferences for MonoVertexRollout %s/%s: error marshaling scale from monovertex: %w",
						monoVertexRollout.Namespace, monoVertexRollout.Name, err)
				}
				originalScaleDefinition = string(jsonBytes)
			}
			numaLogger.Debugf("OriginalScaleDefinition not found in existing MonoVertexRollout status, setting OriginalScaleDefinition to %s", originalScaleDefinition)
			upgradingMonoVertexStatus.OriginalScaleDefinition = originalScaleDefinition
		}

		// replace the entire scale definition in the Rollout-defined spec with upgradingMonoVertexStatus.OriginalScaleDefinition
		if originalScaleDefinition == "null" {
			delete(from, "scale")
		} else {
			var scaleMap map[string]interface{}
			if err := json.Unmarshal([]byte(originalScaleDefinition), &scaleMap); err != nil {
				return false, fmt.Errorf("can't CheckForDifferences for MonoVertexRollout %s/%s: error unmarshaling OriginalScaleDefinition: %w",
					monoVertexRollout.Namespace, monoVertexRollout.Name, err)
			}
			from["scale"] = scaleMap
		}

	case common.LabelValueUpgradePromoted:

		// If we are comparing to an existing "promoted" monovertex, we will just ignore scale altogether

		removeScaleFieldsFunc := func(spec map[string]interface{}) error {

			excludedPaths := []string{"scale.min", "scale.max", "scale.disabled"}

			util.RemovePaths(spec, excludedPaths, ".")

			// if "scale" is there and empty, remove it
			// (this enables accurate comparison between one monovertex with "scale" empty and one with "scale" not present)
			scaleMap, found := spec["scale"].(map[string]interface{})
			if found && len(scaleMap) == 0 {
				unstructured.RemoveNestedField(spec, "scale")
			}

			return nil
		}
		err := removeScaleFieldsFunc(from)
		if err != nil {
			return false, err
		}
		err = removeScaleFieldsFunc(to)
		if err != nil {
			return false, err
		}

	}

	specsEqual := util.CompareStructNumTypeAgnostic(from, to)

	// Check required metadata (labels and annotations)
	requiredLabels, requiredAnnotations := kubernetes.ExtractMetadataSubmaps(requiredMetadata)
	actualLabels := monoVertexDef.GetLabels()
	actualAnnotations := monoVertexDef.GetAnnotations()

	labelsFound := util.IsMapSubset(requiredLabels, actualLabels)
	annotationsFound := util.IsMapSubset(requiredAnnotations, actualAnnotations)
	numaLogger.Debugf("specsEqual: %t, labelsFound=%t, annotationsFound=%v, from=%v, to=%v, requiredLabels=%v, actualLabels=%v, requiredAnnotations=%v, actualAnnotations=%v\n",
		specsEqual, labelsFound, annotationsFound, from, to, requiredLabels, actualLabels, requiredAnnotations, actualAnnotations)

	return !specsEqual || !labelsFound || !annotationsFound, nil

}

// CheckForDifferencesWithRolloutDef tests if there's a meaningful difference between an existing child and the child
// that would be produced by the Rollout definition.
// This implements a function of the progressiveController interface
// In order to do that, it must remove from the check any fields that are manipulated by Numaplane or Numaflow
func (r *MonoVertexRolloutReconciler) CheckForDifferencesWithRolloutDef(ctx context.Context, existingMonoVertex *unstructured.Unstructured, rolloutObject ctlrcommon.RolloutObject, existingChildUpgradeState common.UpgradeState) (bool, error) {
	monoVertexRollout := rolloutObject.(*apiv1.MonoVertexRollout)

	// In order to effectively compare, we need to create a MonoVertex Definition from the MonoVertexRollout which uses the same name as our current MonoVertex
	// (so that won't be interpreted as a difference)
	rolloutBasedMVDef, err := r.makeMonoVertexDefinition(monoVertexRollout, existingMonoVertex.GetName(), monoVertexRollout.Spec.MonoVertex.Metadata)
	if err != nil {
		return false, err
	}

	rolloutDefinedMetadata, _ := rolloutBasedMVDef.Object["metadata"].(map[string]interface{})
	return r.CheckForDifferences(ctx, monoVertexRollout, existingMonoVertex, rolloutBasedMVDef.Object, rolloutDefinedMetadata, existingChildUpgradeState)
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
		return true, fmt.Errorf("unexpected type for ProgressiveRolloutObject in namespace %s: %+v; can't process promoted monovertex pre-upgrade",
			promotedMonoVertexDef.GetNamespace(), rolloutObject)
	}

	if monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus == nil {
		return true, fmt.Errorf("unable to perform pre-upgrade operations for MonoVertexRollout %s/%s because the rollout does not have promotedChildStatus set",
			monoVertexRollout.Namespace, monoVertexRollout.Name)
	}

	requeue, err := computePromotedMonoVertexScaleValues(ctx, monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus, promotedMonoVertexDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed pre-upgrade processing of promoted monovertex")

	return requeue, nil
}

func (r *MonoVertexRolloutReconciler) ProcessPromotedChildPostUpgradeStart(
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
		return true, fmt.Errorf("unexpected type for ProgressiveRolloutObject in namespace %s: %+v; can't process promoted monovertex post-upgrade",
			promotedMonoVertexDef.GetNamespace(), rolloutObject)
	}

	if monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus == nil {
		return true, fmt.Errorf("unable to perform post-upgrade operations for MonoVertexRollout %s/%s because the rollout does not have promotedChildStatus set",
			monoVertexRollout.Namespace, monoVertexRollout.Name)
	}

	// There is only one key-value on this map, so we can just iterate over it instead of having to pass the promotedChild name to this func
	for _, scaleValue := range monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus.ScaleValues {
		if err := setMonoVertexToFixedScale(ctx, promotedMonoVertexDef, scaleValue.ScaleTo, c); err != nil {
			return true, fmt.Errorf("error scaling the existing promoted monovertex %s/%s to the desired scale values: %w",
				promotedMonoVertexDef.GetNamespace(), promotedMonoVertexDef.GetName(), err)
		}

		numaLogger.WithValues("promotedMonoVertexDef", promotedMonoVertexDef, "scaleTo", scaleValue.ScaleTo).Debug("patched the promoted monovertex with the new scale configuration")
	}

	numaLogger.Debug("completed post-upgrade processing of promoted monovertex")

	return false, nil
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
		return true, fmt.Errorf("unexpected type for ProgressiveRolloutObject in namespace %s: %+v; can't process upgrading monovertex pre-upgrade",
			upgradingMonoVertexDef.GetNamespace(), rolloutObject)
	}

	err := scaleDownUpgradingMonoVertex(ctx, monoVertexRollout, upgradingMonoVertexDef)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed pre-upgrade processing of upgrading monovertex")

	return false, nil
}

// scaleDownUpgradingMonoVertex sets the upgrading MonoVertex's scale definition to the number of Pods
// that were removed from the promoted MonoVertex
func scaleDownUpgradingMonoVertex(
	ctx context.Context,
	monoVertexRollout *apiv1.MonoVertexRollout,
	upgradingMonoVertexDef *unstructured.Unstructured,
) error {
	numaLogger := logger.FromContext(ctx)

	// Update the scale values of the Upgrading Child, but first save the original scale values
	originalScaleMinMaxString, err := progressive.ExtractScaleMinMaxAsJSONString(upgradingMonoVertexDef.Object, []string{"spec", "scale"})
	if err != nil {
		return fmt.Errorf("cannot extract the scale min and max values from the upgrading monovertex %s/%s as string: %w",
			upgradingMonoVertexDef.GetNamespace(), upgradingMonoVertexDef.GetName(), err)
	}
	monoVertexRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus.OriginalScaleMinMax = originalScaleMinMaxString
	originalScaleMinMax, err := numaflowtypes.ExtractScaleMinMax(upgradingMonoVertexDef.Object, []string{"spec", "scale"})
	if err != nil {
		return fmt.Errorf("cannot extract the scale min and max values from the upgrading monovertex %s/%s: %w",
			upgradingMonoVertexDef.GetNamespace(), upgradingMonoVertexDef.GetName(), err)
	}

	// set the full OriginalScaleDefinition in the UpgradingMonoVertexStatus as well
	// (this will enable us to compare the Upgrading child to the Rollout definition to see if there are new updates)
	scaleMap := upgradingMonoVertexDef.Object["spec"].(map[string]interface{})["scale"]
	if scaleMap == nil {
		monoVertexRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus.OriginalScaleDefinition = "null"
	} else {
		jsonBytes, err := json.Marshal(scaleMap)
		if err != nil {
			return fmt.Errorf("can't scale down upgrading monovertex %s/%s: error marshaling scale from monovertex: %w",
				upgradingMonoVertexDef.GetNamespace(), upgradingMonoVertexDef.GetName(), err)
		}
		monoVertexRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus.OriginalScaleDefinition = string(jsonBytes)
	}

	// if the new MonoVertex is scaled to zero, we don't rescale it: it's the user's intention that this not be processing any data
	if originalScaleMinMax != nil && originalScaleMinMax.Max != nil && *originalScaleMinMax.Max == 0 {
		numaLogger.Debug("upgrading monovertex is scaled to zero, so no need to scale down")
		return nil
	}

	if monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus == nil {
		return fmt.Errorf("unable to perform pre-upgrade operations for MonoVertexRollout %s/%s because the rollout does not have promotedChildStatus set",
			monoVertexRollout.Namespace, monoVertexRollout.Name)
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
			return fmt.Errorf("error setting scale.min for upgrading monovertex %s/%s: %w",
				upgradingMonoVertexDef.GetNamespace(), upgradingMonoVertexDef.GetName(), err)
		}

		err = unstructured.SetNestedField(upgradingMonoVertexDef.Object, upgradingChildScaleTo, "spec", "scale", "max")
		if err != nil {
			return fmt.Errorf("error setting scale.max for upgrading monovertex %s/%s: %w",
				upgradingMonoVertexDef.GetNamespace(), upgradingMonoVertexDef.GetName(), err)
		}

		err = unstructured.SetNestedField(upgradingMonoVertexDef.Object, false, "spec", "scale", "disabled")
		if err != nil {
			return fmt.Errorf("error setting scale.disabled for upgrading monovertex %s/%s: %w",
				upgradingMonoVertexDef.GetNamespace(), upgradingMonoVertexDef.GetName(), err)
		}
	}
	return nil
}

/*
ProcessUpgradingChildPostUpgradeStart handles the processing of an upgrading monovertex definition after it's been created

Parameters:
  - ctx: the context for managing request-scoped values.
  - rolloutObject: the MonoVertexRollout instance
  - upgradingMonoVertexDef: the definition of the upgrading monovertex as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *MonoVertexRolloutReconciler) ProcessUpgradingChildPostUpgradeStart(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingMonoVertexDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	return false, nil
}

func getScaleValuesFromMonoVertexSpec(monovertexSpec map[string]interface{}) (*apiv1.ScaleDefinition, error) {
	return numaflowtypes.ExtractScaleMinMax(monovertexSpec, []string{"scale"})
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
		return true, fmt.Errorf("unexpected type for ProgressiveRolloutObject in namespace %s: %+v; can't process promoted monovertex post-failure",
			promotedMonoVertexDef.GetNamespace(), rolloutObject)
	}

	if monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus == nil {
		return true, fmt.Errorf("unable to perform post-failure operations for MonoVertexRollout %s/%s because the rollout does not have promotedChildStatus set",
			monoVertexRollout.Namespace, monoVertexRollout.Name)
	}

	// after Upgrading MonoVertex has failed, we need to "roll back" the promoted monovertex, which means setting it to its original scale values
	err := scalePromotedMonoVertexToOriginalRolloutDef(ctx, monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus, promotedMonoVertexDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed post-failure processing of promoted monovertex")

	return false, nil
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
		common.LabelKeyNumaflowMonoVertexName, promotedMonoVertexDef.GetName(),
	))
	if err != nil {
		return true, err
	}

	currentPodsCount := int64(len(podsList.Items))

	originalScaleMinMax, err := progressive.ExtractScaleMinMaxAsJSONString(promotedMonoVertexDef.Object, []string{"spec", "scale"})
	if err != nil {
		return true, fmt.Errorf("cannot extract the scale min and max values from the promoted monovertex %s/%s: %w",
			promotedMonoVertexDef.GetNamespace(), promotedMonoVertexDef.GetName(), err)
	}

	scaleTo := progressive.CalculateScaleMinMaxValues(int(currentPodsCount))

	// if there's an HPA defined for the promoted child, store it in the OriginalHPADefinition field
	hpaGVK := schema.GroupVersionKind{
		Group:   "autoscaling",
		Version: "v2",
		Kind:    "HorizontalPodAutoscalerList",
	}
	hpaList, err := kubernetes.ListResourcesOwnedBy(ctx, c, hpaGVK, promotedMonoVertexDef.GetNamespace(), promotedMonoVertexDef)
	if err != nil {
		return true, fmt.Errorf("failed to list HPAs owned by monovertex %s/%s: %w", promotedMonoVertexDef.GetNamespace(), promotedMonoVertexDef.GetName(), err)
	}

	var originalHPADefinition *runtime.RawExtension
	if len(hpaList.Items) > 0 {
		rawExt, err := kubernetes.UnstructuredToRawExtension(&hpaList.Items[0])
		if err != nil {
			return true, fmt.Errorf("failed to convert HPA to RawExtension: %w", err)
		}
		originalHPADefinition = rawExt
	}

	scaleValuesMap := map[string]apiv1.ScaleValues{}
	scaleValuesMap[promotedMonoVertexDef.GetName()] = apiv1.ScaleValues{
		OriginalScaleMinMax:   originalScaleMinMax,
		OriginalHPADefinition: originalHPADefinition,
		ScaleTo:               scaleTo,
		Initial:               currentPodsCount,
	}

	numaLogger.WithValues(
		"promotedChildName", promotedMonoVertexDef.GetName(),
		"scaleValues", scaleValuesMap[promotedMonoVertexDef.GetName()],
	).Debugf("found %d pod(s) for the monovertex, scaling down to %d", currentPodsCount, scaleTo)

	promotedMVStatus.ScaleValues = scaleValuesMap

	// Set ScaleValuesRestoredToOriginal to false in case previously set to true and now scaling back down to recover from a previous failure
	promotedMVStatus.ScaleValuesRestoredToOriginal = false

	// Requeue if it is the first time that ScaleValues is set so that the reconciliation process will store these
	// values in the rollout status in case of failure with the rest of the progressive operations.
	// This will ensure to always calculate the scaleTo value based on the correct number of pods before actually scaling down.
	return true, nil
}

/*
scalePromotedMonoVertexToOriginalRolloutDef scales a monovertex to its original values based on the rollout status.
This includes both the scale values and the original HPA definition if it was defined (for the case that the HPA is used for scaling instead of numaflow).
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
func scalePromotedMonoVertexToOriginalRolloutDef(
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
		return fmt.Errorf("unable to restore scale values for the promoted monovertex %s/%s because the rollout does not have promotedChildStatus scaleValues set",
			promotedMonoVertexDef.GetNamespace(), promotedMonoVertexDef.GetName())
	}

	patchJson := fmt.Sprintf(`{"spec": {"scale": %s}}`, promotedMVStatus.ScaleValues[promotedMonoVertexDef.GetName()].OriginalScaleMinMax)

	if err := kubernetes.PatchResource(ctx, c, promotedMonoVertexDef, patchJson, k8stypes.MergePatchType); err != nil {
		return fmt.Errorf("error scaling the existing promoted monovertex %s/%s to original values: %w",
			promotedMonoVertexDef.GetNamespace(), promotedMonoVertexDef.GetName(), err)
	}

	numaLogger.WithValues("promotedMonoVertexDef", promotedMonoVertexDef).Debug("patched the promoted monovertex with the original scale configuration")

	// if there was an HPA, we removed it during Progressive Uprade: now we need to restore it so the MonoVertex can dynamically scale again
	originalHPADefinition := promotedMVStatus.ScaleValues[promotedMonoVertexDef.GetName()].OriginalHPADefinition
	if originalHPADefinition != nil {
		// Convert the stored HPA definition back to unstructured
		hpaDef, err := kubernetes.RawExtensionToUnstructured(*originalHPADefinition)
		if err != nil {
			return fmt.Errorf("failed to convert OriginalHPADefinition to unstructured: %w", err)
		}

		// Create the HPA
		numaLogger.Infof("Re-creating HPA %s/%s for monovertex %s", hpaDef.GetNamespace(), hpaDef.GetName(), promotedMonoVertexDef.GetName())
		if err := kubernetes.CreateResource(ctx, c, hpaDef); err != nil {
			if apierrors.IsAlreadyExists(err) {
				numaLogger.Debugf("HPA %s/%s already exists", hpaDef.GetNamespace(), hpaDef.GetName())
			} else {
				return fmt.Errorf("failed to create HPA %s/%s: %w", hpaDef.GetNamespace(), hpaDef.GetName(), err)
			}
		}
	}

	promotedMVStatus.ScaleValuesRestoredToOriginal = true
	promotedMVStatus.ScaleValues = nil

	return nil
}

// setMonoVertexToFixedScale sets a monovertex to a scale where min==max and there is no HPA
// this is necessary during Progressive Upgrade
// HPA must be removed in order for the fixed scaling to work
func setMonoVertexToFixedScale(
	ctx context.Context,
	monovertex *unstructured.Unstructured,
	numReplicas int64,
	c client.Client) error {
	numaLogger := logger.FromContext(ctx)

	scaleDefinition := &apiv1.ScaleDefinition{
		Min:      &numReplicas,
		Max:      &numReplicas,
		Disabled: false,
	}

	// if there's an HPA child of this MonoVertex, delete it
	// Otherwise, it will interfere with numaflow's scaling
	hpaGVK := schema.GroupVersionKind{
		Group:   "autoscaling",
		Version: "v2",
		Kind:    "HorizontalPodAutoscalerList",
	}
	hpaList, err := kubernetes.ListResourcesOwnedBy(ctx, c, hpaGVK, monovertex.GetNamespace(), monovertex)
	if err != nil {
		return fmt.Errorf("failed to list HPAs owned by monovertex %s/%s: %w", monovertex.GetNamespace(), monovertex.GetName(), err)
	}
	for _, hpa := range hpaList.Items { // realistically, there should only be zero or one
		numaLogger.Infof("Deleting HPA %s/%s owned by monovertex %s", hpa.GetNamespace(), hpa.GetName(), monovertex.GetName())
		if err := kubernetes.DeleteResource(ctx, c, &hpa); err != nil {
			return fmt.Errorf("failed to delete HPA %s/%s: %w", hpa.GetNamespace(), hpa.GetName(), err)
		}
	}

	return scaleMonoVertex(ctx, monovertex, scaleDefinition, c)
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
	} else {
		minStr := "null"
		maxStr := "null"
		if scaleDefinition.Min != nil {
			minStr = fmt.Sprintf(`%d`, *scaleDefinition.Min)
		}
		if scaleDefinition.Max != nil {
			maxStr = fmt.Sprintf(`%d`, *scaleDefinition.Max)
		}

		scaleValue = fmt.Sprintf(`{"min": %s, "max": %s, "disabled": %t}`, minStr, maxStr, scaleDefinition.Disabled)

	}
	return scaleValue
}

func (r *MonoVertexRolloutReconciler) progressiveUnsupported(ctx context.Context, rolloutObject progressive.ProgressiveRolloutObject) bool {

	return false
}

// SkipProgressiveAssessment checks if we should skip the progressive assessment and force promote based on the definition of the MonoVertexRollout
func (r *MonoVertexRolloutReconciler) SkipProgressiveAssessment(ctx context.Context, rolloutObject progressive.ProgressiveRolloutObject) (bool, progressive.SkipProgressiveAssessmentReason, error) {
	monoVertexRollout := rolloutObject.(*apiv1.MonoVertexRollout)

	// check if MonoVertex definition is set to Paused or scaled to 0, in which case it can't ingest data (so we skip the assessment as an optimization)
	monoVertexSpecMap := make(map[string]interface{})
	err := util.StructToStruct(monoVertexRollout.Spec.MonoVertex.Spec, &monoVertexSpecMap)
	if err != nil {
		return false, progressive.SkipProgressiveAssessmentReasonUndefined, err
	}

	monoVertexDef := &unstructured.Unstructured{Object: map[string]interface{}{"spec": monoVertexSpecMap}}
	canIngestData, err := numaflowtypes.CanMonoVertexIngestData(ctx, monoVertexDef)
	if err != nil {
		return false, progressive.SkipProgressiveAssessmentReasonUndefined, err
	}

	if !canIngestData {
		return true, progressive.SkipProgressiveAssessmentReasonNoDataIngestion, nil
	}

	// check if ForcePromote is set true in the Progressive strategy
	if monoVertexRollout.GetProgressiveStrategy().ForcePromote {
		return true, progressive.SkipProgressiveAssessmentReasonRolloutConfiguration, nil
	}
	// check if Progressive is unsupported for this Rollout
	if r.progressiveUnsupported(ctx, rolloutObject) {
		return true, progressive.SkipProgressiveAssessmentReasonProgressiveUnsupported, nil
	}
	if !canIngestData {
		return true, progressive.SkipProgressiveAssessmentReasonNoDataIngestion, nil
	}

	return false, progressive.SkipProgressiveAssessmentReasonUndefined, nil

}

func (r *MonoVertexRolloutReconciler) UpdateProgressiveMetrics(rolloutObject progressive.ProgressiveRolloutObject) {
	if rolloutObject.GetUpgradingChildStatus() != nil {
		childName := rolloutObject.GetUpgradingChildStatus().Name
		r.customMetrics.IncMonovertexProgressiveStarted(rolloutObject.GetRolloutObjectMeta().GetNamespace(), rolloutObject.GetRolloutObjectMeta().GetName(), childName)
	}
}
