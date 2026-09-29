/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package numaflowcontrollerrollout

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/controller/common/riders"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// instanceIDDisallowedChars matches everything that may not appear in a derived controller instance ID.
// The instance ID becomes a suffix of the Numaflow Service names ("numaflow-server-<instance>"), which are
// RFC 1035 labels: lowercase letters, digits, and dashes only. Dots in a version string would be rejected.
var instanceIDDisallowedChars = regexp.MustCompile(`[^a-z0-9]+`)

// maxControllerInstanceIDLength keeps the longest templated Service name, "numaflow-dex-server-<instance>",
// within the 63 character limit of an RFC 1035 label
const maxControllerInstanceIDLength = validation.DNS1035LabelMaxLength - len("numaflow-dex-server-")

// DeriveControllerInstanceID returns the instance ID for a trial NumaflowController child named childName
// ("<rolloutName>-<nameCount>") running version. The result is "<sanitized version>-<nameCount>", so a later
// trial of the same version, after a failed one was recycled, still gets a distinct instance.
// It returns "" when childName does not end in "-<number>": that is the name of the controller which
// predates Progressive, and it keeps its empty instance.
func DeriveControllerInstanceID(version string, childName string) string {
	index := strings.LastIndex(childName, "-")
	if index <= 0 || index == len(childName)-1 {
		return ""
	}
	nameCount, err := strconv.Atoi(childName[index+1:])
	if err != nil {
		return ""
	}

	suffix := fmt.Sprintf("-%d", nameCount)
	sanitizedVersion := strings.Trim(instanceIDDisallowedChars.ReplaceAllString(strings.ToLower(version), "-"), "-")
	if maxVersionLen := maxControllerInstanceIDLength - len(suffix); len(sanitizedVersion) > maxVersionLen {
		sanitizedVersion = strings.Trim(sanitizedVersion[:maxVersionLen], "-")
	}
	if sanitizedVersion == "" {
		return fmt.Sprintf("%d", nameCount)
	}
	return sanitizedVersion + suffix
}

// CreateUpgradingChildDefinition creates a NumaflowController in an "upgrading" state with the given name
// This implements a function of the progressiveController interface
func (r *NumaflowControllerRolloutReconciler) CreateUpgradingChildDefinition(ctx context.Context, rolloutObject progressive.ProgressiveRolloutObject, name string) (*unstructured.Unstructured, error) {
	nfcRollout := rolloutObject.(*apiv1.NumaflowControllerRollout)
	instanceID := DeriveControllerInstanceID(nfcRollout.Spec.Controller.Version, name)
	return makeNumaflowControllerDefinition(nfcRollout, name, instanceID, common.LabelValueUpgradeTrial)
}

// AssessUpgradingChild makes an assessment of the upgrading child to determine if it was successful, failed, or still not known
// This implements a function of the progressiveController interface
//
// The trial controller instance itself must be healthy (its Deployment rolled out). Beyond that, the assessment
// follows the same shape as ISBServiceRollout's: every ISBServiceRollout and MonoVertexRollout in the namespace
// is expected to run its own trial on this controller instance, and the controller is successful only once all
// of them have succeeded. Any dependent whose trial on this instance failed fails the controller.
func (r *NumaflowControllerRolloutReconciler) AssessUpgradingChild(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	existingUpgradingChildDef *unstructured.Unstructured,
	assessmentSchedule config.AssessmentSchedule) (apiv1.AssessmentResult, string, error) {

	nfcRollout := rolloutObject.(*apiv1.NumaflowControllerRollout)

	numaLogger := logger.FromContext(ctx).WithValues("numaflowcontroller", existingUpgradingChildDef.GetName())
	ctx = logger.WithLogger(ctx, numaLogger)

	// get childStatus and set if nil
	childStatus := nfcRollout.GetUpgradingChildStatus()
	if childStatus == nil {
		if err := nfcRollout.ResetUpgradingChildStatus(existingUpgradingChildDef); err != nil {
			return apiv1.AssessmentResultUnknown, "", err
		}
		childStatus = nfcRollout.GetUpgradingChildStatus()
	}

	assessmentResult, failureReasons, err := assessNumaflowControllerHealth(existingUpgradingChildDef)
	if err != nil {
		return apiv1.AssessmentResultUnknown, "", err
	}
	if assessmentResult == apiv1.AssessmentResultSuccess {
		// the controller itself is healthy; now it must prove itself against the workloads
		instanceID, _, _ := unstructured.NestedString(existingUpgradingChildDef.Object, "spec", "instanceID")
		var failedDependents []string
		assessmentResult, failedDependents, err = r.assessDependents(ctx, existingUpgradingChildDef.GetNamespace(), instanceID)
		if err != nil {
			return apiv1.AssessmentResultUnknown, "", err
		}
		for _, failedDependent := range failedDependents {
			failureReasons = append(failureReasons, fmt.Sprintf("%s failed", failedDependent))
		}
	}

	if assessmentResult != apiv1.AssessmentResultUnknown {
		assessmentEndTime := metav1.NewTime(time.Now())
		childStatus.BasicAssessmentEndTime = &assessmentEndTime
	}
	switch assessmentResult {
	case apiv1.AssessmentResultFailure:
		rawStatus, err := json.Marshal(existingUpgradingChildDef.Object["status"])
		if err != nil {
			return assessmentResult, "", err
		}
		childStatus.FailureReasons = failureReasons
		childStatus.ChildStatus.Raw = rawStatus
	case apiv1.AssessmentResultSuccess:
		// clear values for childStatus and failureReason if previously set
		childStatus.ChildStatus.Raw = nil
		childStatus.FailureReasons = nil
	}
	nfcRollout.SetUpgradingChildStatus(childStatus)

	return assessmentResult, strings.Join(failureReasons, "; "), nil
}

// assessNumaflowControllerHealth assesses the NumaflowController resource on its own:
// Success: it has been reconciled at its current generation and its child resources (Deployment) are healthy
// Failure: its phase is Failed, or its child resources are unhealthy for a reason other than still progressing
// Unknown: otherwise
func assessNumaflowControllerHealth(numaflowController *unstructured.Unstructured) (apiv1.AssessmentResult, []string, error) {
	genericStatus, err := kubernetes.ParseStatus(numaflowController)
	if err != nil {
		return apiv1.AssessmentResultUnknown, nil, fmt.Errorf("failed to parse Status from NumaflowController CR %s/%s: %w", numaflowController.GetNamespace(), numaflowController.GetName(), err)
	}
	var nfcStatus apiv1.NumaflowControllerStatus
	if err := util.StructToStruct(genericStatus, &nfcStatus); err != nil {
		return apiv1.AssessmentResultUnknown, nil, fmt.Errorf("failed to convert NumaflowController Status for %s/%s: %w", numaflowController.GetNamespace(), numaflowController.GetName(), err)
	}

	if nfcStatus.Phase == apiv1.PhaseFailed {
		return apiv1.AssessmentResultFailure, []string{fmt.Sprintf("NumaflowController phase is %s: %s", nfcStatus.Phase, nfcStatus.Message)}, nil
	}

	healthyChildCond := nfcStatus.GetCondition(apiv1.ConditionChildResourceHealthy)
	if healthyChildCond != nil && healthyChildCond.Status == metav1.ConditionFalse && healthyChildCond.Reason != apiv1.ProgressingReasonString {
		return apiv1.AssessmentResultFailure, []string{fmt.Sprintf("condition %s is False for Reason %q: %s", healthyChildCond.Type, healthyChildCond.Reason, healthyChildCond.Message)}, nil
	}

	reconciled := numaflowController.GetGeneration() <= nfcStatus.ObservedGeneration
	if reconciled && healthyChildCond != nil && healthyChildCond.Status == metav1.ConditionTrue {
		return apiv1.AssessmentResultSuccess, nil, nil
	}
	return apiv1.AssessmentResultUnknown, nil, nil
}

// assessDependents assesses the ISBServiceRollouts and MonoVertexRollouts in the namespace against the trial controller
// instance. Each must have an upgrading child bound to instanceID; the controller is successful once all of them have
// succeeded, and failed as soon as one of them has failed.
// Return the AssessmentResult and, if it failed, the names of the dependents that failed.
func (r *NumaflowControllerRolloutReconciler) assessDependents(ctx context.Context, namespace string, instanceID string) (apiv1.AssessmentResult, []string, error) {
	numaLogger := logger.FromContext(ctx)

	type dependent struct {
		kind            string
		name            string
		childGVK        schema.GroupVersionKind
		upgradingStatus *apiv1.UpgradingChildStatus
	}
	var dependents []dependent

	var isbServiceRollouts apiv1.ISBServiceRolloutList
	if err := r.client.List(ctx, &isbServiceRollouts, &client.ListOptions{Namespace: namespace}); err != nil {
		return apiv1.AssessmentResultUnknown, nil, fmt.Errorf("error listing ISBServiceRollouts: %w", err)
	}
	for i := range isbServiceRollouts.Items {
		isbServiceRollout := &isbServiceRollouts.Items[i]
		dependents = append(dependents, dependent{"ISBServiceRollout", isbServiceRollout.Name, numaflowv1.ISBGroupVersionKind, isbServiceRollout.GetUpgradingChildStatus()})
	}
	var monoVertexRollouts apiv1.MonoVertexRolloutList
	if err := r.client.List(ctx, &monoVertexRollouts, &client.ListOptions{Namespace: namespace}); err != nil {
		return apiv1.AssessmentResultUnknown, nil, fmt.Errorf("error listing MonoVertexRollouts: %w", err)
	}
	for i := range monoVertexRollouts.Items {
		monoVertexRollout := &monoVertexRollouts.Items[i]
		dependents = append(dependents, dependent{"MonoVertexRollout", monoVertexRollout.Name, numaflowv1.MonoVertexGroupVersionKind, monoVertexRollout.GetUpgradingChildStatus()})
	}

	if len(dependents) == 0 {
		numaLogger.Warn("Found no ISBServiceRollouts or MonoVertexRollouts in namespace: so NumaflowController is deemed Successful") // not typical but could happen
		return apiv1.AssessmentResultSuccess, nil, nil
	}

	var failedDependents []string
	successfulDependents := 0
	for _, dep := range dependents {
		depLogger := numaLogger.WithValues(strings.ToLower(dep.kind), dep.name)
		if dep.upgradingStatus == nil {
			depLogger.Debug("can't assess NumaflowController; dependent has not yet started upgrading onto this controller instance")
			return apiv1.AssessmentResultUnknown, nil, nil
		}
		// make sure the child that dependent is (or was) upgrading is in fact bound to this controller instance
		child, err := kubernetes.GetResource(ctx, r.client, dep.childGVK, k8stypes.NamespacedName{Namespace: namespace, Name: dep.upgradingStatus.Name})
		if err != nil {
			if apierrors.IsNotFound(err) {
				depLogger.WithValues("child", dep.upgradingStatus.Name).Debug("can't assess NumaflowController; dependent's upgrading child not found")
				return apiv1.AssessmentResultUnknown, nil, nil
			}
			return apiv1.AssessmentResultUnknown, nil, fmt.Errorf("error getting %s %s/%s: %w", dep.childGVK.Kind, namespace, dep.upgradingStatus.Name, err)
		}
		if numaflowtypes.ControllerInstanceIDFromResource(child) != instanceID {
			depLogger.WithValues("child", child.GetName()).Debug("can't assess NumaflowController; dependent's upgrading child is not bound to this controller instance")
			return apiv1.AssessmentResultUnknown, nil, nil
		}
		switch dep.upgradingStatus.AssessmentResult {
		case apiv1.AssessmentResultFailure:
			depLogger.Debug("assessing NumaflowController; dependent is failed")
			failedDependents = append(failedDependents, fmt.Sprintf("%s %s", dep.kind, dep.name))
		case apiv1.AssessmentResultSuccess:
			depLogger.Debug("assessing NumaflowController; dependent succeeded")
			successfulDependents++
		default:
			depLogger.Debug("assessing NumaflowController; dependent assessment is unknown")
		}
	}
	if len(failedDependents) > 0 {
		return apiv1.AssessmentResultFailure, failedDependents, nil
	}
	if successfulDependents == len(dependents) {
		return apiv1.AssessmentResultSuccess, nil, nil
	}
	return apiv1.AssessmentResultUnknown, nil, nil
}

// CheckForDifferences checks to see if the NumaflowController definition matches the spec and the required metadata.
// spec.instanceID is excluded from the comparison: it is bound to a child when the child is created and is never
// part of what the Rollout defines, so two children of the same version on different instances are not different.
func (r *NumaflowControllerRolloutReconciler) CheckForDifferences(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	numaflowControllerDef *unstructured.Unstructured,
	requiredSpec map[string]interface{},
	requiredMetadata map[string]interface{},
	existingChildUpgradeState common.UpgradeState) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	existingSpec := specWithoutInstanceID(numaflowControllerDef.Object["spec"])
	desiredSpec := specWithoutInstanceID(requiredSpec["spec"])
	specsEqual := util.CompareStructNumTypeAgnostic(existingSpec, desiredSpec)

	// Check required metadata (labels and annotations)
	requiredLabels, requiredAnnotations := kubernetes.ExtractMetadataSubmaps(requiredMetadata)
	actualLabels := numaflowControllerDef.GetLabels()
	actualAnnotations := numaflowControllerDef.GetAnnotations()

	labelsFound := util.IsMapSubset(requiredLabels, actualLabels)
	annotationsFound := util.IsMapSubset(requiredAnnotations, actualAnnotations)
	numaLogger.Debugf("specsEqual: %t, labelsFound=%t, annotationsFound=%v, from=%v, to=%v, requiredLabels=%v, actualLabels=%v, requiredAnnotations=%v, actualAnnotations=%v\n",
		specsEqual, labelsFound, annotationsFound, existingSpec, desiredSpec, requiredLabels, actualLabels, requiredAnnotations, actualAnnotations)

	return !specsEqual || !labelsFound || !annotationsFound, nil
}

// specWithoutInstanceID returns a shallow copy of the spec map without the instanceID field
func specWithoutInstanceID(spec interface{}) map[string]interface{} {
	specMap, _ := spec.(map[string]interface{})
	result := make(map[string]interface{}, len(specMap))
	for key, value := range specMap {
		if key == "instanceID" {
			continue
		}
		result[key] = value
	}
	return result
}

// CheckForDifferencesWithRolloutDef tests if there's a meaningful difference between an existing child and the child
// that would be produced by the Rollout definition.
// This implements a function of the progressiveController interface.
func (r *NumaflowControllerRolloutReconciler) CheckForDifferencesWithRolloutDef(
	ctx context.Context,
	existingNumaflowController *unstructured.Unstructured,
	rolloutObject ctlrcommon.RolloutObject,
	existingChildUpgradeState common.UpgradeState) (bool, error) {
	nfcRollout := rolloutObject.(*apiv1.NumaflowControllerRollout)

	// the instance is bound to the existing child, not defined by the Rollout, so carry it over
	instanceID, _, _ := unstructured.NestedString(existingNumaflowController.Object, "spec", "instanceID")
	rolloutBasedDef, err := makeNumaflowControllerDefinition(nfcRollout, existingNumaflowController.GetName(), instanceID, existingChildUpgradeState)
	if err != nil {
		return false, err
	}

	// only the parent-rollout label is required: the upgrade-state label is Numaplane's own and changes over the child's lifetime
	requiredMetadata := map[string]interface{}{
		"labels": map[string]interface{}{
			common.LabelKeyParentRollout: nfcRollout.Name,
		},
	}
	return r.CheckForDifferences(ctx, rolloutObject, existingNumaflowController, rolloutBasedDef.Object, requiredMetadata, existingChildUpgradeState)
}

func (r *NumaflowControllerRolloutReconciler) ProcessPromotedChildPreUpgrade(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	return false, nil
}

func (r *NumaflowControllerRolloutReconciler) ProcessPromotedChildPostUpgradeStart(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	return false, nil
}

func (r *NumaflowControllerRolloutReconciler) ProcessPromotedChildPostFailure(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	return false, nil
}

func (r *NumaflowControllerRolloutReconciler) ProcessUpgradingChildPostFailure(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	return false, nil
}

func (r *NumaflowControllerRolloutReconciler) ProcessUpgradingChildPreUpgrade(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	return false, nil
}

func (r *NumaflowControllerRolloutReconciler) ProcessUpgradingChildPostUpgradeStart(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	return false, nil
}

func (r *NumaflowControllerRolloutReconciler) UpdateProgressiveMetrics(rolloutObject progressive.ProgressiveRolloutObject) {
	// no controller-specific progressive metrics yet
}

// SkipProgressiveAssessment checks if we should skip the progressive assessment and force promote based on the definition of the NumaflowControllerRollout
func (r *NumaflowControllerRolloutReconciler) SkipProgressiveAssessment(ctx context.Context, rolloutObject progressive.ProgressiveRolloutObject) (bool, progressive.SkipProgressiveAssessmentReason, error) {
	nfcRollout := rolloutObject.(*apiv1.NumaflowControllerRollout)
	// check if ForcePromote is set true in the Progressive strategy
	if nfcRollout.GetProgressiveStrategy().ForcePromote {
		return true, progressive.SkipProgressiveAssessmentReasonRolloutConfiguration, nil
	}
	return false, progressive.SkipProgressiveAssessmentReasonUndefined, nil
}

// RolloutController interface:

func (r *NumaflowControllerRolloutReconciler) getCurrentChildCount(rolloutObject ctlrcommon.RolloutObject) (int32, bool) {
	nfcRollout := rolloutObject.(*apiv1.NumaflowControllerRollout)
	if nfcRollout.Status.NameCount == nil {
		return int32(0), false
	}
	return *nfcRollout.Status.NameCount, true
}

func (r *NumaflowControllerRolloutReconciler) updateCurrentChildCount(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, nameCount int32) error {
	nfcRollout := rolloutObject.(*apiv1.NumaflowControllerRollout)
	nfcRollout.Status.NameCount = &nameCount
	return r.updateNumaflowControllerRolloutStatus(ctx, nfcRollout)
}

// IncrementChildCount updates the count of children for the Resource in Kubernetes and returns the index that should be used for the next child
// This implements a function of the RolloutController interface
func (r *NumaflowControllerRolloutReconciler) IncrementChildCount(ctx context.Context, rolloutObject ctlrcommon.RolloutObject) (int32, error) {
	currentNameCount, found := r.getCurrentChildCount(rolloutObject)
	if !found {
		currentNameCount = int32(0)
		if err := r.updateCurrentChildCount(ctx, rolloutObject, int32(0)); err != nil {
			return int32(0), err
		}
	}

	// For readability of the NumaflowController name, keep the count from getting too high by rolling around back to 0
	nextNameCount := currentNameCount + 1
	if nextNameCount > common.MaxNameCount {
		nextNameCount = 0
	}

	if err := r.updateCurrentChildCount(ctx, rolloutObject, nextNameCount); err != nil {
		return int32(0), err
	}
	return currentNameCount, nil
}

// Recycle deletes child; returns true if it was in fact deleted
// This implements a function of the RolloutController interface
// TODO(#1021): delete the retired controller instance once no ISBService or MonoVertex references it. Until then,
// a recyclable NumaflowController is left in place, since Pipelines and MonoVertices may still be bound to it.
func (r *NumaflowControllerRolloutReconciler) Recycle(ctx context.Context, _ ctlrcommon.RolloutObject, numaflowController *unstructured.Unstructured) (bool, error) {
	logger.FromContext(ctx).WithValues("numaflowcontroller", fmt.Sprintf("%s/%s", numaflowController.GetNamespace(), numaflowController.GetName())).
		Debug("not recycling NumaflowController; controller instance retirement is not implemented yet")
	return false, nil
}

// GetDesiredRiders gets the list of Riders as specified in the RolloutObject; NumaflowControllerRollout has none
func (r *NumaflowControllerRolloutReconciler) GetDesiredRiders(rolloutObject ctlrcommon.RolloutObject, childName string, child *unstructured.Unstructured) ([]riders.Rider, error) {
	return []riders.Rider{}, nil
}

// GetExistingRiders gets the list of Riders that already exists; NumaflowControllerRollout has none
func (r *NumaflowControllerRolloutReconciler) GetExistingRiders(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, upgrading bool) (unstructured.UnstructuredList, error) {
	return unstructured.UnstructuredList{}, nil
}

// SetCurrentRiderList updates the list of Riders; NumaflowControllerRollout has none
func (r *NumaflowControllerRolloutReconciler) SetCurrentRiderList(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, riders []riders.Rider) {
}

// GetTemplateArguments is the map of Arguments used for templating the child definition; NumaflowControllerRollout has no templated fields
func (r *NumaflowControllerRolloutReconciler) GetTemplateArguments(child *unstructured.Unstructured) map[string]interface{} {
	return map[string]interface{}{}
}

// updateControllerInstancesStatus reflects the live promoted and trial NumaflowController children in
// Status.ControllerInstances. This is informational: dependents resolve the controller instance to bind to from the
// children's upgrade-state labels (see ctlrcommon.GetControllerInstanceIDs).
// ReferencingInterStepBufferServices and ReferencingMonovertices are consulted by Recycle only (#1021).
func (r *NumaflowControllerRolloutReconciler) updateControllerInstancesStatus(ctx context.Context, nfcRollout *apiv1.NumaflowControllerRollout) error {
	children, err := kubernetes.ListResources(ctx, r.client, apiv1.NumaflowControllerGroupVersionKind, nfcRollout.Namespace,
		client.MatchingLabels{common.LabelKeyParentRollout: nfcRollout.Name})
	if err != nil {
		return fmt.Errorf("error listing NumaflowController children: %w", err)
	}

	controllerInstances := make([]apiv1.ControllerInstanceRef, 0, len(children.Items))
	for _, child := range children.Items {
		upgradeState := common.UpgradeState(child.GetLabels()[common.LabelKeyUpgradeState])
		if upgradeState != common.LabelValueUpgradePromoted && upgradeState != common.LabelValueUpgradeTrial {
			continue
		}
		instanceID, _, _ := unstructured.NestedString(child.Object, "spec", "instanceID")
		version, _, _ := unstructured.NestedString(child.Object, "spec", "version")
		controllerInstances = append(controllerInstances, apiv1.ControllerInstanceRef{
			Name:       child.GetName(),
			InstanceID: instanceID,
			Version:    version,
			State:      string(upgradeState),
		})
	}
	// promoted first, then by name, so the status is stable across reconciliations
	sort.Slice(controllerInstances, func(i, j int) bool {
		if controllerInstances[i].State != controllerInstances[j].State {
			return controllerInstances[i].State == string(common.LabelValueUpgradePromoted)
		}
		return controllerInstances[i].Name < controllerInstances[j].Name
	})
	nfcRollout.Status.ControllerInstances = controllerInstances
	return nil
}
