package progressive

import (
	"context"
	"fmt"
	"strings"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// CombineAssessmentResults folds the assessments of a resource chain into one result.
// Any Failure fails the chain. Any Unknown or empty result leaves the chain Unknown.
// The chain is Success only when every member is Success.
func CombineAssessmentResults(results []apiv1.AssessmentResult) apiv1.AssessmentResult {
	if len(results) == 0 {
		return apiv1.AssessmentResultUnknown
	}
	sawUnknown := false
	for _, result := range results {
		switch result {
		case apiv1.AssessmentResultFailure:
			return apiv1.AssessmentResultFailure
		case apiv1.AssessmentResultSuccess:
			continue
		default:
			sawUnknown = true
		}
	}
	if sawUnknown {
		return apiv1.AssessmentResultUnknown
	}
	return apiv1.AssessmentResultSuccess
}

// AppendChainMember adds result to the chain when the member is part of this chain.
// A Failure contributes reason to the chain's failure reasons.
func AppendChainMember(results []apiv1.AssessmentResult, reasons []string, result apiv1.AssessmentResult, present bool, reason string) ([]apiv1.AssessmentResult, []string) {
	if !present {
		return results, reasons
	}
	results = append(results, result)
	if result == apiv1.AssessmentResultFailure && reason != "" {
		reasons = append(reasons, reason)
	}
	return results, reasons
}

// TrialControllerResourceAssessment returns the resource assessment of the trial NumaflowController
// in namespace, and whether a trial controller is part of the chain.
// No NumaflowControllerRollout, or no trial child, means the controller is not part of the chain.
func TrialControllerResourceAssessment(ctx context.Context, c client.Client, namespace string) (apiv1.AssessmentResult, bool, string, error) {
	nfcRollout, trialChild, err := findTrialController(ctx, c, namespace)
	if err != nil {
		return apiv1.AssessmentResultUnknown, false, "", err
	}
	if nfcRollout == nil || trialChild == nil {
		return apiv1.AssessmentResultUnknown, false, "", nil
	}

	upgrading := nfcRollout.GetUpgradingChildStatus()
	if upgrading == nil || upgrading.Name != trialChild.GetName() {
		return apiv1.AssessmentResultUnknown, true, fmt.Sprintf("NumaflowControllerRollout %s trial %s has no resource assessment yet", nfcRollout.Name, trialChild.GetName()), nil
	}
	return upgrading.EffectiveResourceAssessment(), true, resourceFailureReason("NumaflowControllerRollout", nfcRollout.Name, trialChild.GetName(), upgrading), nil
}

// PipelinesOnTrialISB returns the resource assessments of PipelineRollouts whose upgrading child uses
// the trial ISBService isbsvcName, excluding excludeRolloutName.
// A Pipeline that has not started rolling onto this ISBService is omitted. Issue 1050 holds new updates
// back from starting; this only covers trials already in progress.
func PipelinesOnTrialISB(ctx context.Context, c client.Client, namespace, isbsvcName, excludeRolloutName string) ([]apiv1.AssessmentResult, []string, error) {
	var pipelineRollouts apiv1.PipelineRolloutList
	if err := c.List(ctx, &pipelineRollouts, &client.ListOptions{Namespace: namespace}); err != nil {
		return nil, nil, fmt.Errorf("listing PipelineRollouts in namespace %q: %w", namespace, err)
	}
	var results []apiv1.AssessmentResult
	var reasons []string
	for i := range pipelineRollouts.Items {
		pipelineRollout := &pipelineRollouts.Items[i]
		if pipelineRollout.Name == excludeRolloutName {
			continue
		}
		upgrading := pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus
		if upgrading == nil || upgrading.InterStepBufferServiceName != isbsvcName {
			continue
		}
		results, reasons = appendAssessment(results, reasons, upgrading.EffectiveResourceAssessment(), resourceFailureReason("PipelineRollout", pipelineRollout.Name, upgrading.Name, &upgrading.UpgradingChildStatus))
	}
	return results, reasons, nil
}

// WorkloadsOnTrialController returns the resource assessments of ISBService, Pipeline, and MonoVertex
// trials bound to the trial NumaflowController, excluding the rollout identified by excludeKind and excludeName.
// present is false when there is no trial controller. A workload that is not yet rolling onto that instance is omitted.
func WorkloadsOnTrialController(ctx context.Context, c client.Client, namespace, excludeKind, excludeName string) ([]apiv1.AssessmentResult, []string, bool, error) {
	_, trialChild, err := findTrialController(ctx, c, namespace)
	if err != nil {
		return nil, nil, false, err
	}
	if trialChild == nil {
		return nil, nil, false, nil
	}
	instanceID, err := unstructuredInstanceID(trialChild)
	if err != nil {
		return nil, nil, true, err
	}

	var results []apiv1.AssessmentResult
	var reasons []string
	add := func(kind, rolloutName string, gvk schema.GroupVersionKind, upgrading *apiv1.UpgradingChildStatus) error {
		if kind == excludeKind && rolloutName == excludeName {
			return nil
		}
		if upgrading == nil {
			return nil
		}
		onTrial, err := childOnTrialController(ctx, c, namespace, upgrading.Name, gvk, instanceID)
		if err != nil || !onTrial {
			return err
		}
		results, reasons = appendAssessment(results, reasons, upgrading.EffectiveResourceAssessment(), resourceFailureReason(kind, rolloutName, upgrading.Name, upgrading))
		return nil
	}

	var isbRollouts apiv1.ISBServiceRolloutList
	if err := c.List(ctx, &isbRollouts, &client.ListOptions{Namespace: namespace}); err != nil {
		return nil, nil, true, fmt.Errorf("listing ISBServiceRollouts in namespace %q: %w", namespace, err)
	}
	for i := range isbRollouts.Items {
		isbRollout := &isbRollouts.Items[i]
		if err := add("ISBServiceRollout", isbRollout.Name, numaflowv1.ISBGroupVersionKind, isbRollout.GetUpgradingChildStatus()); err != nil {
			return nil, nil, true, err
		}
	}
	var pipelineRollouts apiv1.PipelineRolloutList
	if err := c.List(ctx, &pipelineRollouts, &client.ListOptions{Namespace: namespace}); err != nil {
		return nil, nil, true, fmt.Errorf("listing PipelineRollouts in namespace %q: %w", namespace, err)
	}
	for i := range pipelineRollouts.Items {
		pipelineRollout := &pipelineRollouts.Items[i]
		if err := add("PipelineRollout", pipelineRollout.Name, numaflowv1.PipelineGroupVersionKind, pipelineRollout.GetUpgradingChildStatus()); err != nil {
			return nil, nil, true, err
		}
	}
	var monoVertexRollouts apiv1.MonoVertexRolloutList
	if err := c.List(ctx, &monoVertexRollouts, &client.ListOptions{Namespace: namespace}); err != nil {
		return nil, nil, true, fmt.Errorf("listing MonoVertexRollouts in namespace %q: %w", namespace, err)
	}
	for i := range monoVertexRollouts.Items {
		monoVertexRollout := &monoVertexRollouts.Items[i]
		if err := add("MonoVertexRollout", monoVertexRollout.Name, numaflowv1.MonoVertexGroupVersionKind, monoVertexRollout.GetUpgradingChildStatus()); err != nil {
			return nil, nil, true, err
		}
	}
	return results, reasons, true, nil
}

func findTrialController(ctx context.Context, c client.Client, namespace string) (*apiv1.NumaflowControllerRollout, *unstructured.Unstructured, error) {
	var nfcRolloutList apiv1.NumaflowControllerRolloutList
	if err := c.List(ctx, &nfcRolloutList, &client.ListOptions{Namespace: namespace}); err != nil {
		return nil, nil, fmt.Errorf("listing NumaflowControllerRollouts in namespace %q: %w", namespace, err)
	}
	if len(nfcRolloutList.Items) == 0 {
		return nil, nil, nil
	}
	if len(nfcRolloutList.Items) > 1 {
		return nil, nil, fmt.Errorf("expected at most 1 NumaflowControllerRollout in namespace %q, found %d", namespace, len(nfcRolloutList.Items))
	}
	nfcRollout := nfcRolloutList.Items[0]
	trialChild, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, &nfcRollout, common.LabelValueUpgradeTrial, nil, false, c)
	if err != nil {
		return nil, nil, err
	}
	return &nfcRollout, trialChild, nil
}

func unstructuredInstanceID(controller *unstructured.Unstructured) (string, error) {
	instanceID, _, err := unstructured.NestedString(controller.Object, "spec", "instanceID")
	if err != nil {
		return "", fmt.Errorf("reading spec.instanceID of NumaflowController %s/%s: %w", controller.GetNamespace(), controller.GetName(), err)
	}
	return instanceID, nil
}

// childOnTrialController reports whether the named child is a trial bound to instanceID.
// A missing child is not on the trial.
func childOnTrialController(ctx context.Context, c client.Client, namespace, childName string, gvk schema.GroupVersionKind, instanceID string) (bool, error) {
	child, err := kubernetes.GetResource(ctx, c, gvk, types.NamespacedName{Namespace: namespace, Name: childName})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("getting %s %s/%s: %w", gvk.Kind, namespace, childName, err)
	}
	if child.GetLabels()[common.LabelKeyUpgradeState] != string(common.LabelValueUpgradeTrial) {
		return false, nil
	}
	return numaflowtypes.ControllerInstanceIDFromResource(child) == instanceID, nil
}

func appendAssessment(results []apiv1.AssessmentResult, reasons []string, result apiv1.AssessmentResult, reason string) ([]apiv1.AssessmentResult, []string) {
	results = append(results, result)
	if result != apiv1.AssessmentResultFailure || reason == "" {
		return results, reasons
	}
	for _, existing := range reasons {
		if existing == reason {
			return results, reasons
		}
	}
	return results, append(reasons, reason)
}

// TrialISBServiceResourceAssessment returns the resource assessment of the named ISBService when that
// child is a trial, and whether it is part of the chain.
// A promoted ISBService is not part of the chain: a Pipeline upgrading on its own does not wait for it.
func TrialISBServiceResourceAssessment(ctx context.Context, c client.Client, namespace, isbsvcName string) (apiv1.AssessmentResult, bool, string, error) {
	if isbsvcName == "" {
		return apiv1.AssessmentResultUnknown, false, "", nil
	}
	isbsvc, err := kubernetes.GetResource(ctx, c, numaflowv1.ISBGroupVersionKind, types.NamespacedName{Namespace: namespace, Name: isbsvcName})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Only a live trial ISBService is part of the chain. A missing one is the Pipeline's own health problem.
			return apiv1.AssessmentResultUnknown, false, "", nil
		}
		return apiv1.AssessmentResultUnknown, false, "", fmt.Errorf("getting ISBService %s/%s: %w", namespace, isbsvcName, err)
	}
	if isbsvc.GetLabels()[common.LabelKeyUpgradeState] != string(common.LabelValueUpgradeTrial) {
		return apiv1.AssessmentResultUnknown, false, "", nil
	}

	parentName := isbsvc.GetLabels()[common.LabelKeyParentRollout]
	if parentName == "" {
		return apiv1.AssessmentResultUnknown, true, fmt.Sprintf("trial ISBService %s/%s has no parent rollout", namespace, isbsvcName), nil
	}
	var isbRollout apiv1.ISBServiceRollout
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: parentName}, &isbRollout); err != nil {
		return apiv1.AssessmentResultUnknown, true, "", fmt.Errorf("getting ISBServiceRollout %s/%s: %w", namespace, parentName, err)
	}
	upgrading := isbRollout.GetUpgradingChildStatus()
	if upgrading == nil || upgrading.Name != isbsvcName {
		return apiv1.AssessmentResultUnknown, true, fmt.Sprintf("ISBServiceRollout %s trial %s has no resource assessment yet", parentName, isbsvcName), nil
	}
	return upgrading.EffectiveResourceAssessment(), true, resourceFailureReason("ISBServiceRollout", parentName, isbsvcName, upgrading), nil
}

func resourceFailureReason(kind, rolloutName, childName string, upgrading *apiv1.UpgradingChildStatus) string {
	if upgrading.EffectiveResourceAssessment() != apiv1.AssessmentResultFailure {
		return ""
	}
	reason := fmt.Sprintf("%s %s trial %s resource assessment failed", kind, rolloutName, childName)
	if len(upgrading.FailureReasons) > 0 {
		reason = fmt.Sprintf("%s: %s", reason, strings.Join(upgrading.FailureReasons, "; "))
	}
	return reason
}
