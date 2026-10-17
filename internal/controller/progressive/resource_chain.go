package progressive

import (
	"context"
	"encoding/json"
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

// CombineMemberAssessments folds ResourceChainMemberStatus assessments into one chain result.
func CombineMemberAssessments(members []apiv1.ResourceChainMemberStatus) apiv1.AssessmentResult {
	results := make([]apiv1.AssessmentResult, len(members))
	for i, member := range members {
		results[i] = member.AssessmentResult
	}
	return CombineAssessmentResults(results)
}

// NewChainMember builds a ResourceChainMemberStatus for a rollout in the chain.
func NewChainMember(kind, name, childName string, result apiv1.AssessmentResult) apiv1.ResourceChainMemberStatus {
	return apiv1.ResourceChainMemberStatus{
		Kind:             kind,
		Name:             name,
		ChildName:        childName,
		AssessmentResult: result,
	}
}

// AppendChainMember adds member to the chain.
// A Failure contributes reason to the chain's failure reasons.
// Call this only for a member that is part of the chain.
func AppendChainMember(members []apiv1.ResourceChainMemberStatus, reasons []string, member apiv1.ResourceChainMemberStatus, reason string) ([]apiv1.ResourceChainMemberStatus, []string) {
	members = append(members, member)
	if member.AssessmentResult == apiv1.AssessmentResultFailure && reason != "" {
		reasons = append(reasons, reason)
	}
	return members, reasons
}

// TrialControllerResourceAssessment returns the resource assessment of the trial NumaflowController
// in namespace, and whether a trial controller is part of the chain.
// No NumaflowControllerRollout, or no trial child, means the controller is not part of the chain.
func TrialControllerResourceAssessment(ctx context.Context, c client.Client, namespace string) (apiv1.ResourceChainMemberStatus, bool, string, error) {
	nfcRollout, trialChild, err := findTrialController(ctx, c, namespace)
	if err != nil {
		return apiv1.ResourceChainMemberStatus{}, false, "", err
	}
	if nfcRollout == nil || trialChild == nil {
		return apiv1.ResourceChainMemberStatus{}, false, "", nil
	}

	upgrading := nfcRollout.GetUpgradingChildStatus()
	if upgrading == nil || upgrading.Name != trialChild.GetName() {
		return NewChainMember("NumaflowControllerRollout", nfcRollout.Name, trialChild.GetName(), apiv1.AssessmentResultUnknown), true,
			fmt.Sprintf("NumaflowControllerRollout %s trial %s has no resource assessment yet", nfcRollout.Name, trialChild.GetName()), nil
	}
	return NewChainMember("NumaflowControllerRollout", nfcRollout.Name, trialChild.GetName(), upgrading.EffectiveResourceAssessment()), true,
		resourceFailureReason("NumaflowControllerRollout", nfcRollout.Name, trialChild.GetName(), upgrading), nil
}

// PipelinesOnTrialISB returns the resource assessments of PipelineRollouts that belong on the trial
// ISBService isbsvcName, excluding excludeRolloutName.
// A Pipeline already rolling onto that ISBService contributes its resource assessment.
// A Pipeline whose spec uses the same ISBServiceRollout, but whose upgrading child is not on this
// ISBService yet, contributes Unknown so this Pipeline cannot promote ahead of it.
func PipelinesOnTrialISB(ctx context.Context, c client.Client, namespace, isbsvcName, excludeRolloutName string) ([]apiv1.ResourceChainMemberStatus, []string, error) {
	parentRollout, err := trialISBParentRollout(ctx, c, namespace, isbsvcName)
	if err != nil {
		return nil, nil, err
	}
	var pipelineRollouts apiv1.PipelineRolloutList
	if err := c.List(ctx, &pipelineRollouts, &client.ListOptions{Namespace: namespace}); err != nil {
		return nil, nil, fmt.Errorf("listing PipelineRollouts in namespace %q: %w", namespace, err)
	}
	var members []apiv1.ResourceChainMemberStatus
	var reasons []string
	for i := range pipelineRollouts.Items {
		pipelineRollout := &pipelineRollouts.Items[i]
		if pipelineRollout.Name == excludeRolloutName {
			continue
		}
		upgrading := pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus
		onThisISB := upgrading != nil && upgrading.InterStepBufferServiceName == isbsvcName
		usesParent := false
		if parentRollout != "" {
			usesParent, err = pipelineRolloutUsesISB(pipelineRollout, parentRollout)
			if err != nil {
				return nil, nil, err
			}
		}
		if !onThisISB && !usesParent {
			continue
		}
		if !onThisISB {
			members, reasons = appendMember(members, reasons, NewChainMember("PipelineRollout", pipelineRollout.Name, "", apiv1.AssessmentResultUnknown), "")
			continue
		}
		members, reasons = appendMember(members, reasons,
			NewChainMember("PipelineRollout", pipelineRollout.Name, upgrading.Name, upgrading.EffectiveResourceAssessment()),
			resourceFailureReason("PipelineRollout", pipelineRollout.Name, upgrading.Name, &upgrading.UpgradingChildStatus))
	}
	return members, reasons, nil
}

// trialISBParentRollout returns the ISBServiceRollout that owns the named ISBService child.
// An empty name means the child was not found or has no parent label.
func trialISBParentRollout(ctx context.Context, c client.Client, namespace, isbsvcName string) (string, error) {
	isb := &unstructured.Unstructured{}
	isb.SetGroupVersionKind(numaflowv1.ISBGroupVersionKind)
	err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: isbsvcName}, isb)
	if apierrors.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("getting InterstepBufferService %s/%s: %w", namespace, isbsvcName, err)
	}
	return isb.GetLabels()[common.LabelKeyParentRollout], nil
}

// pipelineRolloutUsesISB reports whether the PipelineRollout spec selects isbRolloutName.
// An empty interStepBufferServiceName selects the rollout named "default".
func pipelineRolloutUsesISB(pipelineRollout *apiv1.PipelineRollout, isbRolloutName string) (bool, error) {
	used := "default"
	if raw := pipelineRollout.Spec.Pipeline.Spec.Raw; len(raw) > 0 {
		var pipelineSpec numaflowtypes.PipelineSpec
		if err := json.Unmarshal(raw, &pipelineSpec); err != nil {
			return false, fmt.Errorf("reading PipelineRollout %s spec: %w", pipelineRollout.Name, err)
		}
		if pipelineSpec.InterStepBufferServiceName != "" {
			used = pipelineSpec.InterStepBufferServiceName
		}
	}
	return used == isbRolloutName, nil
}

// WorkloadsOnTrialController returns the resource assessments of ISBService, Pipeline, and MonoVertex
// rollouts in the namespace, excluding the rollout identified by excludeKind and excludeName.
// present is false when there is no trial controller.
// A rollout whose upgrading child is not bound to that controller instance contributes Unknown.
// A child already promoted onto the instance still counts, so a later workload is not blocked after an earlier one succeeds.
func WorkloadsOnTrialController(ctx context.Context, c client.Client, namespace, excludeKind, excludeName string) ([]apiv1.ResourceChainMemberStatus, []string, bool, error) {
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

	var members []apiv1.ResourceChainMemberStatus
	var reasons []string
	add := func(kind, rolloutName string, gvk schema.GroupVersionKind, upgrading *apiv1.UpgradingChildStatus) error {
		if kind == excludeKind && rolloutName == excludeName {
			return nil
		}
		if upgrading == nil || upgrading.Name == "" {
			members, reasons = appendMember(members, reasons, NewChainMember(kind, rolloutName, "", apiv1.AssessmentResultUnknown), "")
			return nil
		}
		onInstance, err := childOnControllerInstance(ctx, c, namespace, upgrading.Name, gvk, instanceID)
		if err != nil {
			return err
		}
		if !onInstance {
			members, reasons = appendMember(members, reasons, NewChainMember(kind, rolloutName, upgrading.Name, apiv1.AssessmentResultUnknown), "")
			return nil
		}
		members, reasons = appendMember(members, reasons,
			NewChainMember(kind, rolloutName, upgrading.Name, upgrading.EffectiveResourceAssessment()),
			resourceFailureReason(kind, rolloutName, upgrading.Name, upgrading))
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
	return members, reasons, true, nil
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

// childOnControllerInstance reports whether the named child is bound to instanceID.
// A child that already succeeded is relabeled from trial to promoted, so the upgrade-state label is not required.
// A missing child is not on the instance.
func childOnControllerInstance(ctx context.Context, c client.Client, namespace, childName string, gvk schema.GroupVersionKind, instanceID string) (bool, error) {
	child, err := kubernetes.GetResource(ctx, c, gvk, types.NamespacedName{Namespace: namespace, Name: childName})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("getting %s %s/%s: %w", gvk.Kind, namespace, childName, err)
	}
	return numaflowtypes.ControllerInstanceIDFromResource(child) == instanceID, nil
}

func appendMember(members []apiv1.ResourceChainMemberStatus, reasons []string, member apiv1.ResourceChainMemberStatus, reason string) ([]apiv1.ResourceChainMemberStatus, []string) {
	members = append(members, member)
	if member.AssessmentResult != apiv1.AssessmentResultFailure || reason == "" {
		return members, reasons
	}
	for _, existing := range reasons {
		if existing == reason {
			return members, reasons
		}
	}
	return members, append(reasons, reason)
}

// TrialISBServiceResourceAssessment returns the resource assessment of the named ISBService when that
// child is a trial, and whether it is part of the chain.
// A promoted ISBService is not part of the chain: a Pipeline upgrading on its own does not wait for it.
func TrialISBServiceResourceAssessment(ctx context.Context, c client.Client, namespace, isbsvcName string) (apiv1.ResourceChainMemberStatus, bool, string, error) {
	if isbsvcName == "" {
		return apiv1.ResourceChainMemberStatus{}, false, "", nil
	}
	isbsvc, err := kubernetes.GetResource(ctx, c, numaflowv1.ISBGroupVersionKind, types.NamespacedName{Namespace: namespace, Name: isbsvcName})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Only a live trial ISBService is part of the chain. A missing one is the Pipeline's own health problem.
			return apiv1.ResourceChainMemberStatus{}, false, "", nil
		}
		return apiv1.ResourceChainMemberStatus{}, false, "", fmt.Errorf("getting ISBService %s/%s: %w", namespace, isbsvcName, err)
	}
	if isbsvc.GetLabels()[common.LabelKeyUpgradeState] != string(common.LabelValueUpgradeTrial) {
		return apiv1.ResourceChainMemberStatus{}, false, "", nil
	}

	parentName := isbsvc.GetLabels()[common.LabelKeyParentRollout]
	if parentName == "" {
		return NewChainMember("ISBServiceRollout", "", isbsvcName, apiv1.AssessmentResultUnknown), true,
			fmt.Sprintf("trial ISBService %s/%s has no parent rollout", namespace, isbsvcName), nil
	}
	var isbRollout apiv1.ISBServiceRollout
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: parentName}, &isbRollout); err != nil {
		return apiv1.ResourceChainMemberStatus{}, true, "", fmt.Errorf("getting ISBServiceRollout %s/%s: %w", namespace, parentName, err)
	}
	upgrading := isbRollout.GetUpgradingChildStatus()
	if upgrading == nil || upgrading.Name != isbsvcName {
		return NewChainMember("ISBServiceRollout", parentName, isbsvcName, apiv1.AssessmentResultUnknown), true,
			fmt.Sprintf("ISBServiceRollout %s trial %s has no resource assessment yet", parentName, isbsvcName), nil
	}
	return NewChainMember("ISBServiceRollout", parentName, isbsvcName, upgrading.EffectiveResourceAssessment()), true,
		resourceFailureReason("ISBServiceRollout", parentName, isbsvcName, upgrading), nil
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
