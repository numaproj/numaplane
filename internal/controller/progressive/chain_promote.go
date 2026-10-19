package progressive

import (
	"context"
	"fmt"
	"strings"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// trialMember is one current trial resource and its stored resource assessment.
// Child is nil when the rollout belongs in the chain but has not created a trial child yet.
type trialMember struct {
	Kind        string
	RolloutName string
	ChildName   string
	Assessment  apiv1.AssessmentResult
	Child       *unstructured.Unstructured
}

// ChainPromoteAnnotationValue identifies the exact child the promotion annotation was written for.
func ChainPromoteAnnotationValue(name, uid string) string {
	return name + "/" + uid
}

// ChainPromoteAnnotationMatches reports whether obj carries a chain-promote annotation for itself.
// A replaced child with the same name and a new UID does not match.
func ChainPromoteAnnotationMatches(obj *unstructured.Unstructured) bool {
	if obj == nil {
		return false
	}
	name, uid, ok := parseChainPromoteAnnotation(obj.GetAnnotations()[common.AnnotationKeyChainPromote])
	if !ok {
		return false
	}
	return name == obj.GetName() && uid == string(obj.GetUID())
}

// ChainPromoteApplies reports whether obj is still the rollout's current trial child and the
// annotation was written for this exact object. Promotion then follows the normal path and does
// not run health checks again. This must happen before a spec change replaces that trial child.
func ChainPromoteApplies(obj *unstructured.Unstructured, currentTrialChildName string) bool {
	return ChainPromoteAnnotationMatches(obj) && obj.GetName() == currentTrialChildName
}

func parseChainPromoteAnnotation(value string) (string, string, bool) {
	name, uid, ok := strings.Cut(value, "/")
	if !ok || name == "" {
		return "", "", false
	}
	return name, uid, true
}

// ControllerChainFailed reports whether the namespace has a trial NumaflowController whose
// chain assessment is Failure. A new Pipeline, MonoVertex, or ISBService stays Pending until
// that chain succeeds, is reverted, or is force-promoted.
func ControllerChainFailed(ctx context.Context, c client.Client, namespace string) (bool, error) {
	nfcRollout, trialChild, err := findTrialController(ctx, c, namespace)
	if err != nil || nfcRollout == nil || trialChild == nil {
		return false, err
	}
	upgrading := nfcRollout.GetUpgradingChildStatus()
	if upgrading == nil || upgrading.Name != trialChild.GetName() || upgrading.Discontinued {
		return false, nil
	}
	return upgrading.ResourceChainAssessmentResult == apiv1.AssessmentResultFailure, nil
}

// StoredTrialResourceAssessments returns the stored resource assessment of each current trial member.
// selfKind/selfName/selfResult override the caller's own rollout, which may have just saved a
// resource assessment that is not in the API yet.
// A discontinued trial is not a member. A rollout that has not started its trial is Unknown when a
// trial controller (or, for a Pipeline, a trial ISBService it uses) is still in progress.
func StoredTrialResourceAssessments(ctx context.Context, c client.Client, namespace, selfKind, selfName string, selfResult apiv1.AssessmentResult) ([]apiv1.AssessmentResult, error) {
	members, err := currentTrialMembers(ctx, c, namespace)
	if err != nil {
		return nil, err
	}
	results := make([]apiv1.AssessmentResult, 0, len(members)+1)
	foundSelf := false
	for _, member := range members {
		if member.Kind == selfKind && member.RolloutName == selfName {
			foundSelf = true
			if selfResult != "" {
				results = append(results, selfResult)
				continue
			}
		}
		results = append(results, member.Assessment)
	}
	if !foundSelf && selfResult != "" {
		results = append(results, selfResult)
	}
	return results, nil
}

// AnnotateCurrentTrialChildren writes the chain-promote annotation onto each live trial child
// that is part of the chain. The value records that child's name and UID.
func AnnotateCurrentTrialChildren(ctx context.Context, c client.Client, namespace string) error {
	members, err := currentTrialMembers(ctx, c, namespace)
	if err != nil {
		return err
	}
	for _, member := range members {
		if member.Child == nil {
			continue
		}
		value := ChainPromoteAnnotationValue(member.Child.GetName(), string(member.Child.GetUID()))
		if member.Child.GetAnnotations()[common.AnnotationKeyChainPromote] == value {
			continue
		}
		if err := kubernetes.SetAndPatchAnnotations(ctx, c, member.Child, map[string]string{
			common.AnnotationKeyChainPromote: value,
		}); err != nil {
			return fmt.Errorf("annotating %s %s/%s for chain promotion: %w", member.Child.GetKind(), member.Child.GetNamespace(), member.Child.GetName(), err)
		}
	}
	return nil
}

// StampForcePromoteChain sets the force-promote-chain label on every current trial child in the namespace.
// Each rollout then promotes its own trial child through the normal promotion path.
func StampForcePromoteChain(ctx context.Context, c client.Client, namespace string) error {
	for _, gvk := range trialChildGVKs() {
		children, err := kubernetes.ListResources(ctx, c, gvk, namespace, client.MatchingLabels{
			common.LabelKeyUpgradeState: string(common.LabelValueUpgradeTrial),
		})
		if err != nil {
			return fmt.Errorf("listing trial %s in namespace %q: %w", gvk.Kind, namespace, err)
		}
		for i := range children.Items {
			child := &children.Items[i]
			if child.GetLabels()[common.LabelKeyForcePromoteChain] == "true" {
				continue
			}
			if err := kubernetes.SetAndPatchLabels(ctx, c, child, map[string]string{
				common.LabelKeyForcePromoteChain: "true",
			}); err != nil {
				return fmt.Errorf("setting force-promote-chain on %s %s/%s: %w", child.GetKind(), child.GetNamespace(), child.GetName(), err)
			}
		}
	}
	return nil
}

// RemoveTrialDependentsOnInstance marks trial ISBService, Pipeline, and MonoVertex children bound
// to instanceID recyclable and discontinues their Rollouts. They belonged to a trial
// NumaflowController that is being discontinued. A dependent with no spec change of its own keeps
// its promoted child; discontinuing the Rollout clears the progressive-failed condition, which
// otherwise stays set until declareSuccess or Discontinue.
func RemoveTrialDependentsOnInstance(ctx context.Context, c client.Client, namespace, instanceID string) error {
	if instanceID == "" {
		return nil
	}
	numaLogger := logger.FromContext(ctx)
	for _, gvk := range []schema.GroupVersionKind{
		numaflowv1.ISBGroupVersionKind,
		numaflowv1.PipelineGroupVersionKind,
		numaflowv1.MonoVertexGroupVersionKind,
	} {
		children, err := kubernetes.ListResources(ctx, c, gvk, namespace, client.MatchingLabels{
			common.LabelKeyUpgradeState: string(common.LabelValueUpgradeTrial),
		})
		if err != nil {
			return fmt.Errorf("listing trial %s in namespace %q: %w", gvk.Kind, namespace, err)
		}
		reason := common.LabelValueDiscontinueProgressive
		for i := range children.Items {
			child := &children.Items[i]
			if numaflowtypes.ControllerInstanceIDFromResource(child) != instanceID {
				continue
			}
			numaLogger.WithValues("kind", gvk.Kind, "name", child.GetName(), "instanceID", instanceID).
				Debug("removing trial dependent of discontinued NumaflowController")
			if err := ctlrcommon.MarkRecyclable(ctx, c, &reason, child); err != nil {
				return fmt.Errorf("removing trial %s %s/%s bound to controller instance %q: %w", gvk.Kind, namespace, child.GetName(), instanceID, err)
			}
			if err := discontinueDependentRollout(ctx, c, gvk.Kind, child); err != nil {
				return err
			}
		}
	}
	return nil
}

// discontinueDependentRollout applies the same status update as Discontinue to the Rollout that
// owns child. The progressive-failed condition is cleared here because this Rollout's own
// reconcile does not run Discontinue when only the controller trial was reverted.
func discontinueDependentRollout(ctx context.Context, c client.Client, childKind string, child *unstructured.Unstructured) error {
	parentName := child.GetLabels()[common.LabelKeyParentRollout]
	if parentName == "" {
		return fmt.Errorf("trial %s %s/%s has no parent rollout label", childKind, child.GetNamespace(), child.GetName())
	}
	key := client.ObjectKey{Namespace: child.GetNamespace(), Name: parentName}

	var rollout ProgressiveRolloutObject
	var statusObject client.Object
	rolloutKind := ""
	switch childKind {
	case common.NumaflowISBServiceKind:
		obj := &apiv1.ISBServiceRollout{}
		if err := c.Get(ctx, key, obj); err != nil {
			return fmt.Errorf("getting ISBServiceRollout %s/%s for discontinued trial %s: %w", child.GetNamespace(), parentName, child.GetName(), err)
		}
		rollout, statusObject, rolloutKind = obj, obj, "ISBServiceRollout"
	case common.NumaflowPipelineKind:
		obj := &apiv1.PipelineRollout{}
		if err := c.Get(ctx, key, obj); err != nil {
			return fmt.Errorf("getting PipelineRollout %s/%s for discontinued trial %s: %w", child.GetNamespace(), parentName, child.GetName(), err)
		}
		rollout, statusObject, rolloutKind = obj, obj, "PipelineRollout"
	case common.NumaflowMonoVertexKind:
		obj := &apiv1.MonoVertexRollout{}
		if err := c.Get(ctx, key, obj); err != nil {
			return fmt.Errorf("getting MonoVertexRollout %s/%s for discontinued trial %s: %w", child.GetNamespace(), parentName, child.GetName(), err)
		}
		rollout, statusObject, rolloutKind = obj, obj, "MonoVertexRollout"
	default:
		return fmt.Errorf("trial %s %s/%s is not an ISBService, Pipeline, or MonoVertex", childKind, child.GetNamespace(), child.GetName())
	}

	if rollout.GetUpgradingChildStatus() != nil {
		UpdateUpgradingChildStatus(rollout, func(status *apiv1.UpgradingChildStatus) {
			status.Discontinued = true
		})
	}
	generation := rollout.GetRolloutObjectMeta().Generation
	rollout.GetRolloutStatus().MarkProgressiveUpgradeSucceeded("Discontinued", generation)
	rollout.GetRolloutStatus().MarkDeployed(generation)
	if err := c.Status().Update(ctx, statusObject); err != nil {
		return fmt.Errorf("discontinuing %s %s/%s after removing its trial child: %w", rolloutKind, child.GetNamespace(), parentName, err)
	}
	return nil
}

// ReopenFailedResourceAssessment starts a new resource-health window after a sticky basic failure
// so a resource that becomes healthy can save Resource Assessment Success.
func ReopenFailedResourceAssessment(rollout ProgressiveRolloutObject) {
	UpdateUpgradingChildStatus(rollout, func(status *apiv1.UpgradingChildStatus) {
		if status.BasicAssessmentResult != apiv1.AssessmentResultFailure {
			return
		}
		now := metav1.NewTime(time.Now())
		status.BasicAssessmentResult = ""
		status.BasicAssessmentEndTime = nil
		status.TrialWindowStartTime = nil
		status.BasicAssessmentStartTime = &now
		status.FailureReasons = nil
	})
}

func currentTrialMembers(ctx context.Context, c client.Client, namespace string) ([]trialMember, error) {
	var members []trialMember

	nfcRollout, trialController, err := findTrialController(ctx, c, namespace)
	if err != nil {
		return nil, err
	}
	if nfcRollout != nil && trialController != nil {
		if member, include := memberFromRollout("NumaflowControllerRollout", nfcRollout.Name, nfcRollout.GetUpgradingChildStatus(), trialController); include {
			members = append(members, member)
		}
	}

	var isbRollouts apiv1.ISBServiceRolloutList
	if err := c.List(ctx, &isbRollouts, &client.ListOptions{Namespace: namespace}); err != nil {
		return nil, fmt.Errorf("listing ISBServiceRollouts in namespace %q: %w", namespace, err)
	}
	for i := range isbRollouts.Items {
		isbRollout := &isbRollouts.Items[i]
		child, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, isbRollout, common.LabelValueUpgradeTrial, nil, false, c)
		if err != nil {
			return nil, err
		}
		if member, include := memberFromRollout("ISBServiceRollout", isbRollout.Name, isbRollout.GetUpgradingChildStatus(), child); include {
			members = append(members, member)
			continue
		}
		// An existing ISBService that has not started its trial yet blocks promotion.
		// A brand-new rollout with no promoted child is Pending and is not a chain member.
		if trialController != nil && !upgradingDiscontinued(isbRollout.GetUpgradingChildStatus()) && child == nil && !upgradingNamesLiveChild(isbRollout.GetUpgradingChildStatus()) {
			hasPromoted, err := hasPromotedChild(ctx, isbRollout, c)
			if err != nil {
				return nil, err
			}
			if hasPromoted {
				members = append(members, trialMember{Kind: "ISBServiceRollout", RolloutName: isbRollout.Name, Assessment: apiv1.AssessmentResultUnknown})
			}
		}
	}

	var pipelineRollouts apiv1.PipelineRolloutList
	if err := c.List(ctx, &pipelineRollouts, &client.ListOptions{Namespace: namespace}); err != nil {
		return nil, fmt.Errorf("listing PipelineRollouts in namespace %q: %w", namespace, err)
	}
	for i := range pipelineRollouts.Items {
		pipelineRollout := &pipelineRollouts.Items[i]
		child, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, pipelineRollout, common.LabelValueUpgradeTrial, nil, false, c)
		if err != nil {
			return nil, err
		}
		if member, include := memberFromRollout("PipelineRollout", pipelineRollout.Name, pipelineRollout.GetUpgradingChildStatus(), child); include {
			members = append(members, member)
			continue
		}
		if upgradingDiscontinued(pipelineRollout.GetUpgradingChildStatus()) || child != nil || upgradingNamesLiveChild(pipelineRollout.GetUpgradingChildStatus()) {
			continue
		}
		hasPromoted, err := hasPromotedChild(ctx, pipelineRollout, c)
		if err != nil {
			return nil, err
		}
		if !hasPromoted {
			// New PipelineRollout held Pending during a failed chain. It is created after the chain resolves.
			continue
		}
		blocked, err := pipelineAwaitingTrial(ctx, c, namespace, pipelineRollout, trialController != nil)
		if err != nil {
			return nil, err
		}
		if blocked {
			members = append(members, trialMember{Kind: "PipelineRollout", RolloutName: pipelineRollout.Name, Assessment: apiv1.AssessmentResultUnknown})
		}
	}

	var monoVertexRollouts apiv1.MonoVertexRolloutList
	if err := c.List(ctx, &monoVertexRollouts, &client.ListOptions{Namespace: namespace}); err != nil {
		return nil, fmt.Errorf("listing MonoVertexRollouts in namespace %q: %w", namespace, err)
	}
	for i := range monoVertexRollouts.Items {
		monoVertexRollout := &monoVertexRollouts.Items[i]
		child, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, monoVertexRollout, common.LabelValueUpgradeTrial, nil, false, c)
		if err != nil {
			return nil, err
		}
		if member, include := memberFromRollout("MonoVertexRollout", monoVertexRollout.Name, monoVertexRollout.GetUpgradingChildStatus(), child); include {
			members = append(members, member)
			continue
		}
		if trialController != nil && !upgradingDiscontinued(monoVertexRollout.GetUpgradingChildStatus()) && child == nil && !upgradingNamesLiveChild(monoVertexRollout.GetUpgradingChildStatus()) {
			hasPromoted, err := hasPromotedChild(ctx, monoVertexRollout, c)
			if err != nil {
				return nil, err
			}
			if hasPromoted {
				members = append(members, trialMember{Kind: "MonoVertexRollout", RolloutName: monoVertexRollout.Name, Assessment: apiv1.AssessmentResultUnknown})
			}
		}
	}
	return members, nil
}

// memberFromRollout includes a rollout that still has a live trial child.
// The stored resource assessment counts only when the rollout status still names that child.
func memberFromRollout(kind, rolloutName string, upgrading *apiv1.UpgradingChildStatus, trialChild *unstructured.Unstructured) (trialMember, bool) {
	if upgrading != nil && upgrading.Discontinued {
		return trialMember{}, false
	}
	if trialChild == nil {
		return trialMember{}, false
	}
	assessment := apiv1.AssessmentResultUnknown
	if upgrading != nil && upgrading.Name == trialChild.GetName() {
		assessment = upgrading.EffectiveResourceAssessment()
	}
	return trialMember{
		Kind:        kind,
		RolloutName: rolloutName,
		ChildName:   trialChild.GetName(),
		Assessment:  assessment,
		Child:       trialChild,
	}, true
}

// upgradingNamesLiveChild is true when status still names a child that is no longer the trial
// (it was promoted or recycled). That rollout is not a current trial member.
func upgradingNamesLiveChild(upgrading *apiv1.UpgradingChildStatus) bool {
	return upgrading != nil && upgrading.Name != "" && !upgrading.Discontinued
}

func upgradingDiscontinued(upgrading *apiv1.UpgradingChildStatus) bool {
	return upgrading != nil && upgrading.Discontinued
}

func hasPromotedChild(ctx context.Context, rollout ctlrcommon.RolloutObject, c client.Client) (bool, error) {
	promoted, err := ctlrcommon.FindChildrenOfUpgradeState(ctx, rollout, common.LabelValueUpgradePromoted, nil, false, c)
	if err != nil {
		return false, err
	}
	return len(promoted.Items) > 0, nil
}

// pipelineAwaitingTrial reports whether a Pipeline with no trial child still belongs in the chain.
// A trial controller means every Pipeline must move onto it. Otherwise a Pipeline that selects a
// rollout which currently has a trial ISBService must move onto that ISBService.
func pipelineAwaitingTrial(ctx context.Context, c client.Client, namespace string, pipelineRollout *apiv1.PipelineRollout, trialController bool) (bool, error) {
	if trialController {
		return true, nil
	}
	isbs, err := kubernetes.ListResources(ctx, c, numaflowv1.ISBGroupVersionKind, namespace, client.MatchingLabels{
		common.LabelKeyUpgradeState: string(common.LabelValueUpgradeTrial),
	})
	if err != nil {
		return false, fmt.Errorf("listing trial ISBServices in namespace %q: %w", namespace, err)
	}
	for i := range isbs.Items {
		parent := isbs.Items[i].GetLabels()[common.LabelKeyParentRollout]
		if parent == "" {
			continue
		}
		uses, err := pipelineRolloutUsesISB(pipelineRollout, parent)
		if err != nil {
			return false, err
		}
		if uses {
			return true, nil
		}
	}
	return false, nil
}

func trialChildGVKs() []schema.GroupVersionKind {
	return []schema.GroupVersionKind{
		apiv1.NumaflowControllerGroupVersionKind,
		numaflowv1.ISBGroupVersionKind,
		numaflowv1.PipelineGroupVersionKind,
		numaflowv1.MonoVertexGroupVersionKind,
	}
}
