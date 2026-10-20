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
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

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

// ControllerChainFailed reports whether the namespace has a trial NumaflowController, in a
// Progressive upgrade, whose chain assessment is Failure. A new Pipeline, MonoVertex, or
// ISBService stays Pending until that chain succeeds, is reverted, or is force-promoted.
func ControllerChainFailed(ctx context.Context, c client.Client, namespace string) (bool, error) {
	nfcRollout, trialChild, err := findTrialController(ctx, c, namespace)
	if err != nil || nfcRollout == nil || trialChild == nil {
		return false, err
	}
	if nfcRollout.Status.UpgradeInProgress != apiv1.UpgradeStrategyProgressive {
		return false, nil
	}
	upgrading := nfcRollout.GetUpgradingChildStatus()
	if upgrading == nil || upgrading.Name != trialChild.GetName() || upgrading.Discontinued {
		return false, nil
	}
	return upgrading.ResourceChainAssessmentResult == apiv1.AssessmentResultFailure, nil
}

// OutsideTrialChain reports whether a rollout takes no part in the current trial chain: its trial
// was discontinued, or it has never created a child (a new rollout held Pending while the chain is failed).
func OutsideTrialChain(ctx context.Context, c client.Client, rollout ctlrcommon.RolloutObject, upgrading *apiv1.UpgradingChildStatus) (bool, error) {
	if upgrading != nil {
		return upgrading.Discontinued, nil
	}
	promoted, err := ctlrcommon.FindChildrenOfUpgradeState(ctx, rollout, common.LabelValueUpgradePromoted, nil, false, c)
	if err != nil {
		return false, err
	}
	return len(promoted.Items) == 0, nil
}

// AnnotateCurrentTrialChildren writes the chain-promote annotation onto the trial child of each
// chain member whose resource assessment is Success. The value records that child's name and UID.
// A member whose named child is no longer its rollout's trial is skipped.
func AnnotateCurrentTrialChildren(ctx context.Context, c client.Client, namespace string, members []apiv1.ResourceChainMemberStatus) error {
	for _, member := range members {
		if member.AssessmentResult != apiv1.AssessmentResultSuccess {
			continue
		}
		child, err := memberTrialChild(ctx, c, namespace, member)
		if err != nil {
			return err
		}
		if child == nil {
			continue
		}
		value := ChainPromoteAnnotationValue(child.GetName(), string(child.GetUID()))
		if child.GetAnnotations()[common.AnnotationKeyChainPromote] == value {
			continue
		}
		if err := kubernetes.SetAndPatchAnnotations(ctx, c, child, map[string]string{
			common.AnnotationKeyChainPromote: value,
		}); err != nil {
			return fmt.Errorf("annotating %s %s/%s for chain promotion: %w", child.GetKind(), namespace, child.GetName(), err)
		}
	}
	return nil
}

// StampForcePromoteChain sets the force-promote-chain label on the trial child of each chain member.
// While a trial NumaflowController exists, a member's trial that is not bound to it is not part of
// that trial stack and is left alone. Each rollout then promotes its own trial child through the
// normal promotion path.
func StampForcePromoteChain(ctx context.Context, c client.Client, namespace string, members []apiv1.ResourceChainMemberStatus) error {
	_, trialController, err := findTrialController(ctx, c, namespace)
	if err != nil {
		return err
	}
	trialInstanceID := ""
	if trialController != nil {
		if trialInstanceID, err = unstructuredInstanceID(trialController); err != nil {
			return err
		}
	}
	for _, member := range members {
		child, err := memberTrialChild(ctx, c, namespace, member)
		if err != nil {
			return err
		}
		if child == nil || child.GetLabels()[common.LabelKeyForcePromoteChain] == "true" {
			continue
		}
		if trialInstanceID != "" && member.Kind != "NumaflowControllerRollout" &&
			numaflowtypes.ControllerInstanceIDFromResource(child) != trialInstanceID {
			continue
		}
		if err := kubernetes.SetAndPatchLabels(ctx, c, child, map[string]string{
			common.LabelKeyForcePromoteChain: "true",
		}); err != nil {
			return fmt.Errorf("setting force-promote-chain on %s %s/%s: %w", child.GetKind(), namespace, child.GetName(), err)
		}
	}
	return nil
}

// DiscontinueTrialOnRecyclableController discontinues this rollout's trial when that trial is bound
// to a NumaflowController marked recyclable, such as a reverted trial controller. It returns true
// if the trial was discontinued. A promoted child is left alone: it moves to the newly promoted
// controller through a normal progressive upgrade.
func DiscontinueTrialOnRecyclableController(ctx context.Context, rolloutObject ProgressiveRolloutObject, controller progressiveController, c client.Client) (bool, error) {
	trialChildren, err := ctlrcommon.FindChildrenOfUpgradeState(ctx, rolloutObject, common.LabelValueUpgradeTrial, nil, false, c)
	if err != nil || len(trialChildren.Items) == 0 {
		return false, err
	}
	namespace := rolloutObject.GetRolloutObjectMeta().Namespace
	recyclableControllers, err := kubernetes.ListResources(ctx, c, apiv1.NumaflowControllerGroupVersionKind, namespace, client.MatchingLabels{
		common.LabelKeyUpgradeState: string(common.LabelValueUpgradeRecyclable),
	})
	if err != nil {
		return false, fmt.Errorf("listing recyclable NumaflowControllers in namespace %q: %w", namespace, err)
	}
	recyclableInstances := map[string]struct{}{}
	for i := range recyclableControllers.Items {
		instanceID, err := unstructuredInstanceID(&recyclableControllers.Items[i])
		if err != nil {
			return false, err
		}
		if instanceID != "" {
			recyclableInstances[instanceID] = struct{}{}
		}
	}
	for i := range trialChildren.Items {
		child := &trialChildren.Items[i]
		if _, recyclable := recyclableInstances[numaflowtypes.ControllerInstanceIDFromResource(child)]; !recyclable {
			continue
		}
		logger.FromContext(ctx).WithValues("child", child.GetName()).
			Debug("discontinuing trial bound to a recyclable NumaflowController")
		if err := Discontinue(ctx, rolloutObject, controller, c); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// memberTrialChild returns the member's named child while it is still that rollout's trial, otherwise nil.
func memberTrialChild(ctx context.Context, c client.Client, namespace string, member apiv1.ResourceChainMemberStatus) (*unstructured.Unstructured, error) {
	if member.ChildName == "" {
		return nil, nil
	}
	gvk, ok := childGVKForRolloutKind(member.Kind)
	if !ok {
		return nil, nil
	}
	child, err := kubernetes.GetResource(ctx, c, gvk, types.NamespacedName{Namespace: namespace, Name: member.ChildName})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting %s %s/%s: %w", gvk.Kind, namespace, member.ChildName, err)
	}
	labels := child.GetLabels()
	if labels[common.LabelKeyUpgradeState] != string(common.LabelValueUpgradeTrial) || labels[common.LabelKeyParentRollout] != member.Name {
		return nil, nil
	}
	return child, nil
}

func childGVKForRolloutKind(rolloutKind string) (schema.GroupVersionKind, bool) {
	switch rolloutKind {
	case "NumaflowControllerRollout":
		return apiv1.NumaflowControllerGroupVersionKind, true
	case "ISBServiceRollout":
		return numaflowv1.ISBGroupVersionKind, true
	case "PipelineRollout":
		return numaflowv1.PipelineGroupVersionKind, true
	case "MonoVertexRollout":
		return numaflowv1.MonoVertexGroupVersionKind, true
	}
	return schema.GroupVersionKind{}, false
}
