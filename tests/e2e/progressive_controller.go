package e2e

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	"github.com/numaproj/numaplane/internal/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	// BrokenNumaflowControllerVersion is installed by tests/manifests/default/controller_def_0.0.0.yaml.
	// The definition exists, and its controller image does not.
	BrokenNumaflowControllerVersion = "0.0.0"
	// BrokenJetstreamVersion is a JetStream version whose image does not exist.
	BrokenJetstreamVersion = "0.0.0"

	// A healthy controller succeeds on the first healthy observation. The end window only fails a
	// controller that never becomes healthy, so it stays long enough for an image pull.
	ControllerSuccessSchedule = "0,300,0,5"
	// A broken image never becomes healthy, so the assessment fails when this window ends.
	ControllerFailureSchedule = "0,45,0,5"

	// FastWorkloadSchedule succeeds after a short consecutive healthy period.
	FastWorkloadSchedule = "5,180,15,5"
	// FailureWorkloadSchedule gives a bad image about a minute before the workload fails.
	FailureWorkloadSchedule = "5,60,15,5"
	// RecoverWorkloadSchedule is for a healthy workload moving onto a controller that just started.
	// A one-minute window can close while the daemon is still rolling out, and that Failure is kept.
	RecoverWorkloadSchedule = "5,300,15,5"
	// SlowWorkloadSchedule delays assessment so a sibling can finish its own assessment first.
	SlowWorkloadSchedule = "240,360,15,5"
	// ISBSchedule succeeds on the first healthy observation.
	ISBSchedule = "0,180,0,5"
)

// ProgressivePipelineSpec is a small generator pipeline used by the controller progressive suites.
func ProgressivePipelineSpec(isbRolloutName string) numaflowv1.PipelineSpec {
	pull := corev1.PullIfNotPresent
	min := int32(1)
	sleep := uint32(15)
	rpu := int64(5)
	duration := metav1.Duration{Duration: time.Second}
	return numaflowv1.PipelineSpec{
		InterStepBufferServiceName: isbRolloutName,
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: "in",
				Source: &numaflowv1.Source{
					Generator: &numaflowv1.GeneratorSource{
						RPU:      &rpu,
						Duration: &duration,
					},
				},
				Scale: numaflowv1.Scale{Min: &min, Max: &min, ZeroReplicaSleepSeconds: &sleep},
			},
			{
				Name: "cat",
				UDF: &numaflowv1.UDF{
					Container: &numaflowv1.Container{
						Image:           "quay.io/numaio/numaflow-go/map-cat:stable",
						ImagePullPolicy: &pull,
					},
				},
				Scale: numaflowv1.Scale{Min: &min, Max: &min, ZeroReplicaSleepSeconds: &sleep},
			},
			{
				Name: "out",
				Sink: &numaflowv1.Sink{
					AbstractSink: numaflowv1.AbstractSink{
						Log: &numaflowv1.Log{},
					},
				},
				Scale: numaflowv1.Scale{Min: &min, Max: &min, ZeroReplicaSleepSeconds: &sleep},
			},
		},
		Edges: []numaflowv1.Edge{
			{From: "in", To: "cat"},
			{From: "cat", To: "out"},
		},
	}
}

// ProgressivePipelineSpecWithBadImage is ProgressivePipelineSpec with a UDF image that cannot be pulled.
func ProgressivePipelineSpecWithBadImage(isbRolloutName string) numaflowv1.PipelineSpec {
	spec := ProgressivePipelineSpec(isbRolloutName)
	spec.Vertices[1].UDF.Container.Image = "badcat"
	return spec
}

// ProgressivePipelineSpecWithEnv is ProgressivePipelineSpec plus an env var that forces a new Pipeline child.
func ProgressivePipelineSpecWithEnv(isbRolloutName, value string) numaflowv1.PipelineSpec {
	spec := ProgressivePipelineSpec(isbRolloutName)
	spec.Vertices[1].UDF.Container.Env = []corev1.EnvVar{{Name: "E2E_REV", Value: value}}
	return spec
}

// ProgressiveISBSpec is a small JetStream InterStepBufferService.
func ProgressiveISBSpec(version string) numaflowv1.InterStepBufferServiceSpec {
	vol, err := apiresource.ParseQuantity("10Mi")
	Expect(err).ShouldNot(HaveOccurred())
	return numaflowv1.InterStepBufferServiceSpec{
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: version,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &vol,
			},
		},
	}
}

// ProgressiveMonoVertexSpec is a one-pod MonoVertex that reads a source and drops the result.
func ProgressiveMonoVertexSpec() numaflowv1.MonoVertexSpec {
	min := int32(1)
	return numaflowv1.MonoVertexSpec{
		Scale: numaflowv1.Scale{Min: &min, Max: &min},
		Source: &numaflowv1.Source{
			UDSource: &numaflowv1.UDSource{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-rs/simple-source:stable",
				},
			},
		},
		Sink: &numaflowv1.Sink{
			AbstractSink: numaflowv1.AbstractSink{
				Blackhole: &numaflowv1.Blackhole{},
			},
		},
	}
}

// ProgressiveMonoVertexSpecWithEnv is ProgressiveMonoVertexSpec plus an env var that forces a new MonoVertex child.
func ProgressiveMonoVertexSpecWithEnv(value string) numaflowv1.MonoVertexSpec {
	spec := ProgressiveMonoVertexSpec()
	spec.Source.UDSource.Container.Env = []corev1.EnvVar{{Name: "E2E_REV", Value: value}}
	return spec
}

// ProgressiveMonoVertexSpecWithBadImage is ProgressiveMonoVertexSpec with a source image that cannot be pulled.
func ProgressiveMonoVertexSpecWithBadImage() numaflowv1.MonoVertexSpec {
	spec := ProgressiveMonoVertexSpec()
	spec.Source.UDSource.Container.Image = "badcat"
	return spec
}

// PipelineStrategyForSchedule returns a Pipeline progressive strategy with the given assessment schedule.
func PipelineStrategyForSchedule(schedule string) apiv1.PipelineStrategy {
	return apiv1.PipelineStrategy{
		PipelineTypeRolloutStrategy: apiv1.PipelineTypeRolloutStrategy{
			PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
				Progressive: apiv1.ProgressiveStrategy{AssessmentSchedule: schedule},
			},
		},
	}
}

// MonoVertexStrategyForSchedule returns a MonoVertex progressive strategy with the given assessment schedule.
func MonoVertexStrategyForSchedule(schedule string) *apiv1.PipelineTypeRolloutStrategy {
	return &apiv1.PipelineTypeRolloutStrategy{
		PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
			Progressive: apiv1.ProgressiveStrategy{AssessmentSchedule: schedule},
		},
	}
}

// SetNumaflowControllerVersion sets the NumaflowControllerRollout version and assessment schedule.
func SetNumaflowControllerVersion(version, schedule string) {
	UpdateNumaflowControllerRolloutInK8S(func(rollout apiv1.NumaflowControllerRollout) (apiv1.NumaflowControllerRollout, error) {
		rollout.Spec.Controller.Version = version
		if rollout.Spec.Strategy == nil {
			rollout.Spec.Strategy = &apiv1.NumaflowControllerRolloutStrategy{}
		}
		rollout.Spec.Strategy.Progressive.AssessmentSchedule = schedule
		return rollout, nil
	})
}

// SetPipelineAssessmentSchedule sets the progressive assessment schedule on a PipelineRollout.
func SetPipelineAssessmentSchedule(name, schedule string) {
	UpdatePipelineRolloutInK8S(Namespace, name, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
		if rollout.Spec.Strategy == nil {
			strategy := PipelineStrategyForSchedule(schedule)
			rollout.Spec.Strategy = &strategy
			return rollout, nil
		}
		rollout.Spec.Strategy.Progressive.AssessmentSchedule = schedule
		return rollout, nil
	})
}

// SetMonoVertexAssessmentSchedule sets the progressive assessment schedule on a MonoVertexRollout.
func SetMonoVertexAssessmentSchedule(name, schedule string) {
	UpdateMonoVertexRolloutInK8S(name, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
		if rollout.Spec.Strategy == nil {
			rollout.Spec.Strategy = MonoVertexStrategyForSchedule(schedule)
			return rollout, nil
		}
		rollout.Spec.Strategy.Progressive.AssessmentSchedule = schedule
		return rollout, nil
	})
}

// SetISBServiceAssessmentSchedule sets the progressive assessment schedule on an ISBServiceRollout.
func SetISBServiceAssessmentSchedule(name, schedule string) {
	UpdateISBServiceRolloutInK8S(name, func(rollout apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
		if rollout.Spec.Strategy == nil {
			rollout.Spec.Strategy = &apiv1.ISBServiceRolloutStrategy{}
		}
		rollout.Spec.Strategy.Progressive.AssessmentSchedule = schedule
		return rollout, nil
	})
}

// CreatePipelineRolloutObject creates a PipelineRollout and does not wait for a Pipeline child.
// A new Pipeline stays Pending while a controller chain is failed, so the usual create helper would time out.
func CreatePipelineRolloutObject(name string, spec numaflowv1.PipelineSpec, strategy *apiv1.PipelineStrategy) {
	obj := createPipelineRolloutSpec(name, Namespace, spec, strategy, apiv1.Metadata{})
	_, err := pipelineRolloutClient.Create(ctx, obj, metav1.CreateOptions{})
	Expect(err).ShouldNot(HaveOccurred())
}

// CreateMonoVertexRolloutObject creates a MonoVertexRollout and does not wait for a MonoVertex child.
func CreateMonoVertexRolloutObject(name string, spec numaflowv1.MonoVertexSpec, strategy *apiv1.PipelineTypeRolloutStrategy) {
	obj := createMonoVertexRolloutSpec(name, Namespace, spec, strategy, apiv1.Metadata{})
	_, err := monoVertexRolloutClient.Create(ctx, obj, metav1.CreateOptions{})
	Expect(err).ShouldNot(HaveOccurred())
}

// CreateISBServiceRolloutObject creates an ISBServiceRollout and does not wait for an ISBService child.
func CreateISBServiceRolloutObject(name string, spec numaflowv1.InterStepBufferServiceSpec) {
	obj := createISBServiceRolloutSpec(name, Namespace, spec)
	_, err := isbServiceRolloutClient.Create(ctx, obj, metav1.CreateOptions{})
	Expect(err).ShouldNot(HaveOccurred())
}

// SetObjectLabel sets one label on a Kubernetes object.
func SetObjectLabel(obj metav1.Object, key, value string) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[key] = value
	obj.SetLabels(labels)
}

// UpdateNumaflowControllerInK8S updates a NumaflowController child, retrying on conflict.
func UpdateNumaflowControllerInK8S(name string, f func(*apiv1.NumaflowController) error) {
	By("updating NumaflowController " + name)
	backoff := wait.Backoff{Steps: 10, Duration: 100 * time.Millisecond, Factor: 2.0, Jitter: 0.1}
	err := retry.RetryOnConflict(backoff, func() error {
		controller, err := numaflowControllerClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if err := f(controller); err != nil {
			return err
		}
		_, err = numaflowControllerClient.Update(ctx, controller, metav1.UpdateOptions{})
		return err
	})
	Expect(err).ShouldNot(HaveOccurred())
}

// GetNumaflowControllerRolloutProgressiveStatus returns the controller rollout's progressive status.
func GetNumaflowControllerRolloutProgressiveStatus() apiv1.NumaflowControllerProgressiveStatus {
	rollout, err := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
	Expect(err).ShouldNot(HaveOccurred())
	return rollout.Status.ProgressiveStatus
}

func listNumaflowControllers(upgradeState common.UpgradeState) ([]apiv1.NumaflowController, error) {
	list, err := numaflowControllerClient.List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s", common.LabelKeyParentRollout, numaflowControllerRolloutName,
			common.LabelKeyUpgradeState, upgradeState),
	})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func findNumaflowController(upgradeState common.UpgradeState, version string) (apiv1.NumaflowController, bool) {
	controllers, err := listNumaflowControllers(upgradeState)
	if err != nil {
		return apiv1.NumaflowController{}, false
	}
	var found *apiv1.NumaflowController
	for i := range controllers {
		if version != "" && controllers[i].Spec.Version != version {
			continue
		}
		if found == nil || controllers[i].CreationTimestamp.After(found.CreationTimestamp.Time) {
			found = &controllers[i]
		}
	}
	if found == nil {
		return apiv1.NumaflowController{}, false
	}
	return *found, true
}

// PromotedNumaflowController returns the current promoted NumaflowController.
func PromotedNumaflowController() apiv1.NumaflowController {
	var found apiv1.NumaflowController
	CheckEventually("verifying a promoted NumaflowController exists", func() bool {
		controller, ok := findNumaflowController(common.LabelValueUpgradePromoted, "")
		if ok {
			found = controller
		}
		return ok
	}).Should(BeTrue())
	return found
}

// TrialNumaflowControllerName returns the trial controller name when one of version is present.
func TrialNumaflowControllerName(version string) (string, bool) {
	controller, ok := findNumaflowController(common.LabelValueUpgradeTrial, version)
	if !ok {
		return "", false
	}
	return controller.Name, true
}

// WaitForTrialNumaflowController waits until a trial NumaflowController of version exists.
func WaitForTrialNumaflowController(version string) apiv1.NumaflowController {
	var found apiv1.NumaflowController
	CheckEventually(fmt.Sprintf("verifying a trial NumaflowController of version %s exists", version), func() bool {
		controller, ok := findNumaflowController(common.LabelValueUpgradeTrial, version)
		if ok {
			found = controller
		}
		return ok
	}).Should(BeTrue())
	return found
}

// controllerAssessmentMatchesVersion matches the assessment. A non-empty version ignores a previous
// upgrade that is still recorded: that upgrade can still say Success for a few seconds after the next
// version bump, and a discontinued Failure can still name the version that was just reverted.
func controllerAssessmentMatchesVersion(expected apiv1.AssessmentResult, version string) bool {
	status := GetNumaflowControllerRolloutProgressiveStatus()
	upgrading := status.UpgradingNumaflowControllerStatus
	if upgrading == nil {
		return false
	}
	if version != "" && (upgrading.Version != version || upgrading.Discontinued) {
		return false
	}
	matches := upgrading.ResourceAssessmentResult == expected &&
		upgrading.ResourceChainAssessmentResult == expected &&
		upgrading.AssessmentResult == expected &&
		upgrading.BasicAssessmentEndTime != nil
	if expected == apiv1.AssessmentResultSuccess {
		return matches && !upgrading.Discontinued && !upgrading.ForcedSuccess
	}
	return matches
}

// VerifyNumaflowControllerAssessment waits until the controller's own assessment, its chain assessment,
// and the promotion decision all equal expected, with basicAssessmentEndTime set.
// Use this when the controller itself is healthy or itself failed. A chain that fails because a
// dependent failed keeps the controller's own resourceAssessmentResult as Success.
func VerifyNumaflowControllerAssessment(expected apiv1.AssessmentResult) {
	VerifyNumaflowControllerAssessmentForVersion(expected, "")
}

// VerifyNumaflowControllerAssessmentForVersion waits until the assessment is for version.
func VerifyNumaflowControllerAssessmentForVersion(expected apiv1.AssessmentResult, version string) {
	CheckEventually(fmt.Sprintf("verifying NumaflowController assessment is %s", expected), func() bool {
		return controllerAssessmentMatchesVersion(expected, version)
	}).Should(BeTrue())
}

// currentTrialControllerStatus returns the upgrading status only while it describes the live trial
// NumaflowController. A reverted trial leaves its Discontinued status, including its Failure, until
// the next trial replaces it, and consecutive trials can share a version.
func currentTrialControllerStatus() *apiv1.UpgradingChildStatus {
	upgrading := GetNumaflowControllerRolloutProgressiveStatus().UpgradingNumaflowControllerStatus
	if upgrading == nil || upgrading.Discontinued {
		return nil
	}
	trial, ok := findNumaflowController(common.LabelValueUpgradeTrial, "")
	if !ok || trial.Name != upgrading.Name {
		return nil
	}
	return &upgrading.UpgradingChildStatus
}

// VerifyNumaflowControllerChainFailed waits until the current trial's chain assessment and promotion decision are Failure.
func VerifyNumaflowControllerChainFailed() {
	failed := func() bool {
		upgrading := currentTrialControllerStatus()
		return upgrading != nil &&
			upgrading.ResourceChainAssessmentResult == apiv1.AssessmentResultFailure &&
			upgrading.AssessmentResult == apiv1.AssessmentResultFailure
	}
	CheckEventually("verifying NumaflowController chain assessment is Failure", failed).Should(BeTrue())
}

// VerifyNumaflowControllerAssessmentStays checks the assessment again for a few seconds.
// A later status update must not clear a completed result.
func VerifyNumaflowControllerAssessmentStays(expected apiv1.AssessmentResult) {
	VerifyNumaflowControllerAssessmentStaysForVersion(expected, "")
}

// VerifyNumaflowControllerAssessmentStaysForVersion checks the assessment of version again for a few seconds.
func VerifyNumaflowControllerAssessmentStaysForVersion(expected apiv1.AssessmentResult, version string) {
	CheckConsistently(fmt.Sprintf("verifying NumaflowController assessment stays %s", expected), func() bool {
		return controllerAssessmentMatchesVersion(expected, version)
	}).WithTimeout(5 * time.Second).Should(BeTrue())
}

// VerifyPromotedNumaflowControllerVersion waits until the promoted controller runs version.
func VerifyPromotedNumaflowControllerVersion(version string) {
	CheckEventually(fmt.Sprintf("verifying promoted NumaflowController version %s", version), func() bool {
		_, ok := findNumaflowController(common.LabelValueUpgradePromoted, version)
		return ok
	}).Should(BeTrue())
}

// VerifyNumaflowControllerRolloutUpgradeStrategy waits until the controller rollout reports strategy.
func VerifyNumaflowControllerRolloutUpgradeStrategy(strategy apiv1.UpgradeStrategy) {
	CheckEventually(fmt.Sprintf("verifying NumaflowControllerRollout upgrade strategy %s", strategy), func() bool {
		rollout, err := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		return err == nil && rollout.Status.UpgradeInProgress == strategy
	}).Should(BeTrue())
}

// VerifyNumaflowControllerDiscontinued waits until reverting version has recycled the trial and left version promoted.
func VerifyNumaflowControllerDiscontinued(version string) {
	CheckEventually(fmt.Sprintf("verifying NumaflowController trial discontinued and %s remains promoted", version), func() bool {
		trials, err := listNumaflowControllers(common.LabelValueUpgradeTrial)
		if err != nil || len(trials) > 0 {
			return false
		}
		_, promoted := findNumaflowController(common.LabelValueUpgradePromoted, version)
		if !promoted {
			return false
		}
		status := GetNumaflowControllerRolloutProgressiveStatus()
		rollout, err := numaflowControllerRolloutClient.Get(ctx, numaflowControllerRolloutName, metav1.GetOptions{})
		if err != nil || rollout.Status.UpgradeInProgress != apiv1.UpgradeStrategyNoOp {
			return false
		}
		return status.UpgradingNumaflowControllerStatus != nil && status.UpgradingNumaflowControllerStatus.Discontinued
	}).Should(BeTrue())
}

// VerifyNumaflowControllerDeleted waits until the named NumaflowController and its Deployment are gone.
func VerifyNumaflowControllerDeleted(name, instanceID string) {
	CheckEventually(fmt.Sprintf("verifying NumaflowController %s is deleted", name), func() bool {
		_, err := numaflowControllerClient.Get(ctx, name, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}).Should(BeTrue())

	deploymentName := numaflowControllerDeploymentName
	if strings.TrimSpace(instanceID) != "" {
		deploymentName = fmt.Sprintf("%s-%s", numaflowControllerDeploymentName, instanceID)
	}
	CheckEventually(fmt.Sprintf("verifying Deployment %s is deleted", deploymentName), func() bool {
		_, err := kubeClient.AppsV1().Deployments(Namespace).Get(ctx, deploymentName, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}).Should(BeTrue())
}

// VerifyNumaflowControllerForcePromoted waits until force-promote-chain has promoted version.
func VerifyNumaflowControllerForcePromoted(version string) {
	CheckEventually(fmt.Sprintf("verifying NumaflowController %s was force-promoted", version), func() bool {
		status := GetNumaflowControllerRolloutProgressiveStatus()
		upgrading := status.UpgradingNumaflowControllerStatus
		if upgrading == nil || !upgrading.ForcedSuccess || upgrading.AssessmentResult != apiv1.AssessmentResultSuccess {
			return false
		}
		_, promoted := findNumaflowController(common.LabelValueUpgradePromoted, version)
		return promoted
	}).Should(BeTrue())
}

// VerifyNumaflowControllerFailureIncludes waits until the controller failure reasons contain reason.
func VerifyNumaflowControllerFailureIncludes(reason string) {
	CheckEventually(fmt.Sprintf("verifying NumaflowController failure includes %q", reason), func() bool {
		upgrading := currentTrialControllerStatus()
		if upgrading == nil {
			return false
		}
		return strings.Contains(strings.Join(upgrading.FailureReasons, "; "), reason)
	}).Should(BeTrue())
}

func controllerResourceUIDs(controllerName string) (map[string]string, error) {
	selector := fmt.Sprintf("%s=%s", common.LabelKeyNumaplaneInstance, controllerName)
	ids := map[string]string{}
	add := func(kind, name, uid string) {
		ids[kind+"/"+name] = uid
	}

	deployments, err := kubeClient.AppsV1().Deployments(Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	for i := range deployments.Items {
		add("Deployment", deployments.Items[i].Name, string(deployments.Items[i].UID))
	}
	configMaps, err := kubeClient.CoreV1().ConfigMaps(Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	for i := range configMaps.Items {
		add("ConfigMap", configMaps.Items[i].Name, string(configMaps.Items[i].UID))
	}
	roles, err := kubeClient.RbacV1().Roles(Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	for i := range roles.Items {
		add("Role", roles.Items[i].Name, string(roles.Items[i].UID))
	}
	return ids, nil
}

// SnapshotControllerResources records Deployment, ConfigMap, and Role UIDs owned by a NumaflowController child.
func SnapshotControllerResources(controllerName string) map[string]string {
	var snapshot map[string]string
	CheckEventually(fmt.Sprintf("verifying NumaflowController %s has Deployments, ConfigMaps, and Roles", controllerName), func() bool {
		ids, err := controllerResourceUIDs(controllerName)
		if err != nil || len(ids) == 0 {
			return false
		}
		hasDeployment, hasConfigMap, hasRole := false, false, false
		for key := range ids {
			switch {
			case strings.HasPrefix(key, "Deployment/"):
				hasDeployment = true
			case strings.HasPrefix(key, "ConfigMap/"):
				hasConfigMap = true
			case strings.HasPrefix(key, "Role/"):
				hasRole = true
			}
		}
		if hasDeployment && hasConfigMap && hasRole {
			snapshot = ids
			return true
		}
		return false
	}).Should(BeTrue())
	return snapshot
}

// VerifyControllerResourcesUnchanged checks that a controller's Deployments, ConfigMaps, and Roles keep their UIDs.
func VerifyControllerResourcesUnchanged(controllerName string, before map[string]string) {
	CheckConsistently(fmt.Sprintf("verifying %s did not replace its Deployments, ConfigMaps, or Roles", controllerName), func() bool {
		current, err := controllerResourceUIDs(controllerName)
		if err != nil {
			return false
		}
		for key, uid := range before {
			if current[key] != uid {
				return false
			}
		}
		return true
	}).WithTimeout(10 * time.Second).Should(BeTrue())
}

// VerifyControllerResourceNamesDisjoint checks that two controllers own different Deployments, ConfigMaps, and Roles.
func VerifyControllerResourceNamesDisjoint(leftName, rightName string) {
	CheckEventually(fmt.Sprintf("verifying %s and %s own different resources", leftName, rightName), func() bool {
		left, err := controllerResourceUIDs(leftName)
		if err != nil || len(left) == 0 {
			return false
		}
		right, err := controllerResourceUIDs(rightName)
		if err != nil || len(right) == 0 {
			return false
		}
		for key := range left {
			if _, overlap := right[key]; overlap {
				return false
			}
		}
		return true
	}).Should(BeTrue())
}

// ControllerInstanceIDOf reads the Numaflow controller instance a Pipeline, ISBService, or MonoVertex is bound to.
func ControllerInstanceIDOf(obj *unstructured.Unstructured) string {
	if obj == nil {
		return ""
	}
	if id := obj.GetAnnotations()[common.AnnotationKeyNumaflowInstanceID]; id != "" {
		return id
	}
	return obj.GetLabels()[common.LabelKeyControllerInstanceID]
}

func childOnInstance(list *unstructured.UnstructuredList, instanceID string) bool {
	if list == nil {
		return false
	}
	for i := range list.Items {
		if ControllerInstanceIDOf(&list.Items[i]) == instanceID {
			return true
		}
	}
	return false
}

// VerifyWorkloadsHaveChildOnInstance waits until each rollout has a live child bound to instanceID.
// The child may already be promoted: a fast workload can finish before the test observes its trial.
func VerifyWorkloadsHaveChildOnInstance(pipelineRolloutName, isbRolloutName, monoVertexRolloutName, instanceID string) {
	CheckEventually(fmt.Sprintf("verifying workloads have a child on controller instance %q", instanceID), func() bool {
		if !rolloutHasChildOnInstance(GetGVRForPipeline(), pipelineRolloutName, instanceID) {
			return false
		}
		if !rolloutHasChildOnInstance(GetGVRForISBService(), isbRolloutName, instanceID) {
			return false
		}
		if monoVertexRolloutName == "" {
			return true
		}
		return rolloutHasChildOnInstance(GetGVRForMonoVertex(), monoVertexRolloutName, instanceID)
	}).Should(BeTrue())
}

func rolloutHasChildOnInstance(gvr schema.GroupVersionResource, rolloutName, instanceID string) bool {
	children, err := GetChildren(gvr, Namespace, rolloutName)
	if err != nil || children == nil {
		return false
	}
	for i := range children.Items {
		labels := children.Items[i].GetLabels()
		if labels[common.LabelKeyUpgradeState] == string(common.LabelValueUpgradeRecyclable) {
			continue
		}
		if ControllerInstanceIDOf(&children.Items[i]) == instanceID {
			return true
		}
	}
	return false
}

// VerifyTrialWorkloadsOnInstance waits until the trial Pipeline, ISBService, and MonoVertex are bound to instanceID.
func VerifyTrialWorkloadsOnInstance(pipelineRolloutName, isbRolloutName, monoVertexRolloutName, instanceID string) {
	CheckEventually(fmt.Sprintf("verifying trial workloads are on controller instance %q", instanceID), func() bool {
		pipelines, err := GetUpgradingPipelines(Namespace, pipelineRolloutName)
		if err != nil || !childOnInstance(pipelines, instanceID) {
			return false
		}
		isbServices, err := GetUpgradingISBServices(Namespace, isbRolloutName)
		if err != nil || !childOnInstance(isbServices, instanceID) {
			return false
		}
		if monoVertexRolloutName == "" {
			return true
		}
		monoVertices, err := GetUpgradingMonoVertices(Namespace, monoVertexRolloutName)
		return err == nil && childOnInstance(monoVertices, instanceID)
	}).Should(BeTrue())
}

// VerifyPromotedWorkloadsOnInstance waits until the promoted workloads are bound to instanceID.
func VerifyPromotedWorkloadsOnInstance(pipelineRolloutName, isbRolloutName, monoVertexRolloutName, instanceID string) {
	CheckEventually(fmt.Sprintf("verifying promoted workloads are on controller instance %q", instanceID), func() bool {
		pipeline, err := GetPromotedPipeline(Namespace, pipelineRolloutName)
		if err != nil || ControllerInstanceIDOf(pipeline) != instanceID {
			return false
		}
		isbService, err := GetPromotedISBService(Namespace, isbRolloutName)
		if err != nil || ControllerInstanceIDOf(isbService) != instanceID {
			return false
		}
		if ControllerInstanceIDOf(pipeline) != ControllerInstanceIDOf(isbService) {
			return false
		}
		if monoVertexRolloutName == "" {
			return true
		}
		monoVertex, err := GetPromotedMonoVertex(Namespace, monoVertexRolloutName)
		return err == nil && ControllerInstanceIDOf(monoVertex) == instanceID
	}).Should(BeTrue())
}

// VerifyPipelineReconciledByInstance waits until the promoted Pipeline is Running and was reconciled
// by the controller instance named in numaflow.numaproj.io/instance.
func VerifyPipelineReconciledByInstance(pipelineRolloutName, instanceID string) {
	CheckEventually(fmt.Sprintf("verifying Pipeline is reconciled by controller instance %q", instanceID), func() bool {
		pipeline, err := GetPromotedPipeline(Namespace, pipelineRolloutName)
		if err != nil || ControllerInstanceIDOf(pipeline) != instanceID {
			return false
		}
		status, err := GetPipelineStatus(pipeline)
		if err != nil {
			return false
		}
		return status.Phase == numaflowv1.PipelinePhaseRunning && status.ObservedGeneration >= pipeline.GetGeneration()
	}).Should(BeTrue())
}

// VerifyPipelinePassesData waits until the log sink has written a generator message.
func VerifyPipelinePassesData(pipelineName string) {
	CheckEventually(fmt.Sprintf("verifying Pipeline %s passes data", pipelineName), func() bool {
		selector := fmt.Sprintf("%s=%s,%s=%s", numaflowv1.KeyPipelineName, pipelineName, numaflowv1.KeyComponent, ComponentVertex)
		pods, err := kubeClient.CoreV1().Pods(Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return false
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			if !strings.Contains(pod.Name, "-out-") {
				continue
			}
			if podLogContains(pod.Name, "numa", "EventTime") || podLogContains(pod.Name, "", "EventTime") {
				return true
			}
		}
		return false
	}).Should(BeTrue())
}

func podLogContains(podName, container, text string) bool {
	opts := &corev1.PodLogOptions{TailLines: ptr.To(int64(200))}
	if container != "" {
		opts.Container = container
	}
	raw, err := kubeClient.CoreV1().Pods(Namespace).GetLogs(podName, opts).DoRaw(ctx)
	if err != nil {
		return false
	}
	return strings.Contains(string(raw), text)
}

// VerifyPipelineChildCountStable checks that a PipelineRollout does not gain or lose children.
func VerifyPipelineChildCountStable(pipelineRolloutName string, count int) {
	CheckConsistently(fmt.Sprintf("verifying PipelineRollout %s keeps %d children", pipelineRolloutName, count), func() int {
		return GetNumberOfChildren(GetGVRForPipeline(), Namespace, pipelineRolloutName)
	}).WithTimeout(10 * time.Second).Should(Equal(count))
}

// VerifyPipelineRolloutPendingWithoutChildren waits until a new PipelineRollout stays Pending with no Pipeline.
func VerifyPipelineRolloutPendingWithoutChildren(name string) {
	pending := func() bool {
		rollout, err := pipelineRolloutClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		return rollout.Status.Phase == apiv1.PhasePending && GetNumberOfChildren(GetGVRForPipeline(), Namespace, name) == 0
	}
	CheckEventually(fmt.Sprintf("verifying PipelineRollout %s is Pending with no Pipeline", name), pending).Should(BeTrue())
	CheckConsistently(fmt.Sprintf("verifying PipelineRollout %s stays Pending with no Pipeline", name), pending).
		WithTimeout(10 * time.Second).Should(BeTrue())
}

// VerifyPromotedPipelineEnv waits until the promoted Pipeline's cat vertex has the env value from a spec change.
func VerifyPromotedPipelineEnv(pipelineRolloutName, value string) {
	VerifyPromotedPipelineSpec(Namespace, pipelineRolloutName, func(spec numaflowv1.PipelineSpec) bool {
		if len(spec.Vertices) < 2 || spec.Vertices[1].UDF == nil || spec.Vertices[1].UDF.Container == nil {
			return false
		}
		for _, env := range spec.Vertices[1].UDF.Container.Env {
			if env.Name == "E2E_REV" && env.Value == value {
				return true
			}
		}
		return false
	})
}

// VerifyPromotedISBVersion waits until the promoted ISBService uses version.
func VerifyPromotedISBVersion(isbRolloutName, version string) {
	VerifyPromotedISBServiceSpec(Namespace, isbRolloutName, func(spec numaflowv1.InterStepBufferServiceSpec) bool {
		return spec.JetStream != nil && spec.JetStream.Version == version
	})
}

// VerifyPromotedMonoVertexEnv waits until the promoted MonoVertex source has the env value from a spec change.
func VerifyPromotedMonoVertexEnv(monoVertexRolloutName, value string) {
	VerifyPromotedMonoVertexSpec(Namespace, monoVertexRolloutName, func(spec numaflowv1.MonoVertexSpec) bool {
		if spec.Source == nil || spec.Source.UDSource == nil || spec.Source.UDSource.Container == nil {
			return false
		}
		for _, env := range spec.Source.UDSource.Container.Env {
			if env.Name == "E2E_REV" && env.Value == value {
				return true
			}
		}
		return false
	})
}

// VerifyResourceAssessment waits until a rollout's own resource assessment equals expected while its chain may still be open.
func VerifyPipelineResourceAssessment(pipelineRolloutName string, expected apiv1.AssessmentResult) {
	CheckEventually(fmt.Sprintf("verifying Pipeline %s resource assessment %s", pipelineRolloutName, expected), func() bool {
		status := GetPipelineRolloutProgressiveStatus(pipelineRolloutName)
		return status.UpgradingPipelineStatus != nil && status.UpgradingPipelineStatus.ResourceAssessmentResult == expected
	}).Should(BeTrue())
}

// VerifyPipelineResourceAssessmentUnresolved waits until the Pipeline has a trial whose own assessment is not Success.
func VerifyPipelineResourceAssessmentUnresolved(pipelineRolloutName string) {
	CheckEventually(fmt.Sprintf("verifying Pipeline %s resource assessment is not Success", pipelineRolloutName), func() bool {
		status := GetPipelineRolloutProgressiveStatus(pipelineRolloutName)
		return status.UpgradingPipelineStatus != nil && status.UpgradingPipelineStatus.ResourceAssessmentResult != apiv1.AssessmentResultSuccess
	}).Should(BeTrue())
}

// VerifyISBResourceAssessment waits until an ISBService's own resource assessment equals expected.
func VerifyISBResourceAssessment(isbRolloutName string, expected apiv1.AssessmentResult) {
	CheckEventually(fmt.Sprintf("verifying ISBService %s resource assessment %s", isbRolloutName, expected), func() bool {
		status := GetISBServiceRolloutProgressiveStatus(isbRolloutName)
		return status.UpgradingISBServiceStatus != nil && status.UpgradingISBServiceStatus.ResourceAssessmentResult == expected
	}).Should(BeTrue())
}

// VerifyISBResourceAssessmentUnresolved waits until the ISBService has a trial whose own assessment is not Success.
func VerifyISBResourceAssessmentUnresolved(isbRolloutName string) {
	CheckEventually(fmt.Sprintf("verifying ISBService %s resource assessment is not Success", isbRolloutName), func() bool {
		status := GetISBServiceRolloutProgressiveStatus(isbRolloutName)
		return status.UpgradingISBServiceStatus != nil && status.UpgradingISBServiceStatus.ResourceAssessmentResult != apiv1.AssessmentResultSuccess
	}).Should(BeTrue())
}

// SetupProgressiveControllerStack creates a controller, ISBService, Pipeline, and MonoVertex and waits until they are running.
func SetupProgressiveControllerStack(version, isbName, pipelineName, monoVertexName, workloadSchedule string) {
	CreateNumaflowControllerRollout(version)
	SetNumaflowControllerVersion(version, ControllerSuccessSchedule)

	CreateISBServiceRollout(isbName, ProgressiveISBSpec(InitialJetstreamVersion))
	SetISBServiceAssessmentSchedule(isbName, ISBSchedule)

	CreateInitialPipelineRollout(pipelineName, GetInstanceName(isbName, 0), ProgressivePipelineSpec(isbName),
		PipelineStrategyForSchedule(workloadSchedule), apiv1.Metadata{})

	if monoVertexName != "" {
		CreateMonoVertexRollout(monoVertexName, Namespace, ProgressiveMonoVertexSpec(),
			MonoVertexStrategyForSchedule(workloadSchedule), apiv1.Metadata{})
	}
}

// VerifySuccessfulControllerUpgrade waits until version is promoted and the workloads are running on its instance.
func VerifySuccessfulControllerUpgrade(version, pipelineName, isbName, monoVertexName string) {
	VerifyNumaflowControllerAssessmentForVersion(apiv1.AssessmentResultSuccess, version)
	VerifyNumaflowControllerAssessmentStaysForVersion(apiv1.AssessmentResultSuccess, version)
	VerifyPromotedNumaflowControllerVersion(version)

	promoted, ok := findNumaflowController(common.LabelValueUpgradePromoted, version)
	Expect(ok).Should(BeTrue())
	VerifyPromotedWorkloadsOnInstance(pipelineName, isbName, monoVertexName, promoted.Spec.InstanceID)
	VerifyPipelineReconciledByInstance(pipelineName, promoted.Spec.InstanceID)
	VerifyPromotedPipelineRunning(Namespace, pipelineName)
	if monoVertexName != "" {
		Expect(VerifyPromotedMonoVertexRunning(Namespace, monoVertexName)).Should(Succeed())
	}

	VerifyPipelineRolloutInProgressStrategy(pipelineName, apiv1.UpgradeStrategyNoOp)
	VerifyISBServiceRolloutInProgressStrategy(isbName, apiv1.UpgradeStrategyNoOp)
	VerifyNumaflowControllerRolloutUpgradeStrategy(apiv1.UpgradeStrategyNoOp)
	VerifyNumaflowControllerRolloutReady()
}

// VerifyPipelineRolloutHeldPending waits until a new PipelineRollout stays Pending, with no child and no child name used.
func VerifyPipelineRolloutHeldPending(name string) {
	verifyRolloutHeldPending(name, func() (apiv1.Phase, *int32, error) {
		rollout, err := pipelineRolloutClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", nil, err
		}
		return rollout.Status.Phase, rollout.Status.NameCount, nil
	}, GetGVRForPipeline())
}

// VerifyMonoVertexRolloutHeldPending waits until a new MonoVertexRollout stays Pending, with no child and no child name used.
func VerifyMonoVertexRolloutHeldPending(name string) {
	verifyRolloutHeldPending(name, func() (apiv1.Phase, *int32, error) {
		rollout, err := monoVertexRolloutClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", nil, err
		}
		return rollout.Status.Phase, rollout.Status.NameCount, nil
	}, GetGVRForMonoVertex())
}

// VerifyISBServiceRolloutHeldPending waits until a new ISBServiceRollout stays Pending, with no child and no child name used.
func VerifyISBServiceRolloutHeldPending(name string) {
	verifyRolloutHeldPending(name, func() (apiv1.Phase, *int32, error) {
		rollout, err := isbServiceRolloutClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", nil, err
		}
		return rollout.Status.Phase, rollout.Status.NameCount, nil
	}, GetGVRForISBService())
}

func verifyRolloutHeldPending(name string, read func() (apiv1.Phase, *int32, error), gvr schema.GroupVersionResource) {
	held := func() bool {
		phase, nameCount, err := read()
		if err != nil {
			return false
		}
		return phase == apiv1.PhasePending && nameCount == nil && GetNumberOfChildren(gvr, Namespace, name) == 0
	}
	CheckEventually(fmt.Sprintf("verifying %s is Pending with no child name used", name), held).Should(BeTrue())
	CheckConsistently(fmt.Sprintf("verifying %s stays Pending with no child name used", name), held).
		WithTimeout(10 * time.Second).Should(BeTrue())
}

// VerifyFirstChildOnInstance waits until the rollout's first child exists on instanceID.
// A rollout held Pending must not consume a name, so that child is <name>-0.
func VerifyFirstChildOnInstance(gvr schema.GroupVersionResource, rolloutName, instanceID string) {
	childName := fmt.Sprintf("%s-0", rolloutName)
	CheckEventually(fmt.Sprintf("verifying %s is on controller instance %q", childName, instanceID), func() bool {
		child, err := GetResource(gvr, Namespace, childName)
		if err != nil {
			return false
		}
		if child.GetLabels()[common.LabelKeyUpgradeState] == string(common.LabelValueUpgradeRecyclable) {
			return false
		}
		return ControllerInstanceIDOf(child) == instanceID
	}).Should(BeTrue())
}

// VerifyNoLiveChildOnInstance waits until no live child of the rollout is bound to instanceID.
func VerifyNoLiveChildOnInstance(gvr schema.GroupVersionResource, rolloutName, instanceID string) {
	CheckEventually(fmt.Sprintf("verifying %s has no live child on controller instance %q", rolloutName, instanceID), func() bool {
		return !rolloutHasChildOnInstance(gvr, rolloutName, instanceID)
	}).Should(BeTrue())
}

// WaitForTrialChildOnInstance returns the newest trial child bound to instanceID.
func WaitForTrialChildOnInstance(gvr schema.GroupVersionResource, rolloutName, instanceID string) *unstructured.Unstructured {
	var found *unstructured.Unstructured
	CheckEventually(fmt.Sprintf("verifying %s has a trial child on controller instance %q", rolloutName, instanceID), func() bool {
		child, ok := newestTrialChild(gvr, rolloutName)
		if !ok || ControllerInstanceIDOf(child) != instanceID {
			return false
		}
		found = child
		return true
	}).Should(BeTrue())
	return found
}

// WaitForNewerTrialChildOnInstance returns a trial child on instanceID other than previousName.
func WaitForNewerTrialChildOnInstance(gvr schema.GroupVersionResource, rolloutName, instanceID, previousName string) *unstructured.Unstructured {
	var found *unstructured.Unstructured
	CheckEventually(fmt.Sprintf("verifying %s replaced trial child %s on controller instance %q", rolloutName, previousName, instanceID), func() bool {
		child, ok := newestTrialChild(gvr, rolloutName)
		if !ok || child.GetName() == previousName || ControllerInstanceIDOf(child) != instanceID {
			return false
		}
		found = child
		return true
	}).Should(BeTrue())
	return found
}

func newestTrialChild(gvr schema.GroupVersionResource, rolloutName string) (*unstructured.Unstructured, bool) {
	list, err := GetChildrenOfUpgradeStrategy(gvr, Namespace, rolloutName, common.LabelValueUpgradeTrial)
	if err != nil || list == nil || len(list.Items) == 0 {
		return nil, false
	}
	newest := &list.Items[0]
	for i := range list.Items {
		if list.Items[i].GetCreationTimestamp().After(newest.GetCreationTimestamp().Time) {
			newest = &list.Items[i]
		}
	}
	return newest.DeepCopy(), true
}

// PipelineInterStepBufferServiceName reads the ISBService name from a Pipeline.
func PipelineInterStepBufferServiceName(pipeline *unstructured.Unstructured) string {
	spec, err := GetPipelineSpec(pipeline)
	if err != nil {
		return ""
	}
	return spec.InterStepBufferServiceName
}

// DeleteProgressiveControllerStack deletes the workloads and then the NumaflowControllerRollout.
func DeleteProgressiveControllerStack(pipelineNames, monoVertexNames, isbNames []string) {
	for _, name := range pipelineNames {
		DeletePipelineRollout(name)
	}
	for _, name := range monoVertexNames {
		DeleteMonoVertexRollout(name)
	}
	for _, name := range isbNames {
		DeleteISBServiceRollout(name)
	}
	DeleteNumaflowControllerRollout()
}
