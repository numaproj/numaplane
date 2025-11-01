package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"k8s.io/utils/ptr"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/retry"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func GetPromotedPipeline(namespace, pipelineRolloutName string) (*unstructured.Unstructured, error) {
	return getChildResource(GetGVRForPipeline(), namespace, pipelineRolloutName)
}
func GetUpgradingPipelines(namespace, pipelineRolloutName string) (*unstructured.UnstructuredList, error) {
	return GetChildrenOfUpgradeStrategy(GetGVRForPipeline(), namespace, pipelineRolloutName, common.LabelValueUpgradeInProgress)
}

func GetPipelineByName(namespace, pipelineName string) (*unstructured.Unstructured, error) {
	return dynamicClient.Resource(GetGVRForPipeline()).Namespace(namespace).Get(ctx, pipelineName, metav1.GetOptions{})
}

func GetPromotedPipelineName(namespace, pipelineRolloutName string) (string, error) {
	pipeline, err := GetPromotedPipeline(namespace, pipelineRolloutName)
	if err != nil {
		return "", err
	}
	return pipeline.GetName(), nil
}

func GetCurrentPipelineCount(pipelineRolloutName string) int {
	rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
	return int(*rollout.Status.NameCount)
}

func VerifyPromotedPipelineSpec(namespace string, pipelineRolloutName string, f func(numaflowv1.PipelineSpec) bool) {
	var retrievedPipelineSpec numaflowv1.PipelineSpec
	CheckEventually("verifying Pipeline Spec", func() bool {
		unstruct, err := GetPromotedPipeline(namespace, pipelineRolloutName)
		if err != nil {
			return false
		}
		if retrievedPipelineSpec, err = GetPipelineSpec(unstruct); err != nil {
			return false
		}

		return f(retrievedPipelineSpec)
	}).Should(BeTrue())
}

func VerifyPromotedPipelineMetadata(namespace string, pipelineRolloutName string, f func(apiv1.Metadata) bool) {
	var retrievedPipelineMetadata apiv1.Metadata
	CheckEventually("verifying Pipeline Metadata", func() bool {
		unstruct, err := GetPromotedPipeline(namespace, pipelineRolloutName)
		if err != nil {
			return false
		}
		if retrievedPipelineMetadata, err = GetPipelineMetadata(unstruct); err != nil {
			return false
		}

		return f(retrievedPipelineMetadata)
	}).Should(BeTrue())
}

// GetPipelineSpecAndStatus retrieves a pipeline by name and returns its spec and status
func GetPipelineSpecAndStatus(namespace, pipelineName string) (*unstructured.Unstructured, numaflowv1.PipelineSpec, numaflowv1.PipelineStatus, error) {
	var retrievedPipelineSpec numaflowv1.PipelineSpec
	var retrievedPipelineStatus numaflowv1.PipelineStatus

	pipeline, err := GetPipelineByName(namespace, pipelineName)
	if err != nil {
		return pipeline, retrievedPipelineSpec, retrievedPipelineStatus, err
	}

	if retrievedPipelineSpec, err = GetPipelineSpec(pipeline); err != nil {
		return pipeline, retrievedPipelineSpec, retrievedPipelineStatus, err
	}

	if retrievedPipelineStatus, err = GetPipelineStatus(pipeline); err != nil {
		return pipeline, retrievedPipelineSpec, retrievedPipelineStatus, err
	}

	return pipeline, retrievedPipelineSpec, retrievedPipelineStatus, nil
}

func VerifyPipelineSpecStatus(namespace string, pipelineName string, f func(numaflowv1.PipelineSpec, numaflowv1.PipelineStatus) bool) {
	CheckEventually("verifying Pipeline Spec", func() bool {
		_, retrievedPipelineSpec, retrievedPipelineStatus, err := GetPipelineSpecAndStatus(namespace, pipelineName)
		if err != nil {
			return false
		}

		return f(retrievedPipelineSpec, retrievedPipelineStatus)
	}).Should(BeTrue())
}

func VerifyPromotedPipelineStatusEventually(namespace string, pipelineRolloutName string, f func(numaflowv1.PipelineSpec, numaflowv1.PipelineStatus) bool) {
	CheckEventually("verify pipeline status", func() bool {
		_, retrievedPipelineSpec, retrievedPipelineStatus, err := GetPromotedPipelineSpecAndStatus(namespace, pipelineRolloutName)
		return err == nil && f(retrievedPipelineSpec, retrievedPipelineStatus)
	}).Should(BeTrue())
}

func VerifyPromotedPipelineStatusConsistently(namespace string, pipelineRolloutName string, f func(numaflowv1.PipelineSpec, numaflowv1.PipelineStatus) bool) {
	CheckConsistently("verify pipeline status", func() bool {
		_, retrievedPipelineSpec, retrievedPipelineStatus, err := GetPromotedPipelineSpecAndStatus(namespace, pipelineRolloutName)
		return err == nil && f(retrievedPipelineSpec, retrievedPipelineStatus)
	}).WithTimeout(15 * time.Second).Should(BeTrue())
}

func VerifyPipelineRolloutDeployed(pipelineRolloutName string) {
	CheckEventually("Verifying that the PipelineRollout is Deployed", func() bool {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return rollout.Status.Phase == apiv1.PhaseDeployed
	}).Should(BeTrue())

	CheckEventually("Verifying that the PipelineRollout is Deployed", func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceDeployed)
	}).Should(Equal(metav1.ConditionTrue))

}

func VerifyPipelineRolloutHealthy(pipelineRolloutName string) {
	CheckEventually("Verifying that the PipelineRollout Child Condition is True", func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceHealthy)
	}).Should(Equal(metav1.ConditionTrue))

	CheckEventually("Verifying that the PipelineRollout Progressive Upgrade Condition is True", func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionProgressiveUpgradeSucceeded)
	}).Should(Or(Equal(metav1.ConditionTrue), Equal(metav1.ConditionUnknown)))

}

func VerifyPromotedPipelineRunning(namespace string, pipelineRolloutName string) {
	By("Verifying that the Pipeline is running")
	VerifyPromotedPipelineStatusEventually(namespace, pipelineRolloutName,
		func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
			return retrievedPipelineStatus.Phase == numaflowv1.PipelinePhaseRunning
		})
	CheckEventually("verify pipeline status", func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
	}).Should(Not(Equal(metav1.ConditionTrue)))

	// Get Pipeline Pods to verify they're all up
	By("Verifying that the Pipeline is ready")
	// check "vertex" Pods
	pipeline, err := GetPromotedPipeline(namespace, pipelineRolloutName)
	Expect(err).ShouldNot(HaveOccurred())
	spec, err := GetPipelineSpec(pipeline)
	Expect(err).ShouldNot(HaveOccurred())

	VerifyVerticesPodsRunning(namespace, pipeline.GetName(), spec.Vertices, ComponentVertex)
	verifyPodsRunning(namespace, 1, getDaemonLabelSelector(pipeline.GetName()))
}

func VerifyPromotedPipelinePaused(namespace string, pipelineRolloutName string) {
	CheckEventually("Verify that Pipeline Rollout condition is Pausing/Paused", func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
	}).Should(Equal(metav1.ConditionTrue))

	By("Verify that Pipeline is paused and fully drained")
	VerifyPromotedPipelineStatusEventually(namespace, pipelineRolloutName,
		func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
			return retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePaused && retrievedPipelineStatus.DrainedOnPause
		})
	// this happens too fast to verify it:
	//verifyPodsRunning(namespace, 0, getVertexLabelSelector(pipelineName))
}

func VerifyPromotedPipelineFailed(namespace, pipelineRolloutName string) {
	By("Verify that Pipeline is Failed")
	VerifyPromotedPipelineStatusEventually(namespace, pipelineRolloutName,
		func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
			return retrievedPipelineStatus.Phase == numaflowv1.PipelinePhaseFailed
		})
}

func VerifyPipelineRolloutConditionPausing(namespace string, pipelineRolloutName string) {
	CheckEventually("Verify that Pipeline Rollout condition is Pausing/Paused", func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
	}).Should(Equal(metav1.ConditionTrue))
}

func VerifyPipelineRolloutProgressiveCondition(pipelineRolloutName string, success metav1.ConditionStatus) {
	CheckEventually(fmt.Sprintf("Verify that PipelineRollout ProgressiveUpgradeSucceeded condition is %s", success), func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionProgressiveUpgradeSucceeded)
	}).Should(Equal(success))
}

func VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName string, inProgressStrategy apiv1.UpgradeStrategy) {
	CheckEventually(fmt.Sprintf("Verifying PipelineRollout InProgressStrategy is %q", string(inProgressStrategy)), func() bool {
		pipelineRollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return pipelineRollout.Status.UpgradeInProgress == inProgressStrategy
	}).Should(BeTrue())
}

func VerifyPipelineRolloutInProgressStrategyConsistently(pipelineRolloutName string, inProgressStrategy apiv1.UpgradeStrategy) {
	CheckConsistently(fmt.Sprintf("Verifying PipelineRollout InProgressStrategy is consistently %q", string(inProgressStrategy)), func() bool {
		pipelineRollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return pipelineRollout.Status.UpgradeInProgress == inProgressStrategy
	}).WithTimeout(10 * time.Second).Should(BeTrue())
}

// GetPipelineSpec from Unstructured type
func GetPipelineSpec(u *unstructured.Unstructured) (numaflowv1.PipelineSpec, error) {
	specMap := u.Object["spec"]
	var pipelineSpec numaflowv1.PipelineSpec
	err := util.StructToStruct(&specMap, &pipelineSpec)
	return pipelineSpec, err
}

// GetPipelineMetadata from Unstructured type
func GetPipelineMetadata(u *unstructured.Unstructured) (apiv1.Metadata, error) {
	labels := u.GetLabels()
	annotations := u.GetAnnotations()
	return apiv1.Metadata{
		Labels:      labels,
		Annotations: annotations,
	}, nil
}

func GetGVRForPipeline() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "pipelines",
	}
}

func UpdatePipelineRolloutInK8S(namespace string, name string, f func(apiv1.PipelineRollout) (apiv1.PipelineRollout, error)) {
	By("updating PipelineRollout")
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rollout, err := pipelineRolloutClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		*rollout, err = f(*rollout)
		if err != nil {
			return err
		}
		_, err = pipelineRolloutClient.Update(ctx, rollout, metav1.UpdateOptions{})
		return err
	})
	Expect(err).ShouldNot(HaveOccurred())
}

func UpdatePipelineInK8S(name string, f func(*unstructured.Unstructured) (*unstructured.Unstructured, error)) {
	By("updating Pipeline")

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pipeline, err := dynamicClient.Resource(GetGVRForPipeline()).Namespace(Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		updatedPipeline, err := f(pipeline)
		if err != nil {
			return err
		}

		_, err = dynamicClient.Resource(GetGVRForPipeline()).Namespace(Namespace).Update(ctx, updatedPipeline, metav1.UpdateOptions{})
		return err
	})
	Expect(err).ShouldNot(HaveOccurred())
}

func GetPromotedPipelineSpecAndStatus(namespace string, pipelineRolloutName string) (*unstructured.Unstructured, numaflowv1.PipelineSpec, numaflowv1.PipelineStatus, error) {

	var retrievedPipelineSpec numaflowv1.PipelineSpec
	var retrievedPipelineStatus numaflowv1.PipelineStatus

	unstruct, err := GetPromotedPipeline(namespace, pipelineRolloutName)
	if err != nil {
		return nil, retrievedPipelineSpec, retrievedPipelineStatus, err
	}
	retrievedPipelineSpec, err = GetPipelineSpec(unstruct)
	if err != nil {
		return unstruct, retrievedPipelineSpec, retrievedPipelineStatus, err
	}

	retrievedPipelineStatus, err = GetPipelineStatus(unstruct)

	if err != nil {
		return unstruct, retrievedPipelineSpec, retrievedPipelineStatus, err
	}
	return unstruct, retrievedPipelineSpec, retrievedPipelineStatus, nil
}

func GetPipelineStatus(u *unstructured.Unstructured) (numaflowv1.PipelineStatus, error) {
	statusMap := u.Object["status"]
	var status numaflowv1.PipelineStatus
	err := util.StructToStruct(&statusMap, &status)
	return status, err
}

func UpdatePipelineSpecInK8S(namespace string, pipelineRolloutName string, f func(numaflowv1.PipelineSpec) (numaflowv1.PipelineSpec, error)) {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		unstruct, err := GetPromotedPipeline(namespace, pipelineRolloutName)
		Expect(err).ShouldNot(HaveOccurred())
		retrievedPipeline := unstruct

		// modify Pipeline Spec to verify it gets auto-healed
		err = UpdatePipelineSpec(retrievedPipeline, f)
		Expect(err).ShouldNot(HaveOccurred())
		_, err = dynamicClient.Resource(GetGVRForPipeline()).Namespace(namespace).Update(ctx, retrievedPipeline, metav1.UpdateOptions{})
		return err
	})

	Expect(err).ShouldNot(HaveOccurred())
}

// Take a Pipeline Unstructured type and update the PipelineSpec in some way
func UpdatePipelineSpec(u *unstructured.Unstructured, f func(numaflowv1.PipelineSpec) (numaflowv1.PipelineSpec, error)) error {

	// get PipelineSpec from unstructured object
	specMap := u.Object["spec"]
	var pipelineSpec numaflowv1.PipelineSpec
	err := util.StructToStruct(&specMap, &pipelineSpec)
	if err != nil {
		return err
	}
	// update PipelineSpec
	pipelineSpec, err = f(pipelineSpec)
	if err != nil {
		return err
	}
	var newMap map[string]interface{}
	err = util.StructToStruct(&pipelineSpec, &newMap)
	if err != nil {
		return err
	}
	u.Object["spec"] = newMap
	return nil
}

func getVertexLabelSelector(pipelineName string) string {
	return fmt.Sprintf("%s=%s,%s=%s", numaflowv1.KeyPipelineName, pipelineName, numaflowv1.KeyComponent, "vertex")
}

func getDaemonLabelSelector(pipelineName string) string {
	return fmt.Sprintf("%s=%s,%s=%s", numaflowv1.KeyPipelineName, pipelineName, numaflowv1.KeyComponent, "daemon")
}

// build watcher functions for both Pipeline and PipelineRollout
func watchPipelineRollout() {

	watchResourceType(func() (watch.Interface, error) {
		watcher, err := pipelineRolloutClient.Watch(context.Background(), metav1.ListOptions{})
		return watcher, err
	}, func(o runtime.Object) Output {
		if rollout, ok := o.(*apiv1.PipelineRollout); ok {
			rollout.ManagedFields = nil
			return Output{
				APIVersion: NumaplaneAPIVersion,
				Kind:       "PipelineRollout",
				Metadata:   rollout.ObjectMeta,
				Spec:       rollout.Spec,
				Status:     rollout.Status,
			}
		}
		return Output{}
	})

}

func watchPipeline() {

	watchResourceType(func() (watch.Interface, error) {
		watcher, err := dynamicClient.Resource(GetGVRForPipeline()).Namespace(Namespace).Watch(context.Background(), metav1.ListOptions{})
		return watcher, err
	}, func(o runtime.Object) Output {
		if obj, ok := o.(*unstructured.Unstructured); ok {
			pl := numaflowv1.Pipeline{}
			err := util.StructToStruct(&obj, &pl)
			if err != nil {
				fmt.Printf("Failed to convert unstruct: %v\n", err)
				return Output{}
			}
			pl.ManagedFields = nil
			return Output{
				APIVersion: NumaflowAPIVersion,
				Kind:       "Pipeline",
				Metadata:   pl.ObjectMeta,
				Spec:       pl.Spec,
				Status:     pl.Status,
			}
		}
		return Output{}
	})

}

func startPipelineRolloutWatches() {
	wg.Add(1)
	go watchPipelineRollout()

	wg.Add(1)
	go watchPipeline()

	wg.Add(1)
	go watchVertices()
}

// shared functions

// create a PipelineRollout of a given spec/name and make sure it's running
func CreatePipelineRollout(name, namespace string, spec numaflowv1.PipelineSpec, failed bool, strategy *apiv1.PipelineStrategy, metadata apiv1.Metadata) {

	pipelineRolloutSpec := createPipelineRolloutSpec(name, namespace, spec, strategy, metadata)
	_, err := pipelineRolloutClient.Create(ctx, pipelineRolloutSpec, metav1.CreateOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	CheckEventually("Verifying that the PipelineRollout was created", func() error {
		_, err := pipelineRolloutClient.Get(ctx, name, metav1.GetOptions{})
		return err
	}).Should(Succeed())

	By("Verifying that the Pipeline was created")
	VerifyPromotedPipelineSpec(namespace, name, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
		return len(spec.Vertices) == len(retrievedPipelineSpec.Vertices) // TODO: make less kludgey
		//return reflect.DeepEqual(pipelineSpec, retrievedPipelineSpec) // this may have had some false negatives due to "lifecycle" field maybe, or null values in one
	})

	VerifyPipelineRolloutDeployed(name)
	VerifyPipelineRolloutInProgressStrategy(name, apiv1.UpgradeStrategyNoOp)

	if !failed {
		VerifyPipelineRolloutHealthy(name)
		VerifyPromotedPipelineRunning(namespace, name)
	}
}

func createPipelineRolloutSpec(name, namespace string, pipelineSpec numaflowv1.PipelineSpec, strategy *apiv1.PipelineStrategy, metadata apiv1.Metadata) *apiv1.PipelineRollout {

	pipelineSpecRaw, err := json.Marshal(pipelineSpec)
	Expect(err).ShouldNot(HaveOccurred())

	pipelineRollout := &apiv1.PipelineRollout{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "numaplane.numaproj.io/v1alpha1",
			Kind:       "PipelineRollout",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: apiv1.PipelineRolloutSpec{
			Strategy: strategy,
			Pipeline: apiv1.Pipeline{
				Spec: runtime.RawExtension{
					Raw: pipelineSpecRaw,
				},
				Metadata: metadata,
			},
		},
	}

	return pipelineRollout

}

// delete a PipelineRollout and verify deletion
func DeletePipelineRollout(name string) {

	By("Deleting PipelineRollout")
	// Foreground deletion somehow changes the timing of things to cause a race condition in the PPND test, in which the same pipeline deleted then re-created causes Numaflow's "Delete Buffers/Buckets" Job
	// to sometimes run at the same time as its "Create Buffers/Buckets" Job, which causes expected buffers/buckets to not exist
	// Therefore, changing to Background Deletion
	//propagationPolicy := metav1.DeletePropagationForeground
	propagationPolicy := metav1.DeletePropagationBackground
	err := pipelineRolloutClient.Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &propagationPolicy})
	Expect(err).ShouldNot(HaveOccurred())

	CheckEventually("Verifying PipelineRollout deletion", func() bool {
		_, err := pipelineRolloutClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if !errors.IsNotFound(err) {
				Fail("An unexpected error occurred when fetching the PipelineRollout: " + err.Error())
			}
			return false
		}
		return true
	}).WithTimeout(DefaultTestTimeout).Should(BeFalse(), "The PipelineRollout should have been deleted but it was found.")

	CheckEventually("Verifying Pipeline deletion", func() bool {
		// ensures deletion of single pipeline
		pipelineRolloutLabel := fmt.Sprintf("numaplane.numaproj.io/parent-rollout-name=%v", name)
		list, err := dynamicClient.Resource(GetGVRForPipeline()).Namespace(Namespace).List(ctx, metav1.ListOptions{LabelSelector: pipelineRolloutLabel})
		if err != nil {
			return false
		}
		if len(list.Items) == 0 {
			return true
		}
		return false
	}).WithTimeout(DefaultTestTimeout).Should(BeTrue(), "The Pipeline should have been deleted but it was found.")
}

// update PipelineRollout and verify correct process
// name - name of PipelineRollout to update
// newSpec - new child Pipeline spec that will be updated in the rollout
// expectedFinalPhase - after updating the Rollout what phase we expect the child Pipeline to be in
// verifySpecFunc - boolean function to verify that updated PipelineRollout has correct spec
// dataLoss - informs us if the update to the PipelineRollout will cause data loss or not
func UpdatePipelineRollout(name string, newSpec numaflowv1.PipelineSpec, expectedFinalPhase numaflowv1.PipelinePhase, verifySpecFunc func(numaflowv1.PipelineSpec) bool, dataLoss bool,
	progressiveFieldChanged bool, expectedSuccess bool, metadata apiv1.Metadata,
) {

	By("Updating Pipeline spec in PipelineRollout")
	rawSpec, err := json.Marshal(newSpec)
	Expect(err).ShouldNot(HaveOccurred())

	pipelineCount := GetCurrentPipelineCount(name)

	// update the PipelineRollout
	UpdatePipelineRolloutInK8S(Namespace, name, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
		rollout.Spec.Pipeline.Spec.Raw = rawSpec
		rollout.Spec.Pipeline.Metadata = metadata
		return rollout, nil
	})

	if UpgradeStrategy == config.PPNDStrategyID && dataLoss {

		switch expectedFinalPhase {
		case numaflowv1.PipelinePhasePausing:
			VerifyPipelineRolloutInProgressStrategy(name, apiv1.UpgradeStrategyPPND)
			VerifyPipelineRolloutConditionPausing(Namespace, name)
		case numaflowv1.PipelinePhasePaused:
			VerifyPipelineRolloutInProgressStrategy(name, apiv1.UpgradeStrategyPPND)
			VerifyPromotedPipelinePaused(Namespace, name)
		case numaflowv1.PipelinePhaseFailed:
			VerifyPromotedPipelineFailed(Namespace, name)
		}

	}

	var expectedPromotedPipelineName, expectedUpgradingPipelineName string

	doProgressive := dataLoss || progressiveFieldChanged
	if UpgradeStrategy == config.ProgressiveStrategyID && doProgressive {
		expectedPromotedPipelineName = fmt.Sprintf("%s-%d", name, pipelineCount-1)
		expectedUpgradingPipelineName = fmt.Sprintf("%s-%d", name, pipelineCount)
		PipelineTransientProgressiveChecks(name, expectedPromotedPipelineName, expectedUpgradingPipelineName)
	}

	// wait for update to reconcile
	time.Sleep(5 * time.Second)

	// FINAL STATE checks:

	// rollout phase will be pending if we are expecting a long pausing state and Pipeline will not be fully updated
	if !(UpgradeStrategy == config.PPNDStrategyID && expectedFinalPhase == numaflowv1.PipelinePhasePausing) {
		By("Verifying Pipeline got updated")
		// get Pipeline to check that spec has been updated to correct spec
		VerifyPromotedPipelineSpec(Namespace, name, verifySpecFunc)
		VerifyPipelineRolloutDeployed(name)
	}
	// slow pausing case
	if expectedFinalPhase == numaflowv1.PipelinePhasePausing && UpgradeStrategy == config.PPNDStrategyID {
		VerifyPipelineRolloutInProgressStrategy(name, apiv1.UpgradeStrategyPPND)
	} else {
		VerifyPipelineRolloutInProgressStrategy(name, apiv1.UpgradeStrategyNoOp)
	}

	if UpgradeStrategy == config.ProgressiveStrategyID && doProgressive {
		PipelineFinalProgressiveChecks(name, expectedPromotedPipelineName, expectedUpgradingPipelineName, expectedSuccess, newSpec)
	}

	switch expectedFinalPhase {
	case numaflowv1.PipelinePhasePausing:
		VerifyPipelineRolloutConditionPausing(Namespace, name)
	case numaflowv1.PipelinePhasePaused:
		VerifyPromotedPipelinePaused(Namespace, name)
	case numaflowv1.PipelinePhaseFailed:
		VerifyPromotedPipelineFailed(Namespace, name)
	case numaflowv1.PipelinePhaseRunning:
		VerifyPromotedPipelineRunning(Namespace, name)
	}
	// child pipeline will only be healthy if it is running
	if expectedFinalPhase == numaflowv1.PipelinePhaseRunning && expectedSuccess {
		VerifyPipelineRolloutHealthy(name)
	}

}

func VerifyPromotedPipelineStaysPaused(pipelineRolloutName string) {
	CheckConsistently("verifying Pipeline stays in paused or otherwise pausing", func() bool {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		_, _, retrievedPipelineStatus, err := GetPromotedPipelineSpecAndStatus(Namespace, pipelineRolloutName)
		if err != nil {
			return false
		}
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused) == metav1.ConditionTrue &&
			(retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePaused || retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePausing)
	}).WithTimeout(10 * time.Second).Should(BeTrue())

	VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

	pipeline, err := GetPromotedPipeline(Namespace, pipelineRolloutName)
	Expect(err).ShouldNot(HaveOccurred())
	verifyPodsRunning(Namespace, 0, getVertexLabelSelector(pipeline.GetName()))
}

func VerifyPipelineDrainedOnPause(pipelineName string) {
	VerifyPipelineSpecStatus(Namespace, pipelineName, func(spec numaflowv1.PipelineSpec, status numaflowv1.PipelineStatus) bool {
		return status.DrainedOnPause
	})
}

func VerifyPipelineDeletion(pipelineName string) {
	CheckEventually(fmt.Sprintf("Verifying that the Pipeline was deleted (%s)", pipelineName), func() bool {
		pipeline, err := dynamicClient.Resource(GetGVRForPipeline()).Namespace(Namespace).Get(ctx, pipelineName, metav1.GetOptions{})
		if err != nil {
			return errors.IsNotFound(err)
		}

		return pipeline == nil
	}).WithTimeout(DefaultTestTimeout).Should(BeTrue(), fmt.Sprintf("The Pipeline %s/%s should have been deleted but it was found.", Namespace, pipelineName))
}

func CreateInitialPipelineRollout(pipelineRolloutName, currentPromotedISBService string, initialPipelineSpec numaflowv1.PipelineSpec, defaultStrategy apiv1.PipelineStrategy, metadata apiv1.Metadata) {
	By("Creating a PipelineRollout")
	CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false, &defaultStrategy, metadata)

	By("Verifying that the Pipeline spec is as expected")
	originalPipelineSpecISBSvcName := initialPipelineSpec.InterStepBufferServiceName
	initialPipelineSpec.InterStepBufferServiceName = currentPromotedISBService
	VerifyPromotedPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
		return reflect.DeepEqual(retrievedPipelineSpec, initialPipelineSpec)
	})
	initialPipelineSpec.InterStepBufferServiceName = originalPipelineSpecISBSvcName
	VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
	VerifyPipelineRolloutHealthy(pipelineRolloutName)
}

func VerifyPipelineProgressiveSuccess(pipelineRolloutName, promotedPipelineName, upgradingPipelineName string, forcedSuccess bool, upgradingPipelineSpec numaflowv1.PipelineSpec) {
	if !forcedSuccess {
		VerifyPromotedPipelineScaledDownForProgressive(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0))
	}
	VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, promotedPipelineName, upgradingPipelineName, true, apiv1.AssessmentResultSuccess, forcedSuccess)

	newPipelineSpecVertices := []numaflowv1.AbstractVertex{}
	for _, vertex := range upgradingPipelineSpec.Vertices {
		newPipelineSpecVertices = append(newPipelineSpecVertices, numaflowv1.AbstractVertex{Name: vertex.Name, Scale: vertex.Scale})
	}
	if !forcedSuccess {
		VerifyVerticesPodsRunning(Namespace, upgradingPipelineName, newPipelineSpecVertices, ComponentVertex)
	}

	// Verify the previously promoted pipeline was deleted
	VerifyPipelineDeletion(promotedPipelineName)

	// Verify no in progress strategy set
	VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
	VerifyPipelineRolloutInProgressStrategyConsistently(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
	VerifyPipelineRolloutProgressiveCondition(pipelineRolloutName, metav1.ConditionTrue)
}

func VerifyPipelineProgressiveFailure(pipelineRolloutName, promotedPipelineName, upgradingPipelineName string, promotedPipelineSpec, upgradingPipelineSpec numaflowv1.PipelineSpec) {
	VerifyPromotedPipelineScaledDownForProgressive(pipelineRolloutName, promotedPipelineName)
	VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, promotedPipelineName, upgradingPipelineName, true, apiv1.AssessmentResultFailure, false)

	// Verify that when the "upgrading" Pipeline fails, it scales down to 0 Pods, and the "promoted" Pipeline scales back up
	originalPipelineSpecVertices := []numaflowv1.AbstractVertex{}
	for _, vertex := range promotedPipelineSpec.Vertices {
		originalPipelineSpecVertices = append(originalPipelineSpecVertices, numaflowv1.AbstractVertex{Name: vertex.Name, Scale: vertex.Scale})
	}
	VerifyVerticesPodsRunning(Namespace, promotedPipelineName, originalPipelineSpecVertices, ComponentVertex)
	upgradingPipelineSpecVerticesZero := []numaflowv1.AbstractVertex{}
	for _, vertex := range upgradingPipelineSpec.Vertices {
		upgradingPipelineSpecVerticesZero = append(upgradingPipelineSpecVerticesZero, numaflowv1.AbstractVertex{Name: vertex.Name, Scale: numaflowv1.Scale{Min: ptr.To(int32(0)), Max: ptr.To(int32(0))}})
	}
	VerifyVerticesPodsRunning(Namespace, upgradingPipelineName, upgradingPipelineSpecVerticesZero, ComponentVertex)

	VerifyPipelineRolloutProgressiveCondition(pipelineRolloutName, metav1.ConditionFalse)
}

func UpdatePipeline(pipelineRolloutName string, spec numaflowv1.PipelineSpec) {
	rawSpec, err := json.Marshal(spec)
	Expect(err).ShouldNot(HaveOccurred())
	UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(pipelineRollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
		pipelineRollout.Spec.Pipeline.Spec.Raw = rawSpec
		return pipelineRollout, nil
	})
}

func VerifyPipelineEvent(namespace, pipelineName, eventType string) {
	CheckEventually(fmt.Sprintf("verifying Pipeline event type %s", eventType), func() bool {
		events, err := kubeClient.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{
			FieldSelector: fmt.Sprintf("involvedObject.name=%s,involvedObject.kind=Pipeline,type=%s", pipelineName, eventType),
		})
		if err != nil {
			return false
		}
		return len(events.Items) > 0
	}).Should(BeTrue())
}
