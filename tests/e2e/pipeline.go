package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/retry"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func GetPipeline(namespace, pipelineRolloutName string) (*unstructured.Unstructured, error) {
	return getChildResource(GetGVRForPipeline(), namespace, pipelineRolloutName)
}

func GetPipelineName(namespace, pipelineRolloutName string) (string, error) {
	pipeline, err := GetPipeline(namespace, pipelineRolloutName)
	if err != nil {
		return "", err
	}
	return pipeline.GetName(), nil
}

func VerifyPipelineSpec(namespace string, pipelineRolloutName string, f func(numaflowv1.PipelineSpec) bool) {
	var retrievedPipelineSpec numaflowv1.PipelineSpec
	CheckEventually("verifying Pipeline Spec", func() bool {
		unstruct, err := GetPipeline(namespace, pipelineRolloutName)
		if err != nil {
			return false
		}
		if retrievedPipelineSpec, err = GetPipelineSpec(unstruct); err != nil {
			return false
		}

		return f(retrievedPipelineSpec)
	}).Should(BeTrue())
}

func VerifyPipelineStatusEventually(namespace string, pipelineRolloutName string, f func(numaflowv1.PipelineSpec, numaflowv1.PipelineStatus) bool) {
	CheckEventually("verify pipeline status", func() bool {
		_, retrievedPipelineSpec, retrievedPipelineStatus, err := GetPipelineSpecAndStatus(namespace, pipelineRolloutName)
		return err == nil && f(retrievedPipelineSpec, retrievedPipelineStatus)
	}).Should(BeTrue())
}

func VerifyPipelineStatusConsistently(namespace string, pipelineRolloutName string, f func(numaflowv1.PipelineSpec, numaflowv1.PipelineStatus) bool) {
	CheckConsistently("verify pipeline status", func() bool {
		_, retrievedPipelineSpec, retrievedPipelineStatus, err := GetPipelineSpecAndStatus(namespace, pipelineRolloutName)
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
	CheckEventually("Verifying that the PipelineRollout Child Condition is Healthy", func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceHealthy)
	}).Should(Equal(metav1.ConditionTrue))
}

func VerifyPipelineRunning(namespace string, pipelineRolloutName string) {
	By("Verifying that the Pipeline is running")
	VerifyPipelineStatusEventually(namespace, pipelineRolloutName,
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
	pipeline, err := GetPipeline(namespace, pipelineRolloutName)
	Expect(err).ShouldNot(HaveOccurred())
	spec, err := GetPipelineSpec(pipeline)
	Expect(err).ShouldNot(HaveOccurred())

	VerifyVerticesPodsRunning(namespace, pipeline.GetName(), spec.Vertices, ComponentVertex)
	verifyPodsRunning(namespace, 1, getDaemonLabelSelector(pipeline.GetName()))
}

func VerifyPipelinePaused(namespace string, pipelineRolloutName string) {
	CheckEventually("Verify that Pipeline Rollout condition is Pausing/Paused", func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
	}).Should(Equal(metav1.ConditionTrue))

	By("Verify that Pipeline is paused and fully drained")
	VerifyPipelineStatusEventually(namespace, pipelineRolloutName,
		func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
			return retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePaused && retrievedPipelineStatus.DrainedOnPause
		})
	// this happens too fast to verify it:
	//verifyPodsRunning(namespace, 0, getVertexLabelSelector(pipelineName))
}

func VerifyPipelineFailed(namespace, pipelineRolloutName string) {
	By("Verify that Pipeline is Failed")
	VerifyPipelineStatusEventually(namespace, pipelineRolloutName,
		func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
			return retrievedPipelineStatus.Phase == numaflowv1.PipelinePhaseFailed
		})
}

func VerifyPipelinePausing(namespace string, pipelineRolloutName string) {
	CheckEventually("Verify that Pipeline Rollout condition is Pausing/Paused", func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
	}).Should(Equal(metav1.ConditionTrue))
}

func VerifyInProgressStrategy(pipelineRolloutName string, inProgressStrategy apiv1.UpgradeStrategy) {
	CheckEventually("Verifying InProgressStrategy", func() bool {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return rollout.Status.UpgradeInProgress == inProgressStrategy
	}).Should(BeTrue())
}

// GetPipelineSpec from Unstructured type
func GetPipelineSpec(u *unstructured.Unstructured) (numaflowv1.PipelineSpec, error) {
	specMap := u.Object["spec"]
	var pipelineSpec numaflowv1.PipelineSpec
	err := util.StructToStruct(&specMap, &pipelineSpec)
	return pipelineSpec, err
}

func GetGVRForPipeline() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "pipelines",
	}
}

func GetGVRForVertex() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "vertices",
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

func GetPipelineSpecAndStatus(namespace string, pipelineRolloutName string) (*unstructured.Unstructured, numaflowv1.PipelineSpec, numaflowv1.PipelineStatus, error) {

	var retrievedPipelineSpec numaflowv1.PipelineSpec
	var retrievedPipelineStatus numaflowv1.PipelineStatus

	unstruct, err := GetPipeline(namespace, pipelineRolloutName)
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
		unstruct, err := GetPipeline(namespace, pipelineRolloutName)
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

func watchVertices() {

	watchResourceType(func() (watch.Interface, error) {
		watcher, err := dynamicClient.Resource(GetGVRForVertex()).Namespace(Namespace).Watch(context.Background(), metav1.ListOptions{})
		return watcher, err
	}, func(o runtime.Object) Output {
		if obj, ok := o.(*unstructured.Unstructured); ok {
			vtx := numaflowv1.Vertex{}
			err := util.StructToStruct(&obj, &vtx)
			if err != nil {
				fmt.Printf("Failed to convert unstruct: %v\n", err)
				return Output{}
			}
			vtx.ManagedFields = nil
			return Output{
				APIVersion: NumaflowAPIVersion,
				Kind:       "Vertex",
				Metadata:   vtx.ObjectMeta,
				Spec:       vtx.Spec,
				Status:     vtx.Status,
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
func CreatePipelineRollout(name, namespace string, spec numaflowv1.PipelineSpec, failed bool) {

	pipelineRolloutSpec := createPipelineRolloutSpec(name, namespace, spec)
	_, err := pipelineRolloutClient.Create(ctx, pipelineRolloutSpec, metav1.CreateOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	CheckEventually("Verifying that the PipelineRollout was created", func() error {
		_, err := pipelineRolloutClient.Get(ctx, name, metav1.GetOptions{})
		return err
	}).Should(Succeed())

	By("Verifying that the Pipeline was created")
	VerifyPipelineSpec(namespace, name, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
		return len(spec.Vertices) == len(retrievedPipelineSpec.Vertices) // TODO: make less kludgey
		//return reflect.DeepEqual(pipelineSpec, retrievedPipelineSpec) // this may have had some false negatives due to "lifecycle" field maybe, or null values in one
	})

	VerifyPipelineRolloutDeployed(name)
	VerifyInProgressStrategy(name, apiv1.UpgradeStrategyNoOp)

	if !failed {
		VerifyPipelineRolloutHealthy(name)
		VerifyPipelineRunning(namespace, name)
	}
}

func createPipelineRolloutSpec(name, namespace string, pipelineSpec numaflowv1.PipelineSpec) *apiv1.PipelineRollout {

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
			Pipeline: apiv1.Pipeline{
				Spec: runtime.RawExtension{
					Raw: pipelineSpecRaw,
				},
			},
		},
	}

	return pipelineRollout

}

// delete a PipelineRollout and verify deletion
func DeletePipelineRollout(name string) {

	By("Deleting PipelineRollout")
	err := pipelineRolloutClient.Delete(ctx, name, metav1.DeleteOptions{})
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
	}).WithTimeout(TestTimeout).Should(BeFalse(), "The PipelineRollout should have been deleted but it was found.")

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
	}).WithTimeout(TestTimeout).Should(BeTrue(), "The Pipeline should have been deleted but it was found.")
}

// update PipelineRollout and verify correct process
// name - name of PipelineRollout to update
// newSpec - new child Pipeline spec that will be updated in the rollout
// expectedFinalPhase - after updating the Rollout what phase we expect the child Pipeline to be in
// verifySpecFunc - boolean function to verify that updated PipelineRollout has correct spec
// dataLoss - informs us if the update to the PipelineRollout will cause data loss or not
func UpdatePipelineRollout(name string, newSpec numaflowv1.PipelineSpec, expectedFinalPhase numaflowv1.PipelinePhase, verifySpecFunc func(numaflowv1.PipelineSpec) bool, dataLoss bool) {

	By("Updating Pipeline spec in PipelineRollout")
	rawSpec, err := json.Marshal(newSpec)
	Expect(err).ShouldNot(HaveOccurred())

	// update the PipelineRollout
	UpdatePipelineRolloutInK8S(Namespace, name, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
		rollout.Spec.Pipeline.Spec.Raw = rawSpec
		return rollout, nil
	})

	if UpgradeStrategy == config.PPNDStrategyID && dataLoss {

		switch expectedFinalPhase {
		case numaflowv1.PipelinePhasePausing:
			VerifyInProgressStrategy(name, apiv1.UpgradeStrategyPPND)
			VerifyPipelinePausing(Namespace, name)
		case numaflowv1.PipelinePhasePaused:
			VerifyInProgressStrategy(name, apiv1.UpgradeStrategyPPND)
			VerifyPipelinePaused(Namespace, name)
		case numaflowv1.PipelinePhaseFailed:
			VerifyPipelineFailed(Namespace, name)
		}

	}

	// wait for update to reconcile
	time.Sleep(5 * time.Second)

	// rollout phase will be pending if we are expecting a long pausing state and Pipeline will not be fully updated
	if !(UpgradeStrategy == config.PPNDStrategyID && expectedFinalPhase == numaflowv1.PipelinePhasePausing) {
		By("Verifying Pipeline got updated")
		// get Pipeline to check that spec has been updated to correct spec
		VerifyPipelineSpec(Namespace, name, verifySpecFunc)
		VerifyPipelineRolloutDeployed(name)
	}
	// child pipeline will only be healthy if it is running
	if expectedFinalPhase == numaflowv1.PipelinePhaseRunning {
		VerifyPipelineRolloutHealthy(name)
	}
	// slow pausing case
	if expectedFinalPhase == numaflowv1.PipelinePhasePausing && UpgradeStrategy == config.PPNDStrategyID {
		VerifyInProgressStrategy(name, apiv1.UpgradeStrategyPPND)
	} else {
		VerifyInProgressStrategy(name, apiv1.UpgradeStrategyNoOp)
	}

	switch expectedFinalPhase {
	case numaflowv1.PipelinePhasePausing:
		VerifyPipelinePausing(Namespace, name)
	case numaflowv1.PipelinePhasePaused:
		VerifyPipelinePaused(Namespace, name)
	case numaflowv1.PipelinePhaseFailed:
		VerifyPipelineFailed(Namespace, name)
	case numaflowv1.PipelinePhaseRunning:
		VerifyPipelineRunning(Namespace, name)
	}

}

func VerifyPipelineStaysPaused(pipelineRolloutName string) {
	CheckConsistently("verifying Pipeline stays in paused or otherwise pausing", func() bool {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		_, _, retrievedPipelineStatus, err := GetPipelineSpecAndStatus(Namespace, pipelineRolloutName)
		if err != nil {
			return false
		}
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused) == metav1.ConditionTrue &&
			(retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePaused || retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePausing)
	}).WithTimeout(10 * time.Second).Should(BeTrue())

	VerifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

	pipeline, err := GetPipeline(Namespace, pipelineRolloutName)
	Expect(err).ShouldNot(HaveOccurred())
	verifyPodsRunning(Namespace, 0, getVertexLabelSelector(pipeline.GetName()))
}
