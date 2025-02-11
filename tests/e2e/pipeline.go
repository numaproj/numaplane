package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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

func getPipeline(namespace, pipelineRolloutName string) (*unstructured.Unstructured, error) {
	return getChildResource(getGVRForPipeline(), namespace, pipelineRolloutName)
}

func getPipelineName(namespace, pipelineRolloutName string) (string, error) {
	pipeline, err := getPipeline(namespace, pipelineRolloutName)
	if err != nil {
		return "", err
	}
	return pipeline.GetName(), nil
}

func VerifyPipelineSpec(namespace string, pipelineRolloutName string, f func(numaflowv1.PipelineSpec) bool) {

	Document("verifying Pipeline Spec")
	var retrievedPipelineSpec numaflowv1.PipelineSpec
	Eventually(func() bool {
		unstruct, err := getPipeline(namespace, pipelineRolloutName)
		if err != nil {
			return false
		}
		if retrievedPipelineSpec, err = getPipelineSpec(unstruct); err != nil {
			return false
		}

		return f(retrievedPipelineSpec)
	}, testTimeout, testPollingInterval).Should(BeTrue())
}

func VerifyPipelineStatusEventually(namespace string, pipelineRolloutName string, f func(numaflowv1.PipelineSpec, numaflowv1.PipelineStatus) bool) {

	Eventually(func() bool {
		_, retrievedPipelineSpec, retrievedPipelineStatus, err := getPipelineSpecAndStatus(namespace, pipelineRolloutName)
		return err == nil && f(retrievedPipelineSpec, retrievedPipelineStatus)
	}, testTimeout).Should(BeTrue())
}

func VerifyPipelineStatusConsistently(namespace string, pipelineRolloutName string, f func(numaflowv1.PipelineSpec, numaflowv1.PipelineStatus) bool) {

	Consistently(func() bool {
		_, retrievedPipelineSpec, retrievedPipelineStatus, err := getPipelineSpecAndStatus(namespace, pipelineRolloutName)
		return err == nil && f(retrievedPipelineSpec, retrievedPipelineStatus)
	}, 15*time.Second, testPollingInterval).Should(BeTrue())

}

func VerifyPipelineRolloutDeployed(pipelineRolloutName string) {
	Document("Verifying that the PipelineRollout is Deployed")
	Eventually(func() bool {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return rollout.Status.Phase == apiv1.PhaseDeployed
	}, testTimeout, testPollingInterval).Should(BeTrue())

	Eventually(func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceDeployed)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))

}

func VerifyPipelineRolloutHealthy(pipelineRolloutName string) {
	Document("Verifying that the PipelineRollout Child Condition is Healthy")
	Eventually(func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceHealthy)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))
}

func VerifyPipelineRunning(namespace string, pipelineRolloutName string, useVerticesScaleValue bool) {
	Document("Verifying that the Pipeline is running")
	VerifyPipelineStatusEventually(namespace, pipelineRolloutName,
		func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
			return retrievedPipelineStatus.Phase == numaflowv1.PipelinePhaseRunning
		})
	Eventually(func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
	}, testTimeout).Should(Not(Equal(metav1.ConditionTrue)))

	// Get Pipeline Pods to verify they're all up
	Document("Verifying that the Pipeline is ready")
	// check "vertex" Pods
	pipeline, err := getPipeline(namespace, pipelineRolloutName)
	Expect(err).ShouldNot(HaveOccurred())
	spec, err := getPipelineSpec(pipeline)
	Expect(err).ShouldNot(HaveOccurred())

	numPods := len(spec.Vertices)
	if useVerticesScaleValue {
		numPods *= GetVerticesScaleValue()
	}
	verifyPodsRunning(namespace, numPods, getVertexLabelSelector(pipeline.GetName()))

	verifyPodsRunning(namespace, 1, getDaemonLabelSelector(pipeline.GetName()))
}

func VerifyPipelinePaused(namespace string, pipelineRolloutName string) {

	Document("Verify that Pipeline Rollout condition is Pausing/Paused")
	Eventually(func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
	}, testTimeout).Should(Equal(metav1.ConditionTrue))

	Document("Verify that Pipeline is paused and fully drained")
	VerifyPipelineStatusEventually(namespace, pipelineRolloutName,
		func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
			return retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePaused && retrievedPipelineStatus.DrainedOnPause

		})
	// this happens too fast to verify it:
	//verifyPodsRunning(namespace, 0, getVertexLabelSelector(pipelineName))
}

func VerifyPipelineFailed(namespace, pipelineRolloutName string) {
	Document("Verify that Pipeline is Failed")
	VerifyPipelineStatusEventually(namespace, pipelineRolloutName,
		func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
			return retrievedPipelineStatus.Phase == numaflowv1.PipelinePhaseFailed
		})
}

func VerifyPipelinePausing(namespace string, pipelineRolloutName string) {

	Document("Verify that Pipeline Rollout condition is Pausing/Paused")
	Eventually(func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
	}, testTimeout).Should(Equal(metav1.ConditionTrue))

}

func VerifyInProgressStrategy(pipelineRolloutName string, inProgressStrategy apiv1.UpgradeStrategy) {
	Document("Verifying InProgressStrategy")
	Eventually(func() bool {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return rollout.Status.UpgradeInProgress == inProgressStrategy
	}, testTimeout, testPollingInterval).Should(BeTrue())
}

// Get PipelineSpec from Unstructured type
func getPipelineSpec(u *unstructured.Unstructured) (numaflowv1.PipelineSpec, error) {
	specMap := u.Object["spec"]
	var pipelineSpec numaflowv1.PipelineSpec
	err := util.StructToStruct(&specMap, &pipelineSpec)
	return pipelineSpec, err
}

func getGVRForPipeline() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "pipelines",
	}
}

func getGVRForVertex() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "vertices",
	}
}

func UpdatePipelineRolloutInK8S(namespace string, name string, f func(apiv1.PipelineRollout) (apiv1.PipelineRollout, error)) {
	Document("updating PipelineRollout")
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

func getPipelineSpecAndStatus(namespace string, pipelineRolloutName string) (*unstructured.Unstructured, numaflowv1.PipelineSpec, numaflowv1.PipelineStatus, error) {

	var retrievedPipelineSpec numaflowv1.PipelineSpec
	var retrievedPipelineStatus numaflowv1.PipelineStatus

	unstruct, err := getPipeline(namespace, pipelineRolloutName)
	if err != nil {
		return nil, retrievedPipelineSpec, retrievedPipelineStatus, err
	}
	retrievedPipelineSpec, err = getPipelineSpec(unstruct)
	if err != nil {
		return unstruct, retrievedPipelineSpec, retrievedPipelineStatus, err
	}

	retrievedPipelineStatus, err = getPipelineStatus(unstruct)

	if err != nil {
		return unstruct, retrievedPipelineSpec, retrievedPipelineStatus, err
	}
	return unstruct, retrievedPipelineSpec, retrievedPipelineStatus, nil
}

func getPipelineStatus(u *unstructured.Unstructured) (numaflowv1.PipelineStatus, error) {
	statusMap := u.Object["status"]
	var status numaflowv1.PipelineStatus
	err := util.StructToStruct(&statusMap, &status)
	return status, err
}

func UpdatePipelineSpecInK8S(namespace string, pipelineRolloutName string, f func(numaflowv1.PipelineSpec) (numaflowv1.PipelineSpec, error)) {

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		unstruct, err := getPipeline(namespace, pipelineRolloutName)
		Expect(err).ShouldNot(HaveOccurred())
		retrievedPipeline := unstruct

		// modify Pipeline Spec to verify it gets auto-healed
		err = updatePipelineSpec(retrievedPipeline, f)
		Expect(err).ShouldNot(HaveOccurred())
		_, err = dynamicClient.Resource(getGVRForPipeline()).Namespace(namespace).Update(ctx, retrievedPipeline, metav1.UpdateOptions{})
		return err
	})

	Expect(err).ShouldNot(HaveOccurred())
}

// Take a Pipeline Unstructured type and update the PipelineSpec in some way
func updatePipelineSpec(u *unstructured.Unstructured, f func(numaflowv1.PipelineSpec) (numaflowv1.PipelineSpec, error)) error {

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
		watcher, err := dynamicClient.Resource(getGVRForPipeline()).Namespace(Namespace).Watch(context.Background(), metav1.ListOptions{})
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
		watcher, err := dynamicClient.Resource(getGVRForVertex()).Namespace(Namespace).Watch(context.Background(), metav1.ListOptions{})
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

	Document("Verifying that the PipelineRollout was created")
	Eventually(func() error {
		_, err := pipelineRolloutClient.Get(ctx, name, metav1.GetOptions{})
		return err
	}, testTimeout, testPollingInterval).Should(Succeed())

	Document("Verifying that the Pipeline was created")
	VerifyPipelineSpec(namespace, name, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
		return len(spec.Vertices) == len(retrievedPipelineSpec.Vertices) // TODO: make less kludgey
		//return reflect.DeepEqual(pipelineSpec, retrievedPipelineSpec) // this may have had some false negatives due to "lifecycle" field maybe, or null values in one
	})

	VerifyPipelineRolloutDeployed(name)
	VerifyInProgressStrategy(name, apiv1.UpgradeStrategyNoOp)

	if !failed {
		VerifyPipelineRolloutHealthy(name)
		VerifyPipelineRunning(namespace, name, true)
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

	Document("Deleting PipelineRollout")
	err := pipelineRolloutClient.Delete(ctx, name, metav1.DeleteOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	Document("Verifying PipelineRollout deletion")
	Eventually(func() bool {
		_, err := pipelineRolloutClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if !errors.IsNotFound(err) {
				Fail("An unexpected error occurred when fetching the PipelineRollout: " + err.Error())
			}
			return false
		}
		return true
	}).WithTimeout(testTimeout).Should(BeFalse(), "The PipelineRollout should have been deleted but it was found.")

	Document("Verifying Pipeline deletion")
	Eventually(func() bool {
		// ensures deletion of single pipeline
		pipelineRolloutLabel := fmt.Sprintf("numaplane.numaproj.io/parent-rollout-name=%v", name)
		list, err := dynamicClient.Resource(getGVRForPipeline()).Namespace(Namespace).List(ctx, metav1.ListOptions{LabelSelector: pipelineRolloutLabel})
		if err != nil {
			return false
		}
		if len(list.Items) == 0 {
			return true
		}
		return false
	}).WithTimeout(testTimeout).Should(BeTrue(), "The Pipeline should have been deleted but it was found.")
}

// update PipelineRollout and verify correct process
// name - name of PipelineRollout to update
// newSpec - new child Pipeline spec that will be updated in the rollout
// expectedFinalPhase - after updating the Rollout what phase we expect the child Pipeline to be in
// verifySpecFunc - boolean function to verify that updated PipelineRollout has correct spec
// dataLoss - informs us if the update to the PipelineRollout will cause data loss or not
func UpdatePipelineRollout(name string, newSpec numaflowv1.PipelineSpec, expectedFinalPhase numaflowv1.PipelinePhase, verifySpecFunc func(numaflowv1.PipelineSpec) bool, dataLoss bool, overrideSourceVertexReplicas bool) {

	Document("Updating Pipeline spec in PipelineRollout")
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

	// TODO: remove this logic once Numaflow is updated to scale vertices for sources other than Kafka and
	// when scaling to 0 is also allowed.
	if overrideSourceVertexReplicas {
		scaleTo := int64(math.Floor(float64(GetVerticesScaleValue()) / float64(2)))

		pipeline, err := getPipeline(Namespace, name)
		Expect(err).ShouldNot(HaveOccurred())
		vertexName := fmt.Sprintf("%s-%s", pipeline.GetName(), PipelineSourceVertexName)

		vertex, err := dynamicClient.Resource(getGVRForVertex()).Namespace(Namespace).Get(ctx, vertexName, metav1.GetOptions{})
		Expect(err).ShouldNot(HaveOccurred())
		err = unstructured.SetNestedField(vertex.Object, scaleTo, "spec", "replicas")
		Expect(err).ShouldNot(HaveOccurred())
		_, err = dynamicClient.Resource(getGVRForVertex()).Namespace(Namespace).Update(ctx, vertex, metav1.UpdateOptions{})
		Expect(err).ShouldNot(HaveOccurred())
	}

	// rollout phase will be pending if we are expecting a long pausing state and Pipeline will not be fully updated
	if expectedFinalPhase != numaflowv1.PipelinePhasePausing {
		Document("Verifying Pipeline got updated")
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
	}

}

func VerifyPipelineStaysPaused(pipelineRolloutName string) {
	Document("verifying Pipeline stays in paused or otherwise pausing")
	Consistently(func() bool {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		_, _, retrievedPipelineStatus, err := getPipelineSpecAndStatus(Namespace, pipelineRolloutName)
		if err != nil {
			return false
		}
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused) == metav1.ConditionTrue &&
			(retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePaused || retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePausing)
	}, 10*time.Second, testPollingInterval).Should(BeTrue())

	VerifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

	pipeline, err := getPipeline(Namespace, pipelineRolloutName)
	Expect(err).ShouldNot(HaveOccurred())
	verifyPodsRunning(Namespace, 0, getVertexLabelSelector(pipeline.GetName()))
}
