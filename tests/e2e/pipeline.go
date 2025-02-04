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

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func getPipeline(namespace, pipelineRolloutName string) (*unstructured.Unstructured, error) {
	return getChildResource(getGVRForPipeline(), namespace, pipelineRolloutName, common.LabelValueUpgradePromoted)
}

func verifyPipelineSpec(namespace string, pipelineRolloutName string, f func(numaflowv1.PipelineSpec) bool) {

	document("verifying Pipeline Spec")
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

func verifyPipelineStatusEventually(namespace string, pipelineRolloutName string, f func(numaflowv1.PipelineSpec, numaflowv1.PipelineStatus) bool) {

	Eventually(func() bool {
		_, retrievedPipelineSpec, retrievedPipelineStatus, err := getPipelineSpecAndStatus(namespace, pipelineRolloutName)
		return err == nil && f(retrievedPipelineSpec, retrievedPipelineStatus)
	}, testTimeout).Should(BeTrue())
}

func verifyPipelineStatusConsistently(namespace string, pipelineRolloutName string, f func(numaflowv1.PipelineSpec, numaflowv1.PipelineStatus) bool) {

	Consistently(func() bool {
		_, retrievedPipelineSpec, retrievedPipelineStatus, err := getPipelineSpecAndStatus(namespace, pipelineRolloutName)
		return err == nil && f(retrievedPipelineSpec, retrievedPipelineStatus)
	}, 30*time.Second, testPollingInterval).Should(BeTrue())

}

func verifyPipelineRolloutDeployed(pipelineRolloutName string) {
	document("Verifying that the PipelineRollout is Deployed")
	Eventually(func() bool {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return rollout.Status.Phase == apiv1.PhaseDeployed
	}, testTimeout, testPollingInterval).Should(BeTrue())

	Eventually(func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceDeployed)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))

}

func verifyPipelineRolloutHealthy(pipelineRolloutName string) {
	document("Verifying that the PipelineRollout Child Condition is Healthy")
	Eventually(func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionChildResourceHealthy)
	}, testTimeout, testPollingInterval).Should(Equal(metav1.ConditionTrue))
}

func verifyPipelineRunning(namespace string, pipelineRolloutName string) {
	document("Verifying that the Pipeline is running")
	verifyPipelineStatusEventually(namespace, pipelineRolloutName,
		func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
			return retrievedPipelineStatus.Phase == numaflowv1.PipelinePhaseRunning
		})
	Eventually(func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
	}, testTimeout).Should(Not(Equal(metav1.ConditionTrue)))

	// Get Pipeline Pods to verify they're all up
	document("Verifying that the Pipeline is ready")
	// check "vertex" Pods
	pipeline, err := getPipeline(namespace, pipelineRolloutName)
	Expect(err).ShouldNot(HaveOccurred())
	spec, err := getPipelineSpec(pipeline)
	Expect(err).ShouldNot(HaveOccurred())
	verifyPodsRunning(namespace, len(spec.Vertices), getVertexLabelSelector(pipeline.GetName()))
	verifyPodsRunning(namespace, 1, getDaemonLabelSelector(pipeline.GetName()))

}

func verifyPipelinePaused(namespace string, pipelineRolloutName string) {

	document("Verify that Pipeline Rollout condition is Pausing/Paused")
	Eventually(func() metav1.ConditionStatus {
		rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused)
	}, testTimeout).Should(Equal(metav1.ConditionTrue))

	document("Verify that Pipeline is paused and fully drained")
	verifyPipelineStatusEventually(namespace, pipelineRolloutName,
		func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
			return retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePaused && retrievedPipelineStatus.DrainedOnPause

		})
	// this happens too fast to verify it:
	//verifyPodsRunning(namespace, 0, getVertexLabelSelector(pipelineName))
}

func verifyInProgressStrategy(pipelineRolloutName string, inProgressStrategy apiv1.UpgradeStrategy) {
	document("Verifying InProgressStrategy")
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

func updatePipelineRolloutInK8S(namespace string, name string, f func(apiv1.PipelineRollout) (apiv1.PipelineRollout, error)) {
	document("updating PipelineRollout")
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

func updatePipelineSpecInK8S(namespace string, pipelineRolloutName string, f func(numaflowv1.PipelineSpec) (numaflowv1.PipelineSpec, error)) {

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
func createPipelineRollout(name, namespace string, spec numaflowv1.PipelineSpec) {

	pipelineRolloutSpec := createPipelineRolloutSpec(name, namespace, spec)
	_, err := pipelineRolloutClient.Create(ctx, pipelineRolloutSpec, metav1.CreateOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	document("Verifying that the PipelineRollout was created")
	Eventually(func() error {
		_, err := pipelineRolloutClient.Get(ctx, name, metav1.GetOptions{})
		return err
	}, testTimeout, testPollingInterval).Should(Succeed())

	document("Verifying that the Pipeline was created")
	verifyPipelineSpec(namespace, name, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
		return len(spec.Vertices) == len(retrievedPipelineSpec.Vertices) // TODO: make less kludgey
		//return reflect.DeepEqual(pipelineSpec, retrievedPipelineSpec) // this may have had some false negatives due to "lifecycle" field maybe, or null values in one
	})

	verifyPipelineRolloutDeployed(name)
	verifyPipelineRolloutHealthy(name)
	verifyInProgressStrategy(name, apiv1.UpgradeStrategyNoOp)

	verifyPipelineRunning(namespace, name)

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
func deletePipelineRollout(name string) {

	document("Deleting PipelineRollout")
	err := pipelineRolloutClient.Delete(ctx, name, metav1.DeleteOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	document("Verifying PipelineRollout deletion")
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

	document("Verifying Pipeline deletion")
	Eventually(func() bool {
		list, err := dynamicClient.Resource(getGVRForPipeline()).Namespace(Namespace).List(ctx, metav1.ListOptions{})
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
func updatePipelineRollout(name string, newSpec numaflowv1.PipelineSpec, expectedFinalPhase numaflowv1.PipelinePhase, dataLoss bool) {

	document("Updating Pipeline spec in PipelineRollout")
	rawSpec, err := json.Marshal(newSpec)
	Expect(err).ShouldNot(HaveOccurred())

	// update the PipelineRollout
	updatePipelineRolloutInK8S(Namespace, name, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
		rollout.Spec.Pipeline.Spec.Raw = rawSpec
		return rollout, nil
	})

	// no data loss OR system pause
	if upgradeStrategy == config.PPNDStrategyID && dataLoss {

		document("Verify that in-progress-strategy gets set to PPND")
		verifyInProgressStrategy(name, apiv1.UpgradeStrategyPPND)

		verifyPipelinePaused(Namespace, name)

	}

	// wait for update to reconcile
	time.Sleep(5 * time.Second)

	document("Verifying Pipeline got updated")
	numPipelineVertices := len(newSpec.Vertices)

	// get Pipeline to check that spec has been updated to correct spec
	verifyPipelineSpec(Namespace, name, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
		return len(retrievedPipelineSpec.Vertices) == numPipelineVertices
	})

	verifyPipelineRolloutDeployed(name)
	if expectedFinalPhase == numaflowv1.PipelinePhaseRunning {
		verifyPipelineRolloutHealthy(name)
	}

	verifyInProgressStrategy(name, apiv1.UpgradeStrategyNoOp)

	if expectedFinalPhase == numaflowv1.PipelinePhasePaused {
		verifyPipelinePaused(Namespace, name)
	} else {
		verifyPipelineRunning(Namespace, name)
	}

}
