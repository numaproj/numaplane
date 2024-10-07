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

package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"
)

func createNumaflowControllerRolloutDef(namespace string, version string) *apiv1.NumaflowControllerRollout {
	return &apiv1.NumaflowControllerRollout{
		TypeMeta: metav1.TypeMeta{
			Kind:       "numaflowcontrollerrollout",
			APIVersion: "numaplane.numaproj.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              NumaflowControllerDeploymentName,
			Namespace:         namespace,
			UID:               "uid",
			CreationTimestamp: metav1.NewTime(time.Now()),
			Generation:        1,
		},
		Spec: apiv1.NumaflowControllerRolloutSpec{
			Controller: apiv1.Controller{Version: version},
		},
	}
}

var _ = Describe("NumaflowControllerRollout Controller", Ordered, func() {
	const namespace = "default"

	Context("When creating a NumaflowControllerRollout resource", func() {
		ctx := context.Background()

		resourceLookupKey := types.NamespacedName{
			Name:      NumaflowControllerDeploymentName,
			Namespace: namespace,
		}

		numaflowControllerResource := apiv1.NumaflowControllerRollout{
			ObjectMeta: metav1.ObjectMeta{
				Name:      NumaflowControllerDeploymentName,
				Namespace: namespace,
			},
			Spec: apiv1.NumaflowControllerRolloutSpec{
				Controller: apiv1.Controller{Version: "1.2.0"},
			},
		}

		It("Should throw a CR validation error", func() {
			By("Creating a NumaflowControllerRollout resource with an invalid name")
			resource := numaflowControllerResource
			resource.Name = "test-numaflow-controller"
			err := k8sClient.Create(ctx, &resource)
			Expect(err).NotTo(Succeed())
			Expect(err.Error()).To(ContainSubstring("The metadata name must be 'numaflow-controller'"))
		})

		It("Should throw duplicate resource error", func() {
			By("Creating duplicate NumaflowControllerRollout resource with the same name")
			resource := numaflowControllerResource
			err := k8sClient.Create(ctx, &resource)
			Expect(err).To(Succeed())

			resource.ResourceVersion = "" // Reset the resource version to create a new resource
			err = k8sClient.Create(ctx, &resource)
			Expect(err).NotTo(Succeed())
			Expect(err.Error()).To(ContainSubstring("numaflowcontrollerrollouts.numaplane.numaproj.io \"numaflow-controller\" already exists"))
		})

		It("Should reconcile the Numaflow Controller Rollout", func() {
			By("Verifying the phase of the NumaflowControllerRollout resource")
			// Loop until the API call returns the desired response or a timeout occurs
			Eventually(func() (apiv1.Phase, error) {
				createdResource := &apiv1.NumaflowControllerRollout{}
				Expect(k8sClient.Get(ctx, resourceLookupKey, createdResource)).To(Succeed())
				return createdResource.Status.Phase, nil
			}, timeout, interval).Should(Equal(apiv1.PhaseDeployed))

			By("Verifying the numaflow controller deployment")
			Eventually(func() bool {
				numaflowDeployment := &appsv1.Deployment{}
				err := k8sClient.Get(ctx, types.NamespacedName{Name: NumaflowControllerDeploymentName, Namespace: namespace}, numaflowDeployment)
				return err == nil
			}, timeout, interval).Should(BeTrue())
		})

		It("Should reconcile the Numaflow Controller Rollout update", func() {
			By("Reconciling the updated resource")
			// Fetch the resource and update the spec
			resource := numaflowControllerResource
			resource.Spec.Controller.Version = "1.2.1"
			Expect(k8sClient.Patch(ctx, &resource, client.Merge)).To(Succeed())

			// Validate the resource status after the update
			By("Verifying the numaflow controller deployment image version")
			Eventually(func() bool {
				numaflowDeployment := &appsv1.Deployment{}
				err := k8sClient.Get(ctx, types.NamespacedName{Name: NumaflowControllerDeploymentName, Namespace: namespace}, numaflowDeployment)
				if err == nil {
					return numaflowDeployment.Spec.Template.Spec.Containers[0].Image == "quay.io/numaproj/numaflow:v1.2.1"
				}
				return false
			}, timeout, interval).Should(BeTrue())
		})

		It("Should have the metrics updated", func() {
			By("Verifying the Numaflow Controller metric")
			Expect(testutil.ToFloat64(customMetrics.NumaflowControllersSynced.WithLabelValues())).Should(BeNumerically(">", 1))
			Expect(testutil.ToFloat64(customMetrics.NumaflowControllerKubectlExecutionCounter.WithLabelValues())).Should(BeNumerically(">", 1))
		})

		It("Should auto heal the Numaflow Controller Deployment with the spec based on the NumaflowControllerRollout version field value when the Deployment spec is changed", func() {
			By("updating the Numaflow Controller Deployment and verifying the changed field is the same as the original and not the modified version")
			verifyAutoHealing(ctx, appsv1.SchemeGroupVersion.WithKind("Deployment"), namespace, "numaflow-controller", "spec.template.spec.serviceAccountName", "someothersaname")
		})

		It("Should auto heal the numaflow-cmd-params-config ConfigMap with the spec based on the NumaflowControllerRollout version field value when the ConfigMap spec is changed", func() {
			By("updating the numaflow-cmd-params-config ConfigMap and verifying the changed field is the same as the original and not the modified version")
			verifyAutoHealing(ctx, corev1.SchemeGroupVersion.WithKind("ConfigMap"), namespace, "numaflow-cmd-params-config", "data.namespaced", "false")
		})

		AfterAll(func() {
			// Cleanup the resource after the tests
			Expect(k8sClient.Delete(ctx, &apiv1.NumaflowControllerRollout{
				ObjectMeta: numaflowControllerResource.ObjectMeta,
			})).Should(Succeed())

			deletedResource := &apiv1.NumaflowControllerRollout{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, deletedResource)
				return errors.IsNotFound(err)
			}, timeout, interval).Should(BeTrue())

			deletingChildResource := &appsv1.Deployment{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, deletingChildResource)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(deletingChildResource.OwnerReferences).Should(HaveLen(1))
			Expect(deletedResource.UID).Should(Equal(deletingChildResource.OwnerReferences[0].UID))

		})
	})
})

var _ = Describe("Apply Ownership Reference", func() {
	Context("ownership reference", func() {
		const resourceName = "test-resource"
		manifests := []string{
			`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: example-deployment
  labels:
    app: example
spec:
  replicas: 2
  selector:
    matchLabels:
      app: example
  template:
    metadata:
      labels:
        app: example
    spec:
      containers:
      - name: example-container
        image: nginx:1.14.2
        ports:
        - containerPort: 80
`,
			`
apiVersion: v1
kind: Service
metadata:
  name: example-service
spec:
  selector:
    app: example
  ports:
    - protocol: TCP
      port: 80
      targetPort: 9376
`}
		emanifests := []string{
			`apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    app: example
  name: example-deployment
  ownerReferences:
  - apiVersion: ""
    blockOwnerDeletion: true
    controller: true
    kind: ""
    name: test-resource
    uid: ""
spec:
  replicas: 2
  selector:
    matchLabels:
      app: example
  template:
    metadata:
      labels:
        app: example
    spec:
      containers:
      - image: nginx:1.14.2
        name: example-container
        ports:
        - containerPort: 80`,
			`apiVersion: v1
kind: Service
metadata:
  name: example-service
  ownerReferences:
  - apiVersion: ""
    blockOwnerDeletion: true
    controller: true
    kind: ""
    name: test-resource
    uid: ""
spec:
  ports:
  - port: 80
    protocol: TCP
    targetPort: 9376
  selector:
    app: example`,
		}

		resource := &apiv1.NumaflowControllerRollout{
			ObjectMeta: metav1.ObjectMeta{
				Name:      resourceName,
				Namespace: "default",
			},
		}
		It("should apply ownership reference correctly", func() {

			manifests, err := applyOwnershipToManifests(manifests, resource)
			Expect(err).To(BeNil())
			Expect(strings.TrimSpace(manifests[0])).To(Equal(strings.TrimSpace(emanifests[0])))
			Expect(strings.TrimSpace(manifests[1])).To(Equal(strings.TrimSpace(emanifests[1])))
		})
		It("should not apply ownership if it already exists", func() {
			manifests, err := applyOwnershipToManifests(emanifests, resource)
			Expect(err).To(BeNil())
			Expect(strings.TrimSpace(manifests[0])).To(Equal(strings.TrimSpace(emanifests[0])))
			Expect(strings.TrimSpace(manifests[1])).To(Equal(strings.TrimSpace(emanifests[1])))

		})
	})
})

func Test_getControllerDeploymentVersion(t *testing.T) {
	testCases := []struct {
		name        string
		containers  []corev1.Container
		expectedTag string
	}{
		{
			name: "standard",
			containers: []corev1.Container{
				{
					Image: "some/path/sidecar:latest",
				},
				{
					Image: "quay.io/numaproj/numaflow:v1.0.2",
				},
			},
			expectedTag: "1.0.2",
		},
		{
			name: "images have no paths",
			containers: []corev1.Container{
				{
					Image: "sidecar",
				},
				{
					Image: "numaflow:v1.0.2", // valid if it's in the default registry
				},
			},
			expectedTag: "1.0.2",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			deployment := appsv1.Deployment{}
			deployment.Spec.Template.Spec.Containers = append(deployment.Spec.Template.Spec.Containers, tc.containers...)
			tag, err := getControllerDeploymentVersion(&deployment)
			assert.Nil(t, err)
			assert.Equal(t, tc.expectedTag, tag)
		})
	}

}

func Test_reconcile_numaflowcontrollerrollout_PPND(t *testing.T) {

	restConfig, numaflowClientSet, numaplaneClient, k8sClientSet, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)

	config.GetConfigManagerInstance().UpdateUSDEConfig(config.USDEConfig{DefaultUpgradeStrategy: config.PPNDStrategyID})
	controllerDefinitions, err := getNumaflowControllerDefinitions()
	assert.Nil(t, err)
	config.GetConfigManagerInstance().GetControllerDefinitionsMgr().UpdateNumaflowControllerDefinitionConfig(*controllerDefinitions)

	ctx := context.Background()

	// other tests may call this, but it fails if called more than once
	if customMetrics == nil {
		customMetrics = metrics.RegisterCustomMetrics()
	}

	recorder := record.NewFakeRecorder(64)

	r, err := NewNumaflowControllerRolloutReconciler(
		numaplaneClient,
		scheme.Scheme,
		restConfig,
		kubernetes.NewKubectl(),
		customMetrics,
		recorder,
	)

	testCases := []struct {
		name                      string
		newControllerVersion      string
		existingControllerVersion string // if "", then there isn't a Controller installed
		stillReconciling          bool
		existingPipeline          *numaflowv1.Pipeline
		expectedRolloutPhase      apiv1.Phase
		// require these Conditions to be set (note that in real life, previous reconciliations may have set other Conditions from before which are still present)
		expectedConditionsSet           map[apiv1.ConditionType]metav1.ConditionStatus
		expectedResultControllerVersion string
	}{
		{
			name:                      "no existing Controller",
			newControllerVersion:      "1.2.0",
			existingControllerVersion: "",
			existingPipeline:          nil,
			expectedRolloutPhase:      apiv1.PhaseDeployed,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionChildResourceDeployed: metav1.ConditionTrue,
			},
			expectedResultControllerVersion: "1.2.0",
		},
		{
			name:                      "new Controller version, pipelines not yet paused",
			newControllerVersion:      "1.2.1",
			existingControllerVersion: "1.2.0",
			existingPipeline:          createDefaultPipelineOfPhase(numaflowv1.PipelinePhasePausing),
			expectedRolloutPhase:      apiv1.PhasePending,
			expectedConditionsSet: map[apiv1.ConditionType]metav1.ConditionStatus{
				apiv1.ConditionPausingPipelines: metav1.ConditionTrue,
			},
			expectedResultControllerVersion: "1.2.0",
		},
		{
			name:                            "new Controller version, pipelines paused",
			newControllerVersion:            "1.2.1",
			existingControllerVersion:       "1.2.0",
			existingPipeline:                createDefaultPipelineOfPhase(numaflowv1.PipelinePhasePaused),
			expectedRolloutPhase:            apiv1.PhaseDeployed,
			expectedConditionsSet:           map[apiv1.ConditionType]metav1.ConditionStatus{}, // not including ConditionPausingPipelines because that was already set before
			expectedResultControllerVersion: "1.2.1",
		},
		/*{
			name: "already updated the Controller version but it's still reconciling",
		},
		{
			name: "new Controller version done reconciling",
		},
		{
			name: "new Controller version, pipelines not paused but failed",
		},
		{
			name: "new Controller version, pipelines not paused but set to allow data loss",
		},*/
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			// first delete previous resources in case they already exist, in Kubernetes
			_ = k8sClientSet.AppsV1().Deployments(defaultNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = k8sClientSet.CoreV1().ConfigMaps(defaultNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = k8sClientSet.RbacV1().Roles(defaultNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = k8sClientSet.RbacV1().RoleBindings(defaultNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).Delete(ctx, defaultPipelineRolloutName, metav1.DeleteOptions{})

			// create NumaflowControllerRollout definition
			rollout := createNumaflowControllerRolloutDef(defaultNamespace, tc.newControllerVersion)

			// the Reconcile() function does this, so we need to do it before calling reconcile() as well
			rollout.Status.Init(rollout.Generation)

			// create the already-existing Deployment in Kubernetes
			if tc.existingControllerVersion != "" {
				imagePath := "quay.io/numaproj/numaflow:v" + tc.existingControllerVersion
				_, err := k8sClientSet.AppsV1().Deployments(defaultNamespace).Create(ctx, createDeploymentDefinition(imagePath), metav1.CreateOptions{})
				assert.NoError(t, err)
			}

			if tc.existingPipeline != nil {
				// create the Pipeline beforehand in Kubernetes, this updates everything but the Status subresource
				pipeline, err := numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).Create(ctx, tc.existingPipeline, metav1.CreateOptions{})
				assert.NoError(t, err)
				pipeline.Status = tc.existingPipeline.Status
				// updating the Status subresource is a separate operation
				_, err = numaflowClientSet.NumaflowV1alpha1().Pipelines(defaultNamespace).UpdateStatus(ctx, pipeline, metav1.UpdateOptions{})
				assert.NoError(t, err)
			}

			// call reconcile()
			_, err = r.reconcile(ctx, rollout, defaultNamespace, time.Now())
			assert.NoError(t, err)

			////// check results:
			// Check Phase of Rollout:
			assert.Equal(t, tc.expectedRolloutPhase, rollout.Status.Phase)
			// Check Deployment
			deploymentRetrieved, err := k8sClientSet.AppsV1().Deployments(defaultNamespace).Get(ctx, "numaflow-controller", metav1.GetOptions{})
			assert.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("quay.io/numaproj/numaflow:v%s", tc.expectedResultControllerVersion), deploymentRetrieved.Spec.Template.Spec.Containers[0].Image)

			// Check Conditions:
			for conditionType, conditionStatus := range tc.expectedConditionsSet {
				found := false
				for _, condition := range rollout.Status.Conditions {
					if condition.Type == string(conditionType) && condition.Status == conditionStatus {
						found = true
						break
					}
				}
				assert.True(t, found, "condition type %s failed, conditions=%+v", conditionType, rollout.Status.Conditions)
			}
		})
	}

}

func createDeploymentDefinition(imagePath string) *appsv1.Deployment {
	labels := map[string]string{
		"app.kubernetes.io/component": "controller-manager",
		"app.kubernetes.io/name":      "controller-manager",
		"app.kubernetes.io/part-of":   "numaflow",
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: defaultNamespace,
			Name:      NumaflowControllerDeploymentName,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "controller-manager",
							Image: imagePath,
						},
					},
				},
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
			},
		},
	}
}
