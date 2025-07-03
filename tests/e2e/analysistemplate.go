package e2e

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"

	argov1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func CreateAnalysisTemplate(name, namespace string, spec argov1alpha1.AnalysisTemplateSpec) {
	analysisTemplateSpec := createAnalysisTemplateSpec(name, namespace, spec)
	_, err := argoAnalysisTemplateClient.Create(ctx, analysisTemplateSpec, metav1.CreateOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	CheckEventually("Verifying that the Analysis Template was created", func() error {
		_, err := argoAnalysisTemplateClient.Get(ctx, name, metav1.GetOptions{})
		return err
	}).Should(Succeed())
}

func createAnalysisTemplateSpec(name, namespace string, spec argov1alpha1.AnalysisTemplateSpec) *argov1alpha1.AnalysisTemplate {
	analysisTemplate := &argov1alpha1.AnalysisTemplate{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "argoproj.io/v1alpha1",
			Kind:       "analysisTemplate",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: argov1alpha1.AnalysisTemplateSpec{
			Metrics: spec.Metrics,
			Args:    spec.Args,
		},
	}

	return analysisTemplate
}

// DeleteAnalysisTemplate will delete and verify deletion of the Analysis Template with the given name.
func DeleteAnalysisTemplate(name string) {
	By("Deleting AnalysisTemplate")
	foregroundDeletion := metav1.DeletePropagationForeground
	err := argoAnalysisTemplateClient.Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &foregroundDeletion})
	Expect(err).ShouldNot(HaveOccurred())

	CheckEventually(fmt.Sprintf("Verifying AnalysisTemplate deletion, (%s)", name), func() bool {
		_, err := argoAnalysisTemplateClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if !errors.IsNotFound(err) {
				Fail("An unexpected error occurred when fetching the AnalysisTemplate: " + err.Error())
			}
			return false
		}
		return true
	}).WithTimeout(TestTimeout).Should(BeFalse(), "The AnalysisTemplate should have been deleted but it was found.")
}

func VerifyAnalysisRunStatus(metricName, name string, expectedStatus argov1alpha1.AnalysisPhase) {
	CheckEventually(fmt.Sprintf("Verifying AnalysisRun status (%s)", name), func() bool {
		analysisRun, err := argoAnalysisRunClient.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if !errors.IsNotFound(err) {
				Fail("An unexpected error occurred when fetching the AnalysisTemplate: " + err.Error())
			}
			return false
		}
		for _, metric := range analysisRun.Status.MetricResults {
			if metric.Name == metricName {
				if metric.Phase == expectedStatus {
					return true
				}
			}
		}
		return false
	}).Should(BeTrue())
}
