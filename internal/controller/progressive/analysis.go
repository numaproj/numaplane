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

package progressive

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	argorolloutsv1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	analysisutil "github.com/argoproj/argo-rollouts/utils/analysis"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// This function is repurposed from the Argo Rollout codebase here:
// https://github.com/argoproj/argo-rollouts/blob/f4f7eabd6bfa8c068abe1a7b62579aafeda25a0e/rollout/analysis.go#L469-L514
func GetAnalysisTemplatesFromRefs(ctx context.Context, templateRefs *[]argorolloutsv1.AnalysisTemplateRef, namespace string, c client.Client) ([]*argorolloutsv1.AnalysisTemplate, []*argorolloutsv1.ClusterAnalysisTemplate, error) {

	numaLogger := logger.FromContext(ctx)
	templates := make([]*argorolloutsv1.AnalysisTemplate, 0)
	clusterTemplates := make([]*argorolloutsv1.ClusterAnalysisTemplate, 0)
	for _, templateRef := range *templateRefs {
		if templateRef.ClusterScope {
			template := &argorolloutsv1.ClusterAnalysisTemplate{}
			err := c.Get(ctx, client.ObjectKey{Name: templateRef.TemplateName, Namespace: "default"}, template)
			if err != nil {
				if k8serrors.IsNotFound(err) {
					numaLogger.Warnf("ClusterAnalysisTemplate '%s' not found", templateRef.TemplateName)
				}
				return nil, nil, err
			}
			clusterTemplates = append(clusterTemplates, template)
			// Look for nested templates
			if template.Spec.Templates != nil {
				innerTemplates, innerClusterTemplates, innerErr := GetAnalysisTemplatesFromRefs(ctx, &template.Spec.Templates, namespace, c)
				if innerErr != nil {
					return nil, nil, innerErr
				}
				clusterTemplates = append(clusterTemplates, innerClusterTemplates...)
				templates = append(templates, innerTemplates...)
			}
		} else {
			template := &argorolloutsv1.AnalysisTemplate{}
			err := c.Get(ctx, client.ObjectKey{Name: templateRef.TemplateName, Namespace: namespace}, template)
			if err != nil {
				if k8serrors.IsNotFound(err) {
					numaLogger.Warnf("AnalysisTemplate '%s' not found", templateRef.TemplateName)
				}
				return nil, nil, err
			}
			templates = append(templates, template)
			// Look for nested templates
			if template.Spec.Templates != nil {
				innerTemplates, innerClusterTemplates, innerErr := GetAnalysisTemplatesFromRefs(ctx, &template.Spec.Templates, namespace, c)
				if innerErr != nil {
					return nil, nil, innerErr
				}
				clusterTemplates = append(clusterTemplates, innerClusterTemplates...)
				templates = append(templates, innerTemplates...)
			}
		}

	}
	uniqueTemplates, uniqueClusterTemplates := analysisutil.FilterUniqueTemplates(templates, clusterTemplates)
	return uniqueTemplates, uniqueClusterTemplates, nil
}

/*
CreateAnalysisRun finds all templates specified in the Analysis field in the spec of a rollout and creates the resulting AnalysisRun in k8s.

Parameters:
  - ctx: the context for managing request-scoped values.
  - analysis: struct which contains templateRefs to AnalysisTemplates and ClusterAnalysisTemplates and arguments that can be passed
    and override values already specified in the templates
  - existingUpgradingChildDef: the definition of the upgrading child as an unstructured object.
  - analysisRunName: name to use for the AnalysisRun
  - ownerReference: reference to the upgrading child this AnalysisRun is associated with - ensures cleanup
  - client: the client used for interacting with the Kubernetes API.
  - promotedChildName - argument we support in templates.

Returns:
  - An error if any issues occur during processing.
*/
func CreateAnalysisRun(ctx context.Context, analysis apiv1.Analysis, existingUpgradingChildDef *unstructured.Unstructured, analysisRunName string, ownerReference metav1.OwnerReference, client client.Client, promotedChildName string) error {

	numaLogger := logger.FromContext(ctx)

	// find all specified templates to merge into single AnalysisRun
	analysisTemplates, clusterAnalysisTemplates, err := GetAnalysisTemplatesFromRefs(ctx, &analysis.Templates, existingUpgradingChildDef.GetNamespace(), client)
	if err != nil {
		return err
	}

	// set special arguments for child name and namespace
	childName := existingUpgradingChildDef.GetName()
	childNamespace := existingUpgradingChildDef.GetNamespace()

	switch existingUpgradingChildDef.GetKind() {
	case "MonoVertex":
		analysis.Args = append(analysis.Args, argorolloutsv1.Argument{Name: "upgrading-monovertex-name", Value: &childName})
		analysis.Args = append(analysis.Args, argorolloutsv1.Argument{Name: "promoted-monovertex-name", Value: &promotedChildName})
		analysis.Args = append(analysis.Args, argorolloutsv1.Argument{Name: "monovertex-namespace", Value: &childNamespace})
	case "Pipeline":
		analysis.Args = append(analysis.Args, argorolloutsv1.Argument{Name: "upgrading-pipeline-name", Value: &childName})
		analysis.Args = append(analysis.Args, argorolloutsv1.Argument{Name: "promoted-pipeline-name", Value: &promotedChildName})
		analysis.Args = append(analysis.Args, argorolloutsv1.Argument{Name: "pipeline-namespace", Value: &childNamespace})
	}

	// create new AnalysisRun in the child namespace from combination of all templates and args
	analysisRun, err := analysisutil.NewAnalysisRunFromTemplates(analysisTemplates, clusterAnalysisTemplates, analysis.Args, nil, nil,
		map[string]string{"app.kubernetes.io/part-of": "numaplane"}, nil, analysisRunName, "", childNamespace)
	if err != nil {
		return err
	}

	// set ownerReference to guarantee AnalysisRun deletion when owner is cleaned up
	analysisRun.SetOwnerReferences([]metav1.OwnerReference{ownerReference})
	if err = client.Create(ctx, analysisRun); err != nil {
		return err
	}

	numaLogger.WithValues("AnalysisRunName", analysisRun.Name).Debug("Successfully created AnalysisRun")

	return nil
}

func PerformAnalysis(
	ctx context.Context,
	existingUpgradingChildDef *unstructured.Unstructured,
	rolloutObject ProgressiveRolloutObject,
	analysis apiv1.Analysis,
	analysisStatus *apiv1.AnalysisStatus,
	c client.Client,
) (*apiv1.AnalysisStatus, error) {
	if analysisStatus == nil {
		return analysisStatus, errors.New("analysisStatus not set")
	}

	analysisRun := &argorolloutsv1.AnalysisRun{}

	analysisRunName := fmt.Sprintf("%s-%s", strings.ToLower(existingUpgradingChildDef.GetKind()), existingUpgradingChildDef.GetName())

	// check if analysisRun has already been created
	if err := c.Get(ctx, client.ObjectKey{Name: analysisRunName, Namespace: existingUpgradingChildDef.GetNamespace()}, analysisRun); err != nil {
		if apierrors.IsNotFound(err) {
			// analysisRun is created the first time the upgrading child is assessed
			ownerRef := *metav1.NewControllerRef(&metav1.ObjectMeta{Name: existingUpgradingChildDef.GetName(),
				Namespace: existingUpgradingChildDef.GetNamespace(),
				UID:       existingUpgradingChildDef.GetUID()},
				existingUpgradingChildDef.GroupVersionKind())
			promotedChildStatus := rolloutObject.GetPromotedChildStatus()
			var promotedChildName string
			if promotedChildStatus != nil {
				promotedChildName = promotedChildStatus.Name
			}
			err := CreateAnalysisRun(ctx, analysis, existingUpgradingChildDef, analysisRunName, ownerRef, c, promotedChildName)
			if err != nil {
				return analysisStatus, err
			}

			// analysisStatus is updated with name of AnalysisRun (which is the same name as the upgrading child)
			// and start time for its assessment
			analysisStatus.AnalysisRunName = analysisRunName
			timeNow := metav1.NewTime(time.Now())
			analysisStatus.StartTime = &timeNow
			return analysisStatus, nil
		} else {
			return analysisStatus, err
		}
	}

	// assess analysisRun status and set endTime if completed
	if analysisRun.Status.Phase.Completed() && analysisStatus.EndTime == nil {
		analysisStatus.EndTime = analysisRun.Status.CompletedAt
	}
	analysisStatus.AnalysisRunName = existingUpgradingChildDef.GetName()
	analysisStatus.Phase = analysisRun.Status.Phase
	return analysisStatus, nil

}

func AssessAnalysisStatus(
	ctx context.Context,
	existingUpgradingChildDef *unstructured.Unstructured,
	analysisStatus *apiv1.AnalysisStatus) (apiv1.AssessmentResult, string, error) {
	numaLogger := logger.FromContext(ctx)

	analysisRunTimeout, err := getAnalysisRunTimeout(ctx)
	if err != nil {
		return apiv1.AssessmentResultUnknown, "", err
	}

	// if analysisStatus is set with an AnalysisRun's name, we must also check that it is in a Completed phase to declare success
	if analysisStatus != nil && analysisStatus.AnalysisRunName != "" {
		numaLogger.WithValues("namespace", existingUpgradingChildDef.GetNamespace(), "name", existingUpgradingChildDef.GetName()).
			Debugf("AnalysisRun %s is in phase %s", analysisStatus.AnalysisRunName, analysisStatus.Phase)
		switch analysisStatus.Phase {
		case argorolloutsv1.AnalysisPhaseSuccessful:
			return apiv1.AssessmentResultSuccess, "", nil
		case argorolloutsv1.AnalysisPhaseError, argorolloutsv1.AnalysisPhaseFailed, argorolloutsv1.AnalysisPhaseInconclusive:
			return apiv1.AssessmentResultFailure, fmt.Sprintf("AnalysisRun %s is in phase %s", analysisStatus.AnalysisRunName, analysisStatus.Phase), nil
		default:
			// if analysisRun is not completed yet, we check if it has exceeded the analysisRunTimeout
			if time.Since(analysisStatus.StartTime.Time) >= analysisRunTimeout {
				return apiv1.AssessmentResultFailure, fmt.Sprintf("AnalysisRun %s in phase %s has exceeded the analysisRunTimeout", analysisStatus.AnalysisRunName, analysisStatus.Phase), nil
			}
			return apiv1.AssessmentResultUnknown, "", nil
		}
	}

	// no AnalysisRun so by default we can mark this successful
	return apiv1.AssessmentResultSuccess, "", nil
}
