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

	argorolloutsv1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	analysisutil "github.com/argoproj/argo-rollouts/utils/analysis"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func GetAnalysisTemplatesFromRefs(ctx context.Context, templateRefs *[]argorolloutsv1.AnalysisTemplateRef, namespace string, c client.Client) ([]*argorolloutsv1.AnalysisTemplate, []*argorolloutsv1.ClusterAnalysisTemplate, error) {

	numaLogger := logger.FromContext(ctx)

	templates := make([]*argorolloutsv1.AnalysisTemplate, 0)
	clusterTemplates := make([]*argorolloutsv1.ClusterAnalysisTemplate, 0)
	for _, templateRef := range *templateRefs {
		if templateRef.ClusterScope {
			template := &argorolloutsv1.ClusterAnalysisTemplate{}
			err := c.Get(ctx, client.ObjectKey{Name: templateRef.TemplateName, Namespace: "default"}, template)
			if err != nil {
				if errors.IsNotFound(err) {
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
				if errors.IsNotFound(err) {
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

func CreateAnalysisRun(ctx context.Context, analysis apiv1.Analysis, existingUpgradingChildDef *unstructured.Unstructured, client client.Client) error {

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
		analysis.Args = append(analysis.Args, argorolloutsv1.Argument{Name: "monovertex-name", Value: &childName})
		analysis.Args = append(analysis.Args, argorolloutsv1.Argument{Name: "monovertex-namespace", Value: &childNamespace})
	case "Pipeline":
		analysis.Args = append(analysis.Args, argorolloutsv1.Argument{Name: "pipeline-name", Value: &childName})
		analysis.Args = append(analysis.Args, argorolloutsv1.Argument{Name: "pipeline-namespace", Value: &childNamespace})
	}

	// create new AnalysisRun in the child namespace from combination of all templates and args
	analysisRun, err := analysisutil.NewAnalysisRunFromTemplates(analysisTemplates, clusterAnalysisTemplates, analysis.Args, nil, nil,
		map[string]string{"app.kubernetes.io/part-of": "numaplane"}, nil, childName, "", childNamespace)
	if err != nil {
		return err
	}

	if err = client.Create(ctx, analysisRun); err != nil {
		return err
	}

	return nil
}
