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
	"sigs.k8s.io/controller-runtime/pkg/client"

	errors "k8s.io/apimachinery/pkg/api/errors"
)

func GetAnalysisTemplatesFromRefs(ctx context.Context, templateRefs *[]argorolloutsv1.AnalysisTemplateRef, namespace string, c client.Client) ([]*argorolloutsv1.AnalysisTemplate, []*argorolloutsv1.ClusterAnalysisTemplate, error) {

	templates := make([]*argorolloutsv1.AnalysisTemplate, 0)
	clusterTemplates := make([]*argorolloutsv1.ClusterAnalysisTemplate, 0)
	for _, templateRef := range *templateRefs {
		if templateRef.ClusterScope {
			template := &argorolloutsv1.ClusterAnalysisTemplate{}
			err := c.Get(ctx, client.ObjectKey{Name: templateRef.TemplateName, Namespace: namespace}, template)
			// template, err := c.clusterAnalysisTemplateLister.Get(templateRef.TemplateName)
			if err != nil {
				if errors.IsNotFound(err) {
					// c.log.Warnf("ClusterAnalysisTemplate '%s' not found", templateRef.TemplateName)
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
			// template, err := c.analysisTemplateLister.AnalysisTemplates(namespace).Get(templateRef.TemplateName)
			err := c.Get(ctx, client.ObjectKey{Name: templateRef.TemplateName, Namespace: namespace}, template)
			if err != nil {
				if errors.IsNotFound(err) {
					// c.log.Warnf("AnalysisTemplate '%s' not found", templateRef.TemplateName)
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
