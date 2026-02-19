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
	"testing"

	argorolloutsv1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func metric(name string) argorolloutsv1.Metric {
	return argorolloutsv1.Metric{
		Name: name,
		Provider: argorolloutsv1.MetricProvider{
			Prometheus: &argorolloutsv1.PrometheusMetric{
				Query: "up",
			},
		},
		SuccessCondition: "true",
	}
}

func metricNames(metrics []argorolloutsv1.Metric) []string {
	names := make([]string, len(metrics))
	for i := range metrics {
		names[i] = metrics[i].Name
	}
	return names
}

func TestDedupMetricsForAnalysisTemplates(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name                           string
		templates                      []*argorolloutsv1.AnalysisTemplate
		clusterTemplates               []*argorolloutsv1.ClusterAnalysisTemplate
		expectedTemplateMetrics        map[string][]string // template name -> expected metric names in order
		expectedClusterTemplateMetrics map[string][]string // cluster template name -> expected metric names in order
	}{
		{
			name:                           "nil and empty slices",
			templates:                      nil,
			clusterTemplates:               nil,
			expectedTemplateMetrics:        nil,
			expectedClusterTemplateMetrics: nil,
		},
		{
			name:                           "empty templates no change",
			templates:                      []*argorolloutsv1.AnalysisTemplate{},
			clusterTemplates:               []*argorolloutsv1.ClusterAnalysisTemplate{},
			expectedTemplateMetrics:        map[string][]string{},
			expectedClusterTemplateMetrics: map[string][]string{},
		},
		{
			name: "AnalysisTemplate with no duplicates unchanged",
			templates: []*argorolloutsv1.AnalysisTemplate{{
				ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "ns"},
				Spec: argorolloutsv1.AnalysisTemplateSpec{
					Metrics: []argorolloutsv1.Metric{metric("a"), metric("b")},
				},
			}},
			clusterTemplates:               nil,
			expectedTemplateMetrics:        map[string][]string{"t1": {"a", "b"}},
			expectedClusterTemplateMetrics: nil,
		},
		{
			name: "AnalysisTemplate with duplicate metric names",
			templates: []*argorolloutsv1.AnalysisTemplate{{
				ObjectMeta: metav1.ObjectMeta{Name: "at"},
				Spec: argorolloutsv1.AnalysisTemplateSpec{
					Metrics: []argorolloutsv1.Metric{metric("m1"), metric("m1"), metric("m2")},
				},
			}},
			clusterTemplates:               nil,
			expectedTemplateMetrics:        map[string][]string{"at": {"m1", "m2"}},
			expectedClusterTemplateMetrics: nil,
		},
		{
			name:      "ClusterAnalysisTemplate with duplicate metric names",
			templates: nil,
			clusterTemplates: []*argorolloutsv1.ClusterAnalysisTemplate{{
				ObjectMeta: metav1.ObjectMeta{Name: "ct1"},
				Spec: argorolloutsv1.AnalysisTemplateSpec{
					Metrics: []argorolloutsv1.Metric{metric("cluster-metric"), metric("cluster-metric")},
				},
			}},
			expectedTemplateMetrics:        nil,
			expectedClusterTemplateMetrics: map[string][]string{"ct1": {"cluster-metric"}},
		},
		{
			name: "both template types with duplicate metric names",
			templates: []*argorolloutsv1.AnalysisTemplate{{
				ObjectMeta: metav1.ObjectMeta{Name: "at"},
				Spec: argorolloutsv1.AnalysisTemplateSpec{
					Metrics: []argorolloutsv1.Metric{metric("m1"), metric("m1"), metric("m2")},
				},
			}},
			clusterTemplates: []*argorolloutsv1.ClusterAnalysisTemplate{{
				ObjectMeta: metav1.ObjectMeta{Name: "ct1"},
				Spec: argorolloutsv1.AnalysisTemplateSpec{
					Metrics: []argorolloutsv1.Metric{metric("cluster-metric"), metric("cluster-metric")},
				},
			}},
			expectedTemplateMetrics:        map[string][]string{"at": {"m1", "m2"}},
			expectedClusterTemplateMetrics: map[string][]string{"ct1": {"cluster-metric"}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotT, gotC, err := dedupMetricsForAnalysisTemplates(ctx, tc.templates, tc.clusterTemplates)
			assert.NoError(t, err)

			if tc.expectedTemplateMetrics == nil {
				assert.Nil(t, gotT)
			} else {
				assert.Len(t, gotT, len(tc.expectedTemplateMetrics))
				for _, tpl := range gotT {
					wantNames, ok := tc.expectedTemplateMetrics[tpl.Name]
					assert.True(t, ok, "unexpected AnalysisTemplate name %q", tpl.Name)
					assert.Equal(t, wantNames, metricNames(tpl.Spec.Metrics), "AnalysisTemplate %q", tpl.Name)
				}
			}

			if tc.expectedClusterTemplateMetrics == nil {
				assert.Nil(t, gotC)
			} else {
				assert.Len(t, gotC, len(tc.expectedClusterTemplateMetrics))
				for _, tpl := range gotC {
					wantNames, ok := tc.expectedClusterTemplateMetrics[tpl.Name]
					assert.True(t, ok, "unexpected ClusterAnalysisTemplate name %q", tpl.Name)
					assert.Equal(t, wantNames, metricNames(tpl.Spec.Metrics), "ClusterAnalysisTemplate %q", tpl.Name)
				}
			}
		})
	}
}
