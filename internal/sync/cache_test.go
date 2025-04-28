package sync

import (
	"testing"
	"time"

	clustercache "github.com/argoproj/gitops-engine/pkg/cache"
	"github.com/argoproj/gitops-engine/pkg/utils/kube"
	"github.com/argoproj/gitops-engine/pkg/utils/kube/kubetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	testcore "k8s.io/client-go/testing"
	"sigs.k8s.io/yaml"

	"github.com/numaproj/numaplane/internal/util/metrics"
)

var (
	testName            = "my-test"
	testNamespace       = "default"
	testCreationTime, _ = time.Parse(time.RFC3339, "2018-09-20T06:47:27Z")
)

func strToUnstructured(jsonStr string) *unstructured.Unstructured {
	obj := make(map[string]interface{})
	err := yaml.Unmarshal([]byte(jsonStr), &obj)
	if err != nil {
		panic(err)
	}
	return &unstructured.Unstructured{Object: obj}
}

func newCluster(t *testing.T, objs ...runtime.Object) clustercache.ClusterCache {
	client := fake.NewSimpleDynamicClient(scheme.Scheme, objs...)
	reactor := client.ReactionChain[0]
	client.PrependReactor("list", "*", func(action testcore.Action) (handled bool, ret runtime.Object, err error) {
		handled, ret, err = reactor.React(action)
		if err != nil || !handled {
			return
		}
		// make sure list response have resource version
		ret.(metav1.ListInterface).SetResourceVersion("123")
		return
	})

	apiResources := []kube.APIResourceInfo{{
		GroupKind:            schema.GroupKind{Group: "", Kind: "Pod"},
		GroupVersionResource: schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Meta:                 metav1.APIResource{Namespaced: true},
	}, {
		GroupKind:            schema.GroupKind{Group: "apps", Kind: "ReplicaSet"},
		GroupVersionResource: schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"},
		Meta:                 metav1.APIResource{Namespaced: true},
	}, {
		GroupKind:            schema.GroupKind{Group: "apps", Kind: "Deployment"},
		GroupVersionResource: schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
		Meta:                 metav1.APIResource{Namespaced: true},
	}}

	cache := clustercache.NewClusterCache(
		&rest.Config{Host: "https://test"}, clustercache.SetKubectl(&kubetest.MockKubectlCmd{APIResources: apiResources, DynamicClient: client}))
	t.Cleanup(func() {
		cache.Invalidate()
	})
	return cache
}

func newFakeLivStateCache(t *testing.T, objs ...runtime.Object) LiveStateCache {
	cluster := newCluster(t, objs...)
	customMetrics := metrics.RegisterCustomMetrics()
	clusterCache := newLiveStateCache(cluster, customMetrics)
	cluster.Invalidate(clustercache.SetPopulateResourceInfoHandler(clusterCache.PopulateResourceInfo))
	return clusterCache
}

func testPod() *corev1.Pod {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "my-app-pod",
			Namespace:         testNamespace,
			UID:               "1",
			ResourceVersion:   "123",
			CreationTimestamp: metav1.NewTime(testCreationTime),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "my-app-rs",
					UID:        "2",
				},
			},
		},
	}
}

func testRS() *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "ReplicaSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "my-app-rs",
			Namespace:         "default",
			UID:               "2",
			ResourceVersion:   "123",
			CreationTimestamp: metav1.NewTime(testCreationTime),
			Annotations: map[string]string{
				"deployment.kubernetes.io/revision": "2",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1beta1",
					Kind:       "Deployment",
					Name:       "my-app",
					UID:        "3",
				},
			},
		},
		Spec:   appsv1.ReplicaSetSpec{},
		Status: appsv1.ReplicaSetStatus{},
	}
}

func testDeploy() *appsv1.Deployment {
	return &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "my-app",
			Namespace:         testNamespace,
			UID:               "3",
			ResourceVersion:   "123",
			CreationTimestamp: metav1.NewTime(testCreationTime),
			Labels: map[string]string{
				"numaplane.numaproj.io/tracking-id": testName,
			},
		},
	}
}

func mustToUnstructured(obj interface{}) *unstructured.Unstructured {
	un, err := kube.ToUnstructured(obj)
	if err != nil {
		panic(err)
	}
	return un
}

func TestGetManagedLiveObjs(t *testing.T) {
	clusterCache := newFakeLivStateCache(t, testPod(), testRS(), testDeploy())

	targetDeploy := strToUnstructured(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
  labels:
    numaplane.numaproj.io/tracking-id: my-example`)

	managedObjs, err := clusterCache.GetManagedLiveObjs(testName, testNamespace, []*unstructured.Unstructured{targetDeploy})
	require.NoError(t, err)
	assert.Equal(t, map[kube.ResourceKey]*unstructured.Unstructured{
		kube.NewResourceKey("apps", "Deployment", "default", "my-app"): mustToUnstructured(testDeploy()),
	}, managedObjs)
}

func TestParseResourceFilter(t *testing.T) {
	testCases := []struct {
		name              string
		rules             string
		expectedResources []ResourceType
		hasErr            bool
	}{
		{
			name:              "valid empty rule",
			rules:             "",
			expectedResources: []ResourceType{},
			hasErr:            false,
		},
		{
			name:  "valid rules",
			rules: "group=apps,kind=Deployment;group=rbac.authorization.k8s.io,kind=RoleBinding",
			expectedResources: []ResourceType{
				{
					Group: "apps",
					Kind:  "Deployment",
				},
				{
					Group: "rbac.authorization.k8s.io",
					Kind:  "RoleBinding",
				},
			},
			hasErr: false,
		},
		{
			name:  "valid rules with empty group",
			rules: "group=apps,kind=Deployment;group=,kind=ConfigMap",
			expectedResources: []ResourceType{
				{
					Group: "apps",
					Kind:  "Deployment",
				},
				{
					Group: "",
					Kind:  "ConfigMap",
				},
			},
			hasErr: false,
		},
		{
			name:   "invalid rules",
			rules:  "group=apps,kind=Deployment;grup=,kind=ConfigMap",
			hasErr: true,
		},
	}
	t.Parallel()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resources, err := ParseResourceFilter(tc.rules)
			if tc.hasErr {
				assert.NotNil(t, err)
				assert.Nil(t, resources)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tc.expectedResources, *resources)
			}
		})
	}
}
