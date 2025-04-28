package sync

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/numaproj/numaplane/internal/util/metrics"

	clustercache "github.com/argoproj/gitops-engine/pkg/cache"
	"github.com/argoproj/gitops-engine/pkg/diff"
	"github.com/argoproj/gitops-engine/pkg/health"
	"github.com/argoproj/gitops-engine/pkg/utils/kube"
	"github.com/cespare/xxhash/v2"
	"golang.org/x/sync/semaphore"
	v1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"

	"github.com/numaproj/numaplane/internal/common"
	controllerConfig "github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
)

// GitOps engine cluster cache tuning options
var (
	// clusterCacheResyncDuration controls the duration of cluster cache refresh.
	// NOTE: this differs from gitops-engine default of 24h
	clusterCacheResyncDuration = 12 * time.Hour

	// clusterCacheWatchResyncDuration controls the maximum duration that group/kind watches are allowed to run
	// for before relisting & restarting the watch
	clusterCacheWatchResyncDuration = 10 * time.Minute

	// clusterSyncRetryTimeoutDuration controls the sync retry duration when cluster sync error happens
	clusterSyncRetryTimeoutDuration = 10 * time.Second

	// The default limit of 50 is chosen based on experiments.
	clusterCacheListSemaphoreSize int64 = 50

	// clusterCacheListPageSize is the page size when performing K8s list requests.
	// 500 is equal to kubectl's size
	clusterCacheListPageSize int64 = 500

	// clusterCacheListPageBufferSize is the number of pages to buffer when performing K8s list requests
	clusterCacheListPageBufferSize int32 = 1

	// clusterCacheRetryLimit sets a retry limit for failed requests during cluster cache sync
	// If set to 1, retries are disabled.
	clusterCacheAttemptLimit int32 = 1

	// clusterCacheRetryUseBackoff specifies whether to use a backoff strategy on cluster cache sync, if retry is enabled
	clusterCacheRetryUseBackoff bool = false
)

type ResourceInfo struct {
	Name string

	Health       *health.HealthStatus
	manifestHash string
}

// LiveStateCache is a cluster caching that stores resource references and ownership
// references. It also stores custom metadata for resources managed by Numaplane. It always
// ensures the cache is up-to-date before returning the resources.
type LiveStateCache interface {
	// GetClusterCache returns synced cluster cache
	GetClusterCache() (clustercache.ClusterCache, error)
	// GetManagedLiveObjs returns state of live objects which correspond to target
	// objects with the specified ResourceInfo name and matching namespace.
	GetManagedLiveObjs(name, namespace string, targetObjs []*unstructured.Unstructured) (map[kube.ResourceKey]*unstructured.Unstructured, error)
	// Init must be executed before cache can be used
	Init(numaLogger *logger.NumaLogger) error
	// PopulateResourceInfo is called by the cache to update ResourceInfo struct for a managed resource
	PopulateResourceInfo(un *unstructured.Unstructured, isRoot bool) (interface{}, bool)
}

type cacheSettings struct {
	clusterSettings clustercache.Settings

	// ignoreResourceUpdates is a flag to enable resource-ignore rules.
	ignoreResourceUpdatesEnabled bool
}

type ObjectUpdatedHandler = func(managedByNumaplane map[string]bool, ref v1.ObjectReference)

type liveStateCache struct {
	clusterCacheConfig *rest.Config
	logger             *logger.NumaLogger
	// TODO: utilize `onObjectUpdated` when switch to change event triggering for sync tasking
	// onObjectUpdated ObjectUpdatedHandler

	cluster       clustercache.ClusterCache
	cacheSettings cacheSettings
	lock          sync.RWMutex
	customMetrics *metrics.CustomMetrics
}

func newLiveStateCache(
	cluster clustercache.ClusterCache, customMetrics *metrics.CustomMetrics,
) LiveStateCache {
	numaLogger := logger.New().WithName("live state cache")
	return &liveStateCache{
		cluster:       cluster,
		logger:        numaLogger,
		customMetrics: customMetrics,
	}
}

func NewLiveStateCache(
	clusterCacheConfig *rest.Config, customMetrics *metrics.CustomMetrics,
) LiveStateCache {
	return &liveStateCache{
		clusterCacheConfig: clusterCacheConfig,
		customMetrics:      customMetrics,
	}
}

func (c *liveStateCache) invalidate(cacheSettings cacheSettings) {
	c.logger.Info("invalidating live state cache")
	c.lock.Lock()
	c.cacheSettings = cacheSettings
	cluster := c.cluster
	c.lock.Unlock()

	cluster.Invalidate(clustercache.SetSettings(cacheSettings.clusterSettings))

	c.logger.Info("live state cache invalidated")
}

func (c *liveStateCache) getCluster() clustercache.ClusterCache {
	// Load the cache setting to see if it changed
	nextCacheSettings, err := c.loadCacheSettings()
	if err != nil {
		c.logger.Error(err, "failed to read cache settings")
	}

	c.lock.Lock()
	needInvalidate := false
	if !reflect.DeepEqual(c.cacheSettings, *nextCacheSettings) {
		c.cacheSettings = *nextCacheSettings
		needInvalidate = true
	}
	c.lock.Unlock()
	if needInvalidate {
		c.invalidate(*nextCacheSettings)
	}

	c.lock.RLock()
	cluster := c.cluster
	cacheSettings := c.cacheSettings
	c.lock.RUnlock()

	if cluster != nil {
		return cluster
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.cluster != nil {
		return c.cluster
	}

	clusterCacheConfig := c.clusterCacheConfig

	// TODO: check if we need set managed namespace
	clusterCacheOpts := []clustercache.UpdateSettingsFunc{
		clustercache.SetListSemaphore(semaphore.NewWeighted(clusterCacheListSemaphoreSize)),
		clustercache.SetListPageSize(clusterCacheListPageSize),
		clustercache.SetListPageBufferSize(clusterCacheListPageBufferSize),
		clustercache.SetWatchResyncTimeout(clusterCacheWatchResyncDuration),
		clustercache.SetClusterSyncRetryTimeout(clusterSyncRetryTimeoutDuration),
		clustercache.SetResyncTimeout(clusterCacheResyncDuration),
		clustercache.SetSettings(cacheSettings.clusterSettings),
		clustercache.SetPopulateResourceInfoHandler(c.PopulateResourceInfo),
		clustercache.SetRetryOptions(clusterCacheAttemptLimit, clusterCacheRetryUseBackoff, isRetryableError),
		clustercache.SetLogr(*logger.New().LogrLogger),
	}

	clusterCache := clustercache.NewClusterCache(clusterCacheConfig, clusterCacheOpts...)

	// Register event handler that is executed whenever the resource gets updated in the cache
	_ = clusterCache.OnResourceUpdated(func(newRes *clustercache.Resource, oldRes *clustercache.Resource, namespaceResources map[kube.ResourceKey]*clustercache.Resource) {
		c.lock.RLock()
		cacheSettings := c.cacheSettings
		c.lock.RUnlock()

		if cacheSettings.ignoreResourceUpdatesEnabled && oldRes != nil && newRes != nil && skipResourceUpdate(resInfo(oldRes), resInfo(newRes)) {
			return
		}

		// TODO: when switch to change event triggering for sync tasking, notify
		//       the Numaplane managing the resources that there are changes happen
	})

	c.cluster = clusterCache

	return clusterCache
}

func (c *liveStateCache) PopulateResourceInfo(un *unstructured.Unstructured, isRoot bool) (interface{}, bool) {
	res := &ResourceInfo{}
	c.lock.RLock()
	settings := c.cacheSettings
	c.lock.RUnlock()

	res.Health, _ = health.GetResourceHealth(un, settings.clusterSettings.ResourceHealthOverride)

	numaplaneInstanceName := getNumaplaneInstanceName(un)
	if isRoot && numaplaneInstanceName != "" {
		res.Name = numaplaneInstanceName
	}

	gvk := un.GroupVersionKind()

	if settings.ignoreResourceUpdatesEnabled && shouldHashManifest(numaplaneInstanceName, gvk) {
		hash, err := generateManifestHash(un)
		if err != nil {
			c.logger.Errorf(err, "failed to generate manifest hash")
		} else {
			res.manifestHash = hash
		}
	}

	// edge case. we do not label CRDs, so they miss the tracking label we inject. But we still
	// want the full resource to be available in our cache (to diff), so we store all CRDs
	return res, res.Name != "" || gvk.Kind == kube.CustomResourceDefinitionKind
}

// getNumaplaneInstanceName gets the Numaplane Object that owns the resource from a label in the resource
func getNumaplaneInstanceName(un *unstructured.Unstructured) string {
	value, err := kubernetes.GetLabel(un, common.LabelKeyNumaplaneInstance)
	if err != nil {
		return ""
	}
	return value
}

func resInfo(r *clustercache.Resource) *ResourceInfo {
	info, ok := r.Info.(*ResourceInfo)
	if !ok || info == nil {
		info = &ResourceInfo{}
	}
	return info
}

// shouldHashManifest validates if the API resource needs to be hashed.
// If there's a instance name from resource tracking, or if this is itself
// a Numaplane object, we should generate a hash. Otherwise, the hashing
// should be skipped to save CPU time.
func shouldHashManifest(instanceName string, gvk schema.GroupVersionKind) bool {
	return instanceName != "" || gvk.Group == "numaplane.numaproj.io"
}

func generateManifestHash(un *unstructured.Unstructured) (string, error) {

	// Only use a noop normalizer for now.
	normalizer := NewNoopNormalizer()

	resource := un.DeepCopy()
	err := normalizer.Normalize(resource)
	if err != nil {
		return "", fmt.Errorf("error normalizing resource: %w", err)
	}

	data, err := resource.MarshalJSON()
	if err != nil {
		return "", fmt.Errorf("error marshaling resource: %w", err)
	}
	hash := hash(data)
	return hash, nil
}

func hash(data []byte) string {
	return strconv.FormatUint(xxhash.Sum64(data), 16)
}

// isRetryableError is a helper method to see whether an error
// returned from the dynamic client is potentially retryable.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	return kerrors.IsInternalError(err) ||
		kerrors.IsInvalid(err) ||
		kerrors.IsTooManyRequests(err) ||
		kerrors.IsServerTimeout(err) ||
		kerrors.IsServiceUnavailable(err) ||
		kerrors.IsTimeout(err) ||
		kerrors.IsUnexpectedObjectError(err) ||
		kerrors.IsUnexpectedServerError(err) ||
		isResourceQuotaConflictErr(err) ||
		isTransientNetworkErr(err) ||
		isExceededQuotaErr(err) ||
		errors.Is(err, syscall.ECONNRESET)
}

func isExceededQuotaErr(err error) bool {
	return kerrors.IsForbidden(err) && strings.Contains(err.Error(), "exceeded quota")
}

func isResourceQuotaConflictErr(err error) bool {
	return kerrors.IsConflict(err) && strings.Contains(err.Error(), "Operation cannot be fulfilled on resourcequota")
}

func isTransientNetworkErr(err error) bool {
	switch err.(type) {
	case net.Error:
		switch err.(type) {
		case *net.DNSError, *net.OpError, net.UnknownNetworkError:
			return true
		case *url.Error:
			// For a URL error, where it replies "connection closed"
			// retry again.
			return strings.Contains(err.Error(), "Connection closed by foreign host")
		}
	}

	errorString := err.Error()
	if exitErr, ok := err.(*exec.ExitError); ok {
		errorString = fmt.Sprintf("%s %s", errorString, exitErr.Stderr)
	}
	if strings.Contains(errorString, "net/http: TLS handshake timeout") ||
		strings.Contains(errorString, "i/o timeout") ||
		strings.Contains(errorString, "connection timed out") ||
		strings.Contains(errorString, "connection reset by peer") {
		return true
	}
	return false
}

func skipResourceUpdate(oldInfo, newInfo *ResourceInfo) bool {
	if oldInfo == nil || newInfo == nil {
		return false
	}
	isSameHealthStatus := (oldInfo.Health == nil && newInfo.Health == nil) || oldInfo.Health != nil && newInfo.Health != nil && oldInfo.Health.Status == newInfo.Health.Status
	isSameManifest := oldInfo.manifestHash != "" && newInfo.manifestHash != "" && oldInfo.manifestHash == newInfo.manifestHash
	return isSameHealthStatus && isSameManifest
}

func (c *liveStateCache) getSyncedCluster() (clustercache.ClusterCache, error) {
	clusterCache := c.getCluster()
	err := clusterCache.EnsureSynced()
	if err != nil {
		return nil, fmt.Errorf("error synchronizing cache state : %w", err)
	}
	return clusterCache, nil
}

func (c *liveStateCache) Init(numaLogger *logger.NumaLogger) error {
	c.logger = numaLogger.WithName("state-cache")
	setting, err := c.loadCacheSettings()
	if err != nil {
		return fmt.Errorf("error on init live state cache: %w", err)
	}
	c.cacheSettings = *setting
	return nil
}

func (c *liveStateCache) loadCacheSettings() (*cacheSettings, error) {
	ignoreResourceUpdatesEnabled := false
	globalConfig, err := controllerConfig.GetConfigManagerInstance().GetConfig()
	if err != nil {
		return nil, fmt.Errorf("error on getting global config: %w", err)
	}
	includedResources, err := ParseResourceFilter(globalConfig.IncludedResources)
	if err != nil {
		return nil, fmt.Errorf("error on parsing resource filter rules: %w", err)
	}
	resourcesFilter := &ResourceFilter{
		IncludedResources: *includedResources,
	}
	clusterSettings := clustercache.Settings{
		ResourcesFilter: resourcesFilter,
	}

	return &cacheSettings{clusterSettings, ignoreResourceUpdatesEnabled}, nil
}

// TODO: move to shared package (util?) since we also use this for riders
//
// ParseResourceFilter parse the given rules to generate the
// included resource to be watched during caching. The rules are
// delimited by ';', and each rule is composed by both 'group'
// and 'kind' which can be empty. For example,
// 'group=apps,kind=Deployment;group=,kind=ConfigMap'. Note that
// empty rule is valid.
func ParseResourceFilter(rules string) (*[]ResourceType, error) {
	filteredResources := make([]ResourceType, 0)
	if rules == "" {
		return &filteredResources, nil
	}
	rulesArr := strings.Split(rules, ";")
	err := fmt.Errorf("malformed resource filter rules %q", rules)
	for _, rule := range rulesArr {
		ruleArr := strings.Split(rule, ",")
		if len(ruleArr) != 2 {
			return nil, err
		}
		groupArr := strings.Split(ruleArr[0], "=")
		kindArr := strings.Split(ruleArr[1], "=")
		if !strings.EqualFold(groupArr[0], "group") || !strings.EqualFold(kindArr[0], "kind") {
			return nil, err
		}
		filteredResource := ResourceType{
			Group: groupArr[1],
			Kind:  kindArr[1],
		}
		filteredResources = append(filteredResources, filteredResource)
	}
	return &filteredResources, nil

}

func (c *liveStateCache) GetClusterCache() (clustercache.ClusterCache, error) {
	clusterCache, err := c.getSyncedCluster()
	if err != nil {
		c.customMetrics.ClusterCacheError.WithLabelValues().Inc()
		return nil, err
	}
	c.customMetrics.KubeResourceCache.WithLabelValues(clusterCache.GetClusterInfo().K8SVersion).Set(float64(clusterCache.GetClusterInfo().ResourcesCount))
	return clusterCache, nil
}

func (c *liveStateCache) GetManagedLiveObjs(
	name, namespace string,
	targetObjs []*unstructured.Unstructured,
) (map[kube.ResourceKey]*unstructured.Unstructured, error) {
	clusterInfo, err := c.getSyncedCluster()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster info: %w", err)
	}
	liveObjs, err := clusterInfo.GetManagedLiveObjs(targetObjs, func(r *clustercache.Resource) bool {
		// distinguish it by numaplane resource's name and namespace
		return resInfo(r).Name == name && r.Ref.Namespace == namespace
	})
	c.customMetrics.KubeResourceMonitored.WithLabelValues().Set(float64(len(liveObjs)))
	return liveObjs, err
}

type NoopNormalizer struct {
}

func (n *NoopNormalizer) Normalize(un *unstructured.Unstructured) error {
	return nil
}

// NewNoopNormalizer returns normalizer that does not apply any resource modifications
func NewNoopNormalizer() diff.Normalizer {
	return &NoopNormalizer{}
}

// ResourceFilter filter resources based on allowed Resource Types
type ResourceFilter struct {
	IncludedResources []ResourceType
}

type ResourceType struct {
	Group string
	Kind  string
}

func (n *ResourceFilter) IsExcludedResource(group, kind, _ string) bool {
	for _, resource := range n.IncludedResources {
		if resource.Kind == "" {
			// When Kind is empty, we only check if Group matches
			if group == resource.Group {
				return false
			}
		} else if resource.Group == "" {
			// When Group is empty, we only check if Kind matches
			if group == resource.Group {
				return false
			}
		} else if group == resource.Group && kind == resource.Kind {
			return false
		}
	}
	return true
}
