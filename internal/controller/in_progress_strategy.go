package controller

import (
	"context"
	"sync"

	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// inProgressStrategyMgr is responsible to maintain a Rollout's inProgressStrategy
// state is maintained both in memory as well as in the Rollout's Status
// in memory always gives us the latest state in case the Informer cache is out of date
// the Rollout's Status is useful as a backup mechanism in case Numaplane has just restarted
type inProgressStrategyMgr struct {
	getRolloutStrategy func(context.Context, client.Object) *apiv1.UpgradeStrategy
	setRolloutStrategy func(context.Context, client.Object, apiv1.UpgradeStrategy)
	store              *inProgressStrategyStore
}

// in memory storage of UpgradeStrategy in progress for a given Rollout
type inProgressStrategyStore struct {
	inProgressUpgradeStrategies map[string]apiv1.UpgradeStrategy
	mutex                       sync.RWMutex
}

func newInProgressStrategyMgr(
	getRolloutStrategy func(context.Context, client.Object) *apiv1.UpgradeStrategy,
	setRolloutStrategy func(context.Context, client.Object, apiv1.UpgradeStrategy)) *inProgressStrategyMgr {

	return &inProgressStrategyMgr{
		getRolloutStrategy: getRolloutStrategy,
		setRolloutStrategy: setRolloutStrategy,
		store:              newInProgressStrategyStore(),
	}
}

func newInProgressStrategyStore() *inProgressStrategyStore {
	return &inProgressStrategyStore{
		inProgressUpgradeStrategies: map[string]apiv1.UpgradeStrategy{},
	}
}

func (mgr *inProgressStrategyMgr) getStrategy(ctx context.Context, rollout client.Object) apiv1.UpgradeStrategy {
	return mgr.synchronize(ctx, rollout)
}

// make sure in-memory value and Rollout Status value are synchronized
// if in-memory value is set, make sure Rollout Status value gets set the same
// if in-memory value isn't set, make sure in-memory value gets set to Rollout Status value
func (mgr *inProgressStrategyMgr) synchronize(ctx context.Context, rollout client.Object) apiv1.UpgradeStrategy {
	namespacedName := k8stypes.NamespacedName{Namespace: rollout.GetNamespace(), Name: rollout.GetName()}

	// first look for value in memory
	foundInMemory, inMemoryStrategy := mgr.store.getStrategy(namespacedName)

	// now look for the value in the Resource
	crDefinedStrategy := mgr.getRolloutStrategy(ctx, rollout)

	// if in-memory value is set, make sure Rollout Status value gets set the same
	if foundInMemory {
		mgr.setRolloutStrategy(ctx, rollout, inMemoryStrategy)
		return inMemoryStrategy
	} else {
		// make sure in-memory value gets set to Rollout Status value
		if crDefinedStrategy != nil {
			mgr.store.setStrategy(namespacedName, *crDefinedStrategy)
			return *crDefinedStrategy
		} else {
			return apiv1.UpgradeStrategyNoOp
		}
	}
}

// store in both memory and the Resource itself
func (mgr *inProgressStrategyMgr) setStrategy(ctx context.Context, rollout client.Object, upgradeStrategy apiv1.UpgradeStrategy) {
	namespacedName := k8stypes.NamespacedName{Namespace: rollout.GetNamespace(), Name: rollout.GetName()}

	mgr.store.setStrategy(namespacedName, upgradeStrategy)
	mgr.setRolloutStrategy(ctx, rollout, upgradeStrategy)
}

func (mgr *inProgressStrategyMgr) unsetStrategy(ctx context.Context, rollout client.Object) {
	mgr.setStrategy(ctx, rollout, apiv1.UpgradeStrategyNoOp)
}

// return whether found, and if so, the value
func (store *inProgressStrategyStore) getStrategy(namespacedName k8stypes.NamespacedName) (bool, apiv1.UpgradeStrategy) {
	key := namespacedNameToKey(namespacedName)
	store.mutex.RLock()
	strategy, found := store.inProgressUpgradeStrategies[key]
	store.mutex.RUnlock()
	return found, strategy
}

func (store *inProgressStrategyStore) setStrategy(namespacedName k8stypes.NamespacedName, upgradeStrategy apiv1.UpgradeStrategy) {
	key := namespacedNameToKey(namespacedName)
	store.mutex.Lock()
	store.inProgressUpgradeStrategies[key] = upgradeStrategy
	store.mutex.Unlock()
}
