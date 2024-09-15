package controller

import (
	"context"
	"sync"

	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// inProgressStrategyMgr is responsible to maintain a Rollout's inProgressStrategy
type inProgressStrategyMgr struct {
	getRolloutStrategy func(context.Context, client.Object) *apiv1.UpgradeStrategy
	setRolloutStrategy func(context.Context, client.Object, apiv1.UpgradeStrategy)
	store              *inProgressStrategyStore
}

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

// first look for value in memory
// if not found, look for value in the Resource itself (could happen if application just started up) and store it
// if not found, store and return UpgradeStrategyNoOp
func (mgr *inProgressStrategyMgr) getStrategy(ctx context.Context, rollout client.Object) apiv1.UpgradeStrategy {
	namespacedName := k8stypes.NamespacedName{Namespace: rollout.GetNamespace(), Name: rollout.GetName()}

	// first look for value in memory
	found, strategy := mgr.store.getStrategy(namespacedName)
	if found {
		return strategy
	}
	// not found in memory, so look for value in the Resource itself (could happen if application just started up) and store it
	crDefinedStrategy := mgr.getRolloutStrategy(ctx, rollout)
	if crDefinedStrategy == nil {
		// not defined in the Resource either
		mgr.store.setStrategy(namespacedName, apiv1.UpgradeStrategyNoOp)
		mgr.setRolloutStrategy(ctx, rollout, apiv1.UpgradeStrategyNoOp)

		return apiv1.UpgradeStrategyNoOp
	} else {
		// found in the Resource, so store that and return it
		mgr.store.setStrategy(namespacedName, *crDefinedStrategy)
		return *crDefinedStrategy
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
