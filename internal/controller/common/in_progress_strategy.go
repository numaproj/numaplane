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

package common

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// InProgressStrategyMgr is responsible to maintain a Rollout's inProgressStrategy
// state is maintained both in memory as well as in the Rollout's Status
// in memory always gives us the latest state in case the Informer cache is out of date
// the Rollout's Status is useful as a backup mechanism in case Numaplane has just restarted
type InProgressStrategyMgr struct {
	getRolloutStrategy func(context.Context, client.Object) *apiv1.UpgradeStrategy
	setRolloutStrategy func(context.Context, client.Object, apiv1.UpgradeStrategy)
	Store              *inProgressStrategyStore
}

// in memory storage of UpgradeStrategy in progress for a given Rollout
type inProgressStrategyStore struct {
	inProgressUpgradeStrategies map[string]apiv1.UpgradeStrategy
	mutex                       sync.RWMutex
}

func NewInProgressStrategyMgr(
	getRolloutStrategy func(context.Context, client.Object) *apiv1.UpgradeStrategy,
	setRolloutStrategy func(context.Context, client.Object, apiv1.UpgradeStrategy)) *InProgressStrategyMgr {

	return &InProgressStrategyMgr{
		getRolloutStrategy: getRolloutStrategy,
		setRolloutStrategy: setRolloutStrategy,
		Store:              newInProgressStrategyStore(),
	}
}

func newInProgressStrategyStore() *inProgressStrategyStore {
	return &inProgressStrategyStore{
		inProgressUpgradeStrategies: map[string]apiv1.UpgradeStrategy{},
	}
}

func (mgr *InProgressStrategyMgr) GetStrategy(ctx context.Context, rollout client.Object) apiv1.UpgradeStrategy {
	return mgr.synchronize(ctx, rollout)
}

// make sure in-memory value and Rollout Status value are synchronized
// if in-memory value is set, make sure Rollout Status value gets set the same
// if in-memory value isn't set, make sure in-memory value gets set to Rollout Status value
func (mgr *InProgressStrategyMgr) synchronize(ctx context.Context, rollout client.Object) apiv1.UpgradeStrategy {
	namespacedName := k8stypes.NamespacedName{Namespace: rollout.GetNamespace(), Name: rollout.GetName()}

	// first look for value in memory
	foundInMemory, inMemoryStrategy := mgr.Store.GetStrategy(namespacedName)

	// now look for the value in the Resource
	crDefinedStrategy := mgr.getRolloutStrategy(ctx, rollout)

	// if in-memory value is set, make sure Rollout Status value gets set the same
	if foundInMemory {
		mgr.setRolloutStrategy(ctx, rollout, inMemoryStrategy)
		return inMemoryStrategy
	} else {
		// make sure in-memory value gets set to Rollout Status value
		if crDefinedStrategy != nil {
			mgr.Store.SetStrategy(namespacedName, *crDefinedStrategy)
			return *crDefinedStrategy
		} else {
			return apiv1.UpgradeStrategyNoOp
		}
	}
}

// store in both memory and the Resource itself
func (mgr *InProgressStrategyMgr) SetStrategy(ctx context.Context, rollout client.Object, upgradeStrategy apiv1.UpgradeStrategy) {
	namespacedName := k8stypes.NamespacedName{Namespace: rollout.GetNamespace(), Name: rollout.GetName()}

	mgr.Store.SetStrategy(namespacedName, upgradeStrategy)
	mgr.setRolloutStrategy(ctx, rollout, upgradeStrategy)
}

func (mgr *InProgressStrategyMgr) UnsetStrategy(ctx context.Context, rollout client.Object) {
	mgr.SetStrategy(ctx, rollout, apiv1.UpgradeStrategyNoOp)
}

// return whether found, and if so, the value
func (store *inProgressStrategyStore) GetStrategy(namespacedName k8stypes.NamespacedName) (bool, apiv1.UpgradeStrategy) {
	key := namespacedNameToKey(namespacedName)
	store.mutex.RLock()
	strategy, found := store.inProgressUpgradeStrategies[key]
	store.mutex.RUnlock()
	return found, strategy
}

func (store *inProgressStrategyStore) SetStrategy(namespacedName k8stypes.NamespacedName, upgradeStrategy apiv1.UpgradeStrategy) {
	key := namespacedNameToKey(namespacedName)
	store.mutex.Lock()
	store.inProgressUpgradeStrategies[key] = upgradeStrategy
	store.mutex.Unlock()
}

// ExtractOriginalScaleMinMax returns a JSON string of the scale definition
// including only min and max fields extracted from the given unstructured object.
func ExtractOriginalScaleMinMax(object map[string]any, pathToScale []string) (*string, error) {
	originalScaleDef, foundScale, err := unstructured.NestedMap(object, pathToScale...)
	if err != nil {
		return nil, err
	}

	if !foundScale {
		return nil, nil
	}

	originalScaleMinMaxOnly := map[string]any{
		"min": originalScaleDef["min"],
		"max": originalScaleDef["max"],
	}

	jsonBytes, err := json.Marshal(originalScaleMinMaxOnly)
	if err != nil {
		return nil, err
	}

	jsonString := string(jsonBytes)
	return &jsonString, nil
}

func namespacedNameToKey(namespacedName k8stypes.NamespacedName) string {
	return fmt.Sprintf("%s/%s", namespacedName.Namespace, namespacedName.Name)
}
