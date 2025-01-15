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

package ppnd

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
)

var (
	once                sync.Once
	pauseModuleInstance *PauseModule
)

func GetPauseModule() *PauseModule {
	once.Do(func() {
		pauseModuleInstance = &PauseModule{PauseRequests: make(map[string]*bool)}
	})

	return pauseModuleInstance
}

type PauseModule struct {
	lock sync.RWMutex
	// map of pause requester to Pause Request
	PauseRequests map[string]*bool // having *bool gives us 3 states: [true=pause-required, false=pause-not-required, nil=unknown]
}

func (pm *PauseModule) NewPauseRequest(requester string) {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	_, alreadyThere := pm.PauseRequests[requester]
	if !alreadyThere {
		pm.PauseRequests[requester] = nil
	}
}

func (pm *PauseModule) DeletePauseRequest(requester string) {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	delete(pm.PauseRequests, requester)
}

// update and return whether the value changed
func (pm *PauseModule) UpdatePauseRequest(requester string, pause bool) bool {
	// first check to see if the same using read lock
	pm.lock.RLock()
	entry := pm.PauseRequests[requester]
	if entry != nil && *entry == pause {
		// nothing to do
		pm.lock.RUnlock()
		return false
	}
	pm.lock.RUnlock()

	// if not the same, use write lock to modify
	pm.lock.Lock()
	defer pm.lock.Unlock()
	pm.PauseRequests[requester] = &pause
	return true
}

func (pm *PauseModule) GetPauseRequest(requester string) (*bool, bool) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	entry, exists := pm.PauseRequests[requester]
	return entry, exists
}

// pause pipeline
func (pm *PauseModule) PausePipeline(ctx context.Context, c client.Client, pipeline *unstructured.Unstructured) error {
	var existingPipelineSpec numaflowtypes.PipelineSpec
	if err := util.StructToStruct(pipeline.Object["spec"], &existingPipelineSpec); err != nil {
		return err
	}

	return pm.UpdatePipelineLifecycle(ctx, c, pipeline, "Paused")
}

// resume pipeline
// lock the maps while we change pipeline lifecycle so nobody changes their pause request
// while we run; otherwise, they may think they are pausing the pipeline while it's running
func (pm *PauseModule) RunPipeline(ctx context.Context, c client.Client, pipeline *unstructured.Unstructured, isbsvcName string, force bool) (bool, error) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()

	if !force {
		// verify that all requests are still to pause, if not we can't run right now
		controllerPauseRequest := pm.PauseRequests[pm.GetNumaflowControllerKey(pipeline.GetNamespace())]
		var existingPipelineSpec numaflowtypes.PipelineSpec
		if err := util.StructToStruct(pipeline.Object["spec"], &existingPipelineSpec); err != nil {
			return false, err
		}

		isbsvcPauseRequest := pm.PauseRequests[pm.GetISBServiceKey(pipeline.GetNamespace(), isbsvcName)]
		if (controllerPauseRequest != nil && *controllerPauseRequest) || (isbsvcPauseRequest != nil && *isbsvcPauseRequest) {
			// somebody is requesting to pause - can't run
			return false, nil
		}
	}

	err := pm.UpdatePipelineLifecycle(ctx, c, pipeline, "Running")
	if err != nil {
		return false, err
	}
	return true, nil
}

func (pm *PauseModule) UpdatePipelineLifecycle(ctx context.Context, c client.Client, pipeline *unstructured.Unstructured, phase string) error {
	patchJson := fmt.Sprintf(`{"spec": {"lifecycle": {"desiredPhase": "%s"}}}`, phase)
	return kubernetes.PatchResource(ctx, c, pipeline, patchJson, k8stypes.MergePatchType)

}

func (pm *PauseModule) GetNumaflowControllerKey(namespace string) string {
	return fmt.Sprintf("NC:%s", namespace)
}

func (pm *PauseModule) GetISBServiceKey(namespace string, name string) string {
	return fmt.Sprintf("I:%s/%s", namespace, name)
}
