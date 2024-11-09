package ppnd

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
)

var (
	once                sync.Once
	pauseModuleInstance *PauseModule
)

func GetPauseModule() *PauseModule {
	once.Do(func() {
		pauseModuleInstance = &PauseModule{pauseRequests: make(map[string]*bool)}
	})

	return pauseModuleInstance
}

type PauseModule struct {
	lock sync.RWMutex
	// map of pause requester to Pause Request
	pauseRequests map[string]*bool // having *bool gives us 3 states: [true=pause-required, false=pause-not-required, nil=unknown]
}

func (pm *PauseModule) NewPauseRequest(requester string) {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	_, alreadyThere := pm.pauseRequests[requester]
	if !alreadyThere {
		pm.pauseRequests[requester] = nil
	}
}

func (pm *PauseModule) DeletePauseRequest(requester string) {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	delete(pm.pauseRequests, requester)
}

// update and return whether the value changed
func (pm *PauseModule) UpdatePauseRequest(requester string, pause bool) bool {
	// first check to see if the same using read lock
	pm.lock.RLock()
	entry := pm.pauseRequests[requester]
	if entry != nil && *entry == pause {
		// nothing to do
		pm.lock.RUnlock()
		return false
	}
	pm.lock.RUnlock()

	// if not the same, use write lock to modify
	pm.lock.Lock()
	defer pm.lock.Unlock()
	pm.pauseRequests[requester] = &pause
	return true
}

func (pm *PauseModule) GetPauseRequest(requester string) (*bool, bool) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	entry, exists := pm.pauseRequests[requester]
	return entry, exists
}

// pause pipeline
func (pm *PauseModule) PausePipeline(ctx context.Context, c client.Client, pipeline *kubernetes.GenericObject) error {
	var existingPipelineSpec ctlrcommon.PipelineSpec
	if err := json.Unmarshal(pipeline.Spec.Raw, &existingPipelineSpec); err != nil {
		return err
	}

	return pm.UpdatePipelineLifecycle(ctx, c, pipeline, "Paused")
}

// resume pipeline
// lock the maps while we change pipeline lifecycle so nobody changes their pause request
// while we run; otherwise, they may think they are pausing the pipeline while it's running
func (pm *PauseModule) RunPipelineIfSafe(ctx context.Context, c client.Client, pipeline *kubernetes.GenericObject) (bool, error) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()

	// verify that all requests are still to pause, if not we can't run right now
	controllerPauseRequest := pm.pauseRequests[pm.GetNumaflowControllerKey(pipeline.Namespace)]
	var existingPipelineSpec ctlrcommon.PipelineSpec
	if err := json.Unmarshal(pipeline.Spec.Raw, &existingPipelineSpec); err != nil {
		return false, err
	}
	isbsvcName := existingPipelineSpec.GetISBSvcName()
	isbsvcPauseRequest := pm.pauseRequests[pm.GetISBServiceKey(pipeline.Namespace, isbsvcName)]
	if (controllerPauseRequest != nil && *controllerPauseRequest) || (isbsvcPauseRequest != nil && *isbsvcPauseRequest) {
		// somebody is requesting to pause - can't run
		return false, nil
	}

	err := pm.UpdatePipelineLifecycle(ctx, c, pipeline, "Running")
	if err != nil {
		return false, err
	}
	return true, nil
}

func (pm *PauseModule) UpdatePipelineLifecycle(ctx context.Context, c client.Client, pipeline *kubernetes.GenericObject, phase string) error {

	patchJson := fmt.Sprintf(`{"spec": {"lifecycle": {"desiredPhase": "%s"}}}`, phase)
	return kubernetes.PatchResource(ctx, c, pipeline, patchJson, k8stypes.MergePatchType)

}

func (pm *PauseModule) GetNumaflowControllerKey(namespace string) string {
	return fmt.Sprintf("NC:%s", namespace)
}

func (pm *PauseModule) GetISBServiceKey(namespace string, name string) string {
	return fmt.Sprintf("I:%s/%s", namespace, name)
}
