package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
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

func (pm *PauseModule) newPauseRequest(requester string) {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	_, alreadyThere := pm.pauseRequests[requester]
	if !alreadyThere {
		pm.pauseRequests[requester] = nil
	}
}

func (pm *PauseModule) deletePauseRequest(requester string) {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	delete(pm.pauseRequests, requester)
}

// update and return whether the value changed
func (pm *PauseModule) updatePauseRequest(requester string, pause bool) bool {
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

func (pm *PauseModule) getPauseRequest(requester string) (*bool, bool) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	entry, exists := pm.pauseRequests[requester]
	return entry, exists
}

// pause pipeline
func (pm *PauseModule) pausePipeline(ctx context.Context, restConfig *rest.Config, pipeline *kubernetes.GenericObject) error {
	var existingPipelineSpec PipelineSpec
	if err := json.Unmarshal(pipeline.Spec.Raw, &existingPipelineSpec); err != nil {
		return err
	}

	return pm.updatePipelineLifecycle(ctx, restConfig, pipeline, "Paused")
}

// resume pipeline
// lock the maps while we change pipeline lifecycle so nobody changes their pause request
// while we run; otherwise, they may think they are pausing the pipeline while it's running
func (pm *PauseModule) runPipelineIfSafe(ctx context.Context, restConfig *rest.Config, pipeline *kubernetes.GenericObject) (bool, error) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()

	// verify that all requests are still to pause, if not we can't run right now
	controllerPauseRequest := pm.pauseRequests[pm.getNumaflowControllerKey(pipeline.Namespace)]
	var existingPipelineSpec PipelineSpec
	if err := json.Unmarshal(pipeline.Spec.Raw, &existingPipelineSpec); err != nil {
		return false, err
	}
	isbsvcName := getISBSvcName(existingPipelineSpec)
	isbsvcPauseRequest := pm.pauseRequests[pm.getISBServiceKey(pipeline.Namespace, isbsvcName)]
	if (controllerPauseRequest != nil && *controllerPauseRequest) || (isbsvcPauseRequest != nil && *isbsvcPauseRequest) {
		// somebody is requesting to pause - can't run
		return false, nil
	}

	err := pm.updatePipelineLifecycle(ctx, restConfig, pipeline, "Running")
	if err != nil {
		return false, err
	}
	return true, nil
}

func (pm *PauseModule) updatePipelineLifecycle(ctx context.Context, restConfig *rest.Config, pipeline *kubernetes.GenericObject, status string) error {
	unstruc, err := kubernetes.ObjectToUnstructured(pipeline)
	if err != nil {
		return err
	}

	// TODO: I noticed if any of these fields are nil, this function errors out - but can't remember why they'd be nil
	err = unstructured.SetNestedField(unstruc.Object, status, "spec", "lifecycle", "desiredPhase")
	if err != nil {
		return err
	}

	resultObj, err := kubernetes.UnstructuredToObject(unstruc)
	if err != nil {
		return err
	}
	if resultObj == nil {
		return fmt.Errorf("error converting unstructured %+v to object, result is nil?", unstruc.Object)
	}

	err = kubernetes.UpdateCR(ctx, restConfig, resultObj, "pipelines")
	if err != nil {
		return err
	}
	*pipeline = *resultObj
	return nil
	//return kubernetes.UpdateUnstructuredCR(ctx, restConfig, unstruc, common.PipelineGVR, pipeline.Namespace, pipeline.Name)*/

}

func (pm *PauseModule) getNumaflowControllerKey(namespace string) string {
	return fmt.Sprintf("NC:%s", namespace)
}

func (pm *PauseModule) getISBServiceKey(namespace string, name string) string {
	return fmt.Sprintf("I:%s/%s", namespace, name)
}
