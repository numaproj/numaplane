package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/numaproj/numaplane/internal/common"
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
		pauseModuleInstance = &PauseModule{
			controllerRequestedPause: make(map[string]*bool),
			isbSvcRequestedPause:     make(map[string]*bool),
		}
	})

	return pauseModuleInstance
}

type PauseModule struct {
	// map of namespace (where Numaflow Controller lives) to PauseRequest
	controllerRequestedPause     map[string]*bool
	controllerRequestedPauseLock sync.RWMutex
	// map of ISBService namespaced name to PauseRequest
	isbSvcRequestedPause     map[string]*bool
	isbSvcRequestedPauseLock sync.RWMutex
}

func (pm *PauseModule) newControllerPauseRequest(namespace string) {
	pm.controllerRequestedPauseLock.Lock()
	defer pm.controllerRequestedPauseLock.Unlock()
	_, alreadyThere := pm.controllerRequestedPause[namespace]
	if !alreadyThere {
		pause := false
		pm.controllerRequestedPause[namespace] = &pause
	}
}

func (pm *PauseModule) deleteControllerPauseRequest(namespace string) {
	pm.controllerRequestedPauseLock.Lock()
	defer pm.controllerRequestedPauseLock.Unlock()
	delete(pm.controllerRequestedPause, namespace)
}

func (pm *PauseModule) getControllerPauseRequest(namespace string) (*bool, bool) {
	pm.controllerRequestedPauseLock.RLock()
	defer pm.controllerRequestedPauseLock.RUnlock()
	entry, exists := pm.controllerRequestedPause[namespace]
	return entry, exists
}

// update and return whether the value changed
func (pm *PauseModule) updateControllerPauseRequest(namespace string, pause bool) bool {
	// first check to see if the same using read lock
	pm.controllerRequestedPauseLock.RLock()
	entry, _ := pm.controllerRequestedPause[namespace]
	if entry != nil && *entry == pause {
		// nothing to do
		pm.controllerRequestedPauseLock.RUnlock()
		return false
	}
	pm.controllerRequestedPauseLock.RUnlock()

	// if not the same, use write lock to modify
	pm.controllerRequestedPauseLock.Lock()
	defer pm.controllerRequestedPauseLock.Unlock()
	pm.controllerRequestedPause[namespace] = &pause
	return true
}

func (pm *PauseModule) getISBSvcPauseRequest(namespace string, isbsvcName string) (*bool, bool) {
	pm.isbSvcRequestedPauseLock.RLock()
	defer pm.isbSvcRequestedPauseLock.RUnlock()
	entry, exists := pm.isbSvcRequestedPause[namespaceName(namespace, isbsvcName)]
	return entry, exists
}

func (pm *PauseModule) pausePipeline(ctx context.Context, restConfig *rest.Config, pipeline *kubernetes.GenericObject) error {
	var existingPipelineSpec PipelineSpec
	if err := json.Unmarshal(pipeline.Spec.Raw, &existingPipelineSpec); err != nil {
		return err
	}

	return pm.updatePipelineLifecycle(ctx, restConfig, pipeline, "Paused")
}

// lock the maps while we change pipeline lifecycle so nobody changes their pause request
// while we run; otherwise, they may think they are pausing the pipeline while it's running
// for performance reasons, caller should check that it's safe to run first, since this operation locks the map
func (pm *PauseModule) runPipelineIfSafe(ctx context.Context, restConfig *rest.Config, pipeline *kubernetes.GenericObject) (bool, error) {
	pm.controllerRequestedPauseLock.RLock()
	defer pm.controllerRequestedPauseLock.RUnlock()
	pm.isbSvcRequestedPauseLock.RLock()
	defer pm.isbSvcRequestedPauseLock.RUnlock()

	// verify that all requests are still to pause, if not we can't run right now
	controllerPauseRequest := pm.controllerRequestedPause[pipeline.Namespace]
	var existingPipelineSpec PipelineSpec
	if err := json.Unmarshal(pipeline.Spec.Raw, &existingPipelineSpec); err != nil {
		return false, err
	}
	isbsvcPauseRequest := pm.isbSvcRequestedPause[getISBSvcName(existingPipelineSpec)]
	if (controllerPauseRequest != nil && *controllerPauseRequest) || (isbsvcPauseRequest != nil && *isbsvcPauseRequest) {
		// somebody is requesting to pause
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

	err = unstructured.SetNestedField(unstruc.Object, status, "spec", "lifecycle", "desiredPhase")
	if err != nil {
		return err
	}

	return kubernetes.UpdateUnstructuredCR(ctx, restConfig, unstruc, common.PipelineGVR, pipeline.Namespace, pipeline.Name)
}

func namespaceName(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}
