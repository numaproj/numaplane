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
	// map of namespace (where Numaflow Controller lives) to Pause Request
	controllerRequestedPause     map[string]*bool // having *bool gives us 3 states: [true=pause-required, false=pause-not-required, nil=unknown]
	controllerRequestedPauseLock sync.RWMutex
	// map of ISBService namespaced name to Pause Request
	isbSvcRequestedPause     map[string]*bool // having *bool gives us 3 states: [true=pause-required, false=pause-not-required, nil=unknown]
	isbSvcRequestedPauseLock sync.RWMutex
}

func (pm *PauseModule) newControllerPauseRequest(namespace string) {
	pm.controllerRequestedPauseLock.Lock()
	defer pm.controllerRequestedPauseLock.Unlock()
	_, alreadyThere := pm.controllerRequestedPause[namespace]
	if !alreadyThere {
		pm.controllerRequestedPause[namespace] = nil
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
	entry := pm.controllerRequestedPause[namespace]
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

func (pm *PauseModule) newISBServicePauseRequest(namespace string, isbsvcName string) {
	namespacedName := namespaceName(namespace, isbsvcName)
	pm.isbSvcRequestedPauseLock.Lock()
	defer pm.isbSvcRequestedPauseLock.Unlock()
	_, alreadyThere := pm.isbSvcRequestedPause[namespacedName]
	if !alreadyThere {
		pm.isbSvcRequestedPause[namespacedName] = nil
	}
}

func (pm *PauseModule) deleteISBServicePauseRequest(namespace string, isbsvcName string) {
	namespacedName := namespaceName(namespace, isbsvcName)
	pm.isbSvcRequestedPauseLock.Lock()
	defer pm.isbSvcRequestedPauseLock.Unlock()
	delete(pm.isbSvcRequestedPause, namespacedName)
}

func (pm *PauseModule) getISBSvcPauseRequest(namespace string, isbsvcName string) (*bool, bool) {
	pm.isbSvcRequestedPauseLock.RLock()
	defer pm.isbSvcRequestedPauseLock.RUnlock()
	entry, exists := pm.isbSvcRequestedPause[namespaceName(namespace, isbsvcName)]
	return entry, exists
}

// update and return whether the value changed
func (pm *PauseModule) updateISBServicePauseRequest(namespace string, isbsvcName string, pause bool) bool {
	namespacedName := namespaceName(namespace, isbsvcName)
	// first check to see if the same using read lock
	pm.isbSvcRequestedPauseLock.RLock()
	entry := pm.isbSvcRequestedPause[namespacedName]
	if entry != nil && *entry == pause {
		// nothing to do
		pm.isbSvcRequestedPauseLock.RUnlock()
		return false
	}
	pm.isbSvcRequestedPauseLock.RUnlock()

	// if not the same, use write lock to modify
	pm.isbSvcRequestedPauseLock.Lock()
	defer pm.isbSvcRequestedPauseLock.Unlock()
	pm.isbSvcRequestedPause[namespacedName] = &pause
	return true

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

	err = unstructured.SetNestedField(unstruc.Object, status, "spec", "lifecycle", "desiredPhase")
	if err != nil {
		return err
	}

	return kubernetes.UpdateUnstructuredCR(ctx, restConfig, unstruc, common.PipelineGVR, pipeline.Namespace, pipeline.Name)
}

func namespaceName(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}
