package controller

import (
	"sync"
)

var (
	once                sync.Once
	pauseModuleInstance *PauseModule
)

func GetPauseModule() *PauseModule {
	once.Do(func() {
		pauseModuleInstance = &PauseModule{
			controllerRequestedPause: make(map[string]*PipelinePauseRequest),
			isbSvcRequestedPause:     make(map[string]*PipelinePauseRequest),
		}
	})

	return pauseModuleInstance
}

type PauseModule struct {
	// map of namespace (where Numaflow Controller lives) to PauseRequest
	controllerRequestedPause     map[string]*PipelinePauseRequest
	controllerRequestedPauseLock sync.RWMutex
	// map of ISBService namespaced name to PauseRequest
	isbSvcRequestedPause     map[string]*PipelinePauseRequest
	isbSvcRequestedPauseLock sync.RWMutex
}

type PipelinePauseRequest struct {
	pause *bool
	mutex sync.RWMutex
}

func (pm *PauseModule) NewControllerPauseRequest(namespace string) {
	pm.controllerRequestedPauseLock.Lock()
	defer pm.controllerRequestedPauseLock.Unlock()
	_, alreadyThere := pm.controllerRequestedPause[namespace]
	if !alreadyThere {
		pause := false
		pm.controllerRequestedPause[namespace] = &PipelinePauseRequest{
			pause: &pause,
		}
	}
}

func (pm *PauseModule) DeleteControllerPauseRequest(namespace string) {
	pm.controllerRequestedPauseLock.Lock()
	defer pm.controllerRequestedPauseLock.Unlock()
	delete(pm.controllerRequestedPause, namespace)
}

func (pm *PauseModule) GetControllerPauseRequest(namespace string) (*bool, bool) {
	pm.controllerRequestedPauseLock.RLock()
	defer pm.controllerRequestedPauseLock.RUnlock()
	entry, exists := pm.controllerRequestedPause[namespace]
	return entry.pause, exists
}
