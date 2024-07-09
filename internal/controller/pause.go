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
