package logger

import (
	"sync"

	"github.com/numaproj/numaplane/internal/controller/config"
)

// Singleton Base Logger from which other loggers can be derived
// maintains current log level for the application as a whole

var (
	baseLogger      *NumaLogger = New()
	baseLoggerMutex sync.RWMutex
)

// SetBaseLogger is intended to be set once when application starts
func SetBaseLogger(nl *NumaLogger) {
	baseLoggerMutex.Lock()
	defer baseLoggerMutex.Unlock()

	baseLogger = nl.DeepCopy()
	// if the Global ConfigMap changes, update the BaseLogger's log level
	config.GetConfigManagerInstance().RegisterCallback(refreshBaseLoggerLevel)
}

// Get the standard NumaLogger with current Log Level - deep copy it in case user modifies it
func GetBaseLogger() *NumaLogger {
	baseLoggerMutex.RLock()
	defer baseLoggerMutex.RUnlock()
	return baseLogger.DeepCopy()
}

// Refresh the Logger's LogLevel based on current config value
func refreshBaseLoggerLevel(newConfig config.GlobalConfig) {

	// if it changed, propagate it to our Base Logger
	if newConfig.LogLevel != baseLogger.LogLevel {

		baseLoggerMutex.Lock()
		defer baseLoggerMutex.Unlock()

		// update the logger with the new log level
		baseLogger.SetLevel(newConfig.LogLevel)
		baseLogger.Infof("log level=%d\n", newConfig.LogLevel)
	}
}
