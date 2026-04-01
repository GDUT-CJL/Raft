package config

import "sync"

var (
	useOptimizedVersion bool
	configMutex         sync.RWMutex
)

func SetUseOptimizedVersion(use bool) {
	configMutex.Lock()
	defer configMutex.Unlock()
	useOptimizedVersion = use
}

func IsUseOptimizedVersion() bool {
	configMutex.RLock()
	defer configMutex.RUnlock()
	return useOptimizedVersion
}
