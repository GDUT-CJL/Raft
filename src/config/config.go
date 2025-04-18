package config

import (
	"os"
	"encoding/json"
	"fmt"
)

type NodeConfig  struct {
	ID   int `json:"id"`
	Port int `json:"port"`
}

func ParseEndpointsConfig(path string) ([]NodeConfig,error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var configs []NodeConfig
	if err := json.Unmarshal(data, &configs); err != nil {
		return nil, fmt.Errorf("failed to parse config: %v", err)
	}

	return configs, nil
}