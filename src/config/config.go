package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// 节点配置结构体
type NodeConfig struct {
	ID         int    `json:"id"`
	RPCAddr    string `json:"rpc_addr"`    // gRPC通信地址
	ClientAddr string `json:"client_addr"` // 客户端TCP连接地址
}

// 批量提交配置结构体
type BatchConfig struct {
	BatchSize    int           `json:"batch_size"`    // 批量提交大小
	BatchTimeout time.Duration `json:"batch_timeout"` // 批量提交超时时间
}

// 全局配置结构体
type GlobalConfig struct {
	Nodes []NodeConfig `json:"nodes"`
	Batch BatchConfig  `json:"batch"`
}

// 加载配置文件
func LoadConfig(path string) (*GlobalConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var config GlobalConfig
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

// 获取当前节点配置
func (gc *GlobalConfig) GetNodeConfig(nodeID int) (*NodeConfig, error) {
	for _, node := range gc.Nodes {
		if node.ID == nodeID {
			return &node, nil
		}
	}
	return nil, fmt.Errorf("node %d not found in configuration", nodeID)
}
