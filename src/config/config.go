package config

import (
	"encoding/json"
	//"fmt"
	"os"
)

// 节点配置结构体
type NodeConfig struct {
	ID         int    `json:"id"`
	RPCAddr    string `json:"rpc_addr"`    // gRPC通信地址
	ClientAddr string `json:"client_addr"` // 客户端TCP连接地址
}

// 加载配置文件
func LoadConfig(path string) ([]NodeConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var configs []NodeConfig
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&configs); err != nil {
		return nil, err
	}

	return configs, nil
}
