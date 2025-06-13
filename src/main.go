package main

// GOOS=linux GOARCH=amd64 go build -o kvstore main.go
import (
	"flag"
	"fmt"

	"course/bridge"
	"course/config"
	"course/mnet"
	"course/raft"
	"course/server"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// 节点结构体
type Node struct {
	kvServer *server.KVServer
	config   config.NodeConfig
}

// 启动单个节点
func startNode(cfg config.NodeConfig, clusterConfigs []config.NodeConfig, nodeId int) (*Node, error) {
	// 准备RPC地址列表
	rpcAddrs := make([]string, len(clusterConfigs))
	for i, nodeCfg := range clusterConfigs {
		rpcAddrs[i] = nodeCfg.RPCAddr
	}

	// 创建持久化存储
	persister := raft.MakePersister()
	// 创建KV服务器
	var kv *server.KVServer
	if cfg.ID == nodeId {
		fmt.Printf("nodeId :%d,cfg.ID = %d\n", nodeId, cfg.ID)
		kv = server.StartKVServer(rpcAddrs, cfg.ID, persister, -1)

		go mnet.StartTCPServer(kv, cfg.ClientAddr, cfg.ID)
	}
	// 启动TCP服务器处理客户端请求
	return &Node{
		kvServer: kv,
		config:   cfg,
	}, nil
}

func main() {
	// 解析命令行参数
	bridge.InitMemPool()
	configPath := flag.String("config", "config/config.json", "Path to cluster configuration file")
	configId := flag.Int("id", 0, "node id")
	flag.Parse()

	// 读取配置文件
	configs, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	// 初始化存储
	bridge.InitStorage()

	// 创建节点
	nodes := make([]*Node, len(configs))
	for i, cfg := range configs {
		node, err := startNode(cfg, configs, *configId)
		if err != nil {
			fmt.Printf("Failed to start node %d: %v\n", cfg.ID, err)
			os.Exit(1)
		}
		nodes[i] = node
		fmt.Printf("Node %d started: RPC=%s, Client=%s\n",
			cfg.ID, cfg.RPCAddr, cfg.ClientAddr)
	}

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// 优雅关闭
	fmt.Println("\nShutting down cluster...")

	time.Sleep(1 * time.Second)
}
