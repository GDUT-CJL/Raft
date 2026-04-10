package main

// CGO_LDFLAGS="-lrocksdb" GOOS=linux GOARCH=amd64 go build -o kvstore main.go
import (
	"flag"
	"fmt"

	"course/bridge"
	"course/config"
	"course/mnet"
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
func startNode(cfg config.NodeConfig, clusterConfigs []config.NodeConfig, nodeId int, batchConfig config.BatchConfig) (*Node, error) {
	// 准备RPC地址列表
	rpcAddrs := make([]string, len(clusterConfigs))
	for i, nodeCfg := range clusterConfigs {
		rpcAddrs[i] = nodeCfg.RPCAddr
	}

	var kv *server.KVServer
	if cfg.ID == nodeId {
		fmt.Printf("nodeId: %d, cfg.ID = %d\n", nodeId, cfg.ID)

		// 启动rpc服务
		kv = server.StartKVServer(rpcAddrs, cfg.ID, -1)

		// 初始化批量管理器
		bm := mnet.NewBatchManager(
			kv,
			kv.GetRaft(),
			batchConfig.BatchSize,
			batchConfig.BatchTimeout,
		)
		mnet.SetBatchManager(bm)
		fmt.Printf("Batch Manager initialized - Size: %d, Timeout: %v\n",
			batchConfig.BatchSize, batchConfig.BatchTimeout)
		// 启动客户端监听服务
		go mnet.StartTCPServer(kv, cfg.ClientAddr, cfg.ID)
	}

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
	globalConfig, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	// 初始化存储
	bridge.InitStorage()

	// 创建节点
	nodes := make([]*Node, len(globalConfig.Nodes))
	for i, cfg := range globalConfig.Nodes {
		node, err := startNode(cfg, globalConfig.Nodes, *configId, globalConfig.Batch)
		if err != nil {
			fmt.Printf("Failed to start node %d: %v\n", cfg.ID, err)
			os.Exit(1)
		}
		nodes[i] = node
		fmt.Printf("Node %d started: RPC=%s, Client=%s\n",
			cfg.ID, cfg.RPCAddr, cfg.ClientAddr)
	}

	// 启动监控
	if nodes[0] != nil && nodes[0].kvServer != nil {
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()

			for range ticker.C {
				bm := mnet.GetBatchManager()
				if bm != nil {
					batchSize, avgLatency, throughput := bm.GetStats()
					fmt.Printf("[Stats] BatchSize=%d, AvgLatency=%dms, Throughput=%d ops\n",
						batchSize, avgLatency, throughput)
				}
			}
		}()
	}
	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// 优雅关闭
	fmt.Println("\nShutting down cluster...")

	// 停止BatchManager
	bm := mnet.GetBatchManager()
	if bm != nil {
		bm.Stop()
	}
	time.Sleep(1 * time.Second)
}
