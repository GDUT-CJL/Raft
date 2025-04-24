package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	"course/bridge"
	"course/labrpc"
	"course/net"
	"course/raft"
	"course/server"
	"course/config"
	"flag"
)

var globalNetwork = labrpc.MakeNetwork()

type Node struct {
	kvServer *server.KVServer
	port     int
}

func startNode(id, port int, cluster []string) (*Node, error) {
	fmt.Printf("[Node %d] Starting on port %d\n", id, port)

	// 1. 创建RPC端点
	peers := make([]*labrpc.ClientEnd, len(cluster))
	for _, nodeStr := range cluster {
		nodeID, _ := strconv.Atoi(nodeStr)
		endName := fmt.Sprintf("node-%d-to-%d", id, nodeID)
		peers[nodeID] = globalNetwork.MakeEnd(endName)
		
		// 关键修复：Connect和AddServer使用相同的servername
		globalNetwork.Connect(endName, nodeID) // nodeID作为servername
		globalNetwork.Enable(endName, true)
	}

	// 2. 创建KV和Raft实例
	persister := raft.MakePersister()
	kv := server.StartKVServer(peers, id, persister, -1) // -1代表暂时不需要进行snapshot

	// 3. 注册Raft RPC服务
	svc := labrpc.MakeServer()
	svc.AddService(labrpc.MakeService(kv.GetRaft()))
	globalNetwork.AddServer(id, svc) // 使用nodeID作为servername

	// 4. 启动TCP服务
	go net.StartTCPServer(kv, port)

	return &Node{
		kvServer: kv,
		port:     port,
	}, nil
}

func main() {
	// 初始化存储
	bridge.InitStorage()
	path := flag.String("path","config/config.json","config path")
	flag.Parse()

	configs,err := config.ParseEndpointsConfig(*path)
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		os.Exit(1)
	}
	cluster := make([]string, len(configs))
	for i, cfg := range configs {
		cluster[i] = strconv.Itoa(cfg.ID)
	}

	nodes := make([]*Node, len(configs))
	for i, cfg := range configs {
		node, err := startNode(cfg.ID, cfg.Port, cluster)
		if err != nil {
			fmt.Printf("Failed to start node %d: %v\n", cfg.ID, err)
			os.Exit(1)
		}
		nodes[i] = node
	}
	// 测试：等待10秒后让Leader节点宕机
	// go func() {
	// 	time.Sleep(20 * time.Second)
	// 	for _, node := range nodes {
	// 		_,isleader := node.kvServer.GetRaft().GetState()
	// 		if isleader{
	// 			fmt.Printf("\n[TEST] Killing Leader (Node on port %d)...\n", node.port)
	// 			node.kvServer.Kill() // 关闭Leader节点
	// 			break
	// 		}
	// 	}
	// }()	

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// 优雅关闭
	fmt.Println("\nShutting down all nodes...")
	for _, node := range nodes {
		node.kvServer.Kill()
	}
	time.Sleep(1 * time.Second)
}