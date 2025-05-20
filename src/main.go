package main

import (
	"course/bridge"
	"course/config"
	"course/labrpc"
	"course/net"
	"course/raft"
	"course/server"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

var globalNetwork = labrpc.MakeNetwork()

type Node struct {
	kvServer *server.KVServer
	port     int
}

func startNode(id, port int, cluster []string) (*Node, error) {
	peers := make([]*labrpc.ClientEnd, len(cluster))
	for _, nodeStr := range cluster {
		nodeID, _ := strconv.Atoi(nodeStr)
		if nodeID == id {
			continue // 跳过自身节点
		}
		endName := fmt.Sprintf("node-%d-to-%d", id, nodeID)
		peers[nodeID] = globalNetwork.MakeEnd(endName)

		// 确保Connect和AddServer使用相同的servername
		globalNetwork.Connect(endName, nodeID)
		globalNetwork.Enable(endName, true)
	}

	// 后续逻辑保持不变
	persister := raft.MakePersister()
	kv := server.StartKVServer(peers, id, persister, -1)

	svc := labrpc.MakeServer()
	svc.AddService(labrpc.MakeService(kv.GetRaft()))
	globalNetwork.AddServer(id, svc)

	go net.StartTCPServer(kv, port)

	return &Node{
		kvServer: kv,
		port:     port,
	}, nil
}

func main() {
	// 初始化存储
	bridge.InitStorage()
	path := flag.String("path", "config/config.json", "config path")
	flag.Parse()

	configs, err := config.ParseEndpointsConfig(*path)
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
	//bridge.DestoryStorage() // 释放内存
	fmt.Println("\nShutting down all nodes...")
	for _, node := range nodes {
		node.kvServer.Kill()
	}
	time.Sleep(1 * time.Second)
}
