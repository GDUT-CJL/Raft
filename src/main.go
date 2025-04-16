package main
// # 终端1
// go run main.go -id=1 -port=8000 -cluster="1,2,3"

// # 终端2 
// go run main.go -id=2 -port=8001 -cluster="1,2,3"

// # 终端3
// go run main.go -id=3 -port=8002 -cluster="1,2,3"
import (
	"course/bridge"
	"course/labrpc"
	"course/net"
	"course/server"
	"course/raft"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
	"sort"
)

func main() {
    // 1. 解析命令行参数（使用0-based ID）
    id := flag.Int("id", 0, "节点ID (0~N-1)")
    port := flag.Int("port", 8000, "TCP服务端口")
    cluster := flag.String("cluster", "0,1,2", "集群节点ID列表（逗号分隔，0-based）")
    flag.Parse()

    // 2. 初始化存储引擎
    bridge.InitStorage()

    // 3. 创建RPC网络
    network := labrpc.MakeNetwork()
    
    // 解析集群节点ID
    clusterNodes := strings.Split(*cluster, ",")
    peers := make([]*labrpc.ClientEnd, len(clusterNodes))
    
    // 4. 建立连接（确保peers数组索引与节点ID对应）
    nodeIDs := make([]int, len(clusterNodes))
    for i, nodeIDStr := range clusterNodes {
        nodeID, _ := strconv.Atoi(nodeIDStr)
        nodeIDs[i] = nodeID
    }

    // 按节点ID排序，确保peers数组索引与ID一致
    sort.Ints(nodeIDs)
    for i, nodeID := range nodeIDs {
        endName := fmt.Sprintf("node-%d-to-%d", *id, nodeID)
        peers[i] = network.MakeEnd(endName)
        network.Connect(endName, nodeID)
        network.Enable(endName, true)
        fmt.Printf("节点 %d 连接到节点 %d\n", *id, nodeID)
    }

    // 5. 创建KV服务器（传入原始ID）
    persister := raft.MakePersister()
    kv := server.StartKVServer(peers, *id, persister, 1000)
    rf := kv.GetRaft()

    // 6. 注册Raft服务
    ser := labrpc.MakeServer()
    ser.AddService(labrpc.MakeService(rf))
    network.AddServer(*id, ser)
    fmt.Printf("节点 %d 服务注册完成\n", *id)

    // 7. 启动TCP服务
    go net.StartTCPServer(kv, *port)

    // 8. 处理信号
    signalChan := make(chan os.Signal, 1)
    signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
    fmt.Printf("节点 %d 启动完成，监听端口 %d\n", *id, *port)
    <-signalChan

    // 9. 清理
    fmt.Println("正在关闭节点...")
    kv.Kill()
    network.Cleanup()
    time.Sleep(1 * time.Second)
}