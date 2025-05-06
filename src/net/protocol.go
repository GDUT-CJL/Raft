package net
import (  
	"fmt"  
	"net"  
	"os"  
	"strings"
	"time"  
	"course/bridge"
	"course/server"
	// "strconv"
	"course/raft"
	"sync/atomic"
	"hash/fnv"
	"sync"
	"bufio"
)  
var (
	batchBuffer []server.Op  // 每个连接独立的缓冲区
    batchMutex 	sync.Mutex  // 每个连接独立的锁
    batchSize   = 1 // 批量大小
    batchTimer  = 50 * time.Millisecond
)

// generateClientID generates a unique client ID for each connection  
func generateClientID(conn net.Conn) int64 {  
    // 结合节点IP和原子计数器
    addr := conn.RemoteAddr().(*net.TCPAddr).IP.String()
    hash := fnv.New64a()
    hash.Write([]byte(addr))
    return int64(hash.Sum64()) ^ (atomic.AddInt64(&clientIDCounter, 1) << 32)
}

func sendBatch(kv *server.KVServer, conn net.Conn, batch []server.Op,rf *raft.Raft) {
	// 不能直接类型转换：[]server.Op → []interface{}
	// 下面这段代码将先获取所有的op操作在组装成[]interface{}再进行传参。
	var batchInterface []interface{}  
	for _, op := range batch {
		batchInterface = append(batchInterface, op)  
	}  
    if index, _, isLeader := rf.Start(batchInterface); isLeader {
        kv.Lock()
        notifyCh := kv.GetNotifyChannel(index)
        kv.Unlock()

        select {
        case <-notifyCh:
            conn.Write([]byte("BATCH_OK\n"))
        case <-time.After(2 * time.Second):
            conn.Write([]byte("BATCH_TIMEOUT\n"))
        }

        go func() {
            kv.RemoveNotifyChannel(index)
        }()
    } else {
        conn.Write([]byte("NOT_LEADER\n"))
    }
}

// handleConnection 处理TCP连接的请求  
func handleConnection(kv *server.KVServer, conn net.Conn) {
    defer func() {
        fmt.Printf("Closing connection from %s\n", conn.RemoteAddr())
        conn.Close()
    }()
    rf := kv.GetRaft()
    reader := bufio.NewReader(conn)
    for {
        // 读取直到遇到换行符,“set k1 v1;”规定命令格式为以 ';' 为结束符
        // 主要是为能够读取到完整的数据，以分号为结束符，避免粘包
        command, err := reader.ReadString('\n')
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error reading from connection: %s\n", err)
            return
        }
        command = strings.TrimSpace(command)// 去掉首尾空白  
        if command == "" {
            continue
        }
        parts := strings.Split(command, " ")
        if len(parts) < 1 {
            conn.Write([]byte("Invalid command\n"))
            continue
        }

        action := strings.ToUpper(parts[0])
        switch action {
        case "SET":
            if len(parts) != 3 {
                conn.Write([]byte("Invalid SET command\n"))
                continue
            }
            if _,isLeader := rf.GetState();isLeader{
                op := server.Op{
                    OpType:   server.Set,
                    Key:      parts[1],
                    Value:    parts[2],
                    ClientId: generateClientID(conn),
                    SeqId:    time.Now().UnixNano(),
                }
                batchMutex.Lock()
                batchBuffer = append(batchBuffer, op)
                if len(batchBuffer) >= batchSize {
                    // 妙用gc回收
                    currentBatch := make([]server.Op, len(batchBuffer))
                    copy(currentBatch, batchBuffer)
                    batchBuffer = nil
                    batchMutex.Unlock()
                    // 异步发送
                    go sendBatch(kv, conn, currentBatch,rf)
                }else{
                    batchMutex.Unlock()
                }
                conn.Write([]byte("ACK\n"))
            }else{
                conn.Write([]byte("is not leader\n"))
            }
        case "GET":
            if len(parts) != 2 {
                conn.Write([]byte("ERR invalid GET command\n"))
                continue
            }
            value := bridge.Array_Get(parts[1])
            if len(value) != 0 {
                conn.Write([]byte(value + "\n"))
            } else {
                conn.Write([]byte("nil\n"))
            }
        case "LEADER":
            currentTerm, isLeader := kv.GetRaft().GetState()
            fmt.Printf("当前状态: %v, 当前任期: %d\n", isLeader, currentTerm)
            if isLeader {
                conn.Write([]byte("isLeader\n"))
            } else {
                conn.Write([]byte("isCluster\n"))
            }
        default:
            conn.Write([]byte("Unknown command\n"))
        }
    }
}