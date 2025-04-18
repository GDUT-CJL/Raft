package net

import (  
	"fmt"  
	"net"  
	"os"  
	"strings"  
	"course/bridge"
	"course/server"
	"time"
	"sync/atomic"
	"hash/fnv"
)  

// Global counter to generate unique Client IDs  
var clientIDCounter int64

// StartTCPServer 启动TCP服务器并监听来自客户端的请求  
func StartTCPServer(kv *server.KVServer, port int) {  
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))  
	if err != nil {  
		fmt.Fprintf(os.Stderr, "Error starting TCP server: %s\n", err)  
		return  
	}  
	defer listener.Close()  
	fmt.Printf("TCP server listening on port %d\n", port)  

	for {  
		conn, err := listener.Accept()  
		if err != nil {  
			fmt.Fprintf(os.Stderr, "Error accepting connection: %s\n", err)  
			continue  
		}  
		// 使用 goroutine 处理连接  
		go handleConnection(kv,conn)  
	}  
}  

// generateClientID generates a unique client ID for each connection  
func generateClientID(conn net.Conn) int64 {  
    // 结合节点IP和原子计数器
    addr := conn.RemoteAddr().(*net.TCPAddr).IP.String()
    hash := fnv.New64a()
    hash.Write([]byte(addr))
    return int64(hash.Sum64()) ^ (atomic.AddInt64(&clientIDCounter, 1) << 32)
}

// handleConnection 处理TCP连接的请求  
func handleConnection(kv *server.KVServer,conn net.Conn) {  
	defer func() {  
		fmt.Printf("Closing connection from %s\n", conn.RemoteAddr())  
		conn.Close()  
	}()  

	buffer := make([]byte, 1024)  
	// 持续读取客户端的请求  
	for {  
		n, err := conn.Read(buffer)  
		if err != nil {  
			fmt.Fprintf(os.Stderr, "Error reading from connection: %s\n", err)  
			return  
		}  

		command := string(buffer[:n])  
		command = strings.TrimSpace(command) // 去掉首尾空白  
		rf := kv.GetRaft()
		// 解析命令  
		parts := strings.Split(command, " ")  
		if len(parts) < 1 {  
			conn.Write([]byte("Invalid command\n"))  
			continue  
		}  
		action := strings.ToUpper(parts[0]) // 转换为大写以支持不区分大小写  

		switch action {  
		case "SET":  
			if len(parts) != 3 {  
				conn.Write([]byte("Invalid SET command\n"))  
				continue  
			} 
			op := server.Op{
				OpType:   server.Set,
				Key:      parts[1],
				Value:    parts[2],
				ClientId: generateClientID(conn),
				SeqId:      time.Now().UnixNano(),
			} 
			//value := parts[2]  
			// bridge.Set(key, value) 
			// 提交到Raft日志
			if _, _, isLeader := rf.Start(op); !isLeader {
				conn.Write([]byte("is not leader\n")) // 不是leader节点不给set，强一致性
			}else{
				conn.Write([]byte("OK\n")) // 返回操作结果  
			}

		case "GET": 
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid GET command\n"))
				continue
			}
			op := server.Op{
				OpType:   server.Get,
				Key:      parts[1],
				ClientId: generateClientID(conn),
				SeqId:    time.Now().UnixNano(),
			}
			_, _, isLeader := rf.Start(op)
			if !isLeader {
				conn.Write([]byte(" not leader\n"))
			} else{
				value := bridge.Get(parts[1])
				conn.Write([]byte("OK " + value + "\n"))
			}
		// 返回该节点是不是leader
		case "LEADER":
			currentTerm, isLeader := rf.GetState()
			fmt.Printf("当前状态: %v, 当前任期: %d\n",isLeader,currentTerm)
			if isLeader{
				conn.Write([]byte("isLeader" + "\n")) // 返回获取的值  
			}else{
				conn.Write([]byte("isCluster" + "\n")) // 返回获取的值  
			}
			//return fmt.Sprintf("OK %v", isLeader)
		default:  
			conn.Write([]byte("Unknown command\n")) // 返回未识别的命令  
		}  
	}  
}  