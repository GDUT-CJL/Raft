package net

import (  
	"fmt"  
	"net"  
	"os"  
	"strings"  
	"course/bridge"

	"course/raft"
)  

// StartTCPServer 启动TCP服务器并监听来自客户端的请求  
func StartTCPServer(raft *raft.Raft,port int) {  
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
		go handleConnection(raft,conn)  
	}  
}  

// handleConnection 处理TCP连接的请求  
func handleConnection(raft *raft.Raft,conn net.Conn) {  
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

		// 解析命令  
		parts := strings.Split(command, " ")  
		if len(parts) < 2 {  
			conn.Write([]byte("Invalid command\n"))  
			continue  
		}  
		action := strings.ToUpper(parts[0]) // 转换为大写以支持不区分大小写  
		key := parts[1]  

		switch action {  
		case "SET":  
			if len(parts) != 3 {  
				conn.Write([]byte("Invalid SET command\n"))  
				continue  
			}  
			value := parts[2]  
			// bridge.Set(key, value) 
			// 提交到Raft日志  
			raft.Start(fmt.Sprintf("SET %s %s", key, value))   
			fmt.Printf("Have Commited SET Log: %s:%s\n", key, value)  
			conn.Write([]byte("OK\n")) // 返回操作结果  

		case "GET":  
			// 直接读取存储  
			value := bridge.Get(key)  
			fmt.Printf("GET command: key = %s, value = %s\n", key, value)  
			conn.Write([]byte(value + "\n")) // 返回获取的值  

		default:  
			conn.Write([]byte("Unknown command\n")) // 返回未识别的命令  
		}  
	}  
}  