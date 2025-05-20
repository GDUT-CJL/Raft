package net

import (
	"course/server"
	"fmt"
	"net"
	"os"
)

// Global counter to generate unique Client IDs
var clientIDCounter int64
var seqIDCounter int64

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
		go handleConnection(kv, conn)
	}
}
