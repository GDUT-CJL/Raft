package mnet

import (
	"course/server"
	"fmt"
	"net"
)

// Global counter to generate unique Client IDs
var clientIDCounter int64

// StartTCPServer 启动TCP服务器并监听来自客户端的请求
// func StartTCPServer(kv *server.KVServer, port int) {
// 	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
// 	if err != nil {
// 		fmt.Fprintf(os.Stderr, "Error starting TCP server: %s\n", err)
// 		return
// 	}
// 	defer listener.Close()
// 	fmt.Printf("TCP server listening on port %d\n", port)

// 	for {
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			fmt.Fprintf(os.Stderr, "Error accepting connection: %s\n", err)
// 			continue
// 		}
// 		// 使用 goroutine 处理连接
// 		go handleConnection(kv,conn)
// 	}
// }

// 启动TCP服务器
func StartTCPServer(kv *server.KVServer, addr string, id int) {
	listener, err := net.Listen("tcp", addr)
	fmt.Printf("Addr:%s\n", addr)
	if err != nil {
		fmt.Printf("Error starting TCP server on %s: %v\n", addr, err)
		return
	}
	defer listener.Close()

	fmt.Printf("TCP server listening on %s\n", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}
		go handleConnection(kv, conn)
	}

}
