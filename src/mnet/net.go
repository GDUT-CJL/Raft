package mnet

import (
	"course/server"
	"fmt"
	"net"
)

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
