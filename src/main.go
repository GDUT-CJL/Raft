// package main
// import (
// 	"flag"
// 	"course/bridge"
// 	"course/net"
// 	"course/raft"
// )
// func main(){
// 	bridge.InitStorage()
// 	id := flag.Int("id",1,"Node ID")
// 	port := flag.Int("port", 8000, "TCP port")//默认8000
// 	flag.Parse()

// 	// 初始化Raft节点
// 	rf := raft.Make(peers, *id, nil, applyCh)
// 	//  ./main -port xxxx
// 	net.StartTCPServer(rf,*port)
// }
