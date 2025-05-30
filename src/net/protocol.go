package net
import (  
	"fmt"  
	"net"  
	"os"  
	"strings"
	"time"  
	"course/bridge"
	"course/server"
	"strconv"
	"course/raft"
	"sync/atomic"
	"hash/fnv"
)  

// generateClientID generates a unique client ID for each connection  
func generateClientID(conn net.Conn) int64 {  
    // 结合节点IP和原子计数器
    addr := conn.RemoteAddr().(*net.TCPAddr).IP.String()
    hash := fnv.New64a()
    hash.Write([]byte(addr))
    return int64(hash.Sum64()) ^ (atomic.AddInt64(&clientIDCounter, 1) << 32)
}

// 将相关操作提交，并应用到相应状态机上
func Commited(kv *server.KVServer,conn net.Conn,parts []string,rf *raft.Raft,opType server.OperationType,part1 string,part2 string){
	op := server.Op{
		OpType:   opType,
		Key:      part1,
		Value:    part2,
		ClientId: generateClientID(conn),
		SeqId:      time.Now().UnixNano(),
	} 
	//value := parts[2]  
	// bridge.Set(key, value) 
	// 使用notifyCh确保先提交到Raft日志后再返回客户端
	if index, _, isLeader := rf.Start(op); isLeader {
		kv.Lock()
		notifyCh := kv.GetNotifyChannel(index)
		kv.Unlock()
		select {
		case <-notifyCh:
			conn.Write([]byte("OK\n")) // 返回操作结果  
		case <-time.After(2*time.Second):
			conn.Write([]byte("Time Out\n")) // 返回操作结果  
		}
		go func() {
			kv.Lock()
			kv.RemoveNotifyChannel(index)
			kv.Unlock()
		}()
	}else{
		conn.Write([]byte("is not leader\n")) // 不是leader节点不允许操作，强一致性
	}
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
			Commited(kv,conn,parts,rf,server.Set,parts[1],parts[2])
		case "GET": 
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid GET command\n"))
				continue
			}
			value := bridge.Array_Get(parts[1])
			if len(value) != 0{
				conn.Write([]byte(value + "\n"))
			}else{
				conn.Write([]byte("nil\n"))
			}
		case "DELETE":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid Delete command\n"))
				continue
			}
			Commited(kv,conn,parts,rf,server.Delete,parts[1],parts[1])
		case "COUNT":
			if len(parts) != 1 {
				conn.Write([]byte("ERR invalid Count command\n"))
				continue
			}
			op := server.Op{
				OpType:   server.Count,
				Key:      parts[0],
				ClientId: generateClientID(conn),
				SeqId:    time.Now().UnixNano(),
			} 
			if _, _, isLeader := rf.Start(op); !isLeader {
				conn.Write([]byte("is not leader\n")) // 不是leader节点不允许操作，强一致性
			}else{
				count := bridge.Array_Count() / rf.GetPeerLen()
				// int 转为 string
				s := strconv.Itoa(count)
				conn.Write([]byte(s + "\n"))
			}
		case "EXIST":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid Exist command\n"))
				continue
			}
			ret := bridge.Array_Exist(parts[1])
			if ret == 0{
				conn.Write([]byte("Exist\n"))
			}else{
				conn.Write([]byte("not Exist\n"))
			}
		case "HSET":
			if len(parts) != 3 {  
				conn.Write([]byte("Invalid HSET command\n"))  
				continue  
			} 
			Commited(kv,conn,parts,rf,server.HSet,parts[1],parts[2])
		case "HGET":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid HGET command\n"))
				continue
			}
			value := bridge.Hash_Get(parts[1])
			if len(value) != 0{
				conn.Write([]byte(value + "\n"))
			}else{
				conn.Write([]byte("nil\n"))
			}
		case "HDELETE":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid HDelete command\n"))
				continue
			}
			Commited(kv,conn,parts,rf,server.HDelete,parts[1],parts[1])
		case "HEXIST":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid HExist command\n"))
				continue
			}
			ret := bridge.Hash_Exist(parts[1])
			if ret == 0{
				conn.Write([]byte("Exist\n"))
			}else{
				conn.Write([]byte("not Exist\n"))
			}
		case "HCOUNT":
			if len(parts) != 1 {
				conn.Write([]byte("ERR invalid HCount command\n"))
				continue
			}
			op := server.Op{
				OpType:   server.HCount,
				Key:      parts[0],
				ClientId: generateClientID(conn),
				SeqId:    time.Now().UnixNano(),
			} 
			if _, _, isLeader := rf.Start(op); !isLeader {
				conn.Write([]byte("is not leader\n")) // 不是leader节点不允许操作，强一致性
			}else{
				count := bridge.Hash_Count()
				// int 转为 string
				s := strconv.Itoa(count)
				conn.Write([]byte(s + "\n"))
			}
		case "RSET":
			if len(parts) != 3 {  
				conn.Write([]byte("Invalid RSET command\n"))  
				continue  
			} 
			Commited(kv,conn,parts,rf,server.RSet,parts[1],parts[2])			
		case "RGET":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid RGET command\n"))
				continue
			}
			value := bridge.RB_Get(parts[1])
			if len(value) != 0{
				conn.Write([]byte(value + "\n"))
			}else{
				conn.Write([]byte("nil\n"))
			}
		case "RDELETE":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid RDelete command\n"))
				continue
			}
			Commited(kv,conn,parts,rf,server.RDelete,parts[1],parts[1])
		case "REXIST":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid RExist command\n"))
				continue
			}
			ret := bridge.RB_Exist(parts[1])
			if ret == 0{
				conn.Write([]byte("Exist\n"))
			}else{
				conn.Write([]byte("not Exist\n"))
			}
		case "RCOUNT":
			if len(parts) != 1 {
				conn.Write([]byte("ERR invalid RCount command\n"))
				continue
			}
			op := server.Op{
				OpType:   server.RCount,
				Key:      parts[0],
				ClientId: generateClientID(conn),
				SeqId:    time.Now().UnixNano(),
			} 
			if _, _, isLeader := rf.Start(op); !isLeader {
				conn.Write([]byte("is not leader\n")) // 不是leader节点不允许操作，强一致性
			}else{
				count := bridge.RB_Count() / rf.GetPeerLen()
				// int 转为 string
				s := strconv.Itoa(count)
				conn.Write([]byte(s + "\n"))
			}
		case "BSET":
			if len(parts) != 3 {  
				conn.Write([]byte("Invalid BSET command\n"))  
				continue  
			} 
			Commited(kv,conn,parts,rf,server.BSet,parts[1],parts[2])			
		case "BGET":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid BGET command\n"))
				continue
			}
			value := bridge.BTree_Get(parts[1])
			if len(value) != 0{
				conn.Write([]byte(value + "\n"))
			}else{
				conn.Write([]byte("nil\n"))
			}
		case "BDELETE":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid BDelete command\n"))
				continue
			}
			Commited(kv,conn,parts,rf,server.BDelete,parts[1],parts[1])
		case "BEXIST":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid BExist command\n"))
				continue
			}
			ret := bridge.BTree_Exist(parts[1])
			if ret == 0{
				conn.Write([]byte("Exist\n"))
			}else{
				conn.Write([]byte("not Exist\n"))
			}
		case "BCOUNT":
			if len(parts) != 1 {
				conn.Write([]byte("ERR invalid BCount command\n"))
				continue
			}
			op := server.Op{
				OpType:   server.BCount,
				Key:      parts[0],
				ClientId: generateClientID(conn),
				SeqId:    time.Now().UnixNano(),
			} 
			if _, _, isLeader := rf.Start(op); !isLeader {
				conn.Write([]byte("is not leader\n")) // 不是leader节点不允许操作，强一致性
			}else{
				count := bridge.BTree_Count()
				// int 转为 string
				s := strconv.Itoa(count)
				conn.Write([]byte(s + "\n"))
			}

		case "ZSET":
			if len(parts) != 3 {  
				conn.Write([]byte("Invalid ZSET command\n"))  
				continue  
			} 
			Commited(kv,conn,parts,rf,server.ZSet,parts[1],parts[2])			
		case "ZGET":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid ZGET command\n"))
				continue
			}
			value := bridge.Skiplist_Get(parts[1])
			if len(value) != 0{
				conn.Write([]byte(value + "\n"))
			}else{
				conn.Write([]byte("nil\n"))
			}
		case "ZDELETE":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid ZDelete command\n"))
				continue
			}
			Commited(kv,conn,parts,rf,server.ZDelete,parts[1],parts[1])
		case "ZEXIST":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid ZExist command\n"))
				continue
			}
			ret := bridge.Skiplist_Exist(parts[1])
			if ret == 0{
				conn.Write([]byte("Exist\n"))
			}else{
				conn.Write([]byte("not Exist\n"))
			}
		case "ZCOUNT":
			if len(parts) != 1 {
				conn.Write([]byte("ERR invalid ZCount command\n"))
				continue
			}
			op := server.Op{
				OpType:   server.ZCount,
				Key:      parts[0],
				ClientId: generateClientID(conn),
				SeqId:    time.Now().UnixNano(),
			} 
			if _, _, isLeader := rf.Start(op); !isLeader {
				conn.Write([]byte("is not leader\n")) // 不是leader节点不允许操作，强一致性
			}else{
				count := bridge.Skiplist_Count()
				// int 转为 string
				s := strconv.Itoa(count)
				conn.Write([]byte(s + "\n"))
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
		default:  
			conn.Write([]byte("Unknown command\n")) // 返回未识别的命令  
		}  
	}  
} 