package net

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"course/bridge"
	"course/raft"
	"course/server"
	"io"
	"strconv"
)

var (
	batchBufferMap = make(map[net.Conn][]server.Op) //key为对应的一个连接 value为操作数组
	batchMutex     = &sync.RWMutex{}                // 批量提交数组的锁
	batchSize      = 1000                           // 一次累计批量提交大小
	batchTimeout   = 3000 * time.Millisecond        // 超时提交时间
)

func generateSeqID() int64 {
	return atomic.AddInt64(&seqIDCounter, 1)
}

// generateClientID generates a unique client ID for each connection
func generateClientID(conn net.Conn) int64 {
	addr := conn.RemoteAddr().(*net.TCPAddr).IP.String()
	hash := fnv.New64a()
	hash.Write([]byte(addr))
	return int64(hash.Sum64()) ^ (atomic.AddInt64(&clientIDCounter, 1) << 32)
}

// sendBatch sends a batch of operations to Raft
func sendBatch(kv *server.KVServer, conn net.Conn, batch []server.Op, rf *raft.Raft) {
	if len(batch) == 0 {
		conn.Write([]byte("EMPTY_BATCH\n"))
		return
	}

	// Convert batch to []interface{}
	batchInterface := make([]interface{}, len(batch))
	for i, op := range batch {
		batchInterface[i] = op
	}

	index, _, isLeader := rf.Start(batchInterface)
	if !isLeader {
		conn.Write([]byte("NOT_LEADER\n"))
		return
	}

	notifyCh := kv.GetNotifyChannel(index)

	select {
	case <-notifyCh:
		conn.Write([]byte("BATCH_OK\n"))
	case <-time.After(5 * time.Second):
		conn.Write([]byte("BATCH_TIMEOUT\n"))
	}

	// Clean up notification channel
	go func() {
		time.Sleep(100 * time.Millisecond) // Small delay to ensure all notifications are processed
		kv.RemoveNotifyChannel(index)
	}()
}

// flushBatch flushes the batch for a specific connection
func flushBatch(kv *server.KVServer, conn net.Conn, rf *raft.Raft) {
	batchMutex.Lock()
	defer batchMutex.Unlock()

	batch := batchBufferMap[conn]
	if len(batch) > 0 {
		// 复制并清空原缓冲区
		batchToSend := make([]server.Op, len(batch))
		copy(batchToSend, batch)
		batchBufferMap[conn] = nil // 清空而非删除
		go sendBatch(kv, conn, batchToSend, rf)
	}
}

// addToBatch 将op日志累计添加到某个map的key连接对应的value的op数组中
func addToBatch(op server.Op, conn net.Conn, kv *server.KVServer, rf *raft.Raft) {
	batchMutex.Lock()
	defer batchMutex.Unlock()

	// 如果对应累计没有数组则创建
	if _, exists := batchBufferMap[conn]; !exists {
		batchBufferMap[conn] = make([]server.Op, 0, batchSize)
	}

	// 将日志添加到缓存区中等待提交
	batchBufferMap[conn] = append(batchBufferMap[conn], op)

	// 如果日志长度大于我们规定的batchSize的长度则进行一次提交
	if len(batchBufferMap[conn]) >= batchSize {
		batch := make([]server.Op, len(batchBufferMap[conn]))
		copy(batch, batchBufferMap[conn])
		delete(batchBufferMap, conn)
		go sendBatch(kv, conn, batch, rf)
	}
}

// 日志批量提交操作
func Commited(optype server.OperationType, key string, value string, conn net.Conn, kv *server.KVServer, rf *raft.Raft) {
	var peer int
	if _, isLeader := rf.GetState(); !isLeader {
		for peer = 0; peer < rf.GetPeerLen(); peer++ {
			if peer == rf.GetLeaderId() {
				break
			}
		}
		s := strconv.Itoa(peer)
		conn.Write([]byte("Leader is Node " + s + "\n"))
		return
	}

	op := server.Op{
		OpType:   optype,
		Key:      key,
		Value:    value,
		ClientId: generateClientID(conn),
		SeqId:    generateSeqID(), // 使用原子递增，防止并发过高而重复
	}
	addToBatch(op, conn, kv, rf)
	conn.Write([]byte("ACK\n"))
}

// handleConnection processes TCP connections
func handleConnection(kv *server.KVServer, conn net.Conn) {
	rf := kv.GetRaft()

	defer func() {
		fmt.Printf("Closing connection from %s\n", conn.RemoteAddr())
		// 重试 3 次清空缓冲区
		// for i := 0; i < 3; i++ {
		// 	flushBatch(kv, conn, rf)
		// 	time.Sleep(10 * time.Millisecond)
		// 	//fmt.Printf("%d\n", i)
		// }
		conn.Close()
	}()
	reader := bufio.NewReader(conn)

	// Start a timer for this connection
	timer := time.NewTimer(batchTimeout)
	defer timer.Stop()

	// Goroutine to handle batch timeouts
	go func() {
		for {
			select {
			case <-timer.C:
				flushBatch(kv, conn, rf)
				timer.Reset(batchTimeout)
			}
		}
	}()

	for {
		command, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return
			} else {
				fmt.Fprintf(os.Stderr, "Error reading from connection: %s\n", err)
				return
			}
		}

		command = strings.TrimSpace(command)
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
		// Array
		case "SET":
			if len(parts) != 3 {
				conn.Write([]byte("Invalid SET command\n"))
				continue
			}
			Commited(server.Set, parts[1], parts[2], conn, kv, rf)
			// Reset timer on each operation
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(batchTimeout)
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
		case "COUNT":
			if len(parts) != 1 {
				conn.Write([]byte("ERR invalid COUNT command\n"))
				continue
			}
			value := bridge.Array_Count()

			str := strconv.Itoa(value)
			if len(str) != 0 {
				conn.Write([]byte(str + "\n"))
			} else {
				conn.Write([]byte("nil\n"))
			}
		case "DELETE":
			if len(parts) != 2 {
				conn.Write([]byte("Invalid DELETE command\n"))
				continue
			}
			Commited(server.Delete, parts[1], parts[1], conn, kv, rf)
			// Reset timer on each operation
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(batchTimeout)

		case "EXIST":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid EXIST command\n"))
				continue
			}
			ret := bridge.Array_Exist(parts[1])
			if ret == 0 {
				conn.Write([]byte("Exist\n"))
			} else {
				conn.Write([]byte("nil\n"))
			}

		// Hash
		case "HSET":
			if len(parts) != 3 {
				conn.Write([]byte("Invalid HSET command\n"))
				continue
			}
			Commited(server.HSet, parts[1], parts[2], conn, kv, rf)
			// Reset timer on each operation
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(batchTimeout)
		case "HGET":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid HGET command\n"))
				continue
			}
			value := bridge.Hash_Get(parts[1])
			if len(value) != 0 {
				conn.Write([]byte(value + "\n"))
			} else {
				conn.Write([]byte("nil\n"))
			}
		case "HCOUNT":
			if len(parts) != 1 {
				conn.Write([]byte("ERR invalid HCOUNT command\n"))
				continue
			}
			value := bridge.Hash_Count()
			str := strconv.Itoa(value)
			if len(str) != 0 {
				conn.Write([]byte(str + "\n"))
			} else {
				conn.Write([]byte("nil\n"))
			}
		case "HDELETE":
			if len(parts) != 2 {
				conn.Write([]byte("Invalid HDELETE command\n"))
				continue
			}
			Commited(server.HDelete, parts[1], parts[1], conn, kv, rf)
			// Reset timer on each operation
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(batchTimeout)

		case "HEXIST":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid HEXIST command\n"))
				continue
			}
			ret := bridge.Hash_Exist(parts[1])
			if ret == 0 {
				conn.Write([]byte("Exist\n"))
			} else {
				conn.Write([]byte("nil\n"))
			}
		// RBTree
		case "RSET":
			if len(parts) != 3 {
				conn.Write([]byte("Invalid RSET command\n"))
				continue
			}
			Commited(server.RSet, parts[1], parts[2], conn, kv, rf)
			// Reset timer on each operation
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(batchTimeout)
		case "RGET":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid RGET command\n"))
				continue
			}
			value := bridge.RB_Get(parts[1])
			if len(value) != 0 {
				conn.Write([]byte(value + "\n"))
			} else {
				conn.Write([]byte("nil\n"))
			}
		case "RCOUNT":
			if len(parts) != 1 {
				conn.Write([]byte("ERR invalid RCOUNT command\n"))
				continue
			}
			value := bridge.RB_Count() / 3
			str := strconv.Itoa(value)
			if len(str) != 0 {
				conn.Write([]byte(str + "\n"))
			} else {
				conn.Write([]byte("nil\n"))
			}
		case "RDELETE":
			if len(parts) != 2 {
				conn.Write([]byte("Invalid RDELETE command\n"))
				continue
			}
			Commited(server.Delete, parts[1], parts[1], conn, kv, rf)
			// Reset timer on each operation
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(batchTimeout)

		case "REXIST":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid REXIST command\n"))
				continue
			}
			ret := bridge.RB_Exist(parts[1])
			if ret == 0 {
				conn.Write([]byte("Exist\n"))
			} else {
				conn.Write([]byte("nil\n"))
			}

		// BTree
		case "BSET":
			if len(parts) != 3 {
				conn.Write([]byte("Invalid BSET command\n"))
				continue
			}
			Commited(server.BSet, parts[1], parts[2], conn, kv, rf)
			// Reset timer on each operation
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(batchTimeout)
		case "BGET":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid BGET command\n"))
				continue
			}
			value := bridge.BTree_Get(parts[1])
			if len(value) != 0 {
				conn.Write([]byte(value + "\n"))
			} else {
				conn.Write([]byte("nil\n"))
			}
		case "BCOUNT":
			if len(parts) != 1 {
				conn.Write([]byte("ERR invalid BCOUNT command\n"))
				continue
			}
			value := bridge.BTree_Count()
			str := strconv.Itoa(value)
			if len(str) != 0 {
				conn.Write([]byte(str + "\n"))
			} else {
				conn.Write([]byte("nil\n"))
			}
		case "BDELETE":
			if len(parts) != 2 {
				conn.Write([]byte("Invalid BDELETE command\n"))
				continue
			}
			Commited(server.BDelete, parts[1], parts[1], conn, kv, rf)
			// Reset timer on each operation
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(batchTimeout)

		case "BEXIST":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid BEXIST command\n"))
				continue
			}
			ret := bridge.BTree_Exist(parts[1])
			if ret == 0 {
				conn.Write([]byte("Exist\n"))
			} else {
				conn.Write([]byte("nil\n"))
			}
		//Skiplist
		case "ZSET":
			if len(parts) != 3 {
				conn.Write([]byte("Invalid ZSET command\n"))
				continue
			}
			Commited(server.ZSet, parts[1], parts[2], conn, kv, rf)
			// 每次提交后就重置超时时间
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(batchTimeout)
		case "ZGET":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid ZGET command\n"))
				continue
			}
			value := bridge.Skiplist_Get(parts[1])
			if len(value) != 0 {
				conn.Write([]byte(value + "\n"))
			} else {
				conn.Write([]byte("nil\n"))
			}
		case "ZCOUNT":
			if len(parts) != 1 {
				conn.Write([]byte("ERR invalid ZCOUNT command\n"))
				continue
			}
			value := bridge.Skiplist_Count()

			str := strconv.Itoa(value)
			if len(str) != 0 {
				conn.Write([]byte(str + "\n"))
			} else {
				conn.Write([]byte("nil\n"))
			}
		case "ZDELETE":
			if len(parts) != 2 {
				conn.Write([]byte("Invalid ZDELETE command\n"))
				continue
			}
			Commited(server.ZDelete, parts[1], parts[1], conn, kv, rf)
			// Reset timer on each operation
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(batchTimeout)

		case "ZEXIST":
			if len(parts) != 2 {
				conn.Write([]byte("ERR invalid ZEXIST command\n"))
				continue
			}
			ret := bridge.Skiplist_Exist(parts[1])
			if ret == 0 {
				conn.Write([]byte("Exist\n"))
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
		case "NUM":
			num := kv.GetSetCounter()
			str := strconv.FormatInt(num, 10)
			conn.Write([]byte(str + "\n"))
		default:
			conn.Write([]byte("Unknown command\n"))
		}
	}
}
