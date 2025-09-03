package mnet

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

func generateSeqID() int64 {
	return atomic.AddInt64(&seqIDCounter, 1)
}

var (
	batchBufferMap  = make(map[net.Conn][]server.Op)
	batchMutex      = &sync.RWMutex{}
	batchSize       = 1000
	batchTimeout    = 3000 * time.Millisecond
	seqIDCounter    int64
	clientIDCounter int64
)

// 定义命令处理器类型
type commandHandler func(parts []string, conn net.Conn, kv *server.KVServer, rf *raft.Raft, timer *time.Timer)

// generateClientID generates a unique client ID for each connection
func generateClientID(conn net.Conn) int64 {
	// 结合节点IP和原子计数器
	addr := conn.RemoteAddr().(*net.TCPAddr).IP.String()
	hash := fnv.New64a()
	hash.Write([]byte(addr))
	return int64(hash.Sum64()) ^ (atomic.AddInt64(&clientIDCounter, 1) << 32)
}

// 命令映射表
/*
	handleWriteOp：处理所有写操作（SET、DELETE等）
	handleReadOp：处理所有读操作（GET等）
	handleCountOp：处理计数操作（COUNT等）
	handleExistOp：处理存在性检查操作（EXIST等）
*/

var commandHandlers = map[string]commandHandler{
	// Array commands
	"SET":    handleWriteOp(server.Set, 3),
	"GET":    handleReadOp(bridge.Array_Get, 2),
	"COUNT":  handleCountOp(bridge.Array_Count, 1),
	"DELETE": handleWriteOp(server.Delete, 2),
	"EXIST":  handleExistOp(bridge.Array_Exist, 2),

	// Hash commands
	"HSET":    handleWriteOp(server.HSet, 3),
	"HGET":    handleReadOp(bridge.Hash_Get, 2),
	"HCOUNT":  handleCountOp(bridge.Hash_Count, 1),
	"HDELETE": handleWriteOp(server.HDelete, 2),
	"HEXIST":  handleExistOp(bridge.Hash_Exist, 2),

	// RBTree commands
	"RSET":    handleWriteOp(server.RSet, 3),
	"RGET":    handleReadOp(bridge.RB_Get, 2),
	"RCOUNT":  handleCountOpWithDivisor(bridge.RB_Count, 1, 3),
	"RDELETE": handleWriteOp(server.RDelete, 2),
	"REXIST":  handleExistOp(bridge.RB_Exist, 2),

	// BTree commands
	"BSET":    handleWriteOp(server.BSet, 3),
	"BGET":    handleReadOp(bridge.BTree_Get, 2),
	"BCOUNT":  handleCountOp(bridge.BTree_Count, 1),
	"BDELETE": handleWriteOp(server.BDelete, 2),
	"BEXIST":  handleExistOp(bridge.BTree_Exist, 2),

	// Skiplist commands
	"ZSET":    handleWriteOp(server.ZSet, 3),
	"ZGET":    handleReadOp(bridge.Skiplist_Get, 2),
	"ZCOUNT":  handleCountOp(bridge.Skiplist_Count, 1),
	"ZDELETE": handleWriteOp(server.ZDelete, 2),
	"ZEXIST":  handleExistOp(bridge.Skiplist_Exist, 2),

	// Special commands
	"LEADER": handleLeaderCommand,
	"NUM":    handleNumCommand,
}

func sendBatch(kv *server.KVServer, conn net.Conn, batch []server.Op, rf *raft.Raft) {
	if len(batch) == 0 {
		conn.Write([]byte("EMPTY_BATCH\n"))
		return
	}

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

	// 延迟清理通知通道
	go func() {
		time.Sleep(100 * time.Millisecond)
		kv.RemoveNotifyChannel(index)
	}()
}

func flushBatch(kv *server.KVServer, conn net.Conn, rf *raft.Raft) {
	batchMutex.Lock()
	defer batchMutex.Unlock()

	batch := batchBufferMap[conn]
	if len(batch) > 0 {
		batchToSend := make([]server.Op, len(batch))
		copy(batchToSend, batch)
		batchBufferMap[conn] = nil
		sendBatch(kv, conn, batchToSend, rf)
	}
}

func addToBatch(op server.Op, conn net.Conn, kv *server.KVServer, rf *raft.Raft) {
	batchMutex.Lock()
	defer batchMutex.Unlock()

	if _, exists := batchBufferMap[conn]; !exists {
		batchBufferMap[conn] = make([]server.Op, 0, batchSize)
	}

	batchBufferMap[conn] = append(batchBufferMap[conn], op)

	if len(batchBufferMap[conn]) >= batchSize {
		batch := make([]server.Op, len(batchBufferMap[conn]))
		copy(batch, batchBufferMap[conn])
		batchBufferMap[conn] = nil
		go sendBatch(kv, conn, batch, rf)
	}
}

func Commited(optype server.OperationType, key string, value string, conn net.Conn, kv *server.KVServer, rf *raft.Raft, timer *time.Timer) {

	if _, isLeader := rf.GetState(); isLeader {
		op := server.Op{
			OpType:   optype,
			Key:      key,
			Value:    value,
			ClientId: generateClientID(conn),
			SeqId:    generateSeqID(),
		}
		addToBatch(op, conn, kv, rf)
		conn.Write([]byte("ACK\n"))

		if !timer.Stop() {
			<-timer.C
		}
		timer.Reset(batchTimeout)
	} else {

	}

}

// 通用写操作处理函数
func handleWriteOp(opType server.OperationType, expectedArgs int) commandHandler {
	return func(parts []string, conn net.Conn, kv *server.KVServer, rf *raft.Raft, timer *time.Timer) {
		if len(parts) != expectedArgs {
			conn.Write([]byte("Invalid " + parts[0] + " command\n"))
			return
		}

		value := parts[1]
		if expectedArgs == 3 {
			value = parts[2]
		}

		Commited(opType, parts[1], value, conn, kv, rf, timer)
	}
}

// 通用读操作处理函数
func handleReadOp(readFunc func(string) string, expectedArgs int) commandHandler {
	return func(parts []string, conn net.Conn, kv *server.KVServer, rf *raft.Raft, timer *time.Timer) {
		if len(parts) != expectedArgs {
			conn.Write([]byte("ERR invalid " + parts[0] + " command\n"))
			return
		}

		value := readFunc(parts[1])
		if len(value) != 0 {
			conn.Write([]byte(value + "\n"))
		} else {
			conn.Write([]byte("nil\n"))
		}
	}
}

// 通用计数操作处理函数
func handleCountOp(countFunc func() int, expectedArgs int) commandHandler {
	return func(parts []string, conn net.Conn, kv *server.KVServer, rf *raft.Raft, timer *time.Timer) {
		if len(parts) != expectedArgs {
			conn.Write([]byte("ERR invalid " + parts[0] + " command\n"))
			return
		}

		value := countFunc()
		str := strconv.Itoa(value)
		if len(str) != 0 {
			conn.Write([]byte(str + "\n"))
		} else {
			conn.Write([]byte("nil\n"))
		}
	}
}

// 带除数的计数操作处理函数（用于RBTree）
func handleCountOpWithDivisor(countFunc func() int, expectedArgs int, divisor int) commandHandler {
	return func(parts []string, conn net.Conn, kv *server.KVServer, rf *raft.Raft, timer *time.Timer) {
		if len(parts) != expectedArgs {
			conn.Write([]byte("ERR invalid " + parts[0] + " command\n"))
			return
		}

		value := countFunc() / divisor
		str := strconv.Itoa(value)
		if len(str) != 0 {
			conn.Write([]byte(str + "\n"))
		} else {
			conn.Write([]byte("nil\n"))
		}
	}
}

// 存在性检查操作处理函数
func handleExistOp(existFunc func(string) int, expectedArgs int) commandHandler {
	return func(parts []string, conn net.Conn, kv *server.KVServer, rf *raft.Raft, timer *time.Timer) {
		if len(parts) != expectedArgs {
			conn.Write([]byte("ERR invalid " + parts[0] + " command\n"))
			return
		}

		ret := existFunc(parts[1])
		if ret == 0 {
			conn.Write([]byte("Exist\n"))
		} else {
			conn.Write([]byte("nil\n"))
		}
	}
}

// Leader命令处理函数
func handleLeaderCommand(parts []string, conn net.Conn, kv *server.KVServer, rf *raft.Raft, timer *time.Timer) {
	currentTerm, isLeader := kv.GetRaft().GetState()
	fmt.Printf("当前状态: %v, 当前任期: %d\n", isLeader, currentTerm)
	if isLeader {
		conn.Write([]byte("isLeader\n"))
	} else {
		conn.Write([]byte("isCluster\n"))
	}
}

// Num命令处理函数
func handleNumCommand(parts []string, conn net.Conn, kv *server.KVServer, rf *raft.Raft, timer *time.Timer) {
	num := kv.GetSetCounter()
	str := strconv.FormatInt(num, 10)
	conn.Write([]byte(str + "\n"))
}

func handleConnection(kv *server.KVServer, conn net.Conn) {
	rf := kv.GetRaft()
	quitCh := make(chan struct{})
	defer func() {
		fmt.Printf("Closing connection from %s\n", conn.RemoteAddr())
		for i := 0; i < 3; i++ {
			flushBatch(kv, conn, rf)
			time.Sleep(10 * time.Millisecond)
		}
		close(quitCh)
		batchMutex.Lock()
		delete(batchBufferMap, conn)
		batchMutex.Unlock()
		conn.Close()
	}()
	reader := bufio.NewReader(conn)

	timer := time.NewTimer(batchTimeout)
	defer timer.Stop()

	go func() {
		for {
			select {
			case <-timer.C:
				flushBatch(kv, conn, rf)
				timer.Reset(batchTimeout)
			case <-quitCh:
				return
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
		if handler, exists := commandHandlers[action]; exists {
			handler(parts, conn, kv, rf, timer)
		} else {
			conn.Write([]byte("Unknown command\n"))
		}
	}
}
