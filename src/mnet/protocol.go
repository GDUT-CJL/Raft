package mnet

import (
	"bufio"
	"bytes"
	"course/bridge"
	"course/config"
	"course/raft"
	"course/server"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 批量管理器结构体
type BatchManager struct {
	batchSize      int
	batchTimeout   time.Duration
	batchBufferMap map[net.Conn][]raft.Op
	batchChanMap   map[net.Conn]chan string // 新增：每个连接的响应通道
	batchMutex     *sync.RWMutex
}

var (
	// 全局批量管理器实例
	globalBatchManager *BatchManager

	// 原子计数器
	clientIDCounter int64
	seqIDCounter    int64
)

// 初始化批量管理器
func InitBatchManager(batchSize int, batchTimeout time.Duration) {
	globalBatchManager = &BatchManager{
		batchSize:      batchSize,
		batchTimeout:   batchTimeout,
		batchBufferMap: make(map[net.Conn][]raft.Op),
		batchChanMap:   make(map[net.Conn]chan string), // 初始化响应通道map
		batchMutex:     &sync.RWMutex{},
	}
}

// 获取批量管理器实例
func GetBatchManager() *BatchManager {
	return globalBatchManager
}

// 获取批量大小
func (bm *BatchManager) GetBatchSize() int {
	return bm.batchSize
}

// 获取批量超时时间
func (bm *BatchManager) GetBatchTimeout() time.Duration {
	return bm.batchTimeout
}

// 动态更新批量参数
func (bm *BatchManager) UpdateBatchParams(batchSize int, batchTimeout time.Duration) {
	bm.batchMutex.Lock()
	defer bm.batchMutex.Unlock()

	bm.batchSize = batchSize
	bm.batchTimeout = batchTimeout
}

// 基于时间戳生成序列ID（毫秒级）
func generateSeqID() int64 {
	now := time.Now().UnixNano() / 1e6               // 毫秒时间戳
	seq := atomic.AddInt64(&seqIDCounter, 1) & 0xFFF // 12位序列号
	return (now << 12) | seq                         // 时间戳左移12位，低12位为序列号
}

// 基于时间生成客户端ID
func generateClientID(conn net.Conn) int64 {
	addr := conn.RemoteAddr().(*net.TCPAddr).IP.String()
	hash := fnv.New64a()
	hash.Write([]byte(addr))

	now := time.Now().UnixNano() / 1e6                         // 毫秒时间戳
	clientSeq := atomic.AddInt64(&clientIDCounter, 1) & 0xFFFF // 16位序列号

	return now ^ (int64(hash.Sum64()) & 0xFFFF) ^ (clientSeq << 32)
}

// 修改sendBatch函数，不直接写响应
func sendBatch(kv *server.KVServer, conn net.Conn, batch []raft.Op, rf *raft.Raft, responseChan chan string) {
	if len(batch) == 0 {
		responseChan <- "EMPTY_BATCH"
		return
	}

	index, _, isLeader := rf.Start(batch)
	if !isLeader {
		responseChan <- "NOT_LEADER"
		return
	}

	notifyCh := kv.GetNotifyChannel(index)

	select {
	case <-notifyCh:
		responseChan <- "OK" // 成功响应

	case <-time.After(30 * time.Second):
		responseChan <- "TIMEOUT"
		//fmt.Printf("[BATCH_TIMEOUT] Batch of %d operations timeout\n", len(batch))
	}

	kv.RemoveNotifyChannel(index)
}

// flushBatch 修改，使用响应通道
func flushBatch(kv *server.KVServer, conn net.Conn, rf *raft.Raft, responseChan chan string) {
	bm := GetBatchManager()
	bm.batchMutex.Lock()
	defer bm.batchMutex.Unlock()

	batch := bm.batchBufferMap[conn]
	if len(batch) > 0 {
		batchToSend := make([]raft.Op, len(batch))
		copy(batchToSend, batch)
		bm.batchBufferMap[conn] = nil

		// 发送批量，并将响应发送到通道
		go sendBatch(kv, conn, batchToSend, rf, responseChan)
	}
}

// addToBatch 修改，返回响应通道
func addToBatch(op raft.Op, conn net.Conn, kv *server.KVServer, rf *raft.Raft) chan string {
	bm := GetBatchManager()
	bm.batchMutex.Lock()
	defer bm.batchMutex.Unlock()

	// 确保连接有响应通道
	if _, exists := bm.batchChanMap[conn]; !exists {
		bm.batchChanMap[conn] = make(chan string, 100) // 带缓冲的通道
	}

	responseChan := bm.batchChanMap[conn]

	if _, exists := bm.batchBufferMap[conn]; !exists {
		bm.batchBufferMap[conn] = make([]raft.Op, 0, bm.batchSize)
	}

	bm.batchBufferMap[conn] = append(bm.batchBufferMap[conn], op)

	// 如果达到批量大小，立即刷新
	if len(bm.batchBufferMap[conn]) >= bm.batchSize {
		batch := make([]raft.Op, len(bm.batchBufferMap[conn]))
		copy(batch, bm.batchBufferMap[conn])
		bm.batchBufferMap[conn] = nil

		// 启动批量发送goroutine
		go func(batch []raft.Op, ch chan string) {
			sendBatch(kv, conn, batch, rf, ch)
		}(batch, responseChan)
	}

	return responseChan
}

var opPool = sync.Pool{
	New: func() interface{} {
		return &raft.Op{}
	},
}

// 修改Commited函数，统一返回响应
func Commited(optype raft.OperationType, key string, klen int, value string, vlen int, conn net.Conn, kv *server.KVServer,
	rf *raft.Raft, timer *time.Timer, safeWrite func([]byte)) {

	// 根据配置选择使用优化版本还是原版本
	if config.IsUseOptimizedVersion() {
		CommitedOptimized(optype, key, klen, value, vlen, conn, kv, rf, safeWrite)
		return
	}

	// 以下是原版本实现
	commitedLegacy(optype, key, klen, value, vlen, conn, kv, rf, timer, safeWrite)
}

func commitedLegacy(optype raft.OperationType, key string, klen int, value string, vlen int, conn net.Conn, kv *server.KVServer,
	rf *raft.Raft, timer *time.Timer, safeWrite func([]byte)) {

	// 检查是否是leader
	if _, isLeader := rf.GetState(); !isLeader {
		// 使用RESP协议格式返回错误
		safeWrite([]byte("-ERR LeaderIP is " + rf.LeaderIP + "\r\n"))
		return
	}

	// 从对象池获取Op
	op := opPool.Get().(*raft.Op)
	op.OpType = optype
	op.Key = key
	op.Klen = klen
	op.Value = value
	op.Vlen = vlen
	op.ClientId = generateClientID(conn)
	op.SeqId = generateSeqID()

	// 添加到批量并获取响应通道
	responseChan := addToBatch(*op, conn, kv, rf)

	// 放回对象池
	opPool.Put(op)

	// Reset timer
	bm := GetBatchManager()
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(bm.GetBatchTimeout())

	// 等待批量响应
	go func() {
		// 设置响应超时
		timeout := time.After(35 * time.Second)

		select {
		case response := <-responseChan:
			// 根据批量响应返回结果
			switch response {
			case "OK":
				safeWrite([]byte("+OK\r\n"))
			case "NOT_LEADER":
				safeWrite([]byte("-ERR Not leader\r\n"))
			case "TIMEOUT":
				safeWrite([]byte("-ERR Timeout\r\n"))
			case "EMPTY_BATCH":
				safeWrite([]byte("+OK\r\n")) // 空批次视为成功
			default:
				safeWrite([]byte(fmt.Sprintf("-ERR %s\r\n", response)))
			}

		case <-timeout:
			safeWrite([]byte("-ERR Batch timeout\r\n"))
		}
	}()
}

// 修改flushBatch的包装函数，用于定时器触发
func flushBatchWithResponse(kv *server.KVServer, conn net.Conn, rf *raft.Raft, safeWrite func([]byte)) {
	bm := GetBatchManager()
	bm.batchMutex.Lock()

	// 获取该连接的响应通道
	responseChan, exists := bm.batchChanMap[conn]
	if !exists {
		bm.batchMutex.Unlock()
		return
	}

	batch := bm.batchBufferMap[conn]
	if len(batch) == 0 {
		bm.batchMutex.Unlock()
		return
	}

	batchToSend := make([]raft.Op, len(batch))
	copy(batchToSend, batch)
	bm.batchBufferMap[conn] = nil
	bm.batchMutex.Unlock()

	// 发送批量
	go sendBatch(kv, conn, batchToSend, rf, responseChan)
}

// 解析RESP协议
func parseRESP(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimSpace(line)
	if len(line) == 0 {
		return nil, fmt.Errorf("empty line")
	}

	// 检查是否是数组类型
	if line[0] != '*' {
		return nil, fmt.Errorf("not an array type")
	}

	// 解析数组元素个数
	arraySize, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array size: %v", err)
	}

	if arraySize <= 0 {
		return nil, fmt.Errorf("invalid array size: %d", arraySize)
	}

	var parts []string

	// 解析每个元素
	for i := 0; i < arraySize; i++ {
		// 读取长度行
		lengthLine, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		lengthLine = strings.TrimSpace(lengthLine)
		if len(lengthLine) == 0 || lengthLine[0] != '$' {
			return nil, fmt.Errorf("invalid bulk string header")
		}

		// 解析字符串长度
		strLength, err := strconv.Atoi(lengthLine[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid string length: %v", err)
		}

		if strLength < 0 {
			return nil, fmt.Errorf("negative string length")
		}

		// 读取实际字符串内容
		var buffer bytes.Buffer
		bytesRead := 0
		for bytesRead < strLength {
			chunk := make([]byte, strLength-bytesRead)
			n, err := reader.Read(chunk)
			if err != nil {
				return nil, err
			}
			buffer.Write(chunk[:n])
			bytesRead += n
		}

		// 读取结尾的\r\n
		crlf := make([]byte, 2)
		_, err = reader.Read(crlf)
		if err != nil {
			return nil, err
		}
		if crlf[0] != '\r' || crlf[1] != '\n' {
			return nil, fmt.Errorf("expected CRLF after bulk string (element %d), got [%d %d]", i, crlf[0], crlf[1])
		}
		parts = append(parts, buffer.String())
	}

	return parts, nil
}

// 统一响应格式函数
func sendRESPResponse(safeWrite func([]byte), respType string, data string) {
	switch respType {
	case "simple":
		safeWrite([]byte(fmt.Sprintf("+%s\r\n", data)))
	case "bulk":
		if data == "" {
			safeWrite([]byte("$-1\r\n")) // Redis nil
		} else {
			// len(data)-1代表去除结尾的空字符
			safeWrite([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(data)-1, data)))
		}
	case "error":
		safeWrite([]byte(fmt.Sprintf("-%s\r\n", data)))
	case "integer":
		safeWrite([]byte(fmt.Sprintf(":%s\r\n", data)))
	}
}

func checkLeaseRead(rf *raft.Raft, safeWrite func([]byte)) bool {
	// 检查是否是 leader
	_, isLeader := rf.GetState()
	if !isLeader {
		sendRESPResponse(safeWrite, "error", "ERR not leader")
		return false
	}

	// 检查租约是否有效
	if !rf.IsLeaseValid() {
		sendRESPResponse(safeWrite, "error", "ERR lease expired")
		return false
	}

	// 等待状态机应用到租约读取点
	readIndex := rf.GetLeaseReadIndex()
	if !rf.WaitForApplied(readIndex) {
		sendRESPResponse(safeWrite, "error", "ERR wait for apply timeout")
		return false
	}

	return true
}

// handleConnection 使用批量管理器
func handleConnection(kv *server.KVServer, conn net.Conn) {
	// 根据配置选择使用哪个版本
	if config.IsUseOptimizedVersion() {
		handleConnectionOptimized(kv, conn)
		return
	}

	// 以下是原版本实现
	handleConnectionLegacy(kv, conn)
}

func handleConnectionLegacy(kv *server.KVServer, conn net.Conn) {
	// 为每个连接创建专用的写入锁
	var writeMutex sync.Mutex
	writer := bufio.NewWriterSize(conn, 64*1024) // 使用带缓冲的writer

	// 包装写入函数，确保线程安全
	safeWrite := func(data []byte) {
		writeMutex.Lock()
		defer writeMutex.Unlock()
		writer.Write(data)
		writer.Flush() // 立即刷新缓冲区
	}

	rf := kv.GetRaft()
	quitCh := make(chan struct{})
	reader := bufio.NewReaderSize(conn, 128*1024)

	// 启动时清理该连接的旧状态
	bm := GetBatchManager()
	bm.batchMutex.Lock()
	delete(bm.batchBufferMap, conn)
	delete(bm.batchChanMap, conn)
	bm.batchMutex.Unlock()

	// Start a timer with configured timeout
	timer := time.NewTimer(bm.GetBatchTimeout())
	defer timer.Stop()

	// Goroutine to handle batch timeouts
	go func() {
		for {
			select {
			case <-timer.C:
				// 触发批量刷新
				flushBatchWithResponse(kv, conn, rf, safeWrite)
				timer.Reset(bm.GetBatchTimeout())
			case <-quitCh:
				// 连接关闭时清理资源
				bm.batchMutex.Lock()
				delete(bm.batchBufferMap, conn)
				delete(bm.batchChanMap, conn)
				bm.batchMutex.Unlock()
				return
			}
		}
	}()

	defer close(quitCh)

	for {
		// 尝试解析RESP协议
		parts, err := parseRESP(reader)
		var action string

		if err != nil {
			if err == io.EOF {
				conn.Close()
				break
			}
			// 关键修复：只要解析失败，就丢弃本次请求之后的所有残留数据
			// 避免下一次循环读到"半截"的 RESP 数据
			if reader.Buffered() > 0 {
				reader.Discard(reader.Buffered())
			}

			fmt.Printf("RESP parse error: %v\n", err)
			sendRESPResponse(safeWrite, "error", "ERR invalid RESP format")
			continue
		} else {
			// RESP解析成功
			if len(parts) < 1 {
				sendRESPResponse(safeWrite, "error", "Invalid command")
				continue
			}
			action = strings.ToUpper(parts[0])
		}

		switch action {
		// Array
		case "SET":
			if len(parts) != 3 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'set' command")
				continue
			}
			Commited(server.Set, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, timer, safeWrite)

		case "GET":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'get' command")
				continue
			}

			// 1. 如果当前是 Leader 且可以 Lease Read，则直接读本地状态机
			if ok := checkLeaseRead(rf, safeWrite); ok {
				// 直接读取本地状态机（线性一致，因为 Lease 保证了 Leader 未变更）
				value := bridge.Array_Get(parts[1], len(parts[1]))
				sendRESPResponse(safeWrite, "bulk", value)
			}
		case "COUNT":
			if len(parts) != 1 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'count' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				// 直接读取本地状态机（线性一致，因为 Lease 保证了 Leader 未变更）
				value := bridge.Array_Count()
				sendRESPResponse(safeWrite, "integer", strconv.Itoa(value))
			}

		case "DELETE":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'delete' command")
				continue
			}
			Commited(server.Delete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, timer, safeWrite)

		case "EXIST":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'exist' command")
				continue
			}
			ret := bridge.Array_Exist(parts[1], len(parts[1]))
			if ret == 0 {
				sendRESPResponse(safeWrite, "integer", "1")
			} else {
				sendRESPResponse(safeWrite, "integer", "0")
			}

		// 其他数据结构的处理类似，需要统一使用sendRESPResponse函数
		// 这里只修改Array相关，其他数据结构按相同模式修改
		// Hash
		case "HSET":
			if len(parts) != 3 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hset' command")
				continue
			}
			Commited(server.HSet, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, timer, safeWrite)

		case "HGET":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hget' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				// 直接读取本地状态机（线性一致，因为 Lease 保证了 Leader 未变更）
				value := bridge.Hash_Get(parts[1], len(parts[1]))
				sendRESPResponse(safeWrite, "bulk", value)
			}

		case "HCOUNT":
			if len(parts) != 1 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hcount' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				// 直接读取本地状态机（线性一致，因为 Lease 保证了 Leader 未变更）
				value := bridge.Hash_Count()
				sendRESPResponse(safeWrite, "integer", strconv.Itoa(value))
			}

		case "HDELETE":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hdelete' command")
				continue
			}
			Commited(server.HDelete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, timer, safeWrite)

		case "HEXIST":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hexist' command")
				continue
			}
			ret := bridge.Hash_Exist(parts[1], len(parts[1]))
			if ret == 0 {
				sendRESPResponse(safeWrite, "integer", "1")
			} else {
				sendRESPResponse(safeWrite, "integer", "0")
			}

		// RBTree
		case "RSET":
			if len(parts) != 3 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hset' command")
				continue
			}
			Commited(server.RSet, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, timer, safeWrite)

		case "RGET":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hget' command")
				continue
			}

			if ok := checkLeaseRead(rf, safeWrite); ok {
				// 直接读取本地状态机（线性一致，因为 Lease 保证了 Leader 未变更）
				value := bridge.RB_Get(parts[1], len(parts[1]))
				sendRESPResponse(safeWrite, "bulk", value)
			}

		case "RCOUNT":
			if len(parts) != 1 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hcount' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				// 直接读取本地状态机（线性一致，因为 Lease 保证了 Leader 未变更）
				value := bridge.RB_Count()
				sendRESPResponse(safeWrite, "integer", strconv.Itoa(value))
			}

		case "RDELETE":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hdelete' command")
				continue
			}
			Commited(server.HDelete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, timer, safeWrite)

		case "REXIST":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hexist' command")
				continue
			}
			ret := bridge.RB_Exist(parts[1], len(parts[1]))
			if ret == 0 {
				sendRESPResponse(safeWrite, "integer", "1")
			} else {
				sendRESPResponse(safeWrite, "integer", "0")
			}

		// BTree
		case "BSET":
			if len(parts) != 3 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hset' command")
				continue
			}
			Commited(server.BSet, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, timer, safeWrite)

		case "BGET":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hget' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				// 直接读取本地状态机（线性一致，因为 Lease 保证了 Leader 未变更）
				value := bridge.BTree_Get(parts[1], len(parts[1]))
				sendRESPResponse(safeWrite, "bulk", value)
			}

		case "BCOUNT":
			if len(parts) != 1 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hcount' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				// 直接读取本地状态机（线性一致，因为 Lease 保证了 Leader 未变更）
				value := bridge.BTree_Count()
				sendRESPResponse(safeWrite, "integer", strconv.Itoa(value))
			}

		case "BDELETE":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hdelete' command")
				continue
			}
			Commited(server.BDelete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, timer, safeWrite)

		case "BEXIST":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hexist' command")
				continue
			}
			ret := bridge.BTree_Exist(parts[1], len(parts[1]))
			if ret == 0 {
				sendRESPResponse(safeWrite, "integer", "1")
			} else {
				sendRESPResponse(safeWrite, "integer", "0")
			}

		// SkipList
		case "ZSET":
			if len(parts) != 3 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hset' command")
				continue
			}
			Commited(server.ZSet, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, timer, safeWrite)

		case "ZGET":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hget' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				// 直接读取本地状态机（线性一致，因为 Lease 保证了 Leader 未变更）
				value := bridge.Skiplist_Get(parts[1], len(parts[1]))
				sendRESPResponse(safeWrite, "bulk", value)
			}

		case "ZCOUNT":
			if len(parts) != 1 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hcount' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				// 直接读取本地状态机（线性一致，因为 Lease 保证了 Leader 未变更）
				value := bridge.Skiplist_Count()
				sendRESPResponse(safeWrite, "integer", strconv.Itoa(value))
			}

		case "ZDELETE":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hdelete' command")
				continue
			}
			Commited(server.ZDelete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, timer, safeWrite)

		case "ZEXIST":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hexist' command")
				continue
			}
			ret := bridge.Skiplist_Exist(parts[1], len(parts[1]))
			if ret == 0 {
				sendRESPResponse(safeWrite, "integer", "1")
			} else {
				sendRESPResponse(safeWrite, "integer", "0")
			}

		// ROCKSDB
		case "RCSET":
			if len(parts) != 3 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hset' command")
				continue
			}
			Commited(server.RCSet, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, timer, safeWrite)
		case "RCGET":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hget' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				// 直接读取本地状态机（线性一致，因为 Lease 保证了 Leader 未变更）
				value := bridge.RC_Get(parts[1], len(parts[1]))
				sendRESPResponse(safeWrite, "bulk", value)
			}

		case "RCCOUNT":
			if len(parts) != 1 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hcount' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				// 直接读取本地状态机（线性一致，因为 Lease 保证了 Leader 未变更）
				value := bridge.RC_Count()
				sendRESPResponse(safeWrite, "integer", strconv.Itoa(value))
			}

		case "RCDELETE":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hdelete' command")
				continue
			}
			Commited(server.RCDelete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, timer, safeWrite)

		case "RCEXIST":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hexist' command")
				continue
			}
			ret := bridge.RC_Exist(parts[1], len(parts[1]))
			if ret == 0 {
				sendRESPResponse(safeWrite, "integer", "1")
			} else {
				sendRESPResponse(safeWrite, "integer", "0")
			}
		case "LEADER":
			if len(parts) != 1 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hget' command")
				continue
			}
			sendRESPResponse(safeWrite, "bulk", rf.LeaderIP)
		default:
			sendRESPResponse(safeWrite, "error", fmt.Sprintf("ERR unknown command '%s'", action))
		}
	}
}

func handleConnectionOptimized(kv *server.KVServer, conn net.Conn) {
	var writeMutex sync.Mutex
	writer := bufio.NewWriterSize(conn, 64*1024)

	safeWrite := func(data []byte) {
		writeMutex.Lock()
		defer writeMutex.Unlock()
		writer.Write(data)
		writer.Flush()
	}

	rf := kv.GetRaft()
	reader := bufio.NewReaderSize(conn, 128*1024)

	for {
		parts, err := parseRESP(reader)
		var action string

		if err != nil {
			if err == io.EOF {
				conn.Close()
				break
			}
			if reader.Buffered() > 0 {
				reader.Discard(reader.Buffered())
			}

			fmt.Printf("RESP parse error: %v\n", err)
			sendRESPResponse(safeWrite, "error", "ERR invalid RESP format")
			continue
		} else {
			if len(parts) < 1 {
				sendRESPResponse(safeWrite, "error", "Invalid command")
				continue
			}
			action = strings.ToUpper(parts[0])
		}

		switch action {
		case "SET":
			if len(parts) != 3 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'set' command")
				continue
			}
			Commited(server.Set, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, nil, safeWrite)

		case "GET":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'get' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				value := bridge.Array_Get(parts[1], len(parts[1]))
				sendRESPResponse(safeWrite, "bulk", value)
			}

		case "COUNT":
			if len(parts) != 1 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'count' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				value := bridge.Array_Count()
				sendRESPResponse(safeWrite, "integer", strconv.Itoa(value))
			}

		case "DELETE":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'delete' command")
				continue
			}
			Commited(server.Delete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, nil, safeWrite)

		case "EXIST":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'exist' command")
				continue
			}
			ret := bridge.Array_Exist(parts[1], len(parts[1]))
			if ret == 0 {
				sendRESPResponse(safeWrite, "integer", "1")
			} else {
				sendRESPResponse(safeWrite, "integer", "0")
			}

		case "HSET":
			if len(parts) != 3 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hset' command")
				continue
			}
			Commited(server.HSet, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, nil, safeWrite)

		case "HGET":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hget' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				value := bridge.Hash_Get(parts[1], len(parts[1]))
				sendRESPResponse(safeWrite, "bulk", value)
			}

		case "HCOUNT":
			if len(parts) != 1 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hcount' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				value := bridge.Hash_Count()
				sendRESPResponse(safeWrite, "integer", strconv.Itoa(value))
			}

		case "HDELETE":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hdelete' command")
				continue
			}
			Commited(server.HDelete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, nil, safeWrite)

		case "HEXIST":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'hexist' command")
				continue
			}
			ret := bridge.Hash_Exist(parts[1], len(parts[1]))
			if ret == 0 {
				sendRESPResponse(safeWrite, "integer", "1")
			} else {
				sendRESPResponse(safeWrite, "integer", "0")
			}

		case "RSET":
			if len(parts) != 3 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'rset' command")
				continue
			}
			Commited(server.RSet, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, nil, safeWrite)

		case "RGET":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'rget' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				value := bridge.RB_Get(parts[1], len(parts[1]))
				sendRESPResponse(safeWrite, "bulk", value)
			}

		case "RCOUNT":
			if len(parts) != 1 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'rcount' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				value := bridge.RB_Count()
				sendRESPResponse(safeWrite, "integer", strconv.Itoa(value))
			}

		case "RDELETE":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'rdelete' command")
				continue
			}
			Commited(server.RDelete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, nil, safeWrite)

		case "REXIST":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'rexist' command")
				continue
			}
			ret := bridge.RB_Exist(parts[1], len(parts[1]))
			if ret == 0 {
				sendRESPResponse(safeWrite, "integer", "1")
			} else {
				sendRESPResponse(safeWrite, "integer", "0")
			}

		case "BSET":
			if len(parts) != 3 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'bset' command")
				continue
			}
			Commited(server.BSet, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, nil, safeWrite)

		case "BGET":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'bget' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				value := bridge.BTree_Get(parts[1], len(parts[1]))
				sendRESPResponse(safeWrite, "bulk", value)
			}

		case "BCOUNT":
			if len(parts) != 1 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'bcount' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				value := bridge.BTree_Count()
				sendRESPResponse(safeWrite, "integer", strconv.Itoa(value))
			}

		case "BDELETE":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'bdelete' command")
				continue
			}
			Commited(server.BDelete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, nil, safeWrite)

		case "BEXIST":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'bexist' command")
				continue
			}
			ret := bridge.BTree_Exist(parts[1], len(parts[1]))
			if ret == 0 {
				sendRESPResponse(safeWrite, "integer", "1")
			} else {
				sendRESPResponse(safeWrite, "integer", "0")
			}

		case "ZSET":
			if len(parts) != 3 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'zset' command")
				continue
			}
			Commited(server.ZSet, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, nil, safeWrite)

		case "ZGET":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'zget' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				value := bridge.Skiplist_Get(parts[1], len(parts[1]))
				sendRESPResponse(safeWrite, "bulk", value)
			}

		case "ZCOUNT":
			if len(parts) != 1 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'zcount' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				value := bridge.Skiplist_Count()
				sendRESPResponse(safeWrite, "integer", strconv.Itoa(value))
			}

		case "ZDELETE":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'zdelete' command")
				continue
			}
			Commited(server.ZDelete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, nil, safeWrite)

		case "ZEXIST":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'zexist' command")
				continue
			}
			ret := bridge.Skiplist_Exist(parts[1], len(parts[1]))
			if ret == 0 {
				sendRESPResponse(safeWrite, "integer", "1")
			} else {
				sendRESPResponse(safeWrite, "integer", "0")
			}

		case "RCSET":
			if len(parts) != 3 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'rcset' command")
				continue
			}
			Commited(server.RCSet, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, nil, safeWrite)

		case "RCGET":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'rcget' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				value := bridge.RC_Get(parts[1], len(parts[1]))
				sendRESPResponse(safeWrite, "bulk", value)
			}

		case "RCCOUNT":
			if len(parts) != 1 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'rccount' command")
				continue
			}
			if ok := checkLeaseRead(rf, safeWrite); ok {
				value := bridge.RC_Count()
				sendRESPResponse(safeWrite, "integer", strconv.Itoa(value))
			}

		case "RCDELETE":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'rcdelete' command")
				continue
			}
			Commited(server.RCDelete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, nil, safeWrite)

		case "RCEXIST":
			if len(parts) != 2 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'rcexist' command")
				continue
			}
			ret := bridge.RC_Exist(parts[1], len(parts[1]))
			if ret == 0 {
				sendRESPResponse(safeWrite, "integer", "1")
			} else {
				sendRESPResponse(safeWrite, "integer", "0")
			}

		case "LEADER":
			if len(parts) != 1 {
				sendRESPResponse(safeWrite, "error", "ERR wrong number of arguments for 'leader' command")
				continue
			}
			sendRESPResponse(safeWrite, "bulk", rf.LeaderIP)

		default:
			sendRESPResponse(safeWrite, "error", fmt.Sprintf("ERR unknown command '%s'", action))
		}
	}
}
