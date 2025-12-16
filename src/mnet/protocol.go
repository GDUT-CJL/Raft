package mnet

import (
	"bufio"
	"bytes"
	"course/bridge"
	"course/raft"
	"course/server"
	"encoding/json"
	"fmt"
	"hash/fnv"
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

func sendBatch(kv *server.KVServer, conn net.Conn, batch []raft.Op, rf *raft.Raft, safeWrite func([]byte)) {
	if len(batch) == 0 {
		safeWrite([]byte("EMPTY_BATCH\n"))
		return
	}

	index, _, isLeader := rf.Start(batch)
	if !isLeader {
		safeWrite([]byte("NOT_LEADER\n"))
		return
	}

	notifyCh := kv.GetNotifyChannel(index)

	select {
	case <-notifyCh:
		// 提交成功后，验证数据是否真的写入
		if verifyBatchApplied(batch) {
			safeWrite([]byte("BATCH_OK\n"))
			//fmt.Printf("[BATCH_SUCCESS] Batch of %d operations applied successfully\n", len(batch))
		} else {
			safeWrite([]byte("BATCH_APPLY_FAILED\n"))
			//fmt.Printf("[BATCH_FAILED] Batch of %d operations failed to apply\n", len(batch))
		}

	case <-time.After(30 * time.Second):
		safeWrite([]byte("BATCH_TIMEOUT\n"))
		//fmt.Printf("[BATCH_TIMEOUT] Batch of %d operations timeout\n", len(batch))
	}

	kv.RemoveNotifyChannel(index)
}

// 验证批量操作是否真的应用了
func verifyBatchApplied(batch []raft.Op) bool {
	for _, op := range batch {
		switch op.OpType {
		case server.Set, server.RSet, server.HSet, server.BSet, server.ZSet, server.RCSet:
			// 验证键是否存在
			if !verifyKeyExists(op.Key) {
				fmt.Printf("[VERIFY_FAILED] Key not found after write: %s\n", op.Key)
				return false
			}
		}
	}
	return true
}

func verifyKeyExists(key string) bool {
	// 根据当前数据结构验证键是否存在
	// 这里需要根据您的实际情况实现
	value := bridge.RB_Get(key, len(key))
	return len(value) != 0
}

// flushBatch 修改为使用批量管理器
func flushBatch(kv *server.KVServer, conn net.Conn, rf *raft.Raft, safeWrite func([]byte)) {
	bm := GetBatchManager()
	bm.batchMutex.Lock()
	defer bm.batchMutex.Unlock()

	batch := bm.batchBufferMap[conn]
	if len(batch) > 0 {
		batchToSend := make([]raft.Op, len(batch))
		copy(batchToSend, batch)
		bm.batchBufferMap[conn] = nil
		go sendBatch(kv, conn, batchToSend, rf, safeWrite)
	}
}

// addToBatch 修改为使用批量管理器
func addToBatch(op raft.Op, conn net.Conn, kv *server.KVServer, rf *raft.Raft, safeWrite func([]byte)) {
	bm := GetBatchManager()
	bm.batchMutex.Lock()
	defer bm.batchMutex.Unlock()

	if _, exists := bm.batchBufferMap[conn]; !exists {
		bm.batchBufferMap[conn] = make([]raft.Op, 0, bm.batchSize)
	}

	bm.batchBufferMap[conn] = append(bm.batchBufferMap[conn], op)

	if len(bm.batchBufferMap[conn]) >= bm.batchSize {
		batch := make([]raft.Op, len(bm.batchBufferMap[conn]))
		copy(batch, bm.batchBufferMap[conn])
		bm.batchBufferMap[conn] = nil
		go sendBatch(kv, conn, batch, rf, safeWrite)
	}
}

var opPool = sync.Pool{
	New: func() interface{} {
		return &raft.Op{}
	},
}

// Commited 函数修改为使用批量管理器
func Commited(optype raft.OperationType, key string, klen int, value string, vlen int, conn net.Conn, kv *server.KVServer,
	rf *raft.Raft, timer *time.Timer, safeWrite func([]byte)) {

	if _, isLeader := rf.GetState(); !isLeader {
		safeWrite([]byte("LeaderIP is  " + rf.LeaderIP + "\n"))
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

	addToBatch(*op, conn, kv, rf, safeWrite)

	// 放回对象池
	opPool.Put(op)

	safeWrite([]byte("ACK\n"))

	// Reset timer
	bm := GetBatchManager()
	if !timer.Stop() {
		<-timer.C
	}
	timer.Reset(bm.GetBatchTimeout())
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

		parts = append(parts, buffer.String())
	}

	return parts, nil
}

// handleConnection 使用批量管理器
func handleConnection(kv *server.KVServer, conn net.Conn) {
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
	defer func() {
		for i := 0; i < 3; i++ {
			flushBatch(kv, conn, rf, safeWrite)
			time.Sleep(50 * time.Millisecond)
		}
		close(quitCh)

		bm := GetBatchManager()
		bm.batchMutex.Lock()
		delete(bm.batchBufferMap, conn)
		bm.batchMutex.Unlock()
		conn.Close()
	}()
	reader := bufio.NewReaderSize(conn, 128*1024)

	// Start a timer with configured timeout
	bm := GetBatchManager()
	timer := time.NewTimer(bm.GetBatchTimeout())
	defer timer.Stop()

	// Goroutine to handle batch timeouts
	go func() {
		for {
			select {
			case <-timer.C:
				flushBatch(kv, conn, rf, safeWrite)
				timer.Reset(bm.GetBatchTimeout())
			case <-quitCh:
				return
			}
		}
	}()
	for {
		// 尝试解析RESP协议
		parts, err := parseRESP(reader)
		var action string

		if err != nil {
			// 直接关闭连接或返回错误，不要回退到文本协议
			safeWrite([]byte("ERR invalid RESP format\n"))
			continue
		} else {
			// RESP解析成功
			if len(parts) < 1 {
				safeWrite([]byte("Invalid command\n"))
				continue
			}
			action = strings.ToUpper(parts[0])
			//debugPrintParts(parts)
		}

		switch action {
		// Array
		case "SET":
			if len(parts) != 3 {
				safeWrite([]byte("Invalid SET command\n"))
				continue
			}
			Commited(server.Set, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, timer, safeWrite)

		case "GET":
			if len(parts) != 2 {
				safeWrite([]byte("ERR invalid GET command\n"))
				continue
			}
			value := bridge.Array_Get(parts[1], len(parts[1]))
			if len(value) != 0 {
				safeWrite([]byte(value + "\n"))
			} else {
				safeWrite([]byte("nil\n"))
			}
		case "COUNT":
			if len(parts) != 1 {
				safeWrite([]byte("ERR invalid COUNT command\n"))
				continue
			}
			value := bridge.Array_Count()

			str := strconv.Itoa(value)
			if len(str) != 0 {
				safeWrite([]byte(str + "\n"))
			} else {
				safeWrite([]byte("nil\n"))
			}
		case "DELETE":
			if len(parts) != 2 {
				safeWrite([]byte("Invalid DELETE command\n"))
				continue
			}
			Commited(server.Delete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, timer, safeWrite)

		case "EXIST":
			if len(parts) != 2 {
				safeWrite([]byte("ERR invalid EXIST command\n"))
				continue
			}
			ret := bridge.Array_Exist(parts[1], len(parts[1]))
			if ret == 0 {
				safeWrite([]byte("Exist\n"))
			} else {
				safeWrite([]byte("nil\n"))
			}

		// Hash
		case "HSET":
			if len(parts) != 3 {
				safeWrite([]byte("Invalid HSET command\n"))
				continue
			}
			Commited(server.HSet, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, timer, safeWrite)

		case "HGET":
			if len(parts) != 2 {
				safeWrite([]byte("ERR invalid HGET command\n"))
				continue
			}
			value := bridge.Hash_Get(parts[1], len(parts[1]))
			if len(value) != 0 {
				safeWrite([]byte(value + "\n"))
			} else {
				safeWrite([]byte("nil\n"))
			}
		case "HCOUNT":
			if len(parts) != 1 {
				safeWrite([]byte("ERR invalid HCOUNT command\n"))
				continue
			}
			value := bridge.Hash_Count()
			str := strconv.Itoa(value)
			if len(str) != 0 {
				safeWrite([]byte(str + "\n"))
			} else {
				safeWrite([]byte("nil\n"))
			}
		case "HDELETE":
			if len(parts) != 2 {
				safeWrite([]byte("Invalid HDELETE command\n"))
				continue
			}
			Commited(server.HDelete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, timer, safeWrite)

		case "HEXIST":
			if len(parts) != 2 {
				safeWrite([]byte("ERR invalid HEXIST command\n"))
				continue
			}
			ret := bridge.Hash_Exist(parts[1], len(parts[1]))
			if ret == 0 {
				safeWrite([]byte("Exist\n"))
			} else {
				safeWrite([]byte("nil\n"))
			}
		// RBTree
		case "RSET":
			if len(parts) != 3 {
				safeWrite([]byte("Invalid RSET command\n"))
				continue
			}
			Commited(server.RSet, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, timer, safeWrite)

		case "RGET":
			if len(parts) != 2 {
				safeWrite([]byte("ERR invalid RGET command\n"))
				continue
			}
			value := bridge.RB_Get(parts[1], len(parts[1]))
			if len(value) != 0 {
				safeWrite([]byte(value + "\n"))
			} else {
				safeWrite([]byte("nil\n"))
			}
		case "RCOUNT":
			if len(parts) != 1 {
				safeWrite([]byte("ERR invalid RCOUNT command\n"))
				continue
			}
			value := bridge.RB_Count()
			str := strconv.Itoa(value)
			if len(str) != 0 {
				safeWrite([]byte(str + "\n"))
			} else {
				safeWrite([]byte("nil\n"))
			}
		case "RDELETE":
			if len(parts) != 2 {
				safeWrite([]byte("Invalid RDELETE command\n"))
				continue
			}
			Commited(server.RDelete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, timer, safeWrite)

		case "REXIST":
			if len(parts) != 2 {
				safeWrite([]byte("ERR invalid REXIST command\n"))
				continue
			}
			ret := bridge.RB_Exist(parts[1], len(parts[1]))
			if ret == 0 {
				safeWrite([]byte("Exist\n"))
			} else {
				safeWrite([]byte("nil\n"))
			}

		// BTree
		case "BSET":
			if len(parts) != 3 {
				safeWrite([]byte("Invalid BSET command\n"))
				continue
			}
			Commited(server.BSet, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, timer, safeWrite)

		case "BGET":
			if len(parts) != 2 {
				safeWrite([]byte("ERR invalid BGET command\n"))
				continue
			}
			value := bridge.BTree_Get(parts[1], len(parts[1]))
			if len(value) != 0 {
				safeWrite([]byte(value + "\n"))
			} else {
				safeWrite([]byte("nil\n"))
			}
		case "BCOUNT":
			if len(parts) != 1 {
				safeWrite([]byte("ERR invalid BCOUNT command\n"))
				continue
			}
			value := bridge.BTree_Count()
			str := strconv.Itoa(value)
			if len(str) != 0 {
				safeWrite([]byte(str + "\n"))
			} else {
				safeWrite([]byte("nil\n"))
			}
		case "BDELETE":
			if len(parts) != 2 {
				safeWrite([]byte("Invalid BDELETE command\n"))
				continue
			}
			Commited(server.BDelete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, timer, safeWrite)

		case "BEXIST":
			if len(parts) != 2 {
				safeWrite([]byte("ERR invalid BEXIST command\n"))
				continue
			}
			ret := bridge.BTree_Exist(parts[1], len(parts[1]))
			if ret == 0 {
				safeWrite([]byte("Exist\n"))
			} else {
				safeWrite([]byte("nil\n"))
			}
		//Skiplist
		case "ZSET":
			if len(parts) != 3 {
				safeWrite([]byte("Invalid ZSET command\n"))
				continue
			}
			Commited(server.ZSet, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, timer, safeWrite)

		case "ZGET":
			if len(parts) != 2 {
				safeWrite([]byte("ERR invalid ZGET command\n"))
				continue
			}
			value := bridge.Skiplist_Get(parts[1], len(parts[1]))
			if len(value) != 0 {
				safeWrite([]byte(value + "\n"))
			} else {
				safeWrite([]byte("nil\n"))
			}
		case "ZCOUNT":
			if len(parts) != 1 {
				safeWrite([]byte("ERR invalid ZCOUNT command\n"))
				continue
			}
			value := bridge.Skiplist_Count()

			str := strconv.Itoa(value)
			if len(str) != 0 {
				safeWrite([]byte(str + "\n"))
			} else {
				safeWrite([]byte("nil\n"))
			}
		case "ZDELETE":
			if len(parts) != 2 {
				safeWrite([]byte("Invalid ZDELETE command\n"))
				continue
			}
			Commited(server.ZDelete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, timer, safeWrite)

		case "ZEXIST":
			if len(parts) != 2 {
				safeWrite([]byte("ERR invalid ZEXIST command\n"))
				continue
			}
			ret := bridge.Skiplist_Exist(parts[1], len(parts[1]))
			if ret == 0 {
				safeWrite([]byte("Exist\n"))
			} else {
				safeWrite([]byte("nil\n"))
			}
		//Rocksdb
		case "RCSET":
			if len(parts) != 3 {
				safeWrite([]byte("Invalid RCSET command\n"))
				continue
			}
			Commited(server.RCSet, parts[1], len(parts[1]), parts[2], len(parts[2]), conn, kv, rf, timer, safeWrite)

		case "RCGET":
			if len(parts) != 2 {
				safeWrite([]byte("ERR invalid RCGET command\n"))
				continue
			}
			value := bridge.RC_Get(parts[1])
			if len(value) != 0 {
				safeWrite([]byte(value + "\n"))
			} else {
				safeWrite([]byte("nil\n"))
			}
		case "RCCOUNT":
			if len(parts) != 1 {
				safeWrite([]byte("ERR invalid RCCOUNT command\n"))
				continue
			}
			value := bridge.RC_Count()

			str := strconv.Itoa(value)
			if len(str) != 0 {
				safeWrite([]byte(str + "\n"))
			} else {
				safeWrite([]byte("nil\n"))
			}
		case "RCDELETE":
			if len(parts) != 2 {
				safeWrite([]byte("Invalid ZDELETE command\n"))
				continue
			}
			Commited(server.RCDelete, parts[1], len(parts[1]), parts[1], len(parts[1]), conn, kv, rf, timer, safeWrite)

		case "LEADER":
			currentTerm, isLeader := kv.GetRaft().GetState()
			fmt.Printf("当前状态: %v, 当前任期: %d\n", isLeader, currentTerm)
			if isLeader {
				safeWrite([]byte("isLeader\n"))
			} else {
				safeWrite([]byte("isCluster\n"))
			}
		// case "STATS":
		// 	stats := rf.GetPerformanceStats()
		// 	statsJSON, _ := json.Marshal(stats)
		// 	safeWrite([]byte(string(statsJSON) + "\n"))
		case "DET":
			rf := kv.GetRaft()
			state := rf.GetDetailedState()
			// 将map转换为JSON格式返回
			jsonData, err := json.MarshalIndent(state, "", "  ")
			if err != nil {
				safeWrite([]byte("Error marshaling state to JSON\n"))
			} else {
				safeWrite([]byte(string(jsonData) + "\n"))
			}

		case "LOG":
			rf := kv.GetRaft()
			state := rf.GetLogConsistencyState()
			jsonData, err := json.MarshalIndent(state, "", "  ")
			if err != nil {
				safeWrite([]byte("Error marshaling log state to JSON\n"))
			} else {
				safeWrite([]byte(string(jsonData) + "\n"))
			}

		case "STATS":
			rf := kv.GetRaft()
			performanceStats := rf.GetPerformanceStats()
			jsonData, err := json.MarshalIndent(performanceStats, "", "  ")
			if err != nil {
				safeWrite([]byte("Error marshaling stats to JSON\n"))
			} else {
				safeWrite([]byte(string(jsonData) + "\n"))
			}

		case "NUM":
			num := kv.GetSetCounter()
			str := strconv.FormatInt(num, 10)
			safeWrite([]byte(str + "\n"))
		default:
			safeWrite([]byte("Unknown command\n"))
		}
	}
}
