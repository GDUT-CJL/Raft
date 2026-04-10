package mnet

import (
	"bufio"
	"bytes"
	"course/bridge"
	"course/raft"
	"course/server"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Commited 处理写操作
func Commited(optype raft.OperationType, key string, klen int, value string, vlen int, conn net.Conn, kv *server.KVServer,
	rf *raft.Raft, timer *time.Timer, safeWrite func([]byte)) {
	CommitedBatch(optype, key, klen, value, vlen, conn, kv, rf, safeWrite)
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

// handleConnection 处理客户端连接
func handleConnection(kv *server.KVServer, conn net.Conn) {
	handleConnectionImpl(kv, conn)
}

func handleConnectionImpl(kv *server.KVServer, conn net.Conn) {
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
