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
	batchBufferMap  = make(map[net.Conn][]server.Op) // Connection-specific buffers
	batchMutex      sync.Mutex                       // Protects batchBufferMap
	batchSize       = 5000                           // Batch size threshold
	batchTimeout    = 5000 * time.Millisecond          // Timeout for flushing
)

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

	kv.Lock()
	notifyCh := kv.GetNotifyChannel(index)
	kv.Unlock()

	select {
	case <-notifyCh:
		conn.Write([]byte("BATCH_OK\n"))
	case <-time.After(2 * time.Second):
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
	batch, exists := batchBufferMap[conn]
	if exists && len(batch) > 0 {
		delete(batchBufferMap, conn) // Remove the batch from the map
		batchMutex.Unlock()
		sendBatch(kv, conn, batch, rf)
	} else {
		batchMutex.Unlock()
	}
}

// addToBatch adds an operation to the batch for a connection
func addToBatch(op server.Op, conn net.Conn, kv *server.KVServer, rf *raft.Raft) {
	batchMutex.Lock()
	defer batchMutex.Unlock()

	// Initialize batch for this connection if it doesn't exist
	if _, exists := batchBufferMap[conn]; !exists {
		batchBufferMap[conn] = make([]server.Op, 0, batchSize)
	}

	batchBufferMap[conn] = append(batchBufferMap[conn], op)

	// Check if we've reached batch size threshold
	if len(batchBufferMap[conn]) >= batchSize {
		batch := make([]server.Op, len(batchBufferMap[conn]))
		copy(batch, batchBufferMap[conn])
		delete(batchBufferMap, conn)
		go sendBatch(kv, conn, batch, rf)
	}
}

// Commited handles operation commitment with batching
func Commited(optype server.OperationType, key string, value string, conn net.Conn, kv *server.KVServer, rf *raft.Raft) {
	if _, isLeader := rf.GetState(); !isLeader {
		conn.Write([]byte("NOT_LEADER\n"))
		return
	}

	op := server.Op{
		OpType:   optype,
		Key:      key,
		Value:    value,
		ClientId: generateClientID(conn),
		SeqId:    time.Now().UnixNano(),
	}

	addToBatch(op, conn, kv, rf)
	conn.Write([]byte("ACK\n"))
}

// handleConnection processes TCP connections
func handleConnection(kv *server.KVServer, conn net.Conn) {
	defer func() {
		fmt.Printf("Closing connection from %s\n", conn.RemoteAddr())
		// Flush any remaining operations before closing
		rf := kv.GetRaft()
		flushBatch(kv, conn, rf)
		conn.Close()
	}()

	rf := kv.GetRaft()
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
			if err == io.EOF{
				return
			}else{
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
            value := bridge.Array_Count() / 3
			str := strconv.Itoa(value)
            if len(str) != 0 {
                conn.Write([]byte(str + "\n"))
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

		default:
			conn.Write([]byte("Unknown command\n"))
		}
	}
}