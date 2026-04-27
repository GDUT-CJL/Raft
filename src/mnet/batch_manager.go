package mnet

import (
	"course/raft"
	"course/server"
	"fmt"
	"hash/fnv"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	globalBatchManager *BatchManager
	bmMutex            sync.RWMutex

	clientIDCounter int64
	seqIDCounter    int64
)

type BatchRequest struct {
	Op      raft.Op
	Conn    net.Conn
	RespCh  chan *BatchResponse
	AddedAt time.Time
}

type BatchResponse struct {
	Success bool
	Error   string
	Value   string
}

type BatchManager struct {
	requestQueue  chan *BatchRequest
	batchSize     int32
	batchTimeout  time.Duration
	minBatchSize  int32
	maxBatchSize  int32
	kv            *server.KVServer
	rf            *raft.Raft
	wg            sync.WaitGroup
	stopCh        chan struct{}
	pendingOps    int64
	avgLatency    int64
	throughput    int64
	lastBatchTime time.Time
}

func SetBatchManager(bm *BatchManager) {
	bmMutex.Lock()
	defer bmMutex.Unlock()
	globalBatchManager = bm
}

func GetBatchManager() *BatchManager {
	bmMutex.RLock()
	defer bmMutex.RUnlock()
	return globalBatchManager
}

func NewBatchManager(kv *server.KVServer, rf *raft.Raft, initialBatchSize int, batchTimeout time.Duration) *BatchManager {
	bm := &BatchManager{
		requestQueue:  make(chan *BatchRequest, 10000),
		batchSize:     int32(initialBatchSize),
		batchTimeout:  batchTimeout,
		minBatchSize:  int32(initialBatchSize / 2),
		maxBatchSize:  int32(initialBatchSize * 4),
		kv:            kv,
		rf:            rf,
		stopCh:        make(chan struct{}),
		lastBatchTime: time.Now(),
	}

	bm.wg.Add(1)
	go bm.batchWorker()

	return bm
}

// 往全局队列中放入请求数据
func (bm *BatchManager) Submit(op raft.Op, conn net.Conn) *BatchResponse {
	respCh := make(chan *BatchResponse, 1)

	req := &BatchRequest{
		Op:      op,
		Conn:    conn,
		RespCh:  respCh,
		AddedAt: time.Now(),
	}

	select {
	case bm.requestQueue <- req:
		atomic.AddInt64(&bm.pendingOps, 1)
		return <-respCh
	default:
		return &BatchResponse{
			Success: false,
			Error:   "Queue full",
		}
	}
}

func (bm *BatchManager) batchWorker() {
	defer bm.wg.Done()

	batch := make([]*BatchRequest, 0, bm.maxBatchSize)
	timer := time.NewTimer(bm.batchTimeout)
	defer timer.Stop()

	for {
		select {
		case <-bm.stopCh:
			if len(batch) > 0 {
				bm.processBatch(batch)
			}
			return

		case req := <-bm.requestQueue:
			batch = append(batch, req)
			atomic.AddInt64(&bm.pendingOps, -1)

			if len(batch) >= int(atomic.LoadInt32(&bm.batchSize)) {
				bm.processBatch(batch)
				batch = make([]*BatchRequest, 0, bm.maxBatchSize)
				bm.adjustBatchSize()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(bm.batchTimeout)
			}

		case <-timer.C:
			if len(batch) > 0 {
				bm.processBatch(batch)
				batch = make([]*BatchRequest, 0, bm.maxBatchSize)
				bm.adjustBatchSize()
			}
			timer.Reset(bm.batchTimeout)
		}
	}
}

func (bm *BatchManager) processBatch(batch []*BatchRequest) {
	if len(batch) == 0 {
		return
	}

	startTime := time.Now()

	ops := make([]raft.Op, len(batch))
	for i, req := range batch {
		ops[i] = req.Op
	}

	index, _, isLeader := bm.rf.Start(ops)
	if !isLeader {
		for _, req := range batch {
			req.RespCh <- &BatchResponse{
				Success: false,
				Error:   "NOT_LEADER",
			}
		}
		return
	}

	notifyCh := bm.kv.GetNotifyChannel(index)

	select {
	case replies := <-notifyCh:
		for i, req := range batch {
			if i < len(replies) {
				req.RespCh <- &BatchResponse{
					Success: replies[i].Err == "OK" || replies[i].Err == "",
					Error:   replies[i].Err,
					Value:   string(rune(replies[i].Value)),
				}
			} else {
				req.RespCh <- &BatchResponse{
					Success: false,
					Error:   "Response mismatch",
				}
			}
		}

	case <-time.After(5 * time.Second):
		for _, req := range batch {
			req.RespCh <- &BatchResponse{
				Success: false,
				Error:   "TIMEOUT",
			}
		}
	}

	bm.kv.RemoveNotifyChannel(index)

	latency := time.Since(startTime).Milliseconds()
	atomic.StoreInt64(&bm.avgLatency, latency)
	atomic.AddInt64(&bm.throughput, int64(len(batch)))
	bm.lastBatchTime = time.Now()
}

func (bm *BatchManager) adjustBatchSize() {
	latency := atomic.LoadInt64(&bm.avgLatency)
	currentSize := atomic.LoadInt32(&bm.batchSize)

	if latency > 100 {
		newSize := currentSize * 9 / 10
		if newSize >= bm.minBatchSize {
			atomic.StoreInt32(&bm.batchSize, newSize)
		}
	} else if latency < 50 {
		newSize := currentSize * 11 / 10
		if newSize <= bm.maxBatchSize {
			atomic.StoreInt32(&bm.batchSize, newSize)
		}
	}
}

func (bm *BatchManager) GetStats() (int32, int64, int64) {
	return atomic.LoadInt32(&bm.batchSize),
		atomic.LoadInt64(&bm.avgLatency),
		atomic.LoadInt64(&bm.throughput)
}

func (bm *BatchManager) Stop() {
	close(bm.stopCh)
	bm.wg.Wait()
}

func CommitedBatch(optype raft.OperationType, key string, klen int, value string, vlen int, conn net.Conn, kv *server.KVServer,
	rf *raft.Raft, safeWrite func([]byte)) {

	if _, isLeader := rf.GetState(); !isLeader {
		safeWrite([]byte("-ERR LeaderIP is " + rf.LeaderIP + "\r\n"))
		return
	}

	bm := GetBatchManager()
	if bm == nil {
		safeWrite([]byte("-ERR Batch manager not initialized\r\n"))
		return
	}

	op := raft.Op{
		OpType:   optype,
		Key:      key,
		Klen:     klen,
		Value:    value,
		Vlen:     vlen,
		ClientId: generateClientID(conn),
		SeqId:    generateSeqID(),
	}

	response := bm.Submit(op, conn)

	if response.Success {
		safeWrite([]byte("+OK\r\n"))
	} else {
		safeWrite([]byte(fmt.Sprintf("-ERR %s\r\n", response.Error)))
	}
}

func generateSeqID() int64 {
	now := time.Now().UnixNano() / 1e6
	seq := atomic.AddInt64(&seqIDCounter, 1) & 0xFFF
	return (now << 12) | seq
}

func generateClientID(conn net.Conn) int64 {
	addr := conn.RemoteAddr().(*net.TCPAddr).IP.String()
	hash := fnv.New64a()
	hash.Write([]byte(addr))

	now := time.Now().UnixNano() / 1e6
	clientSeq := atomic.AddInt64(&clientIDCounter, 1) & 0xFFFF

	return now ^ (int64(hash.Sum64()) & 0xFFFF) ^ (clientSeq << 32)
}
