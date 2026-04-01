package mnet

import (
	"course/raft"
	"course/server"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	globalOptimizedBatchManager *OptimizedBatchManager
	obmMutex                    sync.RWMutex
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

type OptimizedBatchManager struct {
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

func SetOptimizedBatchManager(obm *OptimizedBatchManager) {
	obmMutex.Lock()
	defer obmMutex.Unlock()
	globalOptimizedBatchManager = obm
}

func GetOptimizedBatchManager() *OptimizedBatchManager {
	obmMutex.RLock()
	defer obmMutex.RUnlock()
	return globalOptimizedBatchManager
}

func NewOptimizedBatchManager(kv *server.KVServer, rf *raft.Raft, initialBatchSize int, batchTimeout time.Duration) *OptimizedBatchManager {
	obm := &OptimizedBatchManager{
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

	obm.wg.Add(1)
	go obm.batchWorker()

	return obm
}

func (obm *OptimizedBatchManager) Submit(op raft.Op, conn net.Conn) *BatchResponse {
	respCh := make(chan *BatchResponse, 1)

	req := &BatchRequest{
		Op:      op,
		Conn:    conn,
		RespCh:  respCh,
		AddedAt: time.Now(),
	}

	select {
	case obm.requestQueue <- req:
		atomic.AddInt64(&obm.pendingOps, 1)
		return <-respCh
	default:
		return &BatchResponse{
			Success: false,
			Error:   "Queue full",
		}
	}
}

func (obm *OptimizedBatchManager) batchWorker() {
	defer obm.wg.Done()

	batch := make([]*BatchRequest, 0, obm.maxBatchSize)
	timer := time.NewTimer(obm.batchTimeout)
	defer timer.Stop()

	for {
		select {
		case <-obm.stopCh:
			if len(batch) > 0 {
				obm.processBatch(batch)
			}
			return

		case req := <-obm.requestQueue:
			batch = append(batch, req)
			atomic.AddInt64(&obm.pendingOps, -1)

			if len(batch) >= int(atomic.LoadInt32(&obm.batchSize)) {
				obm.processBatch(batch)
				batch = make([]*BatchRequest, 0, obm.maxBatchSize)
				obm.adjustBatchSize()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(obm.batchTimeout)
			}

		case <-timer.C:
			if len(batch) > 0 {
				obm.processBatch(batch)
				batch = make([]*BatchRequest, 0, obm.maxBatchSize)
				obm.adjustBatchSize()
			}
			timer.Reset(obm.batchTimeout)
		}
	}
}

func (obm *OptimizedBatchManager) processBatch(batch []*BatchRequest) {
	if len(batch) == 0 {
		return
	}

	startTime := time.Now()

	ops := make([]raft.Op, len(batch))
	for i, req := range batch {
		ops[i] = req.Op
	}

	index, _, isLeader := obm.rf.Start(ops)
	if !isLeader {
		for _, req := range batch {
			req.RespCh <- &BatchResponse{
				Success: false,
				Error:   "NOT_LEADER",
			}
		}
		return
	}

	notifyCh := obm.kv.GetNotifyChannel(index)

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

	case <-time.After(30 * time.Second):
		for _, req := range batch {
			req.RespCh <- &BatchResponse{
				Success: false,
				Error:   "TIMEOUT",
			}
		}
	}

	obm.kv.RemoveNotifyChannel(index)

	latency := time.Since(startTime).Milliseconds()
	atomic.StoreInt64(&obm.avgLatency, latency)
	atomic.AddInt64(&obm.throughput, int64(len(batch)))
	obm.lastBatchTime = time.Now()
}

func (obm *OptimizedBatchManager) adjustBatchSize() {
	latency := atomic.LoadInt64(&obm.avgLatency)
	currentSize := atomic.LoadInt32(&obm.batchSize)

	if latency > 100 {
		newSize := currentSize * 9 / 10
		if newSize >= obm.minBatchSize {
			atomic.StoreInt32(&obm.batchSize, newSize)
		}
	} else if latency < 50 {
		newSize := currentSize * 11 / 10
		if newSize <= obm.maxBatchSize {
			atomic.StoreInt32(&obm.batchSize, newSize)
		}
	}
}

func (obm *OptimizedBatchManager) GetStats() (int32, int64, int64) {
	return atomic.LoadInt32(&obm.batchSize),
		atomic.LoadInt64(&obm.avgLatency),
		atomic.LoadInt64(&obm.throughput)
}

func (obm *OptimizedBatchManager) Stop() {
	close(obm.stopCh)
	obm.wg.Wait()
}

func CommitedOptimized(optype raft.OperationType, key string, klen int, value string, vlen int, conn net.Conn, kv *server.KVServer,
	rf *raft.Raft, safeWrite func([]byte)) {

	if _, isLeader := rf.GetState(); !isLeader {
		safeWrite([]byte("-ERR LeaderIP is " + rf.LeaderIP + "\r\n"))
		return
	}

	obm := GetOptimizedBatchManager()
	if obm == nil {
		safeWrite([]byte("-ERR Optimized batch manager not initialized\r\n"))
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

	response := obm.Submit(op, conn)

	if response.Success {
		safeWrite([]byte("+OK\r\n"))
	} else {
		safeWrite([]byte(fmt.Sprintf("-ERR %s\r\n", response.Error)))
	}
}
