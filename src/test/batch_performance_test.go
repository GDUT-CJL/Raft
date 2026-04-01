package test

import (
	"course/mnet"
	"course/raft"
	"course/server"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkBatchOptimization(b *testing.B) {
	kv := server.StartKVServer([]string{"localhost:8000"}, 0, -1)
	rf := kv.GetRaft()

	obm := mnet.NewOptimizedBatchManager(kv, rf, 100, 10*time.Millisecond)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		conn := &mockConn{}
		i := 0
		for pb.Next() {
			op := raft.Op{
				OpType:   0, // Set
				Key:      fmt.Sprintf("key_%d", i%1000),
				Klen:     8,
				Value:    fmt.Sprintf("value_%d", i),
				Vlen:     10,
				ClientId: int64(i % 100),
				SeqId:    int64(i),
			}

			response := obm.Submit(op, conn)
			if !response.Success {
				b.Errorf("Operation failed: %s", response.Error)
			}
			i++
		}
	})
}

func TestBatchPerformance(t *testing.T) {
	kv := server.StartKVServer([]string{"localhost:8000"}, 0, -1)
	rf := kv.GetRaft()

	testCases := []struct {
		name       string
		batchSize  int
		timeout    time.Duration
		numOps     int
		numClients int
	}{
		{"SmallBatch", 50, 5 * time.Millisecond, 10000, 10},
		{"MediumBatch", 100, 10 * time.Millisecond, 10000, 10},
		{"LargeBatch", 200, 20 * time.Millisecond, 10000, 10},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			obm := mnet.NewOptimizedBatchManager(kv, rf, tc.batchSize, tc.timeout)

			var successCount int64
			var failCount int64
			var totalLatency int64

			start := time.Now()
			var wg sync.WaitGroup

			for i := 0; i < tc.numClients; i++ {
				wg.Add(1)
				go func(clientID int) {
					defer wg.Done()

					conn := &mockConn{}
					for j := 0; j < tc.numOps/tc.numClients; j++ {
						opStart := time.Now()

						op := raft.Op{
							OpType:   0, // Set
							Key:      fmt.Sprintf("key_%d_%d", clientID, j),
							Klen:     12,
							Value:    fmt.Sprintf("value_%d_%d", clientID, j),
							Vlen:     14,
							ClientId: int64(clientID),
							SeqId:    int64(j),
						}

						response := obm.Submit(op, conn)

						latency := time.Since(opStart).Milliseconds()
						atomic.AddInt64(&totalLatency, latency)

						if response.Success {
							atomic.AddInt64(&successCount, 1)
						} else {
							atomic.AddInt64(&failCount, 1)
						}
					}
				}(i)
			}

			wg.Wait()
			duration := time.Since(start)

			ops := float64(tc.numOps) / duration.Seconds()
			avgLatency := float64(totalLatency) / float64(tc.numOps)

			t.Logf("Results for %s:", tc.name)
			t.Logf("  Throughput: %.2f ops/s", ops)
			t.Logf("  Average Latency: %.2f ms", avgLatency)
			t.Logf("  Success: %d, Failed: %d", successCount, failCount)
			t.Logf("  Duration: %v", duration)

			batchSize, avgLat, throughput := obm.GetStats()
			t.Logf("  Final Batch Size: %d", batchSize)
			t.Logf("  Manager Avg Latency: %d ms", avgLat)
			t.Logf("  Total Throughput: %d ops", throughput)
		})
	}
}

type mockConn struct {
	net.Conn
}

func (c *mockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}
}

func (c *mockConn) Close() error {
	return nil
}
