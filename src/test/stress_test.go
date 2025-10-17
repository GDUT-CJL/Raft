// stress_test.go
package test

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"
)

// 压力测试结构体
type StressTester struct {
	serverAddr string
	clients    int
	requests   int
	results    chan time.Duration
	wg         sync.WaitGroup
}

func NewStressTester(addr string, clients, requests int) *StressTester {
	return &StressTester{
		serverAddr: addr,
		clients:    clients,
		requests:   requests,
		results:    make(chan time.Duration, clients*requests),
	}
}

// 单个客户端测试
func (st *StressTester) runClient(clientID int) {
	defer st.wg.Done()

	conn, err := net.Dial("tcp", st.serverAddr)
	if err != nil {
		fmt.Printf("Client %d: Failed to connect: %v\n", clientID, err)
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for i := 0; i < st.requests; i++ {
		key := fmt.Sprintf("key-%d-%d", clientID, i)
		value := fmt.Sprintf("value-%d-%d", clientID, i)

		start := time.Now()

		// 发送SET命令 (RESP格式)
		cmd := fmt.Sprintf("*3\r\n$4\r\nHSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
			len(key), key, len(value), value)

		//cmd := fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
		_, err := conn.Write([]byte(cmd))
		if err != nil {
			fmt.Printf("Client %d: Write error: %v\n", clientID, err)
			return
		}

		// 读取响应
		response, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Client %d: Read error: %v\n", clientID, err)
			return
		}

		duration := time.Since(start)
		st.results <- duration

		if response != "ACK\n" && response != "BATCH_OK\n" {
			fmt.Printf("Client %d: Unexpected response: %s\n", clientID, response)
		}
	}
}

// 运行压力测试
func (st *StressTester) Run() StressResults {
	startTime := time.Now()

	// 启动所有客户端
	for i := 0; i < st.clients; i++ {
		st.wg.Add(1)
		go st.runClient(i)
	}

	// 等待所有客户端完成
	st.wg.Wait()
	close(st.results)

	totalTime := time.Since(startTime)

	return st.analyzeResults(totalTime)
}

// 测试结果分析
type StressResults struct {
	TotalRequests  int
	TotalTime      time.Duration
	RequestsPerSec float64
	AvgLatency     time.Duration
	MinLatency     time.Duration
	MaxLatency     time.Duration
	P95Latency     time.Duration
	P99Latency     time.Duration
	ErrorCount     int
}

func (st *StressTester) analyzeResults(totalTime time.Duration) StressResults {
	var (
		totalLatency int64
		minLatency   = time.Hour
		maxLatency   time.Duration
		latencies    []time.Duration
		errorCount   int
		requestCount int
	)

	for duration := range st.results {
		requestCount++
		totalLatency += duration.Nanoseconds()

		if duration < minLatency {
			minLatency = duration
		}
		if duration > maxLatency {
			maxLatency = duration
		}

		latencies = append(latencies, duration)
	}

	// 计算百分位数
	if len(latencies) > 0 {
		// 排序逻辑...
	}

	avgLatency := time.Duration(totalLatency / int64(requestCount))
	rps := float64(requestCount) / totalTime.Seconds()

	return StressResults{
		TotalRequests:  requestCount,
		TotalTime:      totalTime,
		RequestsPerSec: rps,
		AvgLatency:     avgLatency,
		MinLatency:     minLatency,
		MaxLatency:     maxLatency,
		ErrorCount:     errorCount,
	}
}
