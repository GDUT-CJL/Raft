// stress_test.go
// go test -v -run TestStressPerformance
package test

import (
	"bufio"
	"fmt"
	"net"
	"sort"
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
		cmd := fmt.Sprintf("*3\r\n$4\r\nRSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
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

		if response != "+OK\r\n" {
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

// 分析结果
func (st *StressTester) analyzeResults(totalTime time.Duration) StressResults {
	var (
		totalLatency int64
		minLatency   = time.Hour
		maxLatency   time.Duration
		latencies    []time.Duration
		errorCount   int
		requestCount int
	)

	// 收集所有结果,遍历结果通道
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

	// 计算统计指标
	var avgLatency time.Duration
	var p95Latency, p99Latency time.Duration

	if requestCount > 0 {
		// 修复：使用浮点数计算平均延迟，避免精度丢失
		avgLatency = time.Duration(int64(float64(totalLatency) / float64(requestCount)))

		// 修复：实现百分位数计算
		if len(latencies) > 0 {
			// 对延迟进行排序
			sort.Slice(latencies, func(i, j int) bool {
				return latencies[i] < latencies[j]
			})

			// 计算P95 (95分位)
			p95Index := int(float64(len(latencies)) * 0.95)
			if p95Index >= len(latencies) {
				p95Index = len(latencies) - 1
			}
			p95Latency = latencies[p95Index]

			// 计算P99 (99分位)
			p99Index := int(float64(len(latencies)) * 0.99)
			if p99Index >= len(latencies) {
				p99Index = len(latencies) - 1
			}
			p99Latency = latencies[p99Index]
		}
	}

	rps := float64(requestCount) / totalTime.Seconds()

	return StressResults{
		TotalRequests:  requestCount,
		TotalTime:      totalTime,
		RequestsPerSec: rps,
		AvgLatency:     avgLatency,
		MinLatency:     minLatency,
		MaxLatency:     maxLatency,
		P95Latency:     p95Latency,
		P99Latency:     p99Latency,
		ErrorCount:     errorCount, // 注意：需要在实际测试中统计错误
	}
}
