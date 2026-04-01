package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type TestConfig struct {
	ServerAddr      string        `json:"server_addr"`
	Duration        time.Duration `json:"duration"`
	LowConcurrency  int           `json:"low_concurrency"`
	HighConcurrency int           `json:"high_concurrency"`
	LowQPS          int           `json:"low_qps"`
	HighQPS         int           `json:"high_qps"`
	WarmupDuration  time.Duration `json:"warmup_duration"`
}

type Metrics struct {
	TotalRequests    int64
	SuccessRequests  int64
	FailedRequests   int64
	TotalLatency     int64
	MinLatency       int64
	MaxLatency       int64
	Latencies        []int64
	StartTime        time.Time
	EndTime          time.Time
	Throughput       float64
	AvgLatency       float64
	P50Latency       float64
	P95Latency       float64
	P99Latency       float64
	ErrorRate        float64
	ActualQPS        float64
}

type TestResult struct {
	TestName     string
	Duration     time.Duration
	Concurrency  int
	TargetQPS    int
	Metrics      Metrics
	ResourceUsage ResourceUsage
}

type ResourceUsage struct {
	CPUUsage    float64
	MemoryUsage float64
}

type RESPClient struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	mu     sync.Mutex
}

func NewRESPClient(addr string) (*RESPClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &RESPClient{
		conn:   conn,
		reader: bufio.NewReaderSize(conn, 128*1024),
		writer: bufio.NewWriterSize(conn, 64*1024),
	}, nil
}

func (c *RESPClient) Close() error {
	return c.conn.Close()
}

func (c *RESPClient) SendCommand(args ...string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.writer.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		c.writer.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}
	c.writer.Flush()

	line, err := c.reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	line = line[:len(line)-2]

	if len(line) > 0 {
		switch line[0] {
		case '+':
			return line[1:], nil
		case '-':
			return "", fmt.Errorf("error: %s", line[1:])
		case '$':
			if line == "$-1" {
				return "", nil
			}
			var length int
			fmt.Sscanf(line, "$%d", &length)
			data := make([]byte, length+2)
			_, err := io.ReadFull(c.reader, data)
			if err != nil {
				return "", err
			}
			return string(data[:length]), nil
		case ':':
			return line[1:], nil
		}
	}

	return line, nil
}

func runTest(config TestConfig, testName string, concurrency int, targetQPS int) TestResult {
	fmt.Printf("\n========================================\n")
	fmt.Printf("测试: %s\n", testName)
	fmt.Printf("并发数: %d, 目标QPS: %d\n", concurrency, targetQPS)
	fmt.Printf("========================================\n")

	metrics := Metrics{
		MinLatency: int64(^uint64(0) >> 1),
		StartTime:  time.Now(),
		Latencies:  make([]int64, 0, 100000),
	}

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	requestsPerWorker := targetQPS / concurrency
	if requestsPerWorker == 0 {
		requestsPerWorker = 1
	}
	interval := time.Second / time.Duration(requestsPerWorker)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			client, err := NewRESPClient(config.ServerAddr)
			if err != nil {
				fmt.Printf("Worker %d: 连接失败: %v\n", workerID, err)
				return
			}
			defer client.Close()

			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
					start := time.Now()

					key := fmt.Sprintf("key_%d_%d", workerID, time.Now().UnixNano()%10000)
					value := fmt.Sprintf("value_%d", time.Now().UnixNano())

					_, err := client.SendCommand("SET", key, value)
					latency := time.Since(start).Microseconds()

					atomic.AddInt64(&metrics.TotalRequests, 1)
					if err != nil {
						atomic.AddInt64(&metrics.FailedRequests, 1)
					} else {
						atomic.AddInt64(&metrics.SuccessRequests, 1)
						atomic.AddInt64(&metrics.TotalLatency, latency)

						if latency < atomic.LoadInt64(&metrics.MinLatency) {
							atomic.StoreInt64(&metrics.MinLatency, latency)
						}
						if latency > atomic.LoadInt64(&metrics.MaxLatency) {
							atomic.StoreInt64(&metrics.MaxLatency, latency)
						}

						metrics.Latencies = append(metrics.Latencies, latency)
					}
				}
			}
		}(i)
	}

	time.Sleep(config.Duration)
	close(stopCh)
	wg.Wait()

	metrics.EndTime = time.Now()
	actualDuration := metrics.EndTime.Sub(metrics.StartTime).Seconds()

	metrics.Throughput = float64(metrics.SuccessRequests) / actualDuration
	metrics.AvgLatency = float64(metrics.TotalLatency) / float64(metrics.SuccessRequests)
	metrics.ErrorRate = float64(metrics.FailedRequests) / float64(metrics.TotalRequests) * 100
	metrics.ActualQPS = float64(metrics.TotalRequests) / actualDuration

	if len(metrics.Latencies) > 0 {
		sort.Slice(metrics.Latencies, func(i, j int) bool {
			return metrics.Latencies[i] < metrics.Latencies[j]
		})

		metrics.P50Latency = float64(metrics.Latencies[len(metrics.Latencies)*50/100])
		metrics.P95Latency = float64(metrics.Latencies[len(metrics.Latencies)*95/100])
		metrics.P99Latency = float64(metrics.Latencies[len(metrics.Latencies)*99/100])
	}

	return TestResult{
		TestName:    testName,
		Duration:    config.Duration,
		Concurrency: concurrency,
		TargetQPS:   targetQPS,
		Metrics:     metrics,
	}
}

func runWarmup(config TestConfig) {
	fmt.Printf("\n预热阶段 (%v)...\n", config.WarmupDuration)

	client, err := NewRESPClient(config.ServerAddr)
	if err != nil {
		fmt.Printf("预热连接失败: %v\n", err)
		return
	}
	defer client.Close()

	stopCh := time.After(config.WarmupDuration)
	for {
		select {
		case <-stopCh:
			fmt.Println("预热完成")
			return
		default:
			key := fmt.Sprintf("warmup_%d", time.Now().UnixNano())
			client.SendCommand("SET", key, "warmup_value")
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func printMetrics(metrics Metrics) {
	fmt.Printf("\n性能指标:\n")
	fmt.Printf("  总请求数:      %d\n", metrics.TotalRequests)
	fmt.Printf("  成功请求:      %d\n", metrics.SuccessRequests)
	fmt.Printf("  失败请求:      %d\n", metrics.FailedRequests)
	fmt.Printf("  错误率:        %.2f%%\n", metrics.ErrorRate)
	fmt.Printf("  实际QPS:       %.2f\n", metrics.ActualQPS)
	fmt.Printf("  吞吐量:        %.2f ops/s\n", metrics.Throughput)
	fmt.Printf("  平均延迟:      %.2f μs (%.2f ms)\n", metrics.AvgLatency, metrics.AvgLatency/1000)
	fmt.Printf("  最小延迟:      %d μs\n", metrics.MinLatency)
	fmt.Printf("  最大延迟:      %d μs\n", metrics.MaxLatency)
	fmt.Printf("  P50延迟:       %.2f μs (%.2f ms)\n", metrics.P50Latency, metrics.P50Latency/1000)
	fmt.Printf("  P95延迟:       %.2f μs (%.2f ms)\n", metrics.P95Latency, metrics.P95Latency/1000)
	fmt.Printf("  P99延迟:       %.2f μs (%.2f ms)\n", metrics.P99Latency, metrics.P99Latency/1000)
}

func compareResults(legacyResult, optimizedResult TestResult) {
	fmt.Printf("\n========================================\n")
	fmt.Printf("性能对比分析\n")
	fmt.Printf("========================================\n")

	legacy := legacyResult.Metrics
	optimized := optimizedResult.Metrics

	fmt.Printf("\n%-20s %-15s %-15s %-10s\n", "指标", "原版本", "优化版本", "提升")
	fmt.Printf("%-20s %-15s %-15s %-10s\n", "----", "------", "--------", "----")

	throughputImprovement := (optimized.Throughput - legacy.Throughput) / legacy.Throughput * 100
	fmt.Printf("%-20s %-15.2f %-15.2f %+.2f%%\n", "吞吐量 (ops/s)", legacy.Throughput, optimized.Throughput, throughputImprovement)

	latencyImprovement := (legacy.AvgLatency - optimized.AvgLatency) / legacy.AvgLatency * 100
	fmt.Printf("%-20s %-15.2f %-15.2f %+.2f%%\n", "平均延迟 (μs)", legacy.AvgLatency, optimized.AvgLatency, latencyImprovement)

	p99Improvement := (legacy.P99Latency - optimized.P99Latency) / legacy.P99Latency * 100
	fmt.Printf("%-20s %-15.2f %-15.2f %+.2f%%\n", "P99延迟 (μs)", legacy.P99Latency, optimized.P99Latency, p99Improvement)

	qpsImprovement := (optimized.ActualQPS - legacy.ActualQPS) / legacy.ActualQPS * 100
	fmt.Printf("%-20s %-15.2f %-15.2f %+.2f%%\n", "实际QPS", legacy.ActualQPS, optimized.ActualQPS, qpsImprovement)

	errorRateChange := optimized.ErrorRate - legacy.ErrorRate
	fmt.Printf("%-20s %-15.2f%% %-15.2f%% %+.2f%%\n", "错误率", legacy.ErrorRate, optimized.ErrorRate, errorRateChange)

	fmt.Printf("\n关键发现:\n")
	if throughputImprovement > 0 {
		fmt.Printf("  ✅ 吞吐量提升 %.2f%%\n", throughputImprovement)
	}
	if latencyImprovement > 0 {
		fmt.Printf("  ✅ 平均延迟降低 %.2f%%\n", latencyImprovement)
	}
	if p99Improvement > 0 {
		fmt.Printf("  ✅ P99延迟降低 %.2f%%\n", p99Improvement)
	}
	if errorRateChange < 0 {
		fmt.Printf("  ✅ 错误率降低 %.2f%%\n", -errorRateChange)
	}
}

func generateReport(results []TestResult, filename string) error {
	type Report struct {
		Timestamp   string       `json:"timestamp"`
		TestResults []TestResult `json:"test_results"`
	}

	report := Report{
		Timestamp:   time.Now().Format(time.RFC3339),
		TestResults: results,
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filename, data, 0644)
}

func runMixedTest(config TestConfig, testName string, concurrency int, targetQPS int) TestResult {
	fmt.Printf("\n========================================\n")
	fmt.Printf("测试: %s\n", testName)
	fmt.Printf("并发数: %d, 目标QPS: %d\n", concurrency, targetQPS)
	fmt.Printf("========================================\n")

	metrics := Metrics{
		MinLatency: int64(^uint64(0) >> 1),
		StartTime:  time.Now(),
		Latencies:  make([]int64, 0, 100000),
	}

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	requestsPerWorker := targetQPS / concurrency
	if requestsPerWorker == 0 {
		requestsPerWorker = 1
	}
	interval := time.Second / time.Duration(requestsPerWorker)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			client, err := NewRESPClient(config.ServerAddr)
			if err != nil {
				fmt.Printf("Worker %d: 连接失败: %v\n", workerID, err)
				return
			}
			defer client.Close()

			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			key := fmt.Sprintf("mixed_key_%d", workerID)
			client.SendCommand("SET", key, "initial_value")

			opCount := 0

			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
					start := time.Now()

					var err error
					if opCount%10 < 7 {
						key := fmt.Sprintf("key_%d_%d", workerID, time.Now().UnixNano()%10000)
						value := fmt.Sprintf("value_%d", time.Now().UnixNano())
						_, err = client.SendCommand("SET", key, value)
					} else {
						key := fmt.Sprintf("key_%d_%d", workerID, time.Now().UnixNano()%10000)
						_, err = client.SendCommand("GET", key)
					}

					latency := time.Since(start).Microseconds()

					atomic.AddInt64(&metrics.TotalRequests, 1)
					if err != nil {
						atomic.AddInt64(&metrics.FailedRequests, 1)
					} else {
						atomic.AddInt64(&metrics.SuccessRequests, 1)
						atomic.AddInt64(&metrics.TotalLatency, latency)

						if latency < atomic.LoadInt64(&metrics.MinLatency) {
							atomic.StoreInt64(&metrics.MinLatency, latency)
						}
						if latency > atomic.LoadInt64(&metrics.MaxLatency) {
							atomic.StoreInt64(&metrics.MaxLatency, latency)
						}

						metrics.Latencies = append(metrics.Latencies, latency)
					}

					opCount++
				}
			}
		}(i)
	}

	time.Sleep(config.Duration)
	close(stopCh)
	wg.Wait()

	metrics.EndTime = time.Now()
	actualDuration := metrics.EndTime.Sub(metrics.StartTime).Seconds()

	metrics.Throughput = float64(metrics.SuccessRequests) / actualDuration
	metrics.AvgLatency = float64(metrics.TotalLatency) / float64(metrics.SuccessRequests)
	metrics.ErrorRate = float64(metrics.FailedRequests) / float64(metrics.TotalRequests) * 100
	metrics.ActualQPS = float64(metrics.TotalRequests) / actualDuration

	if len(metrics.Latencies) > 0 {
		sort.Slice(metrics.Latencies, func(i, j int) bool {
			return metrics.Latencies[i] < metrics.Latencies[j]
		})

		metrics.P50Latency = float64(metrics.Latencies[len(metrics.Latencies)*50/100])
		metrics.P95Latency = float64(metrics.Latencies[len(metrics.Latencies)*95/100])
		metrics.P99Latency = float64(metrics.Latencies[len(metrics.Latencies)*99/100])
	}

	return TestResult{
		TestName:    testName,
		Duration:    config.Duration,
		Concurrency: concurrency,
		TargetQPS:   targetQPS,
		Metrics:     metrics,
	}
}

func main() {
	config := TestConfig{
		ServerAddr:      "localhost:6379",
		Duration:        30 * time.Second,
		LowConcurrency:  10,
		HighConcurrency: 100,
		LowQPS:          1000,
		HighQPS:         10000,
		WarmupDuration:  5 * time.Second,
	}

	if len(os.Args) > 1 {
		config.ServerAddr = os.Args[1]
	}
	if len(os.Args) > 2 {
		duration, err := time.ParseDuration(os.Args[2])
		if err == nil {
			config.Duration = duration
		}
	}

	fmt.Printf("========================================\n")
	fmt.Printf("Raft 数据库性能测试\n")
	fmt.Printf("========================================\n")
	fmt.Printf("服务器地址: %s\n", config.ServerAddr)
	fmt.Printf("测试时长: %v\n", config.Duration)
	fmt.Printf("低并发: %d, 高并发: %d\n", config.LowConcurrency, config.HighConcurrency)
	fmt.Printf("低QPS: %d, 高QPS: %d\n", config.LowQPS, config.HighQPS)

	runWarmup(config)

	var results []TestResult

	fmt.Printf("\n========================================\n")
	fmt.Printf("测试场景 1: 低吞吐量测试\n")
	fmt.Printf("========================================\n")

	result1 := runTest(config, "低吞吐量测试", config.LowConcurrency, config.LowQPS)
	printMetrics(result1.Metrics)
	results = append(results, result1)

	time.Sleep(5 * time.Second)

	fmt.Printf("\n========================================\n")
	fmt.Printf("测试场景 2: 高吞吐量测试\n")
	fmt.Printf("========================================\n")

	result2 := runTest(config, "高吞吐量测试", config.HighConcurrency, config.HighQPS)
	printMetrics(result2.Metrics)
	results = append(results, result2)

	time.Sleep(5 * time.Second)

	fmt.Printf("\n========================================\n")
	fmt.Printf("测试场景 3: 混合读写测试\n")
	fmt.Printf("========================================\n")

	result3 := runMixedTest(config, "混合读写测试", config.HighConcurrency, config.HighQPS)
	printMetrics(result3.Metrics)
	results = append(results, result3)

	reportFile := fmt.Sprintf("performance_report_%s.json", time.Now().Format("20060102_150405"))
	if err := generateReport(results, reportFile); err != nil {
		fmt.Printf("\n生成报告失败: %v\n", err)
	} else {
		fmt.Printf("\n测试报告已保存: %s\n", reportFile)
	}

	fmt.Printf("\n========================================\n")
	fmt.Printf("测试完成\n")
	fmt.Printf("========================================\n")
}
