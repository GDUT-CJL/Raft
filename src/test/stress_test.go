// stress_test_test.go
package test

//go test -v -run TestStressPerformance
import (
	"fmt"
	"testing"
	"time"
)

func TestStressPerformance(t *testing.T) {
	// 注意：需要先启动服务器
	serverAddr := "192.168.79.129:8000"

	testCases := []struct {
		name     string
		clients  int
		requests int
	}{
		{"LowLoad", 10, 100},    // 10 * 100,（10个客户端,每个客户端发送100条数据）
		{"MediumLoad", 50, 200}, // 50 * 200
		{"HighLoad", 100, 500},  // 100 * 500
		//{"VeryHighLoad", 100, 1000}, // 100 * 1000
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tester := NewStressTester(serverAddr, tc.clients, tc.requests)
			results := tester.Run()

			fmt.Printf("=== %s Test Results ===\n", tc.name)
			fmt.Printf("Total Requests: %d\n", results.TotalRequests)
			fmt.Printf("Total Time: %v\n", results.TotalTime)
			fmt.Printf("Requests/sec: %.2f\n", results.RequestsPerSec)
			fmt.Printf("Avg Latency: %v\n", results.AvgLatency)
			fmt.Printf("Min Latency: %v\n", results.MinLatency)
			fmt.Printf("Max Latency: %v\n", results.MaxLatency)
			fmt.Printf("P95 Latency: %v\n", results.P95Latency)
			fmt.Printf("P99 Latency: %v\n", results.P99Latency)
			fmt.Printf("Errors: %d\n", results.ErrorCount)

			// 性能断言
			if results.RequestsPerSec < 1000 {
				t.Errorf("Low throughput: %.2f req/s", results.RequestsPerSec)
			}
			if results.AvgLatency > 100*time.Millisecond {
				t.Errorf("High latency: %v", results.AvgLatency)
			}
		})
	}
}
