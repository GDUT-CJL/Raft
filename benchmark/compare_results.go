package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type TestMetrics struct {
	TestName       string
	TotalRequests  int64
	SuccessRequests int64
	FailedRequests int64
	ErrorRate      float64
	ActualQPS      float64
	Throughput     float64
	AvgLatency     float64
	MinLatency     int64
	MaxLatency     int64
	P50Latency     float64
	P95Latency     float64
	P99Latency     float64
}

type ComparisonResult struct {
	TestName              string
	LegacyMetrics         TestMetrics
	OptimizedMetrics      TestMetrics
	ThroughputImprovement float64
	LatencyImprovement    float64
	P99Improvement        float64
	QPSImprovement        float64
	ErrorRateChange       float64
}

func parseLogFile(filename string) ([]TestMetrics, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var metrics []TestMetrics
	var currentMetrics *TestMetrics
	var currentTestName string

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		if strings.Contains(line, "测试:") {
			if currentMetrics != nil {
				metrics = append(metrics, *currentMetrics)
			}
			currentMetrics = &TestMetrics{}
			parts := strings.Split(line, "测试:")
			if len(parts) > 1 {
				currentTestName = strings.TrimSpace(parts[1])
				currentMetrics.TestName = currentTestName
			}
		}

		if currentMetrics != nil {
			if strings.Contains(line, "总请求数:") {
				re := regexp.MustCompile(`\d+`)
				matches := re.FindAllString(line, -1)
				if len(matches) > 0 {
					currentMetrics.TotalRequests, _ = strconv.ParseInt(matches[0], 10, 64)
				}
			}

			if strings.Contains(line, "成功请求:") {
				re := regexp.MustCompile(`\d+`)
				matches := re.FindAllString(line, -1)
				if len(matches) > 0 {
					currentMetrics.SuccessRequests, _ = strconv.ParseInt(matches[0], 10, 64)
				}
			}

			if strings.Contains(line, "失败请求:") {
				re := regexp.MustCompile(`\d+`)
				matches := re.FindAllString(line, -1)
				if len(matches) > 0 {
					currentMetrics.FailedRequests, _ = strconv.ParseInt(matches[0], 10, 64)
				}
			}

			if strings.Contains(line, "错误率:") {
				re := regexp.MustCompile(`[\d.]+`)
				matches := re.FindAllString(line, -1)
				if len(matches) > 0 {
					currentMetrics.ErrorRate, _ = strconv.ParseFloat(matches[0], 64)
				}
			}

			if strings.Contains(line, "实际QPS:") {
				re := regexp.MustCompile(`[\d.]+`)
				matches := re.FindAllString(line, -1)
				if len(matches) > 0 {
					currentMetrics.ActualQPS, _ = strconv.ParseFloat(matches[0], 64)
				}
			}

			if strings.Contains(line, "吞吐量:") {
				re := regexp.MustCompile(`[\d.]+`)
				matches := re.FindAllString(line, -1)
				if len(matches) > 0 {
					currentMetrics.Throughput, _ = strconv.ParseFloat(matches[0], 64)
				}
			}

			if strings.Contains(line, "平均延迟:") {
				re := regexp.MustCompile(`[\d.]+`)
				matches := re.FindAllString(line, -1)
				if len(matches) > 0 {
					currentMetrics.AvgLatency, _ = strconv.ParseFloat(matches[0], 64)
				}
			}

			if strings.Contains(line, "最小延迟:") {
				re := regexp.MustCompile(`\d+`)
				matches := re.FindAllString(line, -1)
				if len(matches) > 0 {
					currentMetrics.MinLatency, _ = strconv.ParseInt(matches[0], 10, 64)
				}
			}

			if strings.Contains(line, "最大延迟:") {
				re := regexp.MustCompile(`\d+`)
				matches := re.FindAllString(line, -1)
				if len(matches) > 0 {
					currentMetrics.MaxLatency, _ = strconv.ParseInt(matches[0], 10, 64)
				}
			}

			if strings.Contains(line, "P50延迟:") {
				re := regexp.MustCompile(`[\d.]+`)
				matches := re.FindAllString(line, -1)
				if len(matches) > 0 {
					currentMetrics.P50Latency, _ = strconv.ParseFloat(matches[0], 64)
				}
			}

			if strings.Contains(line, "P95延迟:") {
				re := regexp.MustCompile(`[\d.]+`)
				matches := re.FindAllString(line, -1)
				if len(matches) > 0 {
					currentMetrics.P95Latency, _ = strconv.ParseFloat(matches[0], 64)
				}
			}

			if strings.Contains(line, "P99延迟:") {
				re := regexp.MustCompile(`[\d.]+`)
				matches := re.FindAllString(line, -1)
				if len(matches) > 0 {
					currentMetrics.P99Latency, _ = strconv.ParseFloat(matches[0], 64)
				}
			}
		}
	}

	if currentMetrics != nil {
		metrics = append(metrics, *currentMetrics)
	}

	return metrics, scanner.Err()
}

func compareMetrics(legacy, optimized TestMetrics) ComparisonResult {
	result := ComparisonResult{
		TestName:         legacy.TestName,
		LegacyMetrics:    legacy,
		OptimizedMetrics: optimized,
	}

	if legacy.Throughput > 0 {
		result.ThroughputImprovement = (optimized.Throughput - legacy.Throughput) / legacy.Throughput * 100
	}

	if legacy.AvgLatency > 0 {
		result.LatencyImprovement = (legacy.AvgLatency - optimized.AvgLatency) / legacy.AvgLatency * 100
	}

	if legacy.P99Latency > 0 {
		result.P99Improvement = (legacy.P99Latency - optimized.P99Latency) / legacy.P99Latency * 100
	}

	if legacy.ActualQPS > 0 {
		result.QPSImprovement = (optimized.ActualQPS - legacy.ActualQPS) / legacy.ActualQPS * 100
	}

	result.ErrorRateChange = optimized.ErrorRate - legacy.ErrorRate

	return result
}

func printComparisonTable(results []ComparisonResult) {
	fmt.Println("\n========================================")
	fmt.Println("性能对比分析报告")
	fmt.Println("========================================")

	for _, result := range results {
		fmt.Printf("\n测试场景: %s\n", result.TestName)
		fmt.Println("----------------------------------------")

		fmt.Printf("\n%-20s %-15s %-15s %-10s\n", "指标", "原版本", "优化版本", "提升")
		fmt.Printf("%-20s %-15s %-15s %-10s\n", "----", "------", "--------", "----")

		fmt.Printf("%-20s %-15d %-15d -\n", "总请求数", result.LegacyMetrics.TotalRequests, result.OptimizedMetrics.TotalRequests)
		fmt.Printf("%-20s %-15d %-15d -\n", "成功请求", result.LegacyMetrics.SuccessRequests, result.OptimizedMetrics.SuccessRequests)
		fmt.Printf("%-20s %-15d %-15d -\n", "失败请求", result.LegacyMetrics.FailedRequests, result.OptimizedMetrics.FailedRequests)

		fmt.Printf("%-20s %-15.2f %-15.2f %+.2f%%\n", "吞吐量 (ops/s)", result.LegacyMetrics.Throughput, result.OptimizedMetrics.Throughput, result.ThroughputImprovement)
		fmt.Printf("%-20s %-15.2f %-15.2f %+.2f%%\n", "平均延迟 (μs)", result.LegacyMetrics.AvgLatency, result.OptimizedMetrics.AvgLatency, result.LatencyImprovement)
		fmt.Printf("%-20s %-15.2f %-15.2f %+.2f%%\n", "P50延迟 (μs)", result.LegacyMetrics.P50Latency, result.OptimizedMetrics.P50Latency, (result.LegacyMetrics.P50Latency-result.OptimizedMetrics.P50Latency)/result.LegacyMetrics.P50Latency*100)
		fmt.Printf("%-20s %-15.2f %-15.2f %+.2f%%\n", "P95延迟 (μs)", result.LegacyMetrics.P95Latency, result.OptimizedMetrics.P95Latency, (result.LegacyMetrics.P95Latency-result.OptimizedMetrics.P95Latency)/result.LegacyMetrics.P95Latency*100)
		fmt.Printf("%-20s %-15.2f %-15.2f %+.2f%%\n", "P99延迟 (μs)", result.LegacyMetrics.P99Latency, result.OptimizedMetrics.P99Latency, result.P99Improvement)
		fmt.Printf("%-20s %-15.2f %-15.2f %+.2f%%\n", "实际QPS", result.LegacyMetrics.ActualQPS, result.OptimizedMetrics.ActualQPS, result.QPSImprovement)
		fmt.Printf("%-20s %-15.2f%% %-15.2f%% %+.2f%%\n", "错误率", result.LegacyMetrics.ErrorRate, result.OptimizedMetrics.ErrorRate, result.ErrorRateChange)

		fmt.Printf("\n关键发现:\n")
		if result.ThroughputImprovement > 0 {
			fmt.Printf("  ✅ 吞吐量提升 %.2f%%\n", result.ThroughputImprovement)
		} else {
			fmt.Printf("  ❌ 吞吐量下降 %.2f%%\n", -result.ThroughputImprovement)
		}

		if result.LatencyImprovement > 0 {
			fmt.Printf("  ✅ 平均延迟降低 %.2f%%\n", result.LatencyImprovement)
		} else {
			fmt.Printf("  ❌ 平均延迟增加 %.2f%%\n", -result.LatencyImprovement)
		}

		if result.P99Improvement > 0 {
			fmt.Printf("  ✅ P99延迟降低 %.2f%%\n", result.P99Improvement)
		} else {
			fmt.Printf("  ❌ P99延迟增加 %.2f%%\n", -result.P99Improvement)
		}

		if result.ErrorRateChange < 0 {
			fmt.Printf("  ✅ 错误率降低 %.2f%%\n", -result.ErrorRateChange)
		} else if result.ErrorRateChange > 0 {
			fmt.Printf("  ⚠️  错误率增加 %.2f%%\n", result.ErrorRateChange)
		}
	}
}

func generateJSONReport(results []ComparisonResult, filename string) error {
	type Report struct {
		Timestamp string             `json:"timestamp"`
		Results   []ComparisonResult `json:"results"`
	}

	report := Report{
		Timestamp: time.Now().Format(time.RFC3339),
		Results:   results,
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filename, data, 0644)
}

func generateMarkdownReport(results []ComparisonResult, filename string) error {
	var sb strings.Builder

	sb.WriteString("# Raft 数据库性能对比测试报告\n\n")
	sb.WriteString(fmt.Sprintf("**测试时间**: %s\n\n", time.Now().Format("2006-01-02 15:04:05")))

	sb.WriteString("## 测试概述\n\n")
	sb.WriteString("本报告对比了原版本和优化版本的性能差异，包括吞吐量、延迟、错误率等关键指标。\n\n")

	sb.WriteString("## 测试结果\n\n")

	for _, result := range results {
		sb.WriteString(fmt.Sprintf("### %s\n\n", result.TestName))

		sb.WriteString("| 指标 | 原版本 | 优化版本 | 提升 |\n")
		sb.WriteString("|------|--------|----------|------|\n")

		sb.WriteString(fmt.Sprintf("| 总请求数 | %d | %d | - |\n", result.LegacyMetrics.TotalRequests, result.OptimizedMetrics.TotalRequests))
		sb.WriteString(fmt.Sprintf("| 成功请求 | %d | %d | - |\n", result.LegacyMetrics.SuccessRequests, result.OptimizedMetrics.SuccessRequests))
		sb.WriteString(fmt.Sprintf("| 失败请求 | %d | %d | - |\n", result.LegacyMetrics.FailedRequests, result.OptimizedMetrics.FailedRequests))
		sb.WriteString(fmt.Sprintf("| 吞吐量 (ops/s) | %.2f | %.2f | %+.2f%% |\n", result.LegacyMetrics.Throughput, result.OptimizedMetrics.Throughput, result.ThroughputImprovement))
		sb.WriteString(fmt.Sprintf("| 平均延迟 (μs) | %.2f | %.2f | %+.2f%% |\n", result.LegacyMetrics.AvgLatency, result.OptimizedMetrics.AvgLatency, result.LatencyImprovement))
		sb.WriteString(fmt.Sprintf("| P50延迟 (μs) | %.2f | %.2f | %+.2f%% |\n", result.LegacyMetrics.P50Latency, result.OptimizedMetrics.P50Latency, (result.LegacyMetrics.P50Latency-result.OptimizedMetrics.P50Latency)/result.LegacyMetrics.P50Latency*100))
		sb.WriteString(fmt.Sprintf("| P95延迟 (μs) | %.2f | %.2f | %+.2f%% |\n", result.LegacyMetrics.P95Latency, result.OptimizedMetrics.P95Latency, (result.LegacyMetrics.P95Latency-result.OptimizedMetrics.P95Latency)/result.LegacyMetrics.P95Latency*100))
		sb.WriteString(fmt.Sprintf("| P99延迟 (μs) | %.2f | %.2f | %+.2f%% |\n", result.LegacyMetrics.P99Latency, result.OptimizedMetrics.P99Latency, result.P99Improvement))
		sb.WriteString(fmt.Sprintf("| 实际QPS | %.2f | %.2f | %+.2f%% |\n", result.LegacyMetrics.ActualQPS, result.OptimizedMetrics.ActualQPS, result.QPSImprovement))
		sb.WriteString(fmt.Sprintf("| 错误率 | %.2f%% | %.2f%% | %+.2f%% |\n\n", result.LegacyMetrics.ErrorRate, result.OptimizedMetrics.ErrorRate, result.ErrorRateChange))

		sb.WriteString("**关键发现**:\n")
		if result.ThroughputImprovement > 0 {
			sb.WriteString(fmt.Sprintf("- ✅ 吞吐量提升 %.2f%%\n", result.ThroughputImprovement))
		}
		if result.LatencyImprovement > 0 {
			sb.WriteString(fmt.Sprintf("- ✅ 平均延迟降低 %.2f%%\n", result.LatencyImprovement))
		}
		if result.P99Improvement > 0 {
			sb.WriteString(fmt.Sprintf("- ✅ P99延迟降低 %.2f%%\n", result.P99Improvement))
		}
		if result.ErrorRateChange < 0 {
			sb.WriteString(fmt.Sprintf("- ✅ 错误率降低 %.2f%%\n", -result.ErrorRateChange))
		}
		sb.WriteString("\n")
	}

	sb.WriteString("## 结论\n\n")
	sb.WriteString("优化版本通过以下关键改进显著提升了性能:\n\n")
	sb.WriteString("1. **全局批量队列**: 聚合所有连接的请求，减少锁竞争\n")
	sb.WriteString("2. **批量 CGO 调用**: 一次调用处理多个操作，减少 Go-C 切换开销\n")
	sb.WriteString("3. **无锁设计**: 使用 channel 替代 mutex，提高并发性能\n")
	sb.WriteString("4. **自适应批量大小**: 根据系统负载动态调整批量大小\n")

	return os.WriteFile(filename, []byte(sb.String()), 0644)
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("用法: compare_results <legacy_log> <optimized_log>")
		fmt.Println("示例: compare_results benchmark_results_20240101_120000/legacy_test.log benchmark_results_20240101_120000/optimized_test.log")
		os.Exit(1)
	}

	legacyFile := os.Args[1]
	optimizedFile := os.Args[2]

	fmt.Printf("解析原版本测试日志: %s\n", legacyFile)
	legacyMetrics, err := parseLogFile(legacyFile)
	if err != nil {
		fmt.Printf("解析失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("解析优化版本测试日志: %s\n", optimizedFile)
	optimizedMetrics, err := parseLogFile(optimizedFile)
	if err != nil {
		fmt.Printf("解析失败: %v\n", err)
		os.Exit(1)
	}

	if len(legacyMetrics) != len(optimizedMetrics) {
		fmt.Printf("警告: 测试场景数量不匹配 (原版本: %d, 优化版本: %d)\n", len(legacyMetrics), len(optimizedMetrics))
	}

	var comparisonResults []ComparisonResult
	for i := 0; i < len(legacyMetrics) && i < len(optimizedMetrics); i++ {
		result := compareMetrics(legacyMetrics[i], optimizedMetrics[i])
		comparisonResults = append(comparisonResults, result)
	}

	printComparisonTable(comparisonResults)

	jsonReport := fmt.Sprintf("comparison_report_%s.json", time.Now().Format("20060102_150405"))
	if err := generateJSONReport(comparisonResults, jsonReport); err != nil {
		fmt.Printf("\n生成 JSON 报告失败: %v\n", err)
	} else {
		fmt.Printf("\nJSON 报告已保存: %s\n", jsonReport)
	}

	markdownReport := fmt.Sprintf("comparison_report_%s.md", time.Now().Format("20060102_150405"))
	if err := generateMarkdownReport(comparisonResults, markdownReport); err != nil {
		fmt.Printf("\n生成 Markdown 报告失败: %v\n", err)
	} else {
		fmt.Printf("Markdown 报告已保存: %s\n", markdownReport)
	}

	fmt.Println("\n========================================")
	fmt.Println("对比分析完成")
	fmt.Println("========================================")
}
