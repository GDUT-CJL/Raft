#!/bin/bash

set -e

LEADER_ADDR=${1:-"192.168.1.100:6379"}
TEST_DURATION=${2:-"30s"}
OUTPUT_DIR="benchmark_results_$(date +%Y%m%d_%H%M%S)"

mkdir -p "$OUTPUT_DIR"

echo "========================================"
echo "Raft 数据库性能对比测试"
echo "========================================"
echo "Leader 地址: $LEADER_ADDR"
echo "测试时长: $TEST_DURATION"
echo "输出目录: $OUTPUT_DIR"
echo ""

echo "编译性能测试工具..."
cd benchmark
go build -o performance_benchmark performance_benchmark.go
cd ..

echo ""
echo "========================================"
echo "测试 1: 原版本性能测试"
echo "========================================"
echo "请确保数据库使用原版本启动（不使用 -optimized 参数）"
read -p "按 Enter 键开始测试原版本..."

./benchmark/performance_benchmark "$LEADER_ADDR" "$TEST_DURATION" | tee "$OUTPUT_DIR/legacy_test.log"

echo ""
echo "原版本测试完成，结果已保存到 $OUTPUT_DIR/legacy_test.log"
echo ""
echo "请重启数据库使用优化版本（使用 -optimized 参数）"
read -p "按 Enter 键开始测试优化版本..."

echo ""
echo "========================================"
echo "测试 2: 优化版本性能测试"
echo "========================================"

./benchmark/performance_benchmark "$LEADER_ADDR" "$TEST_DURATION" | tee "$OUTPUT_DIR/optimized_test.log"

echo ""
echo "优化版本测试完成，结果已保存到 $OUTPUT_DIR/optimized_test.log"

echo ""
echo "========================================"
echo "生成对比报告"
echo "========================================"

cat > "$OUTPUT_DIR/comparison_report.md" << 'EOF'
# Raft 数据库性能对比测试报告

## 测试环境

- **测试时间**: $(date)
- **Leader 地址**: $LEADER_ADDR
- **测试时长**: $TEST_DURATION

## 测试场景

1. **低吞吐量测试**: 10 并发，目标 1000 QPS
2. **高吞吐量测试**: 100 并发，目标 10000 QPS
3. **混合读写测试**: 70% 写，30% 读

## 性能对比

### 原版本结果

EOF

grep -A 20 "性能指标:" "$OUTPUT_DIR/legacy_test.log" >> "$OUTPUT_DIR/comparison_report.md"

cat >> "$OUTPUT_DIR/comparison_report.md" << 'EOF'

### 优化版本结果

EOF

grep -A 20 "性能指标:" "$OUTPUT_DIR/optimized_test.log" >> "$OUTPUT_DIR/comparison_report.md"

echo ""
echo "对比报告已生成: $OUTPUT_DIR/comparison_report.md"
echo ""
echo "========================================"
echo "测试完成"
echo "========================================"
echo ""
echo "测试结果保存在: $OUTPUT_DIR/"
ls -lh "$OUTPUT_DIR/"
