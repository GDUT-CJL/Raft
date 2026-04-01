#!/bin/bash

LEADER_ADDR=${1:-"localhost:6379"}
TEST_DURATION="10s"

echo "========================================"
echo "快速性能测试"
echo "========================================"
echo "Leader 地址: $LEADER_ADDR"
echo "测试时长: $TEST_DURATION"
echo ""

if [ ! -f "benchmark/performance_benchmark" ]; then
    echo "编译测试工具..."
    cd benchmark
    go build -o performance_benchmark performance_benchmark.go
    cd ..
fi

echo ""
echo "开始测试..."
echo ""

./benchmark/performance_benchmark "$LEADER_ADDR" "$TEST_DURATION"

echo ""
echo "========================================"
echo "快速测试完成"
echo "========================================"
echo ""
echo "要进行完整的对比测试，请运行:"
echo "  ./run_benchmark.sh $LEADER_ADDR 30s"
