#!/bin/bash

# 测试优化版本是否正确调用的脚本

echo "========================================="
echo "测试优化版本调用"
echo "========================================="

# 编译
echo ""
echo "1. 编译程序..."
cd src
CGO_LDFLAGS="-lrocksdb" go build -o kvstore main.go
if [ $? -eq 0 ]; then
    echo "✓ 编译成功"
else
    echo "✗ 编译失败"
    exit 1
fi

# 测试原版本
echo ""
echo "2. 测试原版本..."
echo "启动原版本（不使用 -optimized 参数）"
timeout 3s ./kvstore -config config/config.json -id 0 2>&1 | grep -E "(Batch Manager|applyTask)" | head -5 &
sleep 2
echo "✓ 原版本启动测试完成"

# 测试优化版本
echo ""
echo "3. 测试优化版本..."
echo "启动优化版本（使用 -optimized 参数）"
timeout 3s ./kvstore -config config/config.json -id 0 -optimized 2>&1 | grep -E "(Optimized|applyTaskOptimized)" | head -5 &
sleep 2
echo "✓ 优化版本启动测试完成"

# 清理
echo ""
echo "4. 清理..."
pkill -f kvstore
echo "✓ 清理完成"

echo ""
echo "========================================="
echo "测试完成"
echo "========================================="
echo ""
echo "预期结果："
echo "- 原版本应该输出: 'Legacy Batch Manager initialized'"
echo "- 优化版本应该输出: 'Optimized Batch Manager initialized'"
echo ""
