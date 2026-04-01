# 性能测试工具总结

## 📦 已创建的文件

### 1. 性能测试程序

#### [benchmark/performance_benchmark.go](file:///home/chen/jlkv/Raft/benchmark/performance_benchmark.go)
**功能**: 主性能测试程序
- ✅ 支持 3 种测试场景（低/高吞吐量、混合读写）
- ✅ 收集详细性能指标（吞吐量、延迟、错误率）
- ✅ 生成 JSON 格式测试报告
- ✅ 支持命令行参数配置

**使用**:
```bash
./benchmark/performance_benchmark <LEADER_IP:PORT> <DURATION>
```

#### [benchmark/compare_results.go](file:///home/chen/jlkv/Raft/benchmark/compare_results.go)
**功能**: 对比分析工具
- ✅ 解析测试日志
- ✅ 计算性能提升百分比
- ✅ 生成 JSON 和 Markdown 格式对比报告
- ✅ 详细的对比表格展示

**使用**:
```bash
./benchmark/compare_results legacy_test.log optimized_test.log
```

### 2. 自动化脚本

#### [run_benchmark.sh](file:///home/chen/jlkv/Raft/run_benchmark.sh)
**功能**: 自动化测试脚本
- ✅ 自动编译测试工具
- ✅ 依次运行原版本和优化版本测试
- ✅ 生成对比报告
- ✅ 交互式提示

**使用**:
```bash
./run_benchmark.sh <LEADER_IP:PORT> <DURATION>
```

#### [quick_test.sh](file:///home/chen/jlkv/Raft/quick_test.sh)
**功能**: 快速测试脚本
- ✅ 快速验证性能（10秒测试）
- ✅ 自动编译工具
- ✅ 简化测试流程

**使用**:
```bash
./quick_test.sh <LEADER_IP:PORT>
```

### 3. 文档

#### [docs/PERFORMANCE_TESTING_GUIDE.md](file:///home/chen/jlkv/Raft/docs/PERFORMANCE_TESTING_GUIDE.md)
**功能**: 完整使用指南
- ✅ 快速开始指南
- ✅ 测试场景说明
- ✅ 性能指标解释
- ✅ 故障排查指南
- ✅ 最佳实践

#### [benchmark/README.sh](file:///home/chen/jlkv/Raft/benchmark/README.sh)
**功能**: 快速帮助信息
- ✅ 使用方法说明
- ✅ 测试场景列表
- ✅ 性能指标说明

## 🚀 快速开始

### 方法一：自动化测试（推荐）

```bash
# 1. 启动原版本数据库（3个节点）
# 虚拟机 1 (Leader)
./kvstore -config config/config.json -id 0

# 虚拟机 2 (Follower)
./kvstore -config config/config.json -id 1

# 虚拟机 3 (Follower)
./kvstore -config config/config.json -id 2

# 2. 运行自动化测试
./run_benchmark.sh 192.168.1.100:6379 30s

# 3. 根据提示重启优化版本数据库
# 虚拟机 1 (Leader)
./kvstore -config config/config.json -id 0 -optimized

# 虚拟机 2 (Follower)
./kvstore -config config/config.json -id 1 -optimized

# 虚拟机 3 (Follower)
./kvstore -config config/config.json -id 2 -optimized

# 4. 按 Enter 继续测试优化版本
```

### 方法二：快速测试

```bash
# 快速验证（10秒测试）
./quick_test.sh 192.168.1.100:6379
```

### 方法三：手动测试

```bash
# 1. 编译测试工具
cd benchmark
go build -o performance_benchmark performance_benchmark.go
go build -o compare_results compare_results.go
cd ..

# 2. 测试原版本
./benchmark/performance_benchmark 192.168.1.100:6379 30s | tee legacy_test.log

# 3. 测试优化版本
./benchmark/performance_benchmark 192.168.1.100:6379 30s | tee optimized_test.log

# 4. 生成对比报告
./benchmark/compare_results legacy_test.log optimized_test.log
```

## 📊 测试场景

### 场景 1: 低吞吐量测试
- **并发数**: 10
- **目标 QPS**: 1000
- **操作类型**: 100% SET
- **目的**: 测试低负载性能

### 场景 2: 高吞吐量测试
- **并发数**: 100
- **目标 QPS**: 10000
- **操作类型**: 100% SET
- **目的**: 测试高负载性能

### 场景 3: 混合读写测试
- **并发数**: 100
- **目标 QPS**: 10000
- **操作类型**: 70% SET, 30% GET
- **目的**: 测试真实场景性能

## 📈 性能指标

### 吞吐量 (Throughput)
- 每秒成功处理的操作数
- 单位: ops/s
- 越高越好

### 延迟 (Latency)
- **平均延迟**: 所有请求的平均响应时间
- **P50 延迟**: 50% 请求的响应时间
- **P95 延迟**: 95% 请求的响应时间
- **P99 延迟**: 99% 请求的响应时间
- 单位: 微秒 (μs)
- 越低越好

### 错误率 (Error Rate)
- 失败请求占总请求的百分比
- 单位: %
- 越低越好

## 📝 测试报告示例

### 控制台输出

```
========================================
测试: 高吞吐量测试
并发数: 100, 目标QPS: 10000
========================================

性能指标:
  总请求数:      300000
  成功请求:      299700
  失败请求:      300
  错误率:        0.10%
  实际QPS:       10000.00
  吞吐量:        9990.00 ops/s
  平均延迟:      10000.50 μs (10.00 ms)
  P50延迟:       9500.25 μs (9.50 ms)
  P95延迟:       15000.75 μs (15.00 ms)
  P99延迟:       25000.50 μs (25.00 ms)
```

### 对比报告

```
========================================
性能对比分析报告
========================================

测试场景: 高吞吐量测试
----------------------------------------

指标                原版本          优化版本        提升
----                ------          --------        ----
吞吐量 (ops/s)      9950.00         49950.00        +402.01%
平均延迟 (μs)       10050.25        2005.50         +80.04%
P99延迟 (μs)        25000.50        5000.75         +80.00%
实际QPS             10000.00        50000.00        +400.00%
错误率              0.50%           0.10%           -0.40%

关键发现:
  ✅ 吞吐量提升 402.01%
  ✅ 平均延迟降低 80.04%
  ✅ P99延迟降低 80.00%
  ✅ 错误率降低 0.40%
```

## 🎯 预期性能提升

基于优化设计，预期性能提升：

| 指标 | 原版本 | 优化版本 | 提升比例 |
|------|--------|----------|----------|
| **吞吐量** | ~10K ops/s | ~50K ops/s | **5x** |
| **平均延迟** | ~50ms | ~10ms | **5x** |
| **P99延迟** | ~200ms | ~30ms | **6.7x** |
| **CPU使用率** | 80% | 40% | **2x** |

## 🔧 自定义测试

### 修改测试参数

编辑 `benchmark/performance_benchmark.go`:

```go
config := TestConfig{
    ServerAddr:      "192.168.1.100:6379",  // Leader 地址
    Duration:        60 * time.Second,      // 测试时长
    LowConcurrency:  20,                    // 低并发数
    HighConcurrency: 200,                   // 高并发数
    LowQPS:          2000,                  // 低 QPS
    HighQPS:         20000,                 // 高 QPS
    WarmupDuration:  10 * time.Second,      // 预热时长
}
```

### 命令行参数

```bash
# 指定服务器地址
./benchmark/performance_benchmark 192.168.1.100:6379

# 指定服务器地址和测试时长
./benchmark/performance_benchmark 192.168.1.100:6379 60s
```

## 📂 生成的文件

测试完成后会生成以下文件：

1. **benchmark_results_YYYYMMDD_HHMMSS/**
   - legacy_test.log: 原版本测试日志
   - optimized_test.log: 优化版本测试日志
   - comparison_report.md: 对比报告

2. **performance_report_YYYYMMDD_HHMMSS.json**
   - 详细测试数据（JSON 格式）

3. **comparison_report_YYYYMMDD_HHMMSS.json**
   - 对比分析数据（JSON 格式）

4. **comparison_report_YYYYMMDD_HHMMSS.md**
   - 对比报告（Markdown 格式）

## 🐛 故障排查

### 问题 1: 连接失败

```bash
# 检查数据库是否启动
netstat -tlnp | grep 6379

# 检查防火墙
sudo ufw status
```

### 问题 2: 高错误率

```bash
# 降低并发数
# 修改配置: HighConcurrency: 50

# 降低 QPS
# 修改配置: HighQPS: 5000
```

### 问题 3: 性能差异不明显

```bash
# 增加批量大小
# 修改配置文件: batch_size: 200

# 延长测试时长
./benchmark/performance_benchmark 192.168.1.100:6379 60s
```

## 📚 相关文档

- [性能测试指南](file:///home/chen/jlkv/Raft/docs/PERFORMANCE_TESTING_GUIDE.md)
- [客户端请求流程](file:///home/chen/jlkv/Raft/docs/CLIENT_REQUEST_FLOW.md)
- [优化修复说明](file:///home/chen/jlkv/Raft/docs/OPTIMIZATION_FIX.md)

## ✅ 总结

已创建完整的性能测试工具集：

- ✅ **性能测试程序**: 支持多种测试场景
- ✅ **对比分析工具**: 自动生成对比报告
- ✅ **自动化脚本**: 简化测试流程
- ✅ **完整文档**: 详细使用指南

现在可以开始测试了！🚀
