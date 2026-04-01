# 性能测试工具使用指南

## 概述

本性能测试工具用于对比 Raft 数据库原版本和优化版本的性能差异，支持多种测试场景和详细的性能指标收集。

## 测试工具组成

### 1. performance_benchmark.go
主要的性能测试程序，支持：
- 低吞吐量测试（10 并发，1000 QPS）
- 高吞吐量测试（100 并发，10000 QPS）
- 混合读写测试（70% 写，30% 读）

### 2. compare_results.go
对比分析工具，用于：
- 解析测试日志
- 生成对比报告（JSON 和 Markdown 格式）
- 计算性能提升百分比

### 3. run_benchmark.sh
自动化测试脚本，用于：
- 自动编译测试工具
- 依次运行原版本和优化版本测试
- 生成对比报告

## 快速开始

### 方法一：使用自动化脚本（推荐）

```bash
# 1. 赋予执行权限
chmod +x run_benchmark.sh

# 2. 运行测试（指定 Leader 地址和测试时长）
./run_benchmark.sh <LEADER_IP:PORT> <DURATION>

# 示例
./run_benchmark.sh 192.168.1.100:6379 30s
```

脚本会自动：
1. 编译测试工具
2. 提示你启动原版本数据库
3. 运行原版本测试
4. 提示你重启优化版本数据库
5. 运行优化版本测试
6. 生成对比报告

### 方法二：手动运行

#### 步骤 1: 编译测试工具

```bash
cd benchmark
go build -o performance_benchmark performance_benchmark.go
go build -o compare_results compare_results.go
cd ..
```

#### 步骤 2: 启动原版本数据库

在 3 个虚拟机中启动数据库（不使用 `-optimized` 参数）：

```bash
# 虚拟机 1 (Leader)
./kvstore -config config/config.json -id 0

# 虚拟机 2 (Follower)
./kvstore -config config/config.json -id 1

# 虚拟机 3 (Follower)
./kvstore -config config/config.json -id 2
```

#### 步骤 3: 运行原版本测试

```bash
./benchmark/performance_benchmark <LEADER_IP:PORT> <DURATION>

# 示例
./benchmark/performance_benchmark 192.168.1.100:6379 30s | tee legacy_test.log
```

#### 步骤 4: 重启优化版本数据库

停止所有节点，然后使用 `-optimized` 参数重启：

```bash
# 虚拟机 1 (Leader)
./kvstore -config config/config.json -id 0 -optimized

# 虚拟机 2 (Follower)
./kvstore -config config/config.json -id 1 -optimized

# 虚拟机 3 (Follower)
./kvstore -config config/config.json -id 2 -optimized
```

#### 步骤 5: 运行优化版本测试

```bash
./benchmark/performance_benchmark <LEADER_IP:PORT> <DURATION>

# 示例
./benchmark/performance_benchmark 192.168.1.100:6379 30s | tee optimized_test.log
```

#### 步骤 6: 生成对比报告

```bash
./benchmark/compare_results legacy_test.log optimized_test.log
```

## 测试场景说明

### 场景 1: 低吞吐量测试

**目的**: 测试系统在低负载下的性能

**参数**:
- 并发数: 10
- 目标 QPS: 1000
- 操作类型: 100% SET

**预期结果**:
- 原版本: ~1000 ops/s
- 优化版本: ~1000 ops/s（差异不大）

### 场景 2: 高吞吐量测试

**目的**: 测试系统在高负载下的性能

**参数**:
- 并发数: 100
- 目标 QPS: 10000
- 操作类型: 100% SET

**预期结果**:
- 原版本: ~5000 ops/s
- 优化版本: ~25000 ops/s（5x 提升）

### 场景 3: 混合读写测试

**目的**: 测试系统在真实场景下的性能

**参数**:
- 并发数: 100
- 目标 QPS: 10000
- 操作类型: 70% SET, 30% GET

**预期结果**:
- 原版本: ~6000 ops/s
- 优化版本: ~30000 ops/s（5x 提升）

## 性能指标说明

### 吞吐量 (Throughput)
- **定义**: 每秒成功处理的操作数
- **单位**: ops/s
- **越高越好**

### 延迟 (Latency)
- **平均延迟**: 所有请求的平均响应时间
- **P50 延迟**: 50% 的请求响应时间低于此值
- **P95 延迟**: 95% 的请求响应时间低于此值
- **P99 延迟**: 99% 的请求响应时间低于此值
- **单位**: 微秒 (μs) 或毫秒 (ms)
- **越低越好**

### QPS (Queries Per Second)
- **定义**: 每秒处理的查询数（包括成功和失败）
- **单位**: queries/s
- **越高越好**

### 错误率 (Error Rate)
- **定义**: 失败请求占总请求的百分比
- **单位**: %
- **越低越好**

## 测试结果示例

### 控制台输出

```
========================================
测试: 高吞吐量测试
并发数: 100, 目标QPS: 10000
========================================

性能指标:
  总请求数:      300000
  成功请求:      298500
  失败请求:      1500
  错误率:        0.50%
  实际QPS:       10000.00
  吞吐量:        9950.00 ops/s
  平均延迟:      10050.25 μs (10.05 ms)
  最小延迟:      1000 μs
  最大延迟:      50000 μs
  P50延迟:       9500.50 μs (9.50 ms)
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
总请求数            300000          300000          -
成功请求            298500          299700          -
失败请求            1500            300             -
吞吐量 (ops/s)      9950.00         49950.00        +402.01%
平均延迟 (μs)       10050.25        2005.50         +80.04%
P50延迟 (μs)        9500.50         1900.25         +80.00%
P95延迟 (μs)        15000.75        3000.50         +80.00%
P99延迟 (μs)        25000.50        5000.75         +80.00%
实际QPS             10000.00        50000.00        +400.00%
错误率              0.50%           0.10%           -0.40%

关键发现:
  ✅ 吞吐量提升 402.01%
  ✅ 平均延迟降低 80.04%
  ✅ P99延迟降低 80.00%
  ✅ 错误率降低 0.40%
```

## 测试报告文件

测试完成后会生成以下文件：

1. **performance_report_YYYYMMDD_HHMMSS.json**
   - JSON 格式的详细测试报告
   - 包含所有测试场景的原始数据

2. **comparison_report_YYYYMMDD_HHMMSS.json**
   - JSON 格式的对比报告
   - 包含性能提升计算

3. **comparison_report_YYYYMMDD_HHMMSS.md**
   - Markdown 格式的对比报告
   - 适合文档和展示

## 自定义测试参数

### 修改测试配置

编辑 `performance_benchmark.go` 中的配置：

```go
config := TestConfig{
    ServerAddr:      "localhost:6379",    // 服务器地址
    Duration:        30 * time.Second,    // 测试时长
    LowConcurrency:  10,                  // 低并发数
    HighConcurrency: 100,                 // 高并发数
    LowQPS:          1000,                // 低 QPS
    HighQPS:         10000,               // 高 QPS
    WarmupDuration:  5 * time.Second,     // 预热时长
}
```

### 命令行参数

```bash
# 指定服务器地址
./performance_benchmark 192.168.1.100:6379

# 指定服务器地址和测试时长
./performance_benchmark 192.168.1.100:6379 60s
```

## 性能优化建议

### 1. 系统配置优化

```bash
# 增加文件描述符限制
ulimit -n 65535

# 优化 TCP 参数
sudo sysctl -w net.core.somaxconn=65535
sudo sysctl -w net.ipv4.tcp_max_syn_backlog=65535
```

### 2. 测试环境优化

- 使用独立的测试机器
- 确保网络延迟低
- 关闭不必要的服务
- 使用 SSD 存储

### 3. 数据库配置优化

根据测试结果调整批量参数：

```json
{
  "batch_size": 100,        // 批量大小
  "batch_timeout": "10ms"   // 批量超时
}
```

## 故障排查

### 问题 1: 连接失败

**错误**: `连接失败: dial tcp: connection refused`

**解决**:
- 检查数据库是否启动
- 检查防火墙设置
- 确认 Leader 地址正确

### 问题 2: 高错误率

**错误**: 错误率 > 5%

**解决**:
- 检查网络稳定性
- 降低并发数或 QPS
- 检查数据库日志

### 问题 3: 性能差异不明显

**可能原因**:
- 批量大小设置过小
- 测试时长过短
- 系统资源不足

**解决**:
- 增加批量大小到 200-500
- 延长测试时长到 60s
- 监控 CPU 和内存使用

## 最佳实践

1. **预热系统**: 每次测试前运行预热阶段
2. **多次测试**: 每个版本测试 3-5 次，取平均值
3. **监控资源**: 使用 `top`、`htop`、`iostat` 监控系统资源
4. **记录环境**: 记录测试环境的硬件配置和网络状况
5. **对比基准**: 建立性能基准，跟踪性能变化

## 总结

本性能测试工具提供了完整的性能对比测试方案，包括：

- ✅ 多种测试场景（低/高吞吐量、混合读写）
- ✅ 详细的性能指标（吞吐量、延迟、错误率）
- ✅ 自动化测试流程
- ✅ 对比分析报告
- ✅ 可视化展示

通过这些工具，你可以全面评估优化版本的性能提升，并根据测试结果进一步优化系统配置。
