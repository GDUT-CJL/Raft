# Raft
Raft of Go

## 领导者选举
1、状态转换（三个转换函数）
    1）becomeFollowerLocked
    2）becomeCandidateLocked
    3）becomeLeaderLocked
2、选举逻辑
    1）选举的循环
    2）单轮选举
    3）单次RPC
    4）请求回应
3、心跳逻辑
    1）心跳的循环
    2）单轮心跳
    3）单词RPC
    4）请求回应

## 日志复制

