#!/bin/bash

# 检查节点状态
check_node() {
    local node=$1
    local port=$2
    echo "检查节点 $node 状态..."
    curl -s http://localhost:$port/health | jq .
}

check_node node0 8000
check_node node1 8010
check_node node2 8020

# 检查Raft集群状态
echo -e "\n检查Raft集群状态..."
docker exec $(docker ps -q -f name=raft_cluster_node0) ./kvstore -id 0 -check-cluster