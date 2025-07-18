#!/bin/bash
#! how to run: 
# docker run -d --network host --name node0 -e NODE_ID=0 kvstore:latest
# docker run -d --network host --name node1 -e NODE_ID=1 kvstore:latest
# docker run -d --network host --name node2 -e NODE_ID=2 kvstore:latest
# 支持环境变量注入 NODE_ID，默认为 0
NODE_ID=${NODE_ID:-0}

# 设置库路径
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

# 执行程序并传递参数
./kvstore -id $NODE_ID -config /app/config.json