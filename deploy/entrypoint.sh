#!/bin/bash
set -x  # 启用详细日志

# 创建配置目录
mkdir -p /app/config

# 方法1：优先使用挂载的配置文件
if [ -f "/cluster_config.json" ]; then
    echo "Using mounted config file"
    cp /cluster_config.json /app/config/config.json
    
# 方法2：使用环境变量
elif [ -n "$CLUSTER_CONFIG" ]; then
    echo "Using environment variable config"
    echo "$CLUSTER_CONFIG" > /app/config/config.json
    
# 方法3：使用默认配置
else
    echo "Using default config"
    cat > /app/config/config.json <<EOF
[
    {
        "id": 0,
        "rpc_addr": "node0:8001",
        "client_addr": "0.0.0.0:8000"
    },
    {
        "id": 1,
        "rpc_addr": "node1:8001",
        "client_addr": "0.0.0.0:8000"
    },
    {
        "id": 2,
        "rpc_addr": "node2:8001",
        "client_addr": "0.0.0.0:8000"
    }
]
EOF
fi

# 验证配置文件
echo "Config file content:"
cat /app/config/config.json

# 检查文件大小
CONFIG_SIZE=$(stat -c%s "/app/config/config.json")
echo "Config file size: $CONFIG_SIZE bytes"

if [ "$CONFIG_SIZE" -eq 0 ]; then
    echo "ERROR: Config file is empty!"
    exit 1
fi
chmod 644 /app/config/config.json
# 设置库路径
export LD_LIBRARY_PATH=/usr/lib:$LD_LIBRARY_PATH

# 启动应用
echo "Starting application..."
exec ./kvstore -id $NODE_ID -config /app/config/config.json