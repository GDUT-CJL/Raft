# 使用最小化的 Ubuntu 基础镜像
# building : docker build -t kvstore:latest .
FROM ubuntu:22.04

# 安装运行时依赖
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libc6 \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 拷贝 C 动态库
COPY src/bridge/libstorage.so /usr/local/lib/libstorage.so

# 更新动态链接缓存
RUN ldconfig /usr/local/lib

# 创建软链接（可选，确保能找到 .so）
RUN ln -sf /usr/local/lib/libstorage.so /app/libstorage.so

# 拷贝 Go 二进制文件
COPY src/kvstore /app/kvstore

# 拷贝配置文件目录
COPY src/config/config.json /app/config.json

# 拷贝入口点脚本
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# 暴露客户端和 RPC 端口
EXPOSE 8000 8001

# 设置入口点
ENTRYPOINT ["/app/entrypoint.sh"]