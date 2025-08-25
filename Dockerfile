# 使用轻量级基础镜像
FROM ubuntu:22.04

# 设置环境变量
ENV LANG=C.UTF-8 \
    TZ=UTC \
    DEBIAN_FRONTEND=noninteractive

# 设置工作目录
WORKDIR /app

# 安装运行时依赖：包括 RocksDB 运行时库
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libc6 \
        ca-certificates \
        iproute2 \
        libgflags-dev \
        libsnappy-dev \
        liblz4-dev \
        libzstd-dev \
        libbz2-dev \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# 如果你使用的是自己编译的 RocksDB 版本，也可以 COPY 预编译的 .so 文件：
COPY src/lib/librocksdb.so.10.6 /usr/lib/librocksdb.so.10.6
RUN ln -s /usr/lib/librocksdb.so.10.6 /usr/lib/librocksdb.so && ldconfig

# 更新动态链接器缓存（重要）
RUN ldconfig

# 复制预编译的二进制文件
COPY src/kvstore .

# 复制预编译的共享库（如 libstorage.so）
COPY src/bridge/libstorage.so /usr/lib/
RUN ldconfig

# 创建配置目录
RUN mkdir -p /app/config

# 复制入口点脚本
COPY deploy/entrypoint.sh .
RUN chmod +x entrypoint.sh

# 暴露端口
EXPOSE 8000 8001

# 设置入口点
ENTRYPOINT ["./entrypoint.sh"]