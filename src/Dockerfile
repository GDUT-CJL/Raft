# 使用 Debian Slim 作为基础镜像（支持 CGO 和 .so）
FROM debian:bookworm-slim

WORKDIR /app

# 安装必要的依赖（根据你 C 引擎实际依赖调整）
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libssl-dev \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# 设置动态链接库路径（必须包含 .so 所在目录）
ENV LD_LIBRARY_PATH /app

# 复制配置文件目录
COPY src/config /app/config

# 复制二进制文件和 .so 文件
COPY src/kvstore .
COPY src/bridge/libstorage.so .

# 暴露程序监听端口（根据你的 config.json 中的 port 修改）
EXPOSE 8000 8001 8002

# 启动命令（默认使用 config.json）
CMD ["./kvstore", "-path", "config/config.json"]