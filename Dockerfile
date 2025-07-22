# 使用轻量级基础镜像
# docker build -t distribute_kvstore:1.0 .

# docker build -t distribute_kvstore:1.0 .
# docker tag 268ef675a20f crpi-cp7w9uw5oadlnxc8.cn-guangzhou.personal.cr.aliyuncs.com/gdut-cjl/kvstore:1.0
# docker push  crpi-cp7w9uw5oadlnxc8.cn-guangzhou.personal.cr.aliyuncs.com/gdut-cjl/kvstore:1.0 
FROM ubuntu:22.04

# 设置环境变量
ENV LANG=C.UTF-8 \
    TZ=UTC \
    DEBIAN_FRONTEND=noninteractive

# 安装运行时依赖
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libc6 ca-certificates \
    iproute2 \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制预编译的二进制文件
COPY src/kvstore .

# 复制预编译的共享库
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