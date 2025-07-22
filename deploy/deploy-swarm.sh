#!/bin/bash
# 
# 1. 构建镜像
docker build -t distribute_kvstore:1.0 .

# 2. 推送镜像 (如果使用多节点)
docker tag 268ef675a20f crpi-cp7w9uw5oadlnxc8.cn-guangzhou.personal.cr.aliyuncs.com/gdut-cjl/kvstore:1.0
docker push  crpi-cp7w9uw5oadlnxc8.cn-guangzhou.personal.cr.aliyuncs.com/gdut-cjl/kvstore:1.0 

# 3. 创建Swarm配置
docker config rm cluster_config_v1
docker config create cluster_config_v1 config.json

# 4. 初始化Swarm集群 (在第一个节点执行)
docker swarm init --advertise-addr 192.168.79.158

# 5. 添加工作节点 (在其他节点执行)
docker swarm join --token <SWARM-TOKEN> <MANAGER-IP>:2377

# 6. 创建网络
docker network create -d overlay --attachable raft_net

# 7. 部署服务
docker stack deploy -c docker-compose.yml raft_cluster

# 8. 监控部署状态
watch docker service ls