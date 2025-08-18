#ifndef JL_CLIENT_H
#define JL_CLIENT_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <ctype.h> // 用于isspace()

struct Jl_Client {
    int connfd;             // 连接描述符
    const char* ip;         // 当前连接节点的IP
    int port;               // 当前连接节点的端口
    char* leader_ip;        // Leader节点的IP地址
    int leader_port;        // Leader节点的端口
    char recv_msg[1024];    // 接收消息缓冲区
};

// 创建客户端实例
struct Jl_Client* Jl_CreatClient(const char* ip, int port);

// 连接到服务器
int Jl_ConnectServer(struct Jl_Client* cli);

// 发送命令（带智能路由）
int Jl_SendCommand(struct Jl_Client* cli, const char* command);

// 接收消息
ssize_t Jl_RecvMsg(struct Jl_Client* cli);

// 获取最后接收到的消息
const char* Jl_GetResponse(struct Jl_Client* cli);

// 关闭连接并释放资源
void Jl_Close(struct Jl_Client* cli);

#endif // JL_CLIENT_H