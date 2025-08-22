#ifndef JL_CLIENT_HPP
#define JL_CLIENT_HPP
#include <string>
#include <string.h>
#include <iostream>
#include <stdlib.h>
#include <unistd.h>
#include <array>
#include <arpa/inet.h>

#define MAX_BUFFER_SIZE 1024
#define MAX_RETRY_ATTEMPTS 3
#define LEADER_ERROR_PREFIX "ErrorNotLeader"

using namespace std;
class JlClient{
public:
    JlClient(string ip, int port);
    ~JlClient();

    // 连接到服务器
    int Jl_ConnectServer();

    // 发送命令（带智能路由）
    int Jl_SendCommand(string command);

    // 接收消息
    ssize_t Jl_RecvMsg();

    // 获取最后接收到的消息
    char* Jl_GetResponse();

    // 关闭连接并释放资源
    void Jl_Close();
private:
    bool is_leader_command(string& command);
    int parse_leader_info(string command);
    int reconnect_to_leader();
private:
    int m_connfd;             // 连接描述符
    string cur_ip;         // 当前连接节点的IP
    int cur_port;               // 当前连接节点的端口
    string leader_ip;        // Leader节点的IP地址
    int leader_port;        // Leader节点的端口
    char recv_msg[1024];    // 接收消息缓冲区    
};

#endif