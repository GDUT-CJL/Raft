#include "JlClient.h"

#define MAX_BUFFER_SIZE 1024
#define MAX_RETRY_ATTEMPTS 3
#define LEADER_ERROR_PREFIX "ErrorNotLeader"

// 判断是否为需要Leader处理的命令
static int is_leader_command(const char* command) {
    if (!command || !*command) return 0;

    const char* leader_commands[] = {"set", "delete", "count"};
    int n_commands = sizeof(leader_commands) / sizeof(leader_commands[0]);

    for (int i = 0; i < n_commands; i++) {
        const char* cmd = leader_commands[i];
        int len = strlen(cmd);
        // 检查前缀匹配（不区分大小写）只检查前面len个字符
        if (strncasecmp(command, cmd, len) == 0) {
            // 检查命令后面是否是分隔符（空格、换行、结束符等）
            char c = command[len];
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\0') {
                return 1;
            }
        }
    }
    return 0;
}

// 从错误消息中解析Leader地址（新格式："ErrorNotLeader 192.168.100.10:8000"）
static int parse_leader_info(struct Jl_Client* cli, const char* error_msg) {
    // 查找错误前缀
    const char* prefix_start = strstr(error_msg, LEADER_ERROR_PREFIX);
    if (!prefix_start) return -1;
    
    // 移动到前缀结束位置
    const char* addr_start = prefix_start + strlen(LEADER_ERROR_PREFIX);
    
    // 跳过可能存在的空格
    while (*addr_start && isspace(*addr_start)) {
        addr_start++;
    }
    
    // 查找地址结束位置（空格或字符串结束）
    const char* addr_end = addr_start;
    while (*addr_end && !isspace(*addr_end) && *addr_end != '\r' && *addr_end != '\n') {
        addr_end++;
    }
    
    // 检查是否找到有效地址
    if (addr_end == addr_start) return -1;
    
    // 提取地址字符串
    size_t addr_len = addr_end - addr_start;
    char leader_addr[64];
    if (addr_len >= sizeof(leader_addr)) addr_len = sizeof(leader_addr) - 1;
    strncpy(leader_addr, addr_start, addr_len);
    leader_addr[addr_len] = '\0';
    
    // 分离IP和端口
    char* colon = strrchr(leader_addr, ':');
    if (!colon) return -1; // 没有找到冒号
    
    *colon = '\0'; // 分割字符串
    int port = atoi(colon + 1);
    
    // 验证IP地址格式
    struct in_addr ip_addr;
    if (inet_pton(AF_INET, leader_addr, &ip_addr) != 1) {
        return -1; // 无效的IP地址
    }
    
    // 更新Leader信息
    if (cli->leader_ip) free(cli->leader_ip);
    cli->leader_ip = strdup(leader_addr);
    cli->leader_port = port;
    
    // 调试信息
    printf("Discovered new leader: %s:%d\n", cli->leader_ip, cli->leader_port);
    
    return 0;
}

// 重新连接到Leader节点
static int reconnect_to_leader(struct Jl_Client* cli) {
    if (!cli->leader_ip || cli->leader_port <= 0) {
        return -1; // 没有有效的Leader信息
    }
    
    // 关闭旧连接
    if (cli->connfd != -1) {
        close(cli->connfd);
        cli->connfd = -1;
    }
    
    // 更新连接信息
    cli->ip = cli->leader_ip;
    cli->port = cli->leader_port;
    
    // 建立新连接
    return Jl_ConnectServer(cli);
}

// 创建客户端实例
struct Jl_Client* Jl_CreatClient(const char* ip, int port) {
    struct Jl_Client* cli = calloc(1, sizeof(struct Jl_Client));
    if (!cli) {
        return NULL;
    }
    
    cli->ip = ip;
    cli->port = port;
    cli->connfd = -1;
    cli->leader_ip = NULL;
    cli->leader_port = -1;
    
    return cli;
}

// 连接到服务器
int Jl_ConnectServer(struct Jl_Client* cli) {
    // 如果已有连接，先关闭
    if (cli->connfd != -1) {
        close(cli->connfd);
        cli->connfd = -1;
    }
    
    // 创建socket
    int connfd = socket(AF_INET, SOCK_STREAM, 0);
    if (connfd < 0) {
        perror("socket");
        return -1;
    }
    
    // 设置服务器地址
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    
    // 转换IP地址
    if (inet_pton(AF_INET, cli->ip, &server_addr.sin_addr) <= 0) {
        perror("inet_pton");
        close(connfd);
        return -1;
    }
    
    server_addr.sin_port = htons(cli->port);
    
    // 连接服务器
    if (connect(connfd, (struct sockaddr*)&server_addr, sizeof(server_addr))){
        perror("connect");
        close(connfd);
        return -1;
    }
    
    cli->connfd = connfd;
    printf("Connected to %s:%d\n", cli->ip, cli->port);
    return 0;
}

// 发送命令（带智能路由和重试机制）
int Jl_SendCommand(struct Jl_Client* cli, const char* command) {
    if (!cli || !command) return -1;
    
    // 检查连接状态
    if (cli->connfd == -1) {
        if (Jl_ConnectServer(cli)) {
            return -1;
        }
    }
    
    int is_leader_cmd = is_leader_command(command);
    int retry_count = 0;
    
    while (retry_count++ < MAX_RETRY_ATTEMPTS) {
        // 如果是写命令且知道Leader，确保连接到Leader
        if (is_leader_cmd && cli->leader_ip) {
            if (strcmp(cli->ip, cli->leader_ip) != 0 || cli->port != cli->leader_port) {
                printf("Redirecting command to leader: %s:%d\n", 
                      cli->leader_ip, cli->leader_port);
                if (reconnect_to_leader(cli)) {
                    // 重连失败，尝试原始连接
                    Jl_ConnectServer(cli);
                }
            }
        }
        
        // 发送命令
        ssize_t sent = send(cli->connfd, command, strlen(command), 0);
        if (sent <= 0) {
            perror("send");
            
            // 尝试重新连接
            if (Jl_ConnectServer(cli)) {
                return -1;
            }
            continue; // 重试发送
        }
        
        // 接收响应
        ssize_t received = Jl_RecvMsg(cli);
        if (received <= 0) {
            // 接收失败，尝试重新连接
            if (Jl_ConnectServer(cli)) {
                return -1;
            }
            continue; // 重试发送
        }
        
        // 检查是否为Leader错误
        if (strstr(cli->recv_msg, LEADER_ERROR_PREFIX)) {
            printf("Received leader redirect: %s", cli->recv_msg);
            // 解析并更新Leader信息
            if (parse_leader_info(cli, cli->recv_msg) == 0) {
                // 重连到Leader并重试
                if (reconnect_to_leader(cli) == 0) {
                    continue; // 使用新Leader重试
                }
            }
            return -1; // 无法处理重定向
        }
        
        // 成功收到有效响应
        return 0;
    }
    
    // 超过最大重试次数
    return -1;
}

// 接收消息
ssize_t Jl_RecvMsg(struct Jl_Client* cli) {
    if (!cli || cli->connfd == -1) return -1;
    
    ssize_t bytes = recv(cli->connfd, cli->recv_msg, MAX_BUFFER_SIZE - 1, 0);
    if (bytes <= 0) {
        if (bytes == 0) {
            fprintf(stderr, "Connection closed by server\n");
        } else {
            perror("recv");
        }
        return -1;
    }
    
    cli->recv_msg[bytes] = '\0'; // 确保字符串终止
    return bytes;
}

// 关闭连接并释放资源
void Jl_Close(struct Jl_Client* cli) {
    if (!cli) return;
    
    if (cli->connfd != -1) {
        close(cli->connfd);
        cli->connfd = -1;
    }
    
    if (cli->leader_ip) {
        free(cli->leader_ip);
        cli->leader_ip = NULL;
    }
    
    free(cli);
}

// 获取最后接收到的消息
const char* Jl_GetResponse(struct Jl_Client* cli) {
    return cli ? cli->recv_msg : NULL;
}
