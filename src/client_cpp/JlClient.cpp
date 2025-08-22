#include "JlClient.hpp"

JlClient::JlClient(string ip, int port):cur_ip(ip),cur_port(port),m_connfd(-1),
        leader_ip(""),leader_port(-1)
{}

JlClient::~JlClient()
{
    Jl_Close();
}

bool JlClient::is_leader_command(string& command) {
    if (command.empty()) return false;

    const array<std::string, 3> leader_commands = {"set", "delete", "count"};
    for (const auto& cmd : leader_commands) {
        // 前缀比较：大小写不敏感
        if (command.size() >= cmd.size()) {
            bool prefix_match = true;
            for (size_t i = 0; i < cmd.size(); ++i) {
                if (std::tolower(static_cast<unsigned char>(command[i])) !=
                    std::tolower(static_cast<unsigned char>(cmd[i]))) {
                    prefix_match = false;
                    break;
                }
            }
            if (prefix_match) {
                // 取 command 的紧跟后面的字符
                char c = command[cmd.size()];
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\0') {
                    return true;
                }
            }
        }
    }
    return false;
}
int JlClient::parse_leader_info(string error_msg)
{
    // 查找错误前缀
    const std::string prefix = LEADER_ERROR_PREFIX;
    auto pos = error_msg.find(prefix);
    if (pos == std::string::npos) return -1;

    // 移动到前缀结束位置
    auto addr_start = pos + prefix.size();

    // 将 error_msg 切片到地址部分，避免修改原字符串
    std::string remainder = error_msg.substr(addr_start);

    // 跳过可能存在的空格
    size_t idx = 0;
    while (idx < remainder.size() && std::isspace(static_cast<unsigned char>(remainder[idx]))) {
        ++idx;
    }
    
    // 剩余字符串中的地址起始位置
    if (idx >= remainder.size()) return -1;
    auto addr_begin = remainder.data() + idx; // 这是一个临时指针，需要改为在 std::string 中操作

    
    // 为了简化，我们直接在 remainder 内部处理：找到地址结束位置
    // 从 idx 开始，找到地址结束位置：空格或换行或回车
    size_t addr_end_idx = idx;
    while (addr_end_idx < remainder.size()) {
        char c = remainder[addr_end_idx];
        if (std::isspace(static_cast<unsigned char>(c)) || c == '\r' || c == '\n') break;
        ++addr_end_idx;
    }

    // 检查是否找到有效地址
    if (addr_end_idx == idx) return -1;

    // 提取地址字符串
    std::string addr = remainder.substr(idx, addr_end_idx - idx);

    // 分离IP和端口
    auto colon_pos = addr.rfind(':');
    if (colon_pos == std::string::npos) return -1; // 没有找到冒号

    std::string ip_str = addr.substr(0, colon_pos);
    std::string port_str = addr.substr(colon_pos + 1);

    if (ip_str.empty() || port_str.empty()) return -1;

    // 验证IP地址格式
    struct in_addr ip_addr;
    if (inet_pton(AF_INET, ip_str.c_str(), &ip_addr) != 1) {
        return -1; // 无效的IP地址
    }

    // 将端口转换为整数
    int port = 0;
    try {
        port = std::stoi(port_str);
    } catch (...) {
        return -1;
    }

    // 更新Leader信息
    // 假设 cli->leader_ip 是 std::string
    leader_ip = ip_str;
    leader_port = port;

    // 调试信息
    std::cout << "Discovered new leader: " << leader_ip << ":" << leader_port << endl;

    return 0;
}

// 重新连接到Leader节点
int JlClient::reconnect_to_leader() {
    if (leader_ip.size() == 0 || leader_port <= 0) {
        return -1; // 没有有效的Leader信息
    }
    
    // 关闭旧连接
    if (m_connfd != -1) {
        close(m_connfd);
        m_connfd = -1;
    }
    
    // 更新连接信息
    cur_ip = leader_ip;
    cur_port = leader_port;
    
    // 建立新连接
    return Jl_ConnectServer();
}
// 连接到服务器
int JlClient::Jl_ConnectServer()
{
    // 如果已有连接，先关闭
    if (m_connfd != -1) {
        close(m_connfd);
        m_connfd = -1;
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
    if (inet_pton(AF_INET, cur_ip.c_str(), &server_addr.sin_addr) <= 0) {
        perror("inet_pton");
        close(connfd);
        return -1;
    }
    
    server_addr.sin_port = htons(cur_port);
    
    // 连接服务器
    if (connect(connfd, (struct sockaddr*)&server_addr, sizeof(server_addr))){
        perror("connect");
        close(connfd);
        return -1;
    }
    
    m_connfd = connfd;
    printf("Connected to %s:%d\n", cur_ip.c_str(), cur_port);
    return 0;
    
}

// 发送命令（带智能路由）
int JlClient::Jl_SendCommand(string command)
{
    if (command.empty()) {
        return -1;
    }

    // 检查连接状态
    if (m_connfd == -1) {
        if (Jl_ConnectServer() != 0) {
            return -1;
        }
    }

    bool is_leader_cmd = is_leader_command(command);
    int retry_count = 0;

    while (retry_count++ < MAX_RETRY_ATTEMPTS) {
        // 如果是写命令且已知 Leader，检查是否需要重定向
        if (is_leader_cmd && !leader_ip.empty()) {
            bool need_redirect =
                (cur_ip != leader_ip) || (cur_port != leader_port);

            if (need_redirect) {
                printf("Redirecting command to leader: %s:%d\n",
                            leader_ip.c_str(), leader_port);

                if (reconnect_to_leader() != 0) {
                    // 重连 Leader 失败，尝试回连默认服务
                    printf("Failed to connect to leader, reconnecting to default server.\n");
                    if (Jl_ConnectServer() != 0) {
                        return -1;
                    }
                }
                // 成功重连 Leader 后，继续发送
            }
        }

        // 发送命令
        ssize_t sent = send(m_connfd, command.c_str(), command.size(), 0);
        if (sent <= 0) {
            perror("send");
            if (Jl_ConnectServer() != 0) {
                return -1;
            }
            continue;
        }

        // 接收响应
        ssize_t received = Jl_RecvMsg();
        if (received <= 0) {
            printf("Receive failed, reconnecting...\n");
            if (Jl_ConnectServer() != 0) {
                return -1;
            }
            continue;
        }

        // 检查是否收到 Leader 重定向
        if (recv_msg && strstr(recv_msg, LEADER_ERROR_PREFIX) != nullptr) {
            printf("Received leader redirect: %s", recv_msg);

            // 解析新的 Leader 信息
            if (parse_leader_info(recv_msg) == 0) {
                // 解析成功，尝试重连新 Leader
                if (reconnect_to_leader() == 0) {
                    continue; // 使用新 Leader 重试
                }
            }
            printf("Failed to handle leader redirect.\n");
            return -1;
        }

        // 成功收到响应
        return 0;
    }

    printf("Command failed after %d retries.\n", MAX_RETRY_ATTEMPTS);
    return -1;
}

// 接收消息
ssize_t JlClient::Jl_RecvMsg()
{
    if (m_connfd == -1) return -1;
    
    ssize_t bytes = recv(m_connfd, recv_msg, MAX_BUFFER_SIZE - 1, 0);
    if (bytes <= 0) {
        if (bytes == 0) {
            fprintf(stderr, "Connection closed by server\n");
        } else {
            perror("recv");
        }
        return -1;
    }
    
    recv_msg[bytes] = '\0'; // 确保字符串终止
    return bytes;
}

// 获取最后接收到的消息
char* JlClient::Jl_GetResponse()
{
    return recv_msg != nullptr ? recv_msg : nullptr;
}

// 关闭连接并释放资源
void JlClient::Jl_Close()
{
    if (m_connfd != -1) {
        close(m_connfd);
        m_connfd = -1;
    }
    leader_ip.clear();  // 正确清空 string
    leader_port = -1;
}

