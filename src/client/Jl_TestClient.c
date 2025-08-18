#include "JlClient.h"

int main(int argc,char* argv[]){
    if(argc < 3)    return -1;
    char* ip = argv[1];
    int port = atoi(argv[2]);
    struct Jl_Client* cli = Jl_CreatClient(ip, port);
    if(cli == NULL){
        printf("cli初始化失败\n");
    }
    
    int ret = Jl_ConnectServer(cli);
    if(ret == -1){
        printf("连接服务器失败\n");
    }
    
    ret = Jl_SendCommand(cli,"set k1 v1\n");
    const char* recv1 = Jl_GetResponse(cli);
    printf("recv1:%s",recv1);
    ret = Jl_SendCommand(cli,"set k5 v5\n");
    const char* recv2 = Jl_GetResponse(cli);
    printf("recv2:%s",recv2);
    
    ret = Jl_SendCommand(cli,"set k6 v6\n");
    const char* recv3 = Jl_GetResponse(cli);
    printf("recv3:%s",recv3);
    
    Jl_Close(cli);
    
    return 0;
}