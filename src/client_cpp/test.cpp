#include "JlClient.hpp"

int main(){
    JlClient client("192.168.79.158",8000);
    client.Jl_ConnectServer();
    client.Jl_SendCommand("set k1 v1\n");
    char* recv = client.Jl_GetResponse();
    cout << "set k1 recv :" <<  string(recv) <<endl;
    cout << "-------------------------" <<endl;
    
    client.Jl_SendCommand("set k2 v2\n");
    recv = client.Jl_GetResponse();
    cout << "set k1 recv :" <<  string(recv) <<endl;
    cout << "-------------------------" <<endl;
    
    client.Jl_SendCommand("get k1\n");
    recv = client.Jl_GetResponse();
    cout << "k1:" <<  string(recv) <<endl;
    
    client.Jl_Close();
}