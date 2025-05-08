// gcc TestCase.c -o TestCase
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#define ENABLE_ARRAY_TEST   1
#define ENABLE_RBTREE_TEST   0 
#define ENABLE_HASHTABLE_TEST   0
#define ENABLE_SKIPLIST_TEST   0
#define ENABLE_BTREE_TEST       0
#define ENABLE_DHASH_TEST       0


#define ENABLE_LOG   0

#define MAX_REQUEST_NUM			25000
#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

#define LOG(_fmt, ...) fprintf(stdout, "[%s:%d] " _fmt, __FILE__, __LINE__, __VA_ARGS__)


#define MAX_BUFFER_SIZE     1024
char buffer[MAX_BUFFER_SIZE] = {0};
int Connect_Server(const char* ip,int port){
    int connfd = socket(AF_INET,SOCK_STREAM,0);

    struct sockaddr_in kvs_addr;
    memset(&kvs_addr,0,sizeof(kvs_addr));
    kvs_addr.sin_family = AF_INET;
    kvs_addr.sin_addr.s_addr = inet_addr(ip);
    kvs_addr.sin_port = htons(port);

    int ret = connect(connfd,(struct sockaddr*)&kvs_addr,sizeof(struct sockaddr));
    if(ret != 0){
        perror("connect");
        return -1;
    }
    return connfd;
}

int send_msg(int connfd,char* msg){
    int ret = send(connfd,msg,strlen(msg),0);
    //printf("Sending message: %s\n", msg);
    if(ret == -1){
        perror("send:");
        exit(-1);
    }
    else if(ret > 0){
        return ret;
    }
    else{
        perror("send:");
        close(connfd);
    }

}

int recv_msg(int connfd, char* msg){
    int ret = recv(connfd,msg,MAX_BUFFER_SIZE,0);
    if(ret == -1){
        perror("recv:");
        exit(-1);
    }
    msg[ret] = '\0';
    return ret;
}

int equal(char* res,char* pattern,char* casename){
    if(strcmp(res,pattern) == 0){
        #if ENABLE_LOG
        LOG("PASS -----> '%s'\n",casename);
        #endif
    }else{
        #if ENABLE_LOG
        LOG("NO PASS -----> %s != %s\n",res,pattern);
        #endif
    }
}

int test_case(int connfd,char* cmd, char* pattern,char* casename){
    int ret = send_msg(connfd,cmd);
    if(ret == -1) return -1;
    recv_msg(connfd,buffer);
    if(ret == -1) return -1;

    equal(buffer,pattern,casename);
}

void array_connect_10w(int connfd){
    int i;
    for(i = 0; i < MAX_REQUEST_NUM ; ++i){
        char msg[512] = {0};
        snprintf(msg,512,"set key%d value%d\n",i,i);
        //printf("%s\n",msg);
        //snprintf(msg,512,"get key%d",i);
        test_case(connfd,msg,"OK\n","SetName");
    }
}

void rbtree_connect_10w(int connfd){
    int i;
    for(i = 0; i < MAX_REQUEST_NUM ; ++i){
        char msg[512] = {0};
        snprintf(msg,512,"rset key%d value%d",i,i);
        test_case(connfd,msg,"OK\n","SetName");
    }
}

void hashtable_connect_10w(int connfd){
    int i;
    for(i = 0; i < MAX_REQUEST_NUM ; ++i){
        char msg[512] = {0};
        snprintf(msg,512,"hset key%d value%d",i,i);
        test_case(connfd,msg,"OK\n","SetName");
    }
}

void skiplist_connect_10w(int connfd){
    int i;
    for(i = 0; i < MAX_REQUEST_NUM ; ++i){
        char msg[512] = {0};
        snprintf(msg,512,"zset key%d value%d",i,i);
        test_case(connfd,msg,"OK\n","SetName");
    }
}

void btree_connect_10w(int connfd){
    int i;
    for(i = 0; i < MAX_REQUEST_NUM ; ++i){
        char msg[512] = {0};
        snprintf(msg,512,"bset key%d value%d",i,i);
        test_case(connfd,msg,"OK\n","SetName");
    }
}

void dhash_connect_10w(int connfd){
    int i;
    for(i = 0; i < MAX_REQUEST_NUM ; ++i){
        char msg[512] = {0};
        snprintf(msg,512,"dset key%d value%d",i,i);
        test_case(connfd,msg,"OK\n","SetName");
    }
}

int main(int argc,char *argv[])
{
    assert(argc == 3);
    if(argc < 3) {
        LOG("argc < %s\n","3");
        return -1;
    }

    const char* ip = argv[1];
    int port = atoi(argv[2]);
    int connfd = Connect_Server(ip,port);
    
#if 0
    // array
    test_case(connfd,"set k1 v1","OK\n","set name");
    test_case(connfd,"set k2 v2","OK\n","set name");
    test_case(connfd,"set k3 v3","OK\n","set name");
    test_case(connfd,"set k4 v4","OK\n","set name");

    // test_case(connfd,"count","1\n","count");
    // test_case(connfd,"get k2","v2\n","get name");
    // test_case(connfd,"delete k1","OK\n","delete name");
    // test_case(connfd,"exist k2","NO EXIST\n","exist name");

    // test_case(connfd,"delete k2","OK\n","delete name");
    // test_case(connfd,"delete k3","OK\n","delete name");

    // // rbtree
     test_case(connfd,"rset k2 v2","OK\n","rset name");
     test_case(connfd,"rset k3 v3","OK\n","rset name");
     test_case(connfd,"rset k4 v4","OK\n","rset name");
     test_case(connfd,"rset k5 v5  ","OK\n","rset name");
    // test_case(connfd,"rcount","1\n","count");
    // test_case(connfd,"rget k2","v2\n","get name");
    // test_case(connfd,"rdelete k2","OK\n","delete name");
    // test_case(connfd,"rexist k2","NO EXIST\n","exist name");

    // // // hashtable
    test_case(connfd,"hset k1 v1","OK\n","hset name");
    test_case(connfd,"hset k2 v2","OK\n","hset name");
    // test_case(connfd,"hcount","1\n","count");
    // test_case(connfd,"hget k2","v2\n","get name");
    test_case(connfd,"hdelete k1","OK\n","hdelete name");

    // test_case(connfd,"hexist k2","NO EXIST\n","exist name");

    // // // skiplist
 
    // test_case(connfd,"zexist k2","NO EXIST\n","exist name");

    // // btree
    test_case(connfd,"bset k5 v5","OK\n","bset name");
    test_case(connfd,"bset k6 v6","OK\n","bset name");
    test_case(connfd,"bset k7 v7","OK\n","bset name");
    test_case(connfd,"bset k8 v8","OK\n","bset name");
    // test_case(connfd,"bcount","1\n","count");
    // test_case(connfd,"bget k2","v2\n","get name");
    // test_case(connfd,"bexist k2","EXIST\n","exist name");
    test_case(connfd,"bdelete k5","OK\n","bdelete name");
    test_case(connfd,"bdelete k6","OK\n","bdelete name");

    // // Dhashtable
    // test_case(connfd,"dset k2 v2","OK\n","set name");
    // test_case(connfd,"dcount","1\n","count");
    // test_case(connfd,"dget k2","v2\n","get name");
    // test_case(connfd,"ddelete k2","OK\n","delete name");
    // test_case(connfd,"dexist k2","NO EXIST\n","exist name");

#endif

#if ENABLE_ARRAY_TEST
    struct timeval array_begin;
    gettimeofday(&array_begin,NULL);
    array_connect_10w(connfd);
    struct timeval array_end;
    gettimeofday(&array_end,NULL);
    double array_time_used = TIME_SUB_MS(array_end,array_begin);
    LOG("array used time:%f ms,qps: %.2f\n",array_time_used,MAX_REQUEST_NUM / (array_time_used / 1000));
#endif 

#if ENABLE_RBTREE_TEST
    struct timeval rb_begin;
    gettimeofday(&rb_begin,NULL);
    rbtree_connect_10w(connfd);
    struct timeval rb_end;
    gettimeofday(&rb_end,NULL);
    double rb_time_used = TIME_SUB_MS(rb_end,rb_begin);
    LOG("rbtree used time:%f ms,qps: %.2f\n",rb_time_used,MAX_REQUEST_NUM / (rb_time_used / 1000));
#endif

#if ENABLE_HASHTABLE_TEST
    struct timeval hash_begin;
    gettimeofday(&hash_begin,NULL);
    hashtable_connect_10w(connfd);
    struct timeval hash_end;
    gettimeofday(&hash_end,NULL);
    double hash_time_used = TIME_SUB_MS(hash_end,hash_begin);
    LOG("hash used time:%f ms,qps: %.2f\n",hash_time_used,MAX_REQUEST_NUM / (hash_time_used / 1000));
#endif

#if ENABLE_SKIPLIST_TEST
    struct timeval skiplist_begin;
    gettimeofday(&skiplist_begin,NULL);
    skiplist_connect_10w(connfd);
    struct timeval skiplist_end;
    gettimeofday(&skiplist_end,NULL);
    double skiplist_time_used = TIME_SUB_MS(skiplist_end,skiplist_begin);
    LOG("skiplist used time:%f ms,qps: %.2f\n",skiplist_time_used,MAX_REQUEST_NUM / (skiplist_time_used / 1000));
#endif

#if ENABLE_BTREE_TEST
    struct timeval btree_begin;
    gettimeofday(&btree_begin,NULL);
    btree_connect_10w(connfd);
    struct timeval btree_end;
    gettimeofday(&btree_end,NULL);
    double btree_time_used = TIME_SUB_MS(btree_end,btree_begin);
    LOG("btree used time:%f ms,qps: %.2f\n",btree_time_used,MAX_REQUEST_NUM / (btree_time_used / 1000));
#endif

#if ENABLE_DHASH_TEST
    struct timeval dhash_begin;
    gettimeofday(&dhash_begin,NULL);
    dhash_connect_10w(connfd);
    struct timeval dhash_end;
    gettimeofday(&dhash_end,NULL);
    double dhash_time_used = TIME_SUB_MS(dhash_end,dhash_begin);
    LOG("dhash used time:%f ms,qps: %.2f\n",dhash_time_used,MAX_REQUEST_NUM / (dhash_time_used / 1000));
#endif
    close(connfd);
}