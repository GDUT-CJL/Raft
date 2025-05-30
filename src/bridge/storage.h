#ifndef STORAGE_H  
#define STORAGE_H  

#ifdef __cplusplus  
extern "C" {  
#endif  

// 定义你的函数和结构体  
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#define MAX_MSGBUFFER_LENGTH	1024
#define MAX_ARRAY_NUMS	102400

void* kvs_malloc(size_t size);
void kvs_free(void* ptr);

typedef struct kvs_array_item_s{
	char* key;
	char* value;
    long long expired;
}kvs_array_item_t;

typedef struct kvs_array_s{
	kvs_array_item_t* array;
	int array_count;
	pthread_mutex_t array_mutex;
}kvs_array_t;

struct kvs_array_s* array_table;

// ------------------------------ array -------------------------------- //
int init_array();
void dest_array();
int set(char* key,char* value);
char* get(const char* key);
int delete(const char* key);
int count();
int exist(const char* key);


// ------------------------------ hash -------------------------------- //
#define MAX_HASHSIZE    102400
typedef struct hashnode_s{
    char* key;
    char* value;
    struct hashnode_s* next;
}hashnode_t;
typedef struct hashtable_s{
    hashnode_t** nodes;
    int max_slots;  // hash表的最大槽位
    int count;  // hash表当前的数目
	pthread_mutex_t mutex;
}hashtable_t;
hashtable_t* Hash;
int init_hashtable();
void dest_hashtable();
int hexist(char* key);
int hset(char* key,char* value);
char* hget(char* key);
int hdelete(char* key);
int hcount();

// ------------------------ RBTree ------------------------- //
#define MAX_RBTREE_SIZE     512
typedef enum { RED, BLACK } Color;
// 红黑树节点结构
typedef struct RBNode {
    char* key;
    char* value;
    Color color;
    struct RBNode *left;
    struct RBNode *right;
    struct RBNode *parent;
} RBNode;
// 全局NIL节点，表示叶子节点
RBNode *NIL;
RBNode* root;
int init_rbtree();
void dest_rbtree();
int rexist(const char* key);
int rset(char* key,char* value);
char* rget(const char* key);
int rdelete(char* key);
int rcount();
/*------------------------------ BTree --------------------------------------*/
// 定义键值对的类型
    typedef char** B_KEY_TYPE;    // key类型
    typedef char** B_VALUE_TYPE;  // value类型
    typedef char*  B_KEY_SUB_TYPE;   // key的实际类型
    typedef char*  B_VALUE_SUB_TYPE; // value的实际类型

typedef struct _btree_node{
    B_KEY_TYPE keys;
    B_VALUE_TYPE values;
    struct _btree_node **children;
    int num;  // 当前节点的实际元素数量
    int leaf; // 当前节点是否为叶子节点
}btree_node;
// 实际上，B树的阶数由用户初始化时定义
// #define BTREE_M 6         // M阶
// #define SUB_M BTREE_M>>1  // M/2
typedef struct _btree{
    int m;      // m阶B树
    int count;  // B树所有的元素数量
    struct _btree_node *root_node;
}btree;
typedef btree kv_btree_t;
kv_btree_t* kv_b;
int btree_insert_key(char* key, char* value);
int init_btree(int m);
int dest_btree();
int bset(char* key,char* value);
char* bget(char *key);
int bdelete(char* key);
int bcount();
int bexist(char* key);
int btree_destroy(btree *T);

/*------------------------------ Skiplist --------------------------------------*/
#define     SKIPTABLE_MAX_LEVEL     5
typedef struct skipnode_s{
    char* key;
    char* value;
    struct skipnode_s** next;
}skipnode_t;

typedef struct skiplist_s{
    int nodeNum;
    int cur_level;
    int max_level;
    skipnode_t* head;
}skiplist_t;
skiplist_t* sklist;
int init_skipTable();
int dest_skiplist();
int zset(char* key, char* value);
char* zget(char* key);
int zdelete(char* key);
int zcount();
int zexist(char* key);


#ifdef __cplusplus  
}  
#endif  

#endif // STORAGE_H  