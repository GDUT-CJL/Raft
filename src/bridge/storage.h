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

/*------------------------------ mempool --------------------------------------*/
#include <stdint.h>

// mempool
#define JL_MP_ALIGNMENT     32
#define JL_MAX_POOLSIZE     4096
#define FLUSH_THRESHOLD 1024 // 刷盘阈值：1MB

#define JL_MAX_ALLOC_FROM_POOL     (JL_MAX_POOLSIZE - 1)
#define jl_align(n, alignment) (((n)+(alignment-1)) & ~(alignment-1))
#define jl_align_ptr(p, alignment) (void *)((((size_t)p)+(alignment-1)) & ~(alignment-1))

static inline unsigned char* jl_align_ptr_fast(unsigned char* p) {
    return (unsigned char*)(((uintptr_t)p + 7) & ~(uintptr_t)7);
}
struct jl_pool_s *p;    //全局内存池  

typedef struct jl_large_s
{
    void* alloc;
    struct jl_large_s* next;
}jl_large_t;

typedef struct jl_data_s
{
    unsigned char* end;
    unsigned char* last;
    struct jl_pool_s* next;
    unsigned int failed;
    size_t data_size;         // 有效数据大小
}jl_data_t;

typedef struct jl_pool_s
{
    struct jl_data_s d ;
    struct jl_large_s* large;
    struct jl_pool_s* current;
    size_t max;

    size_t allocated_size;       // 当前已分配的内存大小（用于刷盘判断）
    //pthread_mutex_t lock;        // 互斥锁，用于线程安全
}jl_pool_t;

int initPool();
struct jl_pool_s* jl_create_mempool(size_t size);
void jl_destory_mempool(struct jl_pool_s *pool);
void jl_reset_pool(struct jl_pool_s *pool);
void* jl_alloc_block(struct jl_pool_s* pool,int size);
void* jl_alloc_large(struct jl_pool_s* pool,int size);
void* jl_alloc(struct jl_pool_s* pool,int size);
void* jl_calloc(jl_pool_t* pool,int size);
void jl_free(jl_pool_t* pool,void* p);
int jl_flush_to_disk(jl_pool_t* pool, const char* filename);

#ifdef __cplusplus  
}  
#endif  

#endif // STORAGE_H  