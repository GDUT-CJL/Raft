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
#include <unistd.h>
#include <ctype.h>
#include <stdarg.h> // for va_list
#include "rocksdb/c.h" // for rockdb

// ------------------------------ cgo_log -------------------------------- //
// 定义日志回调函数类型
typedef void (*LogCallback)(const char* message, int level);
// 设置日志回调函数
void set_storage_log_callback(LogCallback callback);
void storage_log(const char* format, int level, ...);
// 日志级别
#define LOG_DEBUG 0
#define LOG_INFO 1  
#define LOG_WARN 2
#define LOG_ERROR 3

// ------------------------------ bstring -------------------------------- //
typedef struct{
    uint8_t* data;// 具体的数据
    size_t len;// 数据的长度
    size_t cap;// 数据的容量
    uint32_t flags;// 标志定义
}bstring_t;

// 标志定义
#define BSTRING_OWNED   0x01    // 数据为自有内存
#define BSTRING_CONST   0x02    // 数据为常量（不可修改）
#define BSTRING_REF     0x04    // 数据为引用（不复制）

// 创建和销毁
bstring_t* bstring_new(void);// 创建一个bstring_t的结构体变量
bstring_t* bstring_new_with_capacity(size_t capacity);// 通过传入的大小创建一个bstring_t结构体
bstring_t* bstring_new_from_data(const void *data, size_t len);// 通过一个数据和数据长度创建一个bstring_t结构体
bstring_t* bstring_new_from_cstr(const char *cstr);// 通过c风格字符串创建一个bstring_t结构体
void bstring_free(bstring_t *bs);// 销毁bstring_t结构体

// 基础操作
size_t bstring_len(const bstring_t *bs);// 返回字符串长度
const uint8_t* bstring_data(const bstring_t *bs);// 返回字符串数据
bool bstring_empty(const bstring_t *bs);// 判断字符串是否为空

// 修改操作
int bstring_append(bstring_t *bs, const void *data, size_t len);// 修改字符串数据域
int bstring_append_cstr(bstring_t *bs, const char *cstr);// 通过c风格字符串修改字符串数据域
int bstring_append_bstring(bstring_t *bs, const bstring_t *other);// 通过另一个bstring_t修改数据

// 清空和重置
void bstring_clear(bstring_t *bs);// 清空数据域
void bstring_reset(bstring_t *bs);// 重置数据域

// 比较操作
int bstring_compare(const bstring_t *bs1, const bstring_t *bs2);// 比较大小
bool bstring_equal(const bstring_t *bs1, const bstring_t *bs2);// 比较是否相等

// 打印函数
void bstring_print_hex(const bstring_t *bs);
void bstring_print_text(const bstring_t *bs);
void bstring_print_detailed(const bstring_t *bs);

// 转换函数
char* bstring_to_cstr(const bstring_t* bs);

// ------------------------------ array -------------------------------- //
#define MAX_ARRAY_NUMS	102400
typedef struct kvs_array_item_s{
	bstring_t* key;
	bstring_t* value;
    long long expired;
}kvs_array_item_t;

typedef struct kvs_array_s{
	kvs_array_item_t* array;
	int array_count;
	pthread_mutex_t array_mutex;
}kvs_array_t;

struct kvs_array_s* array_table;
int init_array();
void dest_array();
int set(char* key,size_t klen,char* value,size_t vlen);
uint8_t* get(const char* key, size_t klen, size_t* out_vlen);
int delete(const char* key,size_t klen);
int count();
int exist(const char* key,size_t klen);


// ------------------------------ hash -------------------------------- //
#define MAX_HASHSIZE    102400
typedef struct hashnode_s{
    bstring_t* key;
    bstring_t* value;
    struct hashnode_s* next;
}hashnode_t;
typedef struct hashtable_s{
    hashnode_t** nodes;
    int max_slots;  // hash表的最大槽位
    int count;  // hash表当前的数目
}hashtable_t;
hashtable_t* Hash;
int init_hashtable();
void dest_hashtable();
int hexist(char* key,size_t klen);
int hset(char* key,size_t klen,char* value,size_t vlen);
uint8_t* hget(char* key,size_t klen,size_t* out_vlen);
int hdelete(char* key,size_t klen);
int hcount();
// ------------------------ RBTree ------------------------- //
#define MAX_RBTREE_SIZE     512
typedef enum { RED, BLACK } Color;
// 红黑树节点结构
typedef struct RBNode {
    bstring_t* key;
    bstring_t* value;
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
int rexist(const char* key,size_t klen);
int rset(char* key,size_t klen,char* value,size_t vlen);
uint8_t* rget(const char* key,size_t klen,size_t* out_len);
int rdelete(char* key,size_t klen);
int rcount();
/*------------------------------ BTree --------------------------------------*/
typedef bstring_t** B_KEY_TYPE;    // key类型
typedef bstring_t** B_VALUE_TYPE;  // value类型
typedef bstring_t*  B_KEY_SUB_TYPE;   // key的实际类型
typedef bstring_t*  B_VALUE_SUB_TYPE; // value的实际类型

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
int btree_insert_key(char* key,size_t klen, char* value,size_t vlen);
int init_btree(int m);
int dest_btree();
int bset(char* key,size_t klen,char* value,size_t vlen);
uint8_t* bget(char *key,size_t klen,size_t* out_len);
int bdelete(char* key,size_t klen);
int bcount();
int bexist(char* key,size_t klen);
int btree_destroy(btree *T);
/*------------------------------ Skiplist --------------------------------------*/
#define     SKIPTABLE_MAX_LEVEL     5
typedef struct skipnode_s{
    bstring_t* key;
    bstring_t* value;
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
int zset(char* key, size_t klen,char* value,size_t vlen);
uint8_t* zget(char* key,size_t klen,size_t* out_vlen);
int zdelete(char* key,size_t klen);
int zcount();
int zexist(char* key,size_t klen);
/*------------------------------ rockdb --------------------------------------*/
#pragma once
#define PATH_TO_ROCKSDB "./rocksdb_data"
#define PATH_TO_ROCKSDB_BACKUP "./rocksdb_backup"
struct rocks_obj {
    rocksdb_t *db;
    rocksdb_backup_engine_t* be;
    rocksdb_options_t *options;
    rocksdb_writeoptions_t *writeoptions;
    rocksdb_readoptions_t *readoptions;
    rocksdb_restore_options_t *restore_options;
};

struct rocks_obj* rocksdb_obj;
// 初始化 RocksDB
int init_rocksdb();
// 关闭并清理 RocksDB 资源
void close_Rocksdb();
// 设置键值对
int rc_set(char* key,size_t klen, char* value,size_t vlen);
// 获取键值对
uint8_t* rc_get(const char* key, size_t klen, size_t* vallen);
// 删除键值对
int rc_delete(const char* key,size_t klen);
// 判断key是否存在
int rc_exist(const char* key, size_t klen);
// 创建备份
int kvs_rocksdb_create_backup() ;
// 批量写入接口
int kvs_rocksdb_batch_set(const char** keys, const char** values, int count);


/*------------------------------ mempool --------------------------------------*/
#include <stdint.h>
void* kvs_malloc(size_t size);
void kvs_free(void* ptr);
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

/*------------------------------ Snapshot --------------------------------------*/
// 快照数据结构
typedef struct storage_snapshot_s {
    uint32_t version;
    uint64_t timestamp;
} storage_snapshot_t;

// 快照函数声明
int storage_create_snapshot(char** snapshot_data, size_t* snapshot_size);
int storage_restore_snapshot(const char* snapshot_data, size_t snapshot_size);
void storage_free_snapshot(char* snapshot_data);

// 各个数据结构的快照函数
int array_snapshot(char** data, size_t* size);
int array_restore(const char* data, size_t size);

int hash_snapshot(char** data, size_t* size);
int hash_restore(const char* data, size_t size);

int rbtree_snapshot(char** data, size_t* size);
int rbtree_restore(const char* data, size_t size);

int btree_snapshot(char** data, size_t* size);
int btree_restore(const char* data, size_t size);

int skiplist_snapshot(char** data, size_t* size);
int skiplist_restore(const char* data, size_t size);

#ifdef __cplusplus  
}  
#endif  

#endif // STORAGE_H  