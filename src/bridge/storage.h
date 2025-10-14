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
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <ctype.h>
#define MAX_MSGBUFFER_LENGTH	1024
#define MAX_ARRAY_NUMS	102400

void* kvs_malloc(size_t size);
void kvs_free(void* ptr);
// ------------------------------ bstring -------------------------------- //
typedef struct{
    uint8_t* data;
    size_t len;
    size_t cap;
    uint32_t flags;
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
int set(char* key,size_t klen,char* value,size_t vlen);
uint8_t* get(const char* key, size_t klen, size_t* out_vlen);
//char* get(const char* key,size_t klen);
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


#ifdef __cplusplus  
}  
#endif  

#endif // STORAGE_H  