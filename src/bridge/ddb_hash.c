#include "storage.h"
// djb2哈希算法 - 修改为支持bstring
static unsigned long _hash(const bstring_t *key, int capacity) {
    if (!key || bstring_empty(key)) return 0;
    
    unsigned long hash = 5381;
    const uint8_t *data = bstring_data(key);
    size_t len = bstring_len(key);
    
    for (size_t i = 0; i < len; i++) {
        hash = ((hash << 5) + hash) + data[i]; // hash * 33 + c
    }
    return hash % capacity;
}

hashnode_t* _createNode(bstring_t* key,bstring_t* value){
    hashnode_t* node = (hashnode_t*)kvs_malloc(sizeof(hashnode_t));
    if(!node) return NULL;

    node->key = key;
    node->value = value;
    
    node->next = NULL;// 作为链表节点，这一步也很重要不要遗忘，方便以后添加
    return node; 
}

int init_hashtable(){
    Hash = (hashtable_t*)kvs_malloc(sizeof(hashtable_t));
    if(!Hash)   return -1;
    Hash->nodes = (hashnode_t**)kvs_malloc(sizeof(hashnode_t*) * MAX_HASHSIZE);
    if(!Hash->nodes) {
        kvs_free(Hash);
        return -1;
    }
    // 初始化所有槽位为NULL
    for (int i = 0; i < MAX_HASHSIZE; i++) {
        Hash->nodes[i] = NULL;
    }
    Hash->max_slots = MAX_HASHSIZE;
    Hash->count = 0;
    return 0;
}

void dest_hashtable() {
    if (!Hash) return;
    
    for (int i = 0; i < Hash->max_slots; i++) {
        hashnode_t *node = Hash->nodes[i];
        while (node != NULL) {
            hashnode_t *temp = node;
            node = node->next;
            
            // 释放bstring内存
            if (temp->key) bstring_free(temp->key);
            if (temp->value) bstring_free(temp->value);
            
            kvs_free(temp);
        }
        Hash->nodes[i] = NULL;
    }
    
    kvs_free(Hash->nodes);
    kvs_free(Hash);
    Hash = NULL;
}

int hexist(char* key,size_t klen){
    if(!key || klen < 0) return -1;
    
    bstring_t* bkey = bstring_new_from_data(key,klen);
    if (!bkey) return -1;
    
    int idx = _hash(bkey,MAX_HASHSIZE);
    hashnode_t* node = Hash->nodes[idx];
    
    int result = -1;
    while (node != NULL) {
        if(bstring_equal(node->key,bkey)){
            result = 0;
            break;
        }
        node = node->next;
    }
    
    bstring_free(bkey);  // 释放临时key
    return result;   
}

int hset(char* key,size_t klen,char* value,size_t vlen){
    // 检查参数和全局哈希表
    if(key == NULL || value == NULL || klen <= 0 || vlen < 0) return -1;
    if (Hash == NULL) return -1;
    
    bstring_t* bkey = bstring_new_from_data(key, klen);
    if (!bkey) return -1;
    
    bstring_t* bvalue = bstring_new_from_data(value, vlen);
    if (!bvalue) {
        bstring_free(bkey);
        return -1;
    }
    
    int idx = _hash(bkey, MAX_HASHSIZE);
    
    // 检查槽位索引有效性
    if (idx < 0 || idx >= Hash->max_slots) {
        bstring_free(bkey);
        bstring_free(bvalue);
        return -1;
    }
    
    hashnode_t* node = Hash->nodes[idx];
    hashnode_t* prev = NULL;
    
    // 遍历链表查找是否已存在
    while(node != NULL){
        if(bstring_equal(node->key, bkey)){
            // 找到已存在的key，更新value
            if(node->value) {
                bstring_free(node->value);
            }
            node->value = bvalue;
            bstring_free(bkey);  // 释放临时key
            return 0;
        }
        prev = node;
        node = node->next;
    }
    
    // 创建新节点
    hashnode_t *new_node = _createNode(bkey, bvalue);
    if (!new_node) {
        bstring_free(bkey);
        bstring_free(bvalue);
        return -1;
    }
    
    // 头插法
    new_node->next = Hash->nodes[idx];
    Hash->nodes[idx] = new_node;
    Hash->count++;
    
    return 0;
}

uint8_t* hget(char* key,size_t klen,size_t* out_vlen){
    if(key == NULL || klen < 0) return NULL;
    
    bstring_t* bkey = bstring_new_from_data(key,klen);
    if (!bkey) return NULL;
    
    int idx = _hash(bkey,MAX_HASHSIZE);
    hashnode_t* node = Hash->nodes[idx];
    uint8_t* result = NULL;
    
    while (node != NULL) {
        if(bstring_equal(node->key,bkey)){
            *out_vlen = node->value->len;
            result = node->value->data;
            break;
        }
        node = node->next;
    }
    
    bstring_free(bkey);  // 释放临时key
    return result;
}

int hdelete(char* key,size_t klen){
    if (key == NULL || klen < 0) return -1;
    
    bstring_t* bkey = bstring_new_from_data(key,klen);
    if (!bkey) return -1;
    
    int idx = _hash(bkey, MAX_HASHSIZE);
    hashnode_t* head = Hash->nodes[idx];
    
    // 检查头节点是否为NULL
    if (head == NULL) {
        bstring_free(bkey);  // 释放临时key
        return -1;
    }
    
    // 头节点匹配的情况
    if(bstring_equal(head->key, bkey)){
        hashnode_t* new_head = head->next;
        Hash->nodes[idx] = new_head;

        // 使用bstring_free而不是kvs_free
        if(head->key) bstring_free(head->key);
        if(head->value) bstring_free(head->value);

        kvs_free(head);
        bstring_free(bkey);  // 释放临时key

        Hash->count--;
        return 0;
    }

    // 如果不是头节点则遍历链表
    hashnode_t* cur = head;
    while(cur->next != NULL){
        if(bstring_equal(cur->next->key, bkey)) break;
        cur = cur->next;
    }

    if(cur->next == NULL){
        bstring_free(bkey);  // 释放临时key
        return -1;
    }

    hashnode_t* tmp = cur->next;
    cur->next = cur->next->next;

    // 使用bstring_free释放bstring对象
    if(tmp->key) bstring_free(tmp->key);
    if(tmp->value) bstring_free(tmp->value);

    kvs_free(tmp);
    bstring_free(bkey);  // 释放临时key

    Hash->count--;
    return 0;
}

int hcount(){
    return Hash->count;
}