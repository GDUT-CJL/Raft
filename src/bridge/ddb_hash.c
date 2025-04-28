#include "storage.h"

// ---------------------------------------Hash----------------------------------------- //
// djb2哈希算法
/*
typedef struct hashnode_s{
    char* key;
    char* value;
    struct hashnode_s* next;
}hashnode_t;

typedef struct hashtable_s{
    hashnode_t** nodes;
    int max_slots;  // hash表的最大槽位
    int count;  // hash表当前的数目
}hashtable_t;
*/

static unsigned long _hash(const char *key, int capacity) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++))
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    return hash % capacity;
}

hashnode_t* _createNode(char* key,char* value){
    hashnode_t* node = (hashnode_t*)kvs_malloc(sizeof(hashnode_t));
    if(!node) return NULL;

    node->key = (char*)kvs_malloc(sizeof(char));
    if(!node->key){
        kvs_free(node);
        return NULL;
    } 
    node->value = (char*)kvs_malloc(sizeof(char));
    if(!node->value){
        kvs_free(node->key);
        kvs_free(node);
        return NULL;
    }
    strncpy(node->key,key,strlen(key)+1); 
    strncpy(node->value,value,strlen(key)+1);
    node->next = NULL;// 作为链表节点，这一步也很重要不要遗忘，方便以后添加
    return node; 
}

int init_hashtable(){
    Hash = (hashtable_t*)kvs_malloc(sizeof(hashtable_t));
    if(!Hash)   return -1;
    Hash->nodes = (hashnode_t**)kvs_malloc(sizeof(hashnode_t*) * MAX_HASHSIZE);
    if(!Hash->nodes)    return -1;
    
    if (pthread_mutex_init(&Hash->mutex, NULL) != 0) {
        kvs_free(Hash->nodes);  
        kvs_free(Hash);  
        return -1; // 处理mutex初始化错误  
    }
    Hash->max_slots = MAX_HASHSIZE;
    Hash->count = 0;
    return 0;
}

void dest_hashtable(){
    if(!Hash)   return;
    for(int i = 0 ; i < Hash->count; ++i){
        hashnode_t* node = Hash->nodes[i];
        while (node != NULL)
        {
            hashnode_t* temp = node;
            node = node->next;
            Hash->nodes[i] = node;
            kvs_free(temp);
        }
    }
    kvs_free(Hash);
}

int hexist(char* key){
    if(key == NULL) return -1;
    int idx = _hash(key,MAX_HASHSIZE);
    hashnode_t* node = Hash->nodes[idx];
    while (node != NULL)
    {
        if(strcmp(node->key,key) == 0){
            return 0;
        }
        node = node->next;
    }
    return -1;   
}

int hset(char* key,char* value){
    if(key == NULL || value == NULL) return -1;
    int idx = _hash(key,MAX_HASHSIZE);
    pthread_mutex_lock(&Hash->mutex);
    hashnode_t* node = Hash->nodes[idx];
    // 遍历整个链表
    while(node != NULL){
        if(strcmp(node->key,key) == 0){//exist
            pthread_mutex_unlock(&Hash->mutex);
            return 1;
        }
        node = node->next;
    }
    // 头插法
    hashnode_t *new_node = _createNode(key,value);
    new_node->next = Hash->nodes[idx];
    Hash->nodes[idx] = new_node;
    Hash->count++;
    pthread_mutex_unlock(&Hash->mutex);
    return 0;
} 

char* hget(char* key){
    if(key == NULL) return NULL;
    int idx = _hash(key,MAX_HASHSIZE);
    hashnode_t* node = Hash->nodes[idx];
    while (node != NULL)
    {
        if(strcmp(node->key,key) == 0){
            return node->value;
        }
        node = node->next;
    }
    return NULL;
} 

// hash删除需要区分两种情况，头节点和非同节点
int hdelete(char* key) {
    if (key == NULL || Hash == NULL) return -1;
    
    int idx = _hash(key, MAX_HASHSIZE);
    if (idx < 0 || idx >= MAX_HASHSIZE) return -1;
    
    hashnode_t* head = Hash->nodes[idx];
    if (head == NULL) return -1;  // Key doesn't exist

    // Check head node
    if (strcmp(head->key, key) == 0) {
        hashnode_t* new_head = head->next;
        Hash->nodes[idx] = new_head;

        if (head->key != NULL) kvs_free(head->key);
        if (head->value != NULL) kvs_free(head->value);
        kvs_free(head);

        Hash->count--;
        return 0;
    }

    // Search in the list
    hashnode_t* cur = head;
    while (cur->next != NULL) {
        if (strcmp(cur->next->key, key) == 0) {
            hashnode_t* tmp = cur->next;
            cur->next = cur->next->next;

            if (tmp->key != NULL) kvs_free(tmp->key);
            if (tmp->value != NULL) kvs_free(tmp->value);
            kvs_free(tmp);

            Hash->count--;
            return 0;
        }
        cur = cur->next;
    }

    return -1;  // Key not found
}

int hcount(){
    return Hash->count;
}
