#include "storage.h"

static skipnode_t* _createNode(bstring_t* key, bstring_t* value, int level) {
    if (bstring_empty(key) || bstring_empty(value) || level < 0) return NULL;

    skipnode_t* node = (skipnode_t*)kvs_malloc(sizeof(skipnode_t));
    if (node == NULL) return NULL;

    node->key = key;
    node->value = value;

    node->next = (skipnode_t**)kvs_malloc(sizeof(skipnode_t*) * level);
    if (node->next == NULL) {
        bstring_free(node->key);
        bstring_free(node->value);
        kvs_free(node);
        return NULL;
    }

    for (int i = 0; i < level; ++i) {
        node->next[i] = NULL;
    }

    return node;
}

int init_skipTable(){
    sklist = (skiplist_t*)kvs_malloc(sizeof(skiplist_t));
    if(!sklist) return -1;

    sklist->head =(skipnode_t*)calloc(1, sizeof(skipnode_t));
    sklist->head->key = NULL;
    sklist->head->value = NULL;
    sklist->max_level = SKIPTABLE_MAX_LEVEL;
    sklist->cur_level = 0;
    sklist->nodeNum = 0;

    sklist->head->next = (skipnode_t**)calloc(SKIPTABLE_MAX_LEVEL, sizeof(skipnode_t*));
    if (sklist->head->next == NULL) {
        kvs_free(sklist->head);
        kvs_free(sklist);
        sklist->head = NULL;
        return -1;
    }

    for (int i=0; i < SKIPTABLE_MAX_LEVEL; i++) {
        sklist->head->next[i] = NULL;
    }
    return 0;
}

int dest_sklistable_node(skipnode_t* node){
    if(node == NULL)  return -1;
    if(node->key != NULL){
        kvs_free(node->key);
        node->key = NULL;
    }

    if(node->value != NULL){
        kvs_free(node->value);
        node->value = NULL;
    }

    if(node->next != NULL){
        kvs_free(node->next);
        node->next = NULL;
    }

    kvs_free(node);
    node = NULL;
    return 0;
}

int dest_skiplist(){
    if (!sklist) return -1;
    
    // 删除所有数据节点
    skipnode_t* cur_node = sklist->head->next[0];
    while (cur_node != NULL) {
        skipnode_t* next_node = cur_node->next[0];
        
        // 释放键值对
        if (cur_node->key) {
            bstring_free(cur_node->key);
            cur_node->key = NULL;
        }
        if (cur_node->value) {
            bstring_free(cur_node->value);
            cur_node->value = NULL;
        }
        
        // 释放next指针数组
        if (cur_node->next) {
            kvs_free(cur_node->next);
            cur_node->next = NULL;
        }
        
        // 释放节点本身
        kvs_free(cur_node);
        cur_node = next_node;
    }
    
    // 删除头节点
    if (sklist->head) {
        if (sklist->head->next) {
            kvs_free(sklist->head->next);
            sklist->head->next = NULL;
        }
        kvs_free(sklist->head);
        sklist->head = NULL;
    }
    
    // 重置跳表状态
    sklist->max_level = 0;
    sklist->cur_level = 0;
    sklist->nodeNum = 0;
    
    // 释放跳表结构本身
    kvs_free(sklist);
    sklist = NULL;
    
    return 0;
}

skipnode_t* kvs_skiplist_search(bstring_t* key){
    if(bstring_empty(key) || sklist == NULL || sklist->head == NULL)     return NULL;
    int levelIndex = sklist->cur_level - 1;
    int i;
    skipnode_t* cur = sklist->head;
    for(i = levelIndex; i >=0; --i){
        while(cur->next[i] != NULL && bstring_compare(cur->next[i]->key,key) < 0){
            cur = cur->next[i];
        }
    }
    if(cur->next[0] != NULL && bstring_compare(cur->next[0]->key,key) == 0){
        return cur->next[0];
    }
    else{
        return NULL;
    }
}

int zset(char* key,size_t klen, char* value,size_t vlen){
     // 找到每一层上应该插入新节点的前驱节点
    bstring_t* bkey = bstring_new_from_data(key,klen);
    bstring_t* bvalue = bstring_new_from_data(value,vlen);
    skipnode_t* update[sklist->max_level];  // 查找的路径
    skipnode_t* p = sklist->head;
    // 从高层到低层，逐步找到新节点应插入的前驱位置。
    for(int i=sklist->cur_level-1; i>=0; i--){
        while(p->next[i] != NULL && bstring_compare(p->next[i]->key, bkey)<0){
            p = p->next[i];
        }
        update[i] = p;
    }
    // 将节点插入
    if(p->next[0]!=NULL && bstring_compare(p->next[0]->key, bkey)==0)//检查是否已存在相同键
    {
        p->next[0]->value = bvalue;
        //strncpy(p->next[0]->value, value,strlen(value));
        return 0;  // already have same key
    }else{
        // 新节点的层数--概率0.5
        int newlevel = 1;
        while((rand()%2) && newlevel < sklist->max_level){
            ++newlevel;
        }
        // 创建新节点
        skipnode_t* new_node = _createNode(bkey, bvalue, newlevel);
        if(new_node == NULL) return -1;
        // 完善当前层级之上的查找路径（也就是头节点）
        if(newlevel > sklist->cur_level){// 如果新节点层级高于现有最大层
            for(int i=sklist->cur_level; i<newlevel; i++){
                update[i] = sklist->head;
            }
            sklist->cur_level = newlevel;//更新update[]数组，对应上层指向头节点，提升sklist->cur_level。
        }
        // 更新新节点的前后指向
        for(int i=0; i < newlevel; i++){
            new_node->next[i] = update[i]->next[i];
            update[i]->next[i] = new_node;
        }
        sklist->nodeNum++;
        return 0;
    }
}

uint8_t* zget(char* key,size_t klen,size_t* out_vlen){
    if(key==NULL || sklist==NULL) return NULL;
    bstring_t* bkey = bstring_new_from_data(key,klen);
    skipnode_t* node = kvs_skiplist_search(bkey);
    if(node != NULL){
        *out_vlen = node->value->len;
        return node->value->data;
    }else{
        return NULL;
    }
}

// 删除元素
// 返回值：0成功，-1失败，-2没有
int zdelete(char* key,size_t klen){
    // 查找节点
    if(key == NULL) return -1;
    bstring_t* bkey = bstring_new_from_data(key,klen);
    skipnode_t* update[sklist->max_level];
    skipnode_t* p = sklist->head;
    for(int i=sklist->cur_level-1; i>=0; i--){
        while(p->next[i]!=NULL && bstring_compare(p->next[i]->key, bkey)<0)
        {
            p = p->next[i];
        }
        update[i] = p;
    }
    // 删除节点并更新指向信息
    if(p->next[0]!=NULL && bstring_compare(p->next[0]->key, bkey)==0)
    {
        skipnode_t* node_d = p->next[0];  // 待删除元素
        for(int i=0; i<sklist->cur_level; i++){
            if(update[i]->next[i] == node_d){
                update[i]->next[i] = node_d->next[i];
            }
        }
        int ret = dest_sklistable_node(node_d);
        if(ret == 0){
            sklist->nodeNum--;
            for(int i=0; i<sklist->max_level; i++){
                if(sklist->head->next[i] == NULL){
                    sklist->cur_level = i;
                    break;
                }
            }
            
        }
        return ret;
    }else{
        return -2;  // no such key
    }
}

int zcount(){
    return sklist->nodeNum;
}

int zexist(char* key,size_t klen){
    bstring_t* bkey = bstring_new_from_data(key,klen);
    if(kvs_skiplist_search(bkey) == NULL){
        return -1;
    }
    return 0;
}

// 在 ddb_skiplist.c 中添加以下函数

// 计算跳表序列化所需大小
size_t skiplist_calculate_size() {
    if (!sklist || !sklist->head) return 0;
    
    size_t size = sizeof(int) * 3; // max_level, cur_level, nodeNum
    size += sizeof(sklist->nodeNum); // 节点数量
    
    // 遍历最底层计算所有节点的键值对大小
    skipnode_t* current = sklist->head->next[0];
    while (current) {
        if (current->key && current->value) {
            size += sizeof(int); // 节点层数
            size += 2 * sizeof(size_t); // key_len + value_len
            size += current->key->len + current->value->len;
        }
        current = current->next[0];
    }
    
    return size;
}

// 跳表快照 - 按最底层顺序序列化
int skiplist_snapshot(char** data, size_t* size) {
    if (!data || !size || !sklist || !sklist->head) return -1;
    
    // 计算总大小
    *size = skiplist_calculate_size();
    *data = (char*)kvs_malloc(*size);
    if (!*data) return -1;
    
    char* ptr = *data;
    char* end = *data + *size;
    
    // 写入跳表基本信息
    memcpy(ptr, &sklist->max_level, sizeof(sklist->max_level));
    ptr += sizeof(sklist->max_level);
    memcpy(ptr, &sklist->cur_level, sizeof(sklist->cur_level));
    ptr += sizeof(sklist->cur_level);
    memcpy(ptr, &sklist->nodeNum, sizeof(sklist->nodeNum));
    ptr += sizeof(sklist->nodeNum);
    
    // 写入节点数量
    int node_count = sklist->nodeNum;
    memcpy(ptr, &node_count, sizeof(node_count));
    ptr += sizeof(node_count);
    
    // 按最底层顺序序列化所有节点
    skipnode_t* current = sklist->head->next[0];
    while (current && ptr < end) {
        if (!current->key || !current->value) {
            current = current->next[0];
            continue;
        }
        
        // 计算当前节点的实际层数
        int node_level = 0;
        for (int i = 0; i < sklist->max_level; i++) {
            if (i < sklist->cur_level && current->next[i] != NULL) {
                node_level = i + 1;
            }
        }
        if (node_level == 0) node_level = 1; // 至少有一层
        
        // 写入节点层数
        if (ptr + sizeof(int) > end) break;
        memcpy(ptr, &node_level, sizeof(node_level));
        ptr += sizeof(node_level);
        
        // 写入key
        size_t key_len = current->key->len;
        if (ptr + sizeof(size_t) > end) break;
        memcpy(ptr, &key_len, sizeof(key_len));
        ptr += sizeof(key_len);
        
        if (ptr + key_len > end) break;
        memcpy(ptr, current->key->data, key_len);
        ptr += key_len;
        
        // 写入value
        size_t value_len = current->value->len;
        if (ptr + sizeof(size_t) > end) break;
        memcpy(ptr, &value_len, sizeof(value_len));
        ptr += sizeof(value_len);
        
        if (ptr + value_len > end) break;
        memcpy(ptr, current->value->data, value_len);
        ptr += value_len;
        
        current = current->next[0];
    }
    
    // 检查是否成功序列化了所有节点
    if (current != NULL) {
        kvs_free(*data);
        *data = NULL;
        return -1;
    }
    
    return 0;
}

// 跳表恢复 - 反序列化
int skiplist_restore(const char* data, size_t size) {
    if (!data || size < sizeof(int) * 4) return -1;
    
    const char* ptr = data;
    const char* end = data + size;
    
    // 读取跳表基本信息
    int max_level, cur_level, nodeNum, node_count;
    memcpy(&max_level, ptr, sizeof(max_level));
    ptr += sizeof(max_level);
    memcpy(&cur_level, ptr, sizeof(cur_level));
    ptr += sizeof(cur_level);
    memcpy(&nodeNum, ptr, sizeof(nodeNum));
    ptr += sizeof(nodeNum);
    memcpy(&node_count, ptr, sizeof(node_count));
    ptr += sizeof(node_count);
    
    // 清空现有跳表
    if (sklist) {
        dest_skiplist();
    } else {
        // 初始化跳表
        if (init_skipTable() != 0) return -1;
    }
    
    // 恢复所有节点
    for (int i = 0; i < node_count && ptr < end; i++) {
        // 读取节点层数
        int node_level;
        if (ptr + sizeof(int) > end) break;
        memcpy(&node_level, ptr, sizeof(node_level));
        ptr += sizeof(node_level);
        
        // 读取key
        size_t key_len;
        if (ptr + sizeof(size_t) > end) break;
        memcpy(&key_len, ptr, sizeof(key_len));
        ptr += sizeof(key_len);
        
        if (ptr + key_len > end) break;
        char* key_data = (char*)kvs_malloc(key_len + 1);
        if (!key_data) break;
        memcpy(key_data, ptr, key_len);
        key_data[key_len] = '\0';
        ptr += key_len;
        
        // 读取value
        size_t value_len;
        if (ptr + sizeof(size_t) > end) {
            kvs_free(key_data);
            break;
        }
        memcpy(&value_len, ptr, sizeof(value_len));
        ptr += sizeof(value_len);
        
        if (ptr + value_len > end) {
            kvs_free(key_data);
            break;
        }
        char* value_data = (char*)kvs_malloc(value_len + 1);
        if (!value_data) {
            kvs_free(key_data);
            break;
        }
        memcpy(value_data, ptr, value_len);
        value_data[value_len] = '\0';
        ptr += value_len;
        
        // 插入节点到跳表
        if (zset(key_data, key_len, value_data, value_len) != 0) {
            kvs_free(key_data);
            kvs_free(value_data);
            break;
        }
        
        kvs_free(key_data);
        kvs_free(value_data);
    }
    
    // 验证恢复的节点数量
    if (sklist->nodeNum != node_count) {
        // 恢复不完整，清理部分恢复的数据
        dest_skiplist();
        return -1;
    }
    
    return 0;
}