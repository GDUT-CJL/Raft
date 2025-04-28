#include "storage.h"

static skipnode_t* _createNode(char* key, char* value, int level) {
    if (key == NULL || value == NULL || level < 0) return NULL;

    skipnode_t* node = (skipnode_t*)kvs_malloc(sizeof(skipnode_t));
    if (node == NULL) return NULL;

    node->key = (char*)kvs_malloc(strlen(key) + 1);
    if (node->key == NULL) {
        kvs_free(node);
        return NULL;
    }

    node->value = (char*)kvs_malloc(strlen(value) + 1);
    if (node->value == NULL) {
        kvs_free(node->key);
        kvs_free(node);
        return NULL;
    }

    strncpy(node->key, key, strlen(key) + 1);
    strncpy(node->value, value, strlen(value) + 1);

    node->next = (skipnode_t**)kvs_malloc(sizeof(skipnode_t*) * level);
    if (node->next == NULL) {
        kvs_free(node->key);
        kvs_free(node->value);
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
    if(sklist == NULL) return -1;
    // 删除所有数据节点
    skipnode_t* cur_node = sklist->head->next[0];// 指向跳表最底层的第一个数据节点
    skipnode_t* nxt_node = cur_node->next[0];// 指向cur_node的下一个节点
    while(cur_node != NULL){
        // 调整信息
        for(int i=0; i<sklist->max_level; i++){
            sklist->head->next[i] = cur_node->next[i];
        }
        sklist->nodeNum--;
        // 开始删除
        nxt_node = cur_node->next[0];
        if(dest_sklistable_node(cur_node) != 0){
            return -1;
        }
        cur_node = nxt_node;
    }
    // 删除头节点
    if(sklist->head){
        kvs_free(sklist->head->next);
        sklist->head->next = NULL;
        kvs_free(sklist->head);
        sklist->head = NULL;
    }
    sklist->nodeNum = 0;
    sklist->cur_level = 0;
    return 0;
}

skipnode_t* kvs_skiplist_search(char* key){
    if(key == NULL || sklist == NULL || sklist->head == NULL)     return NULL;
    int levelIndex = sklist->cur_level - 1;
    int i;
    skipnode_t* cur = sklist->head;
    for(i = levelIndex; i >=0; --i){
        while(cur->next[i] != NULL && strcmp(cur->next[i]->key,key) < 0){
            cur = cur->next[i];
        }
    }
    if(cur->next[0] != NULL && strcmp(cur->next[0]->key,key) == 0){
        return cur->next[0];
    }
    else{
        return NULL;
    }
}

int zset(char* key, char* value){
    // 寻找新节点应该插入的位置
    skipnode_t* update[sklist->max_level];  // 查找的路径
    skipnode_t* p = sklist->head;
    for(int i=sklist->cur_level-1; i>=0; i--){
        while(p->next[i] != NULL && strcmp(p->next[i]->key, key)<0){
            p = p->next[i];
        }
        update[i] = p;
    }
    // 将节点插入
    if(p->next[0]!=NULL && strcmp(p->next[0]->key, key)==0)
    {
        return -2;  // already have same key
    }else{
        // 新节点的层数--概率0.5
        int newlevel = 1;
        while((rand()%2) && newlevel < sklist->max_level){
            ++newlevel;
        }
        // 创建新节点
        skipnode_t* new_node = _createNode(key, value, newlevel);
        if(new_node == NULL) return -1;
        // 完善当前层级之上的查找路径（也就是头节点）
        if(newlevel > sklist->cur_level){
            for(int i=sklist->cur_level; i<newlevel; i++){
                update[i] = sklist->head;
            }
            sklist->cur_level = newlevel;
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

char* zget(char* key){
    if(key==NULL || sklist==NULL) return NULL;
    skipnode_t* node = kvs_skiplist_search(key);
    if(node != NULL){
        return node->value;
    }else{
        return NULL;
    }
}

// 删除元素
// 返回值：0成功，-1失败，-2没有
int zdelete(char* key){
    // 查找节点
    skipnode_t* update[sklist->max_level];
    skipnode_t* p = sklist->head;
    for(int i=sklist->cur_level-1; i>=0; i--){
        while(p->next[i]!=NULL && strcmp(p->next[i]->key, key)<0)
        {
            p = p->next[i];
        }
        update[i] = p;
    }
    // 删除节点并更新指向信息
    if(p->next[0]!=NULL && strcmp(p->next[0]->key, key)==0)
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

int zexist(char* key){
    if(kvs_skiplist_search(key) == NULL){
        return -1;
    }
    return 0;
}