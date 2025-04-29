// https://github.com/jjejdhhd/kv-store/tree/main/kv-store-v1/btree.c

#include"storage.h"
/*-----------------------------函数声明------------------------------*/


/*-----------------------------函数定义------------------------------*/
// 创建单个节点，leaf表示是否为叶子节点
btree_node* btree_node_create(btree *T, int leaf){
    btree_node* new = (btree_node*)kvs_malloc(sizeof(btree_node));
    if(new == NULL){
        printf("new kvs_malloc failed!\n");
        return NULL;
    }
    new->keys = (B_KEY_TYPE)calloc(T->m-1, sizeof(B_KEY_SUB_TYPE));
    if(new->keys == NULL){
        printf("keys kvs_malloc failed!\n");
        kvs_free(new);
        new = NULL;
        return NULL;
    }
    new->values = (B_VALUE_TYPE)calloc(T->m-1, sizeof(B_VALUE_SUB_TYPE));
    if(new->values == NULL){
        printf("values kvs_malloc failed!\n");
        kvs_free(new->keys);
        new->keys = NULL;
        kvs_free(new);
        new = NULL;
        return NULL;
    }
    new->children = (btree_node **)calloc(T->m, sizeof(btree_node*));
    if(new->children == NULL){
        printf("children kvs_malloc failed!\n");
        kvs_free(new->values);
        new->values = NULL;
        kvs_free(new->keys);
        new->keys = NULL;
        kvs_free(new);
        new = NULL;
        return NULL;
    }
    new->num = 0;
    new->leaf = leaf;
    return new;
}

// 删除单个节点
void btree_node_destroy(btree_node *cur) {
    if (cur == NULL) return;
    #if KV_BTYPE_CHAR_CHAR
        for (int i = 0; i < cur->num; i++) {
            kvs_free(cur->keys[i]);
            kvs_free(cur->values[i]);
        }
    #endif
    if (cur->keys) kvs_free(cur->keys);
    if (cur->values) kvs_free(cur->values);
    if (cur->children) kvs_free(cur->children);
    kvs_free(cur);
}
// 初始化m阶B树：分配内存，最后记得销毁B树btree_destroy()
// 返回值：0成功，-1失败
int btree_init(int m) {  
    // 为 B 树分配内存  
    kv_b = (btree*)kvs_malloc(sizeof(btree));  
    if (!kv_b)  
        return -1; // 内存分配失败  

    // 为根节点分配内存  
    kv_b->root_node = (btree_node*)kvs_malloc(sizeof(btree_node));  
    if (!kv_b->root_node) {  
        kvs_free(kv_b); // 释放已分配的 btree 内存  
        return -1; // 内存分配失败  
    }  

    // 初始化根节点字段  
    kv_b->root_node->keys = (char**)kvs_malloc(sizeof(char**) * (m - 1)); // 分配键的内存  
    kv_b->root_node->values = (char**)kvs_malloc(sizeof(char**) * (m - 1)); // 分配值的内存  
    kv_b->root_node->children = (btree_node**)kvs_malloc(sizeof(btree_node*) * m); // 分配子节点的内存  
    
    // 检查内存分配  
    if (!kv_b->root_node->keys || !kv_b->root_node->values || !kv_b->root_node->children) {  
        kvs_free(kv_b->root_node->keys);  
        kvs_free(kv_b->root_node->values);  
        kvs_free(kv_b->root_node->children);  
        kvs_free(kv_b->root_node); // 释放根节点内存  
        kvs_free(kv_b); // 释放 B 树内存  
        return -1; // 内存分配失败  
    }  

    // 初始化根节点的字段  
    kv_b->root_node->num = 0; // 当前节点元素数量  
    kv_b->root_node->leaf = 1; // 初始为叶子节点  
    kv_b->m = m; // 设置 B 树的阶数  
    kv_b->count = 0; // 初始化元素数量  

    return 0; // 成功  
}  

// 递归删除给定节点作为根节点的子树
void btree_node_destroy_recurse(btree_node *cur){
    int i = 0;
    if(cur->leaf == 1){
        btree_node_destroy(cur);
    }else{
        for(i=0; i<cur->num+1; i++){
            btree_node_destroy_recurse(cur->children[i]);
        }
    }
}

// 释放btree内存
// 返回值：0成功，-1失败
int btree_destroy(btree *T){
    if(T){
        // 删除所有节点
        if(T->root_node != NULL){
            btree_node_destroy_recurse(T->root_node);
        }
        // 删除btree
        kvs_free(T);
        T = NULL;
    }
    return 0;
}


// 根节点分裂
btree_node* btree_root_split(btree *T){
    // 创建兄弟节点
    btree_node *brother = btree_node_create(T, T->root_node->leaf);
    int i = 0;
    for(i=0; i<((T->m-1)>>1); i++){
            brother->keys[i] = T->root_node->keys[i+(T->m>>1)];
            brother->values[i] = T->root_node->values[i+(T->m>>1)];
            T->root_node->keys[i+(T->m>>1)] = NULL;
            T->root_node->values[i+(T->m>>1)] = NULL;
        brother->children[i] = T->root_node->children[i+(T->m>>1)];
        T->root_node->children[i+(T->m>>1)] = NULL;
        brother->num++;
        T->root_node->num--;
    }
    // 还需要复制最后一个指针
    brother->children[brother->num] = T->root_node->children[T->m-1];
    T->root_node->children[T->m-1] = NULL;
    
    // 创建新的根节点
    btree_node *new_root = btree_node_create(T, 0);
    new_root->keys[0] = T->root_node->keys[T->root_node->num-1];
    new_root->values[0] = T->root_node->values[T->root_node->num-1];
    T->root_node->keys[T->root_node->num-1] = NULL;
    T->root_node->values[T->root_node->num-1] = NULL;
    T->root_node->num--;
    new_root->num = 1;
    new_root->children[0] = T->root_node;
    new_root->children[1] = brother;
    T->root_node = new_root;

    return T->root_node;
}

// 索引为idx的孩子节点分裂
btree_node* btree_child_split(btree *T, btree_node* cur, int idx){
    // 创建孩子的兄弟节点
    btree_node *full_child = cur->children[idx];
    btree_node *new_child = btree_node_create(T, cur->children[idx]->leaf);
    int i = 0;
    for(i=0; i<((T->m-1)>>1); i++){
        new_child->keys[i] = full_child->keys[i+(T->m>>1)];
        new_child->values[i] = full_child->values[i+(T->m>>1)];
        full_child->keys[i+(T->m>>1)] = NULL;
        full_child->values[i+(T->m>>1)] = NULL;
        new_child->children[i] = full_child->children[i+(T->m>>1)];
        full_child->children[i+(T->m>>1)] = NULL;
        new_child->num++;
        full_child->num--;
    }
    new_child->children[new_child->num] = full_child->children[T->m-1];
    full_child->children[T->m-1] = NULL;

    // 把孩子的元素拿上来
    // 调整自己的key和children
    for(i=cur->num; i>idx; i--){
        cur->keys[i] = cur->keys[i-1];
        cur->values[i] = cur->values[i-1];
        cur->children[i+1] = cur->children[i];
    }
    cur->children[idx+1] = new_child;
    cur->keys[idx] = full_child->keys[full_child->num-1];
    cur->values[idx] = full_child->values[full_child->num-1];
    full_child->keys[full_child->num-1] = NULL;
    full_child->values[full_child->num-1] = NULL;
    cur->num++;
    full_child->num--;

    return cur;
}


// btree插入元素：先分裂，再插入，必然在叶子节点插入
// 返回值：0表示成功、-1表示失败、-2表示已经有key
int btree_insert_key(B_KEY_SUB_TYPE key, B_VALUE_SUB_TYPE value){
    btree_node *cur = kv_b->root_node;
    if(key == NULL || value == NULL){

        // printf("illegal insert: key=%s, value=%s\n", key, value);
        return -1;
    }

    if(cur == NULL){

        btree_node *new = btree_node_create(kv_b, 1);
        // 复制key
        char* kcopy = (char*)kvs_malloc(strlen(key)+1);
        if(kcopy == NULL) return -1;
        strncpy(kcopy, key, strlen(key)+1);
        // 复制value
        char* vcopy = (char*)kvs_malloc(strlen(value)+1);
        if(vcopy == NULL){
            kvs_free(kcopy);
            kcopy = NULL;
            return -1;
        }
        strncpy(vcopy, value, strlen(value)+1);
        new->keys[0] = kcopy;
        new->values[0] = vcopy;
        new->num = 1;
        kv_b->root_node = new;
        kv_b->count++;
    }else{

    // 函数整体逻辑：从根节点逐步找到元素要插入的叶子节点，先分裂、再添加
        // 先查看根节点是否需要分裂
        if(cur->num == kv_b->m-1){
            cur = btree_root_split(kv_b);

        }
        // 从根节点开始寻找要插入的叶子节点
        while(cur->leaf == 0){
            // 找到下一个要比较的孩子节点
            int next_idx = 0;  // 要进入的孩子节点的索引
            int i = 0;
            for(i=0; i<cur->num; i++){
                if(strcmp(key, cur->keys[i]) == 0){
                    // printf("insert failed! already has key=%d!\n", key);
                    return -2;
                }else if(strcmp(key, cur->keys[i]) < 0){
                    next_idx = i;
                    break;
                }else if(i == cur->num-1){
                    next_idx = cur->num;
                }
            }
            // 查看孩子是否需要分裂，不需要就进入
            if(cur->children[next_idx]->num == kv_b->m-1){
                cur = btree_child_split(kv_b, cur, next_idx);
            }else{
                cur = cur->children[next_idx];
            }
        }

        // 将新元素插入到叶子节点中
        int i = 0;
        int pos = 0;  // 要插入的位置
        for(i=0; i<cur->num; i++){
            if(strcmp(key, cur->keys[i]) == 0){
                // printf("insert failed! already has key=%d!\n", key);
                return -2;
            }else if(strcmp(key, cur->keys[i]) < 0){
                pos = i;
                break;
            }else if(i == cur->num-1){
                pos = cur->num;
            }
        }
        // 插入元素
        // 复制key
        char* kcopy = (char*)kvs_malloc(strlen(key)+1);
        if(kcopy == NULL) return -1;
        strncpy(kcopy, key, strlen(key)+1);
        // 复制value
        char* vcopy = (char*)kvs_malloc(strlen(value)+1);
        if(vcopy == NULL){
            kvs_free(kcopy);
            kcopy = NULL;
            return -1;
        }
        strncpy(vcopy, value, strlen(value)+1);
        if(pos == cur->num){
            cur->keys[cur->num] = kcopy;
            cur->values[cur->num] = vcopy;
        }else{
            for(i=cur->num; i>pos; i--){
                cur->keys[i] = cur->keys[i-1];
                cur->values[i] = cur->values[i-1];
            }
            cur->keys[pos] = kcopy;
            cur->values[pos] = vcopy;
        }
        kv_b->count++;
        cur->num++;
    }
    return 0;
}

// 借位：将cur节点的idx_key元素下放到idx_dest孩子
btree_node *btree_borrow(btree_node *cur, int idx_key, int idx_dest){
    int idx_sour = (idx_key == idx_dest) ? idx_dest+1 : idx_key;
    btree_node *node_dest = cur->children[idx_dest];  // 目的节点
    btree_node *node_sour = cur->children[idx_sour];  // 源节点
    if(idx_key == idx_dest){
        // 自己下去作为目的节点的最后一个元素
        node_dest->keys[node_dest->num] = cur->keys[idx_key];
        node_dest->values[node_dest->num] = cur->values[idx_key];
        node_dest->children[node_dest->num+1] = node_sour->children[0];
        node_dest->num++;
        // 把源节点的第一个元素请上来
        cur->keys[idx_key] = node_sour->keys[0];
        cur->values[idx_key] = node_sour->values[0];
        for(int i=0; i<node_sour->num-1; i++){
            node_sour->keys[i] = node_sour->keys[i+1];
            node_sour->values[i] = node_sour->values[i+1];
            node_sour->children[i] = node_sour->children[i+1];
        }
        node_sour->children[node_sour->num-1] = node_sour->children[node_sour->num];
        node_sour->children[node_sour->num] = NULL;
        node_sour->keys[node_sour->num-1] = NULL;
        node_sour->values[node_sour->num-1] = NULL;
        node_sour->num--;
    }else{
        // 自己下去作为目的节点的第一个元素
        node_dest->children[node_dest->num+1] = node_dest->children[node_dest->num];
        for(int i=node_dest->num; i>0; i--){
            node_dest->keys[i] = node_dest->keys[i-1];
            node_dest->values[i] = node_dest->values[i-1];
            node_dest->children[i] = node_dest->children[i-1];
        }
        node_dest->keys[0] = cur->keys[idx_key];
        node_dest->values[0] = cur->values[idx_key];
        node_dest->children[0] = node_sour->children[node_sour->num];
        node_dest->num++;
        // 把源节点的最后一个元素请上来
        cur->keys[idx_key] = node_sour->keys[node_sour->num-1];
        cur->values[idx_key] = node_sour->values[node_sour->num-1];
        node_sour->keys[node_sour->num-1] = NULL;
        node_sour->values[node_sour->num-1] = NULL;
		node_sour->keys[node_sour->num - 1] = NULL;
    	node_sour->values[node_sour->num - 1] = NULL;
        node_sour->children[node_sour->num] = NULL;
        node_sour->num--;
    }
    return node_dest;
}

// 合并：将cur节点的idx元素向下合并
btree_node *btree_merge(btree *T, btree_node *cur, int idx){
    btree_node *left = cur->children[idx];
    btree_node *right = cur->children[idx+1];
    // 保存原始数量
    int original_num = cur->num;
    
    // 下移父节点键到左孩子
    left->keys[left->num] = cur->keys[idx];
    left->values[left->num] = cur->values[idx];
    left->num++;
    
    // 左移父节点的键和子指针
    for(int i=idx; i < original_num - 1; i++){
        cur->keys[i] = cur->keys[i+1];
        cur->values[i] = cur->values[i+1];
        cur->children[i+1] = cur->children[i+2];
    }
    
    // 释放原最后一个键和值
    kvs_free(cur->keys[original_num - 1]);
    kvs_free(cur->values[original_num - 1]);
    cur->keys[original_num - 1] = NULL;
    cur->values[original_num - 1] = NULL;
    cur->children[original_num] = NULL;
    cur->num = original_num - 1;  // 更新父节点键数量
    
    // 复制右兄弟的键和子节点到左兄弟
    for(int i=0; i<right->num; i++){
        left->keys[left->num] = right->keys[i];
        left->values[left->num] = right->values[i];
        left->children[left->num] = right->children[i];
        left->num++;
    }
    left->children[left->num] = right->children[right->num];
    
    // 销毁右兄弟并更新根节点
    btree_node_destroy(right);
    if(T->root_node == cur && cur->num == 0){
        btree_node_destroy(cur);
        T->root_node = left;
    }
    return left;
}

// 找出当前节点索引为idx_key的元素的前驱节点
btree_node* btree_precursor_node(btree *T, btree_node *cur, int idx_key){
    if(cur->leaf == 0){
        cur = cur->children[idx_key];
        while(cur->leaf == 0){
            cur = cur->children[cur->num];
        }
    }
    return cur;
}

// 找出当前节点索引为idx_key的元素的后继节点
btree_node* btree_successor_node(btree *T, btree_node *cur, int idx_key){
    if(cur->leaf == 0){
        cur = cur->children[idx_key+1];
        while(cur->leaf == 0){
            cur = cur->children[0];
        }
    }
    return cur;
}


// btree删除元素：先合并/借位，再删除，必然在叶子节点删除
// 返回值：0成功，-1失败，-2没有
int btree_delete_key(btree *T, B_KEY_SUB_TYPE key) {
    if (T->root_node != NULL && key != NULL)
    {
        btree_node *cur = T->root_node;
        // 在去往叶子节点的过程中不断调整(合并/借位)
        while (cur->leaf == 0) {
            // 看看要去哪个孩子
            int idx_next = 0; // 下一个要去的孩子节点索引
            int idx_bro = 0;
            int idx_key = 0;
            if (strcmp(key, cur->keys[0]) < 0)
            {
                idx_next = 0;
                idx_bro = 1;
            }
            else if (strcmp(key, cur->keys[cur->num - 1]) > 0)
            {
                idx_next = cur->num;
                idx_bro = cur->num - 1;
            } else {
                for (int i = 0; i < cur->num; i++) {
                    if (strcmp(key, cur->keys[i]) == 0)
                    {
                        // 哪边少去哪边
                        if (cur->children[i]->num <= cur->children[i + 1]->num) {
                            idx_next = i;
                            idx_bro = i + 1;
                        } else {
                            idx_next = i + 1;
                            idx_bro = i;
                        }
                        break;
                    }
                    else if ((i < cur->num - 1) && (strcmp(key, cur->keys[i]) > 0) && (strcmp(key, cur->keys[i + 1]) < 0))
                    {
                        idx_next = i + 1;
                        // 谁多谁是兄弟
                        if (cur->children[i]->num > cur->children[i + 2]->num) {
                            idx_bro = i;
                        } else {
                            idx_bro = i + 2;
                        }
                        break;
                    }
                }
            }
            idx_key = (idx_next < idx_bro) ? idx_next : idx_bro;
            // 依据孩子节点的元素数量进行调整
            if (cur->children[idx_next]->num <= ((T->m >> 1) - 1)) {
                // 借位：下一孩子的元素少，下一孩子的兄弟节点的元素多
                if (cur->children[idx_bro]->num >= (T->m >> 1)) {
                    cur = btree_borrow(cur, idx_key, idx_next);
                }
                // 合并：两个孩子都不多
                else {
                    cur = btree_merge(T, cur, idx_key);
                }
            }
            else if (strcmp(cur->keys[idx_key], key) == 0)
            {
                // 若当前元素就是要删除的节点，那一定要送下去
                // 但是不能借位,而是将前驱元素搬上来
                btree_node *pre;
                B_KEY_SUB_TYPE tmp;
                if (idx_key == idx_next) {
                    // 找到前驱节点
                    pre = btree_precursor_node(T, cur, idx_key);
                    // 交换 当前元素 和 前驱节点的最后一个元素
                    tmp = pre->keys[pre->num - 1];
                    pre->keys[pre->num - 1] = cur->keys[idx_key];
                    cur->keys[idx_key] = tmp;
                    tmp = pre->values[pre->num - 1];
                    pre->values[pre->num - 1] = cur->values[idx_key];
                    cur->values[idx_key] = tmp;
                } else {
                    // 找到后继节点
                    pre = btree_successor_node(T, cur, idx_key);
                    // 交换 当前元素 和 后继节点的第一个元素
                    tmp = pre->keys[0];
                    pre->keys[0] = cur->keys[idx_key];
                    cur->keys[idx_key] = tmp;
                    tmp = pre->values[0];
                    pre->values[0] = cur->values[idx_key];
                    cur->values[idx_key] = tmp;
                }
                cur = cur->children[idx_next];
                // cur = btree_borrow(cur, idx_key, idx_next);
            } else {
                cur = cur->children[idx_next];
            }
        }
        // 叶子节点删除元素
        for (int i = 0; i < cur->num; i++) {
            if (strcmp(cur->keys[i], key) == 0)
            {
                if (cur->num == 1 && T->root_node != NULL) {
                    // 若B树只剩最后一个元素
                    btree_node_destroy(cur);
                    T->root_node = NULL;
                    T->count = 0;
                } else {
                    if (i != cur->num - 1) {
                        for (int j = i; j < (cur->num - 1); j++) {
                            cur->keys[j] = cur->keys[j + 1];
                            cur->values[j] = cur->values[j + 1];
                        }
                    }
                    kvs_free(cur->keys[cur->num - 1]);
                    kvs_free(cur->values[cur->num - 1]);
                    cur->keys[cur->num - 1] = NULL;
                    cur->values[cur->num - 1] = NULL;
                    cur->num--;
                    T->count--;
                }
                return 0;
            } else if (i == cur->num - 1) {
                // printf("there is no key=%d\n", key);
                return -2;
            }
        }
    }
    return -1;
}
// 打印当前节点信息
void btree_node_print(btree_node *cur){
    if(cur == NULL){
        printf("NULL\n");
    }else{
        printf("leaf:%d, num:%d, key:|", cur->leaf, cur->num);
        for(int i=0; i<cur->num; i++){
            printf("%s|", cur->keys[i]);
        }
        printf("\n");
    }
}

// 先序遍历给定节点为根节点的子树(递归)
void btree_traversal_node(btree *T, btree_node *cur){
    // 打印当前节点信息
    btree_node_print(cur);

    // 依次打印所有子节点信息
    if(cur->leaf == 0){
        int i = 0;
        for(i=0; i<cur->num+1; i++){
            btree_traversal_node(T, cur->children[i]);
        }
    }
}

// btree遍历
void btree_traversal(btree *T){
    if(T->root_node != NULL){
        btree_traversal_node(T, T->root_node);
    }else{
        // printf("btree_traversal(): There is no key in B-tree!\n");
    }
}

btree_node* btree_search_key(btree *T, B_KEY_SUB_TYPE key){
	if (key == NULL) {   
        return NULL;  
    }  
    else if(key != NULL){
        btree_node *cur = T->root_node;
        // 先寻找是否为非叶子节点
        while(cur->leaf == 0){
            if(strcmp(key, cur->keys[0]) < 0){
                cur = cur->children[0];
            }else if(strcmp(key, cur->keys[cur->num-1]) > 0){
                cur = cur->children[cur->num];
            }else{
                for(int i=0; i<cur->num; i++){
                    if(strcmp(cur->keys[i], key) == 0){
                        return cur;
                    }else if((i<cur->num-1) && (strcmp(key,cur->keys[i])>0) && (strcmp(key,cur->keys[i+1])<0)){
                        cur = cur->children[i+1];
                    }
                }
            }
        }
        // 在寻找是否为叶子节点
        if(cur->leaf == 1){
            for(int i=0; i<cur->num; i++){
                if(strcmp(cur->keys[i],key) == 0){
                    return cur;
                }
            }
        }
    }
    // 都没找到返回NULL
    return NULL;
}

// 获取B树的高度
int btree_depth(btree *T){
    int depth = 0;
    btree_node *cur = T->root_node;
    while(cur != NULL){
        depth++;
        cur = cur->children[0];
    }
    return depth;
}
/*------------------------------------------------------------------*/

/*----------------------------kv存储协议-----------------------------*/
// 初始化
// 参数：kv_b要传地址
// 返回值：0成功，-1失败
int init_btree(int m){
    return btree_init(m);
}
// 销毁
// 参数：kv_b要传地址
// 返回值：0成功，-1失败
int dest_btree(){
    return btree_destroy(kv_b);
}
// 插入指令：有就报错，没有就创建
// 返回值：0表示成功、-1表示失败、-2表示已经有key
int bset(char* key,char* value){
    return btree_insert_key(key, value);
}
// 查找指令
// 返回值：正常返回node，NULL表示没有
char* bget(char* key){
    btree_node* node = btree_search_key(kv_b, key);
    if(node != NULL){
        for(int i=0; i<node->num; i++){
            if(strcmp(node->keys[i],key) == 0){
                return node->values[i];
            }
        }
    }
    return NULL;
}
// 删除指令
// 返回值：0成功，-1失败，-2没有
int bdelete(char* key){
    return btree_delete_key(kv_b, key);
}
// 计数指令
int bcount(){
    return kv_b->count;
}
// 存在指令
// 返回值：0成功
int bexist(char* key){
    btree_node* node = btree_search_key(kv_b, key);
	if(node == NULL){
		return 1;
	}
	return 0;
}