#include "storage.h"
int rb_count = 0;
// 初始化NIL节点
static void initNIL() {
    NIL = (RBNode*)kvs_malloc(sizeof(RBNode));
    NIL->color = BLACK;
    NIL->left = NIL->right = NIL->parent = NULL;
}
// 创建新节点
static RBNode* createNode(bstring_t* key,bstring_t* value) {
    if(bstring_empty(key) || bstring_empty(value)) return NULL;

    RBNode *node = (RBNode*)kvs_malloc(sizeof(RBNode));
    if(!node) return NULL;
    node->key = key;
    node->value = value;
    node->color = RED; // 新节点初始为红色
    node->left = node->right = node->parent = NIL;
    return node;
}

// 左旋
static void leftRotate(RBNode **root, RBNode *x) {
    RBNode *y = x->right;
    x->right = y->left;
    if (y->left != NIL) {
        y->left->parent = x;
    }
    y->parent = x->parent;
    if (x->parent == NIL) {
        *root = y;
    } else if (x == x->parent->left) {
        x->parent->left = y;
    } else {
        x->parent->right = y;
    }
    y->left = x;
    x->parent = y;
}

// 右旋
static void rightRotate(RBNode **root, RBNode *y) {
    RBNode *x = y->left;
    y->left = x->right;
    if (x->right != NIL) {
        x->right->parent = y;
    }
    x->parent = y->parent;
    if (y->parent == NIL) {
        *root = x;
    } else if (y == y->parent->left) {
        y->parent->left = x;
    } else {
        y->parent->right = x;
    }
    x->right = y;
    y->parent = x;
}

// 插入修复
static void insertFixup(RBNode **root, RBNode *z) {
    while (z->parent->color == RED) {
        if (z->parent == z->parent->parent->left) {
            RBNode *y = z->parent->parent->right;
            if (y->color == RED) {
                z->parent->color = BLACK;
                y->color = BLACK;
                z->parent->parent->color = RED;
                z = z->parent->parent;
            } else {
                if (z == z->parent->right) {
                    z = z->parent;
                    leftRotate(root, z);
                }
                z->parent->color = BLACK;
                z->parent->parent->color = RED;
                rightRotate(root, z->parent->parent);
            }
        } else {
            RBNode *y = z->parent->parent->left;
            if (y->color == RED) {
                z->parent->color = BLACK;
                y->color = BLACK;
                z->parent->parent->color = RED;
                z = z->parent->parent;
            } else {
                if (z == z->parent->left) {
                    z = z->parent;
                    rightRotate(root, z);
                }
                z->parent->color = BLACK;
                z->parent->parent->color = RED;
                leftRotate(root, z->parent->parent);
            }
        }
    }
    (*root)->color = BLACK;
}

// 插入节点
static int insert(RBNode **root, char* key,size_t klen, char* value,size_t vlen) {
    bstring_t* bkey = bstring_new_from_data(key,klen);
    bstring_t* bvalue = bstring_new_from_data(value,vlen);
    RBNode *z = createNode(bkey,bvalue);
    
    if(z == NULL) return -1;
    RBNode *y = NIL;
    RBNode *x = *root;
    while (x != NIL) {
        y = x;
        if (bstring_compare(z->key,x->key) == 0){
            x->value = z->value;
            return 0;
        }
        else if(bstring_compare(z->key,x->key) < 0) {
            x = x->left;
        } else {
            x = x->right;
        }
    }
    z->parent = y;
    if (y == NIL) {
        *root = z;
    } else if (bstring_compare(z->key,y->key) < 0) { 
        y->left = z;
    } else {
        y->right = z;
    }
        
    insertFixup(root, z);

    rb_count++;

    return 0;
}

// 查找节点
static RBNode* search(RBNode *root,const char* key,size_t klen) {
    RBNode *current = root;
    bstring_t* bkey = bstring_new_from_data(key,klen);
    while (current != NIL && bstring_compare(current->key,bkey) != 0) {
        if (bstring_compare(bkey,current->key) < 0) {
            current = current->left;
        } else {
            current = current->right;
        }
    }
    return current;
}

// 查找最小节点
static RBNode* minimum(RBNode *node) {
    while (node->left != NIL) {
        node = node->left;
    }
    return node;
}

// 删除修复
static void deleteFixup(RBNode **root, RBNode *x) {
    while (x != *root && x->color == BLACK) {
        if (x == x->parent->left) {
            RBNode *w = x->parent->right;
            if (w->color == RED) {
                w->color = BLACK;
                x->parent->color = RED;
                leftRotate(root, x->parent);
                w = x->parent->right;
            }
            if (w->left->color == BLACK && w->right->color == BLACK) {
                w->color = RED;
                x = x->parent;
            } else {
                if (w->right->color == BLACK) {
                    w->left->color = BLACK;
                    w->color = RED;
                    rightRotate(root, w);
                    w = x->parent->right;
                }
                w->color = x->parent->color;
                x->parent->color = BLACK;
                w->right->color = BLACK;
                leftRotate(root, x->parent);
                x = *root;
            }
        } else {
            RBNode *w = x->parent->left;
            if (w->color == RED) {
                w->color = BLACK;
                x->parent->color = RED;
                rightRotate(root, x->parent);
                w = x->parent->left;
            }
            if (w->right->color == BLACK && w->left->color == BLACK) {
                w->color = RED;
                x = x->parent;
            } else {
                if (w->left->color == BLACK) {
                    w->right->color = BLACK;
                    w->color = RED;
                    leftRotate(root, w);
                    w = x->parent->left;
                }
                w->color = x->parent->color;
                x->parent->color = BLACK;
                w->left->color = BLACK;
                rightRotate(root, x->parent);
                x = *root;
            }
        }
    }
    x->color = BLACK;
}

// 删除节点
static int rb_delete(RBNode **root, char* key,size_t klen) {
    RBNode *z = search(*root, key,klen);
    if (z == NIL) return -1;

    RBNode *y = z;
    RBNode *x;
    Color yOriginalColor = y->color;

    if (z->left == NIL) {
        x = z->right;
        if (z->parent == NIL) {
            *root = x;
        } else if (z == z->parent->left) {
            z->parent->left = x;
        } else {
            z->parent->right = x;
        }
        x->parent = z->parent;
    } else if (z->right == NIL) {
        x = z->left;
        if (z->parent == NIL) {
            *root = x;
        } else if (z == z->parent->left) {
            z->parent->left = x;
        } else {
            z->parent->right = x;
        }
        x->parent = z->parent;
    } else {
        y = minimum(z->right);
        yOriginalColor = y->color;
        x = y->right;
        if (y->parent == z) {
            x->parent = y;
        } else {
            if (y->parent == NIL) {
                *root = x;
            } else if (y == y->parent->left) {
                y->parent->left = x;
            } else {
                y->parent->right = x;
            }
            x->parent = y->parent;
            y->right = z->right;
            y->right->parent = y;
        }
        if (z->parent == NIL) {
            *root = y;
        } else if (z == z->parent->left) {
            z->parent->left = y;
        } else {
            z->parent->right = y;
        }
        y->parent = z->parent;
        y->left = z->left;
        y->left->parent = y;
        y->color = z->color;
    }
    kvs_free(z);
    if (yOriginalColor == BLACK) {
        deleteFixup(root, x);
    }
    rb_count--;
    return 0;
}

int rexist(const char* key,size_t klen){
    if(key == NULL) return -1;
    RBNode* Node = search(root,key,klen);
    if(Node->key == 0) return -1;
    return 0;
}
int rset(char* key,size_t klen,char* value,size_t vlen){
    if(key == NULL || value == NULL) return -1;
    if(insert(&root,key,klen,value,vlen) == -1) return -1;

    return 0;
}
uint8_t* rget(const char* key,size_t klen,size_t* out_len){
    if(key == NULL) return NULL;
    RBNode* get = search(root,key,klen);
    if(get == NIL) return NULL;
    *out_len = get->value->len;
    return get->value->data;
}
int rdelete(char* key,size_t klen){
    if(key == NULL) return -1;
    int ret = rb_delete(&root,key,klen);
    if(ret == 0) return 0;
    return -1;
}
int rcount(){
    return rb_count;
}

int init_rbtree(){
    initNIL();
    root = (RBNode*)kvs_malloc(MAX_RBTREE_SIZE);
    root = NIL;
    if(root == NULL) return -1;
    return 0;
}

// 递归销毁红黑树的所有节点
static void destroy_rbtree_subtree(RBNode* node) {
    if (node == NULL || node == NIL) {
        return;
    }
    
    // 递归销毁左子树
    destroy_rbtree_subtree(node->left);
    // 递归销毁右子树
    destroy_rbtree_subtree(node->right);
    
    // 释放当前节点的键值对
    if (node->key) {
        bstring_free(node->key);
        node->key = NULL;
    }
    if (node->value) {
        bstring_free(node->value);
        node->value = NULL;
    }
    
    // 释放节点本身
    kvs_free(node);
}
// 完整的红黑树销毁函数
void dest_rbtree() {
    if (root && root != NIL) {
        destroy_rbtree_subtree(root);
        root = NULL;
    }
    if (NIL) {
        kvs_free(NIL);
        NIL = NULL;
    }
    rb_count = 0;
}

// 计算红黑树序列化所需大小（修复版）
size_t rbtree_calculate_size_node(RBNode* node) {
    if (!node || node == NIL) {
        return sizeof(uint8_t); // NIL标记的大小
    }
    
    size_t size = sizeof(uint8_t); // 节点标记
    size += sizeof(Color); // 节点颜色
    size += 2 * sizeof(size_t); // key_len + value_len
    
    if (node->key) {
        size += node->key->len;
    }
    if (node->value) {
        size += node->value->len;
    }
    
    // 递归计算左右子树
    size += rbtree_calculate_size_node(node->left);
    size += rbtree_calculate_size_node(node->right);
    
    return size;
}

// 红黑树快照 - 前序遍历序列化
int rbtree_snapshot_node(RBNode* node, char** ptr, char* end) {
    if (!node || node == NIL) {
        // 写入NIL节点标记
        if (*ptr + sizeof(uint8_t) > end) return -1;
        uint8_t nil_marker = 0;
        memcpy(*ptr, &nil_marker, sizeof(nil_marker));
        *ptr += sizeof(nil_marker);
        return 0;
    }
    
    // 检查剩余空间
    if (*ptr + sizeof(uint8_t) + sizeof(Color) + 2 * sizeof(size_t) > end) {
        return -1;
    }
    
    // 写入节点标记（非NIL节点）
    uint8_t node_marker = 1;
    memcpy(*ptr, &node_marker, sizeof(node_marker));
    *ptr += sizeof(node_marker);
    
    // 写入节点颜色
    memcpy(*ptr, &node->color, sizeof(node->color));
    *ptr += sizeof(node->color);
    
    // 写入key
    size_t key_len = node->key ? node->key->len : 0;
    memcpy(*ptr, &key_len, sizeof(key_len));
    *ptr += sizeof(key_len);
    
    if (key_len > 0) {
        if (*ptr + key_len > end) return -1;
        memcpy(*ptr, node->key->data, key_len);
        *ptr += key_len;
    }
    
    // 写入value
    size_t value_len = node->value ? node->value->len : 0;
    memcpy(*ptr, &value_len, sizeof(value_len));
    *ptr += sizeof(value_len);
    
    if (value_len > 0) {
        if (*ptr + value_len > end) return -1;
        memcpy(*ptr, node->value->data, value_len);
        *ptr += value_len;
    }
    
    // 递归序列化左右子树
    if (rbtree_snapshot_node(node->left, ptr, end) != 0) return -1;
    if (rbtree_snapshot_node(node->right, ptr, end) != 0) return -1;
    
    return 0;
}

// 红黑树快照主函数
int rbtree_snapshot(char** data, size_t* size) {
    if (!data || !size) {
        return -1;
    }
    
    *data = NULL;
    *size = 0;
    
    // 如果树为空，特殊处理
    if (!root || root == NIL) {
        *size = sizeof(int); // 只需要元素总数
        *data = (char*)kvs_malloc(*size);
        if (!*data) return -1;
        
        // 写入元素总数（0）
        int count = 0;
        memcpy(*data, &count, sizeof(count));
        return 0;
    }
    
    // 计算总大小
    *size = sizeof(int) + rbtree_calculate_size_node(root);
    *data = (char*)kvs_malloc(*size);
    if (!*data) {
        *size = 0;
        return -1;
    }
    
    char* ptr = *data;
    char* end = *data + *size;
    
    // 写入元素总数
    memcpy(ptr, &rb_count, sizeof(rb_count));
    ptr += sizeof(rb_count);
    
    // 序列化树结构
    if (rbtree_snapshot_node(root, &ptr, end) != 0) {
        kvs_free(*data);
        *data = NULL;
        *size = 0;
        return -1;
    }
    
    return 0;
}

// 红黑树恢复 - 反序列化节点
RBNode* rbtree_restore_node(const char** ptr, const char* end, RBNode* parent) {
    if (!ptr || !*ptr || *ptr >= end) return NULL;
    
    // 读取节点标记
    uint8_t node_marker;
    if (*ptr + sizeof(node_marker) > end) return NULL;
    memcpy(&node_marker, *ptr, sizeof(node_marker));
    *ptr += sizeof(node_marker);
    
    // 如果是NIL标记，返回NIL
    if (node_marker == 0) {
        return NIL;
    }
    
    // 读取节点颜色
    Color color;
    if (*ptr + sizeof(color) > end) return NULL;
    memcpy(&color, *ptr, sizeof(color));
    *ptr += sizeof(color);
    
    // 读取key
    size_t key_len;
    if (*ptr + sizeof(key_len) > end) return NULL;
    memcpy(&key_len, *ptr, sizeof(key_len));
    *ptr += sizeof(key_len);
    
    bstring_t* key = NULL;
    if (key_len > 0) {
        if (*ptr + key_len > end) return NULL;
        key = bstring_new_from_data(*ptr, key_len);
        if (!key) return NULL;
        *ptr += key_len;
    } else {
        // key不能为空
        return NULL;
    }
    
    // 读取value
    size_t value_len;
    if (*ptr + sizeof(value_len) > end) {
        bstring_free(key);
        return NULL;
    }
    memcpy(&value_len, *ptr, sizeof(value_len));
    *ptr += sizeof(value_len);
    
    bstring_t* value = NULL;
    if (value_len > 0) {
        if (*ptr + value_len > end) {
            bstring_free(key);
            return NULL;
        }
        value = bstring_new_from_data(*ptr, value_len);
        if (!value) {
            bstring_free(key);
            return NULL;
        }
        *ptr += value_len;
    } else {
        // value不能为空
        bstring_free(key);
        return NULL;
    }
    
    // 创建节点
    RBNode* node = (RBNode*)kvs_malloc(sizeof(RBNode));
    if (!node) {
        bstring_free(key);
        bstring_free(value);
        return NULL;
    }
    
    node->key = key;
    node->value = value;
    node->color = color;
    node->parent = parent;
    
    // 递归恢复左子树
    node->left = rbtree_restore_node(ptr, end, node);
    if (!node->left && node->left != NIL) {
        // 恢复失败，清理
        bstring_free(node->key);
        bstring_free(node->value);
        kvs_free(node);
        return NULL;
    }
    
    // 递归恢复右子树
    node->right = rbtree_restore_node(ptr, end, node);
    if (!node->right && node->right != NIL) {
        // 恢复失败，清理左子树和当前节点
        if (node->left != NIL) {
            // 递归释放左子树
            destroy_rbtree_subtree(node->left);
        }
        bstring_free(node->key);
        bstring_free(node->value);
        kvs_free(node);
        return NULL;
    }
    
    return node;
}

// 红黑树恢复主函数
int rbtree_restore(const char* data, size_t size) {
    if (!data || size < sizeof(int)) {
        return -1;
    }
    
    const char* ptr = data;
    const char* end = data + size;
    
    // 读取元素总数
    int count;
    memcpy(&count, ptr, sizeof(count));
    ptr += sizeof(count);
    
    // 清空现有红黑树
    if (root && root != NIL) {
        destroy_rbtree_subtree(root);
        root = NIL;
    }
    
    // 如果count为0，说明树为空
    if (count == 0) {
        rb_count = 0;
        return 0;
    }
    
    // 确保NIL节点存在
    if (!NIL) {
        initNIL();
    }
    
    // 恢复树结构
    RBNode* new_root = rbtree_restore_node(&ptr, end, NIL);
    if (!new_root) {
        // 恢复失败，确保状态一致
        root = NIL;
        rb_count = 0;
        return -1;
    }
    
    root = new_root;
    rb_count = count;
    
    return 0;
}
