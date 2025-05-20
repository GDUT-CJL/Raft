#include "storage.h"
int rb_count = 0;

// 初始化NIL节点
static void initNIL() {
    NIL = (RBNode*)kvs_malloc(sizeof(RBNode));
    NIL->color = BLACK;
    NIL->left = NIL->right = NIL->parent = NULL;
}

// 创建新节点
static RBNode* createNode(char* key,char* value) {
    if(key == NULL || value == NULL) return NULL;

    RBNode *node = (RBNode*)kvs_malloc(sizeof(RBNode));
    if(!node) return NULL;
    node->key = (char*)kvs_malloc(strlen(key)+1);
    if(!node->key) {
        kvs_free(node);
        return NULL;
    }
    node->value = (char*)kvs_malloc(strlen(value)+1);
    if(!node->value){
        kvs_free(node->key);
        kvs_free(node);
        return NULL;
    }

    strncpy(node->key, key, strlen(key)+1);

    strncpy(node->value, value, strlen(value)+1);


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
static int insert(RBNode **root, char* key, char* value) {
    RBNode *z = createNode(key,value);
    
    if(z == NULL) return -1;
    RBNode *y = NIL;
    RBNode *x = *root;
    while (x != NIL) {
        y = x;
        if (strcmp(z->key,x->key) < 0) {
            x = x->left;
        } else {
            x = x->right;
        }
    }
    z->parent = y;
    if (y == NIL) {
        *root = z;
    } else if (strcmp(z->key,y->key) < 0) { 
        y->left = z;
    } else {
        y->right = z;
    }
        
    insertFixup(root, z);

    rb_count++;

    return 0;
}

// 查找节点
static RBNode* search(RBNode *root,const char* key) {
    RBNode *current = root;
    while (current != NIL && strcmp(current->key,key) != 0) {
        if (strcmp(key,current->key) < 0) {
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
static int rb_delete(RBNode **root, char* key) {
    RBNode *z = search(*root, key);
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

int rexist(const char* key){
    if(key == NULL) return -1;
    RBNode* Node = search(root,key);
    if(Node->key == 0) return -1;
    return 0;
}
int rset(char* key,char* value){
    if(key == NULL || value == NULL) return -1;
    if(insert(&root,key,value) == -1) return -1;

    return 0;
}
char* rget(const char* key){
    if(key == NULL) return NULL;
    RBNode* get = search(root,key);
    if(get == NULL) return NULL;
    return get->value;
}
int rdelete(char* key){
    if(key == NULL) return -1;
    int ret = rb_delete(&root,key);
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

void dest_rbtree(){
    if(root != NULL){
        kvs_free(root);
    }
}