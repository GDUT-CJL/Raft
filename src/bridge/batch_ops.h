#ifndef BATCH_OPS_H
#define BATCH_OPS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "storage.h"

typedef enum {
    OP_SET = 0,
    OP_DELETE = 1,
    OP_GET = 2,
} BatchOpType;

typedef struct {
    BatchOpType op_type;
    int storage_type;  // 0=array, 1=hash, 2=rbtree, 3=btree, 4=skiplist, 5=rocksdb
    const char* key;
    size_t klen;
    const char* value;
    size_t vlen;
} BatchOperation;

typedef struct {
    int success;
    int value;
    char* err_msg;
} BatchResult;

int batch_apply(BatchOperation* ops, int count, BatchResult* results);

int batch_apply_transactional(BatchOperation* ops, int count, BatchResult* results);

#ifdef __cplusplus
}
#endif

#endif
