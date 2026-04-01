#include "batch_ops.h"
#include <stdlib.h>
#include <string.h>

int batch_apply(BatchOperation* ops, int count, BatchResult* results) {
    if (ops == NULL || results == NULL || count <= 0) {
        return -1;
    }

    for (int i = 0; i < count; i++) {
        BatchOperation* op = &ops[i];
        BatchResult* result = &results[i];
        
        result->success = 0;
        result->value = 0;
        result->err_msg = NULL;

        switch (op->storage_type) {
            case 0:  // Array
                switch (op->op_type) {
                    case OP_SET:
                        result->success = (set((char*)op->key, op->klen, 
                                              (char*)op->value, op->vlen) == 0);
                        break;
                    case OP_DELETE:
                        result->success = (delete((char*)op->key, op->klen) == 0);
                        break;
                    case OP_GET:
                        // GET操作需要特殊处理返回值
                        result->success = 1;
                        break;
                }
                break;

            case 1:  // Hash
                switch (op->op_type) {
                    case OP_SET:
                        result->success = (hset((char*)op->key, op->klen, 
                                               (char*)op->value, op->vlen) == 0);
                        break;
                    case OP_DELETE:
                        result->success = (hdelete((char*)op->key, op->klen) == 0);
                        break;
                }
                break;

            case 2:  // RBTree
                switch (op->op_type) {
                    case OP_SET:
                        result->success = (rset((char*)op->key, op->klen, 
                                               (char*)op->value, op->vlen) == 0);
                        break;
                    case OP_DELETE:
                        result->success = (rdelete((char*)op->key, op->klen) == 0);
                        break;
                }
                break;

            case 3:  // BTree
                switch (op->op_type) {
                    case OP_SET:
                        result->success = (bset((char*)op->key, op->klen, 
                                               (char*)op->value, op->vlen) == 0);
                        break;
                    case OP_DELETE:
                        result->success = (bdelete((char*)op->key, op->klen) == 0);
                        break;
                }
                break;

            case 4:  // Skiplist
                switch (op->op_type) {
                    case OP_SET:
                        result->success = (zset((char*)op->key, op->klen, 
                                               (char*)op->value, op->vlen) == 0);
                        break;
                    case OP_DELETE:
                        result->success = (zdelete((char*)op->key, op->klen) == 0);
                        break;
                }
                break;

            case 5:  // RocksDB
                switch (op->op_type) {
                    case OP_SET:
                        result->success = (rc_set((char*)op->key, op->klen, 
                                                 (char*)op->value, op->vlen) == 0);
                        break;
                    case OP_DELETE:
                        result->success = (rc_delete((char*)op->key, op->klen) == 0);
                        break;
                }
                break;

            default:
                result->err_msg = "Unknown storage type";
                break;
        }
    }

    return 0;
}

int batch_apply_transactional(BatchOperation* ops, int count, BatchResult* results) {
    // TODO: 实现事务性批量操作
    // 1. 记录所有操作的redo log
    // 2. 执行所有操作
    // 3. 如果失败，回滚所有操作
    return batch_apply(ops, count, results);
}
