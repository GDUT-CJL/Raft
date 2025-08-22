#include "storage.h"
// 错误处理宏
#define ROCKSDB_CHECK_ERROR(err, cleanup_code) \
    if ((err) != NULL) { \
        fprintf(stderr, "RocksDB Error: %s at %s:%d\n", (err), __FILE__, __LINE__); \
        rocksdb_free((void*)(err)); \
        cleanup_code; \
        return -1; \
    }

// 初始化 RocksDB
int init_rocksdb() {

    rocksdb_obj = (struct rocks_obj*)malloc(sizeof(struct rocks_obj));
    if (!rocksdb_obj) {
        fprintf(stderr, "Memory allocation failed for rocks_obj\n");
        return -1;
    }
    
    // 初始化结构体为零
    memset(rocksdb_obj, 0, sizeof(struct rocks_obj));
    
    char *err = NULL;
    
    // 创建数据库选项
    rocksdb_options_t *options = rocksdb_options_create();
    if (!options) {
        fprintf(stderr, "Failed to create RocksDB options\n");
        free(rocksdb_obj);
        return -1;
    }
    
    // 性能优化配置
    long cpus = sysconf(_SC_NPROCESSORS_ONLN);
    rocksdb_options_increase_parallelism(options, (int)(cpus));
    rocksdb_options_optimize_level_style_compaction(options, 0);
    rocksdb_options_set_create_if_missing(options, 1);
    // 启用布隆过滤器以提高读取性能
    //rocksdb_options_set_bloom_filter(options, 10);
    
    // 设置块大小和缓存大小
    //rocksdb_options_set_block_size(options, 4096);
    //rocksdb_options_set_block_cache(rocksdb_options_create(), 8 * 1024 * 1024); // 8MB缓存
    rocksdb_options_set_write_buffer_size(options, 64 * 1024 * 1024); // 64MB
    rocksdb_options_set_max_write_buffer_number(options, 2);
    // 尝试打开数据库
    rocksdb_t *db = rocksdb_open(options, PATH_TO_ROCKSDB, &err);
    ROCKSDB_CHECK_ERROR(err, {
        rocksdb_options_destroy(options);
        free(rocksdb_obj);
    });
    
    // 打开备份引擎
    rocksdb_backup_engine_t *be = rocksdb_backup_engine_open(options, PATH_TO_ROCKSDB_BACKUP, &err);
    if (err) {
        fprintf(stderr, "Backup engine error: %s\n", err);
        rocksdb_free((void*)err);
        // 备份引擎不是必须的，我们继续但不设置备份引擎
        err = NULL;
    }
    
    // 创建读写选项
    rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    
    // 设置读取选项以提高性能
    rocksdb_readoptions_set_verify_checksums(readoptions, 0); // 生产环境中应设为1
    rocksdb_readoptions_set_fill_cache(readoptions, 1);
    
    // 设置写入选项
    rocksdb_writeoptions_set_sync(writeoptions, 0); // 异步写入以提高性能
    rocksdb_writeoptions_disable_WAL(writeoptions, 1);//禁用WAL预习机制，为了测性能，不推荐禁用
    
    // 填充结构体
    rocksdb_obj->db = db;
    rocksdb_obj->be = be;
    rocksdb_obj->options = options;
    rocksdb_obj->readoptions = readoptions;
    rocksdb_obj->writeoptions = writeoptions;
    
    return 0;
}

// 关闭并清理 RocksDB 资源
void close_Rocksdb() {
    if (!rocksdb_obj) return;
    
    if (rocksdb_obj->db) rocksdb_close(rocksdb_obj->db);
    if (rocksdb_obj->be) rocksdb_backup_engine_close(rocksdb_obj->be);
    if (rocksdb_obj->options) rocksdb_options_destroy(rocksdb_obj->options);
    if (rocksdb_obj->readoptions) rocksdb_readoptions_destroy(rocksdb_obj->readoptions);
    if (rocksdb_obj->writeoptions) rocksdb_writeoptions_destroy(rocksdb_obj->writeoptions);
    if (rocksdb_obj->restore_options) rocksdb_restore_options_destroy(rocksdb_obj->restore_options);
    
    free(rocksdb_obj);
}

// 设置键值对
int rc_set(const char* key, const char* value) {
    if (!rocksdb_obj || !key || !value) {
        fprintf(stderr, "Invalid parameters for kvs_rocksdb_set\n");
        return -1;
    }
    
    char* err = NULL;
    rocksdb_put(rocksdb_obj->db, rocksdb_obj->writeoptions, key, strlen(key), value, strlen(value) + 1, &err);
    
    if (err) {
        fprintf(stderr, "Error in kvs_rocksdb_set: %s\n", err);
        rocksdb_free((void*)err);
        return -1;
    }
    
    return 0;
}

// 获取键值对
char* rc_get(const char* key) {
    if (!rocksdb_obj || !key) {
        fprintf(stderr, "Invalid parameters for kvs_rocksdb_get\n");
        return NULL;
    }
    
    size_t len;
    char* err = NULL;
    char *value = rocksdb_get(rocksdb_obj->db, rocksdb_obj->readoptions, key, strlen(key), &len, &err);
    
    if (err) {
        fprintf(stderr, "Error in kvs_rocksdb_get: %s\n", err);
        rocksdb_free((void*)err);
        return NULL;
    }
    
    return value; // 调用者需要负责释放这个内存
}

// 删除键值对
int rc_delete(const char* key) {
    if (!rocksdb_obj || !key) {
        fprintf(stderr, "Invalid parameters for kvs_rocksdb_delete\n");
        return -1;
    }
    
    char* err = NULL;
    rocksdb_delete(rocksdb_obj->db, rocksdb_obj->writeoptions, key, strlen(key), &err);
    
    if (err) {
        fprintf(stderr, "Error in kvs_rocksdb_delete: %s\n", err);
        rocksdb_free((void*)err);
        return -1;
    }
    
    return 0;
}

// 创建备份
int kvs_rocksdb_create_backup() {
    if (!rocksdb_obj || !rocksdb_obj->be) {
        fprintf(stderr, "Backup engine not available\n");
        return -1;
    }
    
    char* err = NULL;
    rocksdb_backup_engine_create_new_backup(rocksdb_obj->be, rocksdb_obj->db, &err);
    
    if (err) {
        fprintf(stderr, "Error creating backup: %s\n", err);
        rocksdb_free((void*)err);
        return -1;
    }
    
    return 0;
}

// 批量写入接口
int kvs_rocksdb_batch_set(const char** keys, const char** values, int count) {
    if (!rocksdb_obj || !keys || !values || count <= 0) {
        fprintf(stderr, "Invalid parameters for batch_set\n");
        return -1;
    }
    
    rocksdb_writebatch_t* batch = rocksdb_writebatch_create();
    char* err = NULL;
    
    for (int i = 0; i < count; i++) {
        if (keys[i] && values[i]) {
            rocksdb_writebatch_put(batch, keys[i], strlen(keys[i]), values[i], strlen(values[i]) + 1);
        }
    }
    
    rocksdb_write(rocksdb_obj->db, rocksdb_obj->writeoptions, batch, &err);
    rocksdb_writebatch_destroy(batch);
    
    if (err) {
        fprintf(stderr, "Error in batch_set: %s\n", err);
        rocksdb_free((void*)err);
        return -1;
    }
    
    return 0;
}