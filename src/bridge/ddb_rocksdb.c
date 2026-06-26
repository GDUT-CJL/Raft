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
    //rocksdb_writeoptions_disable_WAL(writeoptions, 1);//禁用WAL预习机制，为了测性能，不推荐禁用
    
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
    rocksdb_obj = NULL;
}

// 设置键值对
int rc_set(char* key,size_t klen, char* value,size_t vlen) {
    if (!rocksdb_obj || !key || !value) {
        fprintf(stderr, "Invalid parameters for kvs_rocksdb_set\n");
        return -1;
    }
    
    char* err = NULL;
    rocksdb_put(rocksdb_obj->db, rocksdb_obj->writeoptions, key, klen, value, vlen, &err);
    if (err) {
        fprintf(stderr, "Error in kvs_rocksdb_set: %s\n", err);
        rocksdb_free((void*)err);
        return -1;
    }
    
    return 0;
}

// 获取键值对
uint8_t* rc_get(const char* key, size_t klen, size_t* vallen) {
    if (!rocksdb_obj || !key || !vallen) return NULL;
    char* err = NULL;
    char* value = rocksdb_get(rocksdb_obj->db, rocksdb_obj->readoptions,
                              key, klen, vallen, &err);
    if (err) {
        fprintf(stderr, "Error: %s\n", err);
        rocksdb_free(err);
        return NULL;
    }
    return (uint8_t*)value;  // 转换为无符号字节指针
}

// 删除键值对
int rc_delete(const char* key,size_t klen) {
    if (!rocksdb_obj || !key) {
        fprintf(stderr, "Invalid parameters for kvs_rocksdb_delete\n");
        return -1;
    }
    
    char* err = NULL;
    rocksdb_delete(rocksdb_obj->db, rocksdb_obj->writeoptions, key, klen, &err);
    
    if (err) {
        fprintf(stderr, "Error in kvs_rocksdb_delete: %s\n", err);
        rocksdb_free((void*)err);
        return -1;
    }
    
    return 0;
}

// 判断是否存在key
int rc_exist(const char* key, size_t klen) {
    if (!rocksdb_obj || !key) {
        fprintf(stderr, "Invalid parameters for rc_exist_may\n");
        return -1;  // 参数错误
    }

    // 准备用于 rocksdb_key_may_exist 的输出参数
    char* value = NULL;
    size_t vallen = 0;
    unsigned char value_found = 0;
    char* err = NULL;

    // 快速存在性检查
    unsigned char may = rocksdb_key_may_exist(
        rocksdb_obj->db,
        rocksdb_obj->readoptions,
        key, klen,
        &value, &vallen,
        NULL,0,
        &value_found
    );

    if (may == 0) {
        // 一定不存在
        return 1;   // 返回 1 表示不存在（可根据需要调整）
    } else if (may == 1 && value_found) {
        // 一定存在，且 value 已被填充，需要释放
        rocksdb_free(value);
        return 0;   // 存在
    } else {
        // may == 2 或 may == 1 但 value_found == 0，需要进一步确认
        // 这里重新用 rocksdb_get 确认
        if (value) {
            // 如果 may==1 但 value_found==0，value 可能仍被分配，应释放
            rocksdb_free(value);
            value = NULL;
        }

        char* real_value = rocksdb_get(
            rocksdb_obj->db,
            rocksdb_obj->readoptions,
            key, klen,
            &vallen, &err
        );

        if (err) {
            fprintf(stderr, "RocksDB error in rocksdb_get: %s\n", err);
            rocksdb_free(err);
            return -1;  // 错误
        }

        if (real_value == NULL) {
            return 1;   // 不存在
        } else {
            rocksdb_free(real_value);
            return 0;   // 存在
        }
    }
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

typedef struct snapshot_buf_s {
    char* data;
    size_t size;
    size_t capacity;
} snapshot_buf_t;

static int append_snapshot_bytes(snapshot_buf_t* buf, const void* src, size_t len) {
    if (!buf || (!src && len > 0)) {
        return -1;
    }
    if (len == 0) {
        return 0;
    }
    if (buf->size + len > buf->capacity) {
        size_t new_capacity = buf->capacity == 0 ? 4096 : buf->capacity;
        while (new_capacity < buf->size + len) {
            new_capacity *= 2;
        }
        char* new_data = (char*)malloc(new_capacity);
        if (!new_data) {
            return -1;
        }
        if (buf->data && buf->size > 0) {
            memcpy(new_data, buf->data, buf->size);
            free(buf->data);
        }
        buf->data = new_data;
        buf->capacity = new_capacity;
    }
    memcpy(buf->data + buf->size, src, len);
    buf->size += len;
    return 0;
}

static int append_u32(snapshot_buf_t* buf, uint32_t value) {
    return append_snapshot_bytes(buf, &value, sizeof(value));
}

static int append_u64(snapshot_buf_t* buf, uint64_t value) {
    return append_snapshot_bytes(buf, &value, sizeof(value));
}

static int join_path(char* dest, size_t dest_size, const char* left, const char* right) {
    int n = 0;
    if (!dest || dest_size == 0 || !left || !right) {
        return -1;
    }
    n = snprintf(dest, dest_size, "%s/%s", left, right);
    if (n < 0 || (size_t)n >= dest_size) {
        return -1;
    }
    return 0;
}

static int read_u32(const char** ptr, const char* end, uint32_t* value) {
    if (!ptr || !*ptr || !value || *ptr + sizeof(uint32_t) > end) {
        return -1;
    }
    memcpy(value, *ptr, sizeof(uint32_t));
    *ptr += sizeof(uint32_t);
    return 0;
}

static int read_u64(const char** ptr, const char* end, uint64_t* value) {
    if (!ptr || !*ptr || !value || *ptr + sizeof(uint64_t) > end) {
        return -1;
    }
    memcpy(value, *ptr, sizeof(uint64_t));
    *ptr += sizeof(uint64_t);
    return 0;
}

static int path_exists(const char* path) {
    struct stat st;
    return (path && stat(path, &st) == 0);
}

static int ensure_dir_exists(const char* path) {
    if (!path || path[0] == '\0') {
        return -1;
    }
    if (path_exists(path)) {
        return 0;
    }
    if (mkdir(path, 0755) == 0) {
        return 0;
    }
    if (errno == EEXIST) {
        return 0;
    }
    return -1;
}

static int remove_dir_recursive(const char* path) {
    DIR* dir = NULL;
    struct dirent* entry = NULL;
    char child[4096];
    struct stat st;

    if (!path || !path_exists(path)) {
        return 0;
    }
    if (stat(path, &st) != 0) {
        return -1;
    }
    if (!S_ISDIR(st.st_mode)) {
        return remove(path);
    }

    dir = opendir(path);
    if (!dir) {
        return -1;
    }

    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        snprintf(child, sizeof(child), "%s/%s", path, entry->d_name);
        if (stat(child, &st) != 0) {
            closedir(dir);
            return -1;
        }
        if (S_ISDIR(st.st_mode)) {
            if (remove_dir_recursive(child) != 0) {
                closedir(dir);
                return -1;
            }
        } else if (remove(child) != 0) {
            closedir(dir);
            return -1;
        }
    }

    closedir(dir);
    return rmdir(path);
}

static int pack_backup_dir_recursive(const char* root, const char* relative, snapshot_buf_t* buf, uint64_t* file_count) {
    char full_path[4096];
    DIR* dir = NULL;
    struct dirent* entry = NULL;
    struct stat st;
    char child_relative[4096];

    if (relative[0] == '\0') {
        snprintf(full_path, sizeof(full_path), "%s", root);
    } else {
        snprintf(full_path, sizeof(full_path), "%s/%s", root, relative);
    }

    dir = opendir(full_path);
    if (!dir) {
        return -1;
    }

    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        if (relative[0] == '\0') {
            snprintf(child_relative, sizeof(child_relative), "%s", entry->d_name);
        } else {
            snprintf(child_relative, sizeof(child_relative), "%s/%s", relative, entry->d_name);
        }
        if (join_path(full_path, sizeof(full_path), root, child_relative) != 0) {
            closedir(dir);
            return -1;
        }

        if (stat(full_path, &st) != 0) {
            closedir(dir);
            return -1;
        }

        if (S_ISDIR(st.st_mode)) {
            if (pack_backup_dir_recursive(root, child_relative, buf, file_count) != 0) {
                closedir(dir);
                return -1;
            }
            continue;
        }

        FILE* fp = fopen(full_path, "rb");
        char io_buf[8192];
        size_t read_bytes = 0;
        uint32_t path_len = (uint32_t)strlen(child_relative);
        uint64_t file_size = (uint64_t)st.st_size;

        if (!fp) {
            closedir(dir);
            return -1;
        }

        if (append_u32(buf, path_len) != 0 ||
            append_snapshot_bytes(buf, child_relative, path_len) != 0 ||
            append_u64(buf, file_size) != 0) {
            fclose(fp);
            closedir(dir);
            return -1;
        }

        while ((read_bytes = fread(io_buf, 1, sizeof(io_buf), fp)) > 0) {
            if (append_snapshot_bytes(buf, io_buf, read_bytes) != 0) {
                fclose(fp);
                closedir(dir);
                return -1;
            }
        }
        fclose(fp);
        (*file_count)++;
    }

    closedir(dir);
    return 0;
}

static int mkdirs_for_parent(const char* file_path) {
    char tmp[4096];
    size_t len = 0;
    size_t i = 0;

    if (!file_path) {
        return -1;
    }
    snprintf(tmp, sizeof(tmp), "%s", file_path);
    len = strlen(tmp);

    for (i = 1; i < len; i++) {
        if (tmp[i] == '/' || tmp[i] == '\\') {
            char old = tmp[i];
            tmp[i] = '\0';
            if (strlen(tmp) > 0 && ensure_dir_exists(tmp) != 0) {
                return -1;
            }
            tmp[i] = old;
        }
    }
    return 0;
}

static int unpack_backup_to_dir(const char* data, size_t size, const char* target_dir) {
    const char* ptr = data;
    const char* end = data + size;
    uint64_t file_count = 0;
    uint64_t i = 0;

    if (read_u64(&ptr, end, &file_count) != 0) {
        return -1;
    }

    if (remove_dir_recursive(target_dir) != 0) {
        return -1;
    }
    if (ensure_dir_exists(target_dir) != 0) {
        return -1;
    }

    for (i = 0; i < file_count; i++) {
        uint32_t rel_len = 0;
        uint64_t file_size = 0;
        char rel_path[4096];
        char full_path[4096];
        FILE* fp = NULL;

        if (read_u32(&ptr, end, &rel_len) != 0 || rel_len == 0 || rel_len >= sizeof(rel_path)) {
            return -1;
        }
        if (ptr + rel_len > end) {
            return -1;
        }
        memcpy(rel_path, ptr, rel_len);
        rel_path[rel_len] = '\0';
        ptr += rel_len;

        if (read_u64(&ptr, end, &file_size) != 0 || ptr + file_size > end) {
            return -1;
        }

        if (join_path(full_path, sizeof(full_path), target_dir, rel_path) != 0) {
            return -1;
        }
        if (mkdirs_for_parent(full_path) != 0) {
            return -1;
        }

        fp = fopen(full_path, "wb");
        if (!fp) {
            return -1;
        }
        if (file_size > 0 && fwrite(ptr, 1, (size_t)file_size, fp) != (size_t)file_size) {
            fclose(fp);
            return -1;
        }
        fclose(fp);
        ptr += file_size;
    }

    return ptr == end ? 0 : -1;
}

static int reopen_backup_engine(void) {
    char* err = NULL;

    if (!rocksdb_obj || !rocksdb_obj->options) {
        return -1;
    }
    if (rocksdb_obj->be) {
        rocksdb_backup_engine_close(rocksdb_obj->be);
        rocksdb_obj->be = NULL;
    }

    rocksdb_obj->be = rocksdb_backup_engine_open(rocksdb_obj->options, PATH_TO_ROCKSDB_BACKUP, &err);
    if (err != NULL) {
        fprintf(stderr, "Backup engine reopen error: %s\n", err);
        rocksdb_free((void*)err);
        rocksdb_obj->be = NULL;
        return -1;
    }
    return 0;
}

static int reopen_db_only(void) {
    char* err = NULL;
    rocksdb_t* db = NULL;

    if (!rocksdb_obj || !rocksdb_obj->options) {
        return -1;
    }
    if (rocksdb_obj->db) {
        rocksdb_close(rocksdb_obj->db);
        rocksdb_obj->db = NULL;
    }

    db = rocksdb_open(rocksdb_obj->options, PATH_TO_ROCKSDB, &err);
    if (err != NULL) {
        fprintf(stderr, "RocksDB reopen error: %s\n", err);
        rocksdb_free((void*)err);
        return -1;
    }
    rocksdb_obj->db = db;
    return 0;
}

static int recreate_backup_dir(void) {
    if (remove_dir_recursive(PATH_TO_ROCKSDB_BACKUP) != 0) {
        return -1;
    }
    return ensure_dir_exists(PATH_TO_ROCKSDB_BACKUP);
}

int rocksdb_snapshot(char** data, size_t* size) {
    snapshot_buf_t buf;
    uint64_t header_offset = 0;
    uint64_t file_count = 0;

    memset(&buf, 0, sizeof(buf));
    if (!rocksdb_obj || !rocksdb_obj->db || !rocksdb_obj->be || !data || !size) {
        return -1;
    }

    if (recreate_backup_dir() != 0) {
        return -1;
    }
    if (reopen_backup_engine() != 0) {
        return -1;
    }
    if (kvs_rocksdb_create_backup() != 0) {
        return -1;
    }

    header_offset = buf.size;
    if (append_u64(&buf, 0) != 0) {
        free(buf.data);
        return -1;
    }
    if (pack_backup_dir_recursive(PATH_TO_ROCKSDB_BACKUP, "", &buf, &file_count) != 0) {
        free(buf.data);
        return -1;
    }
    memcpy(buf.data + header_offset, &file_count, sizeof(file_count));

    *data = (char*)kvs_malloc(buf.size);
    if (!*data) {
        free(buf.data);
        return -1;
    }
    memcpy(*data, buf.data, buf.size);
    *size = buf.size;
    free(buf.data);
    return 0;
}

int rocksdb_restore(const char* data, size_t size) {
    char* err = NULL;

    if (!rocksdb_obj || !rocksdb_obj->options || !data || size < sizeof(uint64_t)) {
        return -1;
    }

    if (!rocksdb_obj->restore_options) {
        rocksdb_obj->restore_options = rocksdb_restore_options_create();
        if (!rocksdb_obj->restore_options) {
            return -1;
        }
        rocksdb_restore_options_set_keep_log_files(rocksdb_obj->restore_options, 0);
    }

    if (rocksdb_obj->db) {
        rocksdb_close(rocksdb_obj->db);
        rocksdb_obj->db = NULL;
    }
    if (rocksdb_obj->be) {
        rocksdb_backup_engine_close(rocksdb_obj->be);
        rocksdb_obj->be = NULL;
    }

    if (unpack_backup_to_dir(data, size, PATH_TO_ROCKSDB_BACKUP) != 0) {
        return -1;
    }
    if (ensure_dir_exists(PATH_TO_ROCKSDB) != 0) {
        return -1;
    }

    rocksdb_backup_engine_t* restore_be = NULL;

    restore_be = rocksdb_backup_engine_open(rocksdb_obj->options, PATH_TO_ROCKSDB_BACKUP, &err);
    if (err != NULL) {
        fprintf(stderr, "Error opening backup engine for restore: %s\n", err);
        rocksdb_free((void*)err);
        return -1;
    }

    rocksdb_backup_engine_restore_db_from_latest_backup(
        restore_be,
        PATH_TO_ROCKSDB,
        PATH_TO_ROCKSDB,
        rocksdb_obj->restore_options,
        &err
    );
    rocksdb_backup_engine_close(restore_be);
    if (err != NULL) {
        fprintf(stderr, "Error restoring rocksdb backup snapshot: %s\n", err);
        rocksdb_free((void*)err);
        return -1;
    }

    if (reopen_db_only() != 0) {
        return -1;
    }
    if (reopen_backup_engine() != 0) {
        return -1;
    }

    return 0;
}
