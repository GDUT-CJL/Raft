// gcc -fPIC -c *.c && gcc -shared *.o -o libstorage.so && rm *.o
#include "storage.h"
#define JL_MEMPOOL_SIZE		1 << 12
int initPool(){
	p = jl_create_mempool(JL_MEMPOOL_SIZE);
	if(p == NULL){
		return -1;
	}
    return 0;
}

void* kvs_malloc(size_t size){
	//return malloc(size);
	return jl_alloc(p,size);
}

void kvs_free(void* ptr){
	if(ptr != NULL){
		//free(ptr);
		return jl_free(p,ptr);
	}
}

// 创建整个存储引擎的快照
int storage_create_snapshot(char** snapshot_data, size_t* snapshot_size) {
    if (!snapshot_data || !snapshot_size) {
        storage_log("snapshot_data or snapshot_size = null", LOG_DEBUG);
        return -1;
    }
    
    // 获取各个数据结构的快照
    char* array_data = NULL, *hash_data = NULL, *rbtree_data = NULL, *btree_data = NULL, *skiplist_data = NULL;
    size_t array_size = 0, hash_size = 0, rbtree_size = 0, btree_size = 0, skiplist_size = 0;
    
    int ret = 0;
    // storage_log("Creating array snapshot...", LOG_DEBUG);
    ret |= array_snapshot(&array_data, &array_size);
    // storage_log("array_snapshot result: ret=%d, size=%zu", LOG_DEBUG, ret, array_size);
    
    // storage_log("Creating hash snapshot...", LOG_DEBUG);
    ret |= hash_snapshot(&hash_data, &hash_size);
    // storage_log("hash_snapshot result: ret=%d, size=%zu", LOG_DEBUG, ret, hash_size);
    
    // storage_log("Creating rbtree snapshot...", LOG_DEBUG);
    ret |= rbtree_snapshot(&rbtree_data, &rbtree_size);
    // storage_log("rbtree_snapshot result: ret=%d, size=%zu", LOG_DEBUG, ret, rbtree_size);
    
    
    // storage_log("Creating btree snapshot...", LOG_DEBUG);
    ret |= btree_snapshot(&btree_data, &btree_size);
    // storage_log("btree_snapshot result: ret=%d, size=%zu", LOG_DEBUG, ret, btree_size);
    
    // storage_log("Creating skiplist snapshot...", LOG_DEBUG);
    ret |= skiplist_snapshot(&skiplist_data, &skiplist_size);
    // storage_log("skiplist_snapshot result: ret=%d, size=%zu", LOG_DEBUG, ret, skiplist_size);
    
    if (ret != 0) {
        goto error;
    }
    
    // 计算总大小
    size_t total_size = sizeof(uint32_t) + sizeof(uint64_t) + 
                       (5 * sizeof(size_t)) + 
                       array_size + hash_size + rbtree_size + btree_size + skiplist_size;
    
    // 分配内存
    char* snapshot = (char*)kvs_malloc(total_size);
    if (!snapshot) goto error;
    
    char* ptr = snapshot;
    
    // 写入版本号和时间戳
    uint32_t version = 1;
    uint64_t timestamp = (uint64_t)time(NULL);
    memcpy(ptr, &version, sizeof(version));
    ptr += sizeof(version);
    memcpy(ptr, &timestamp, sizeof(timestamp));
    ptr += sizeof(timestamp);
    
    // 写入各个数据结构的大小
    memcpy(ptr, &array_size, sizeof(array_size));
    ptr += sizeof(array_size);
    memcpy(ptr, &hash_size, sizeof(hash_size));
    ptr += sizeof(hash_size);
    memcpy(ptr, &rbtree_size, sizeof(rbtree_size));
    ptr += sizeof(rbtree_size);
    memcpy(ptr, &btree_size, sizeof(btree_size));
    ptr += sizeof(btree_size);
    memcpy(ptr, &skiplist_size, sizeof(skiplist_size));
    ptr += sizeof(skiplist_size);
    
    // 写入各个数据结构的数据
    if (array_size > 0 && array_data) {
        memcpy(ptr, array_data, array_size);
        ptr += array_size;
        kvs_free(array_data);
        array_data = NULL;
    }
    if (hash_size > 0 && hash_data) {
        memcpy(ptr, hash_data, hash_size);
        ptr += hash_size;
        kvs_free(hash_data);
        hash_data = NULL;
    }
    if (rbtree_size > 0 && rbtree_data) {
        memcpy(ptr, rbtree_data, rbtree_size);
        ptr += rbtree_size;
        kvs_free(rbtree_data);
        rbtree_data = NULL;
    }
    if (btree_size > 0 && btree_data) {
        memcpy(ptr, btree_data, btree_size);
        ptr += btree_size;
        kvs_free(btree_data);
        btree_data = NULL;
    }
    if (skiplist_size > 0 && skiplist_data) {
        memcpy(ptr, skiplist_data, skiplist_size);
        ptr += skiplist_size;
        kvs_free(skiplist_data);
        skiplist_data = NULL;
    }
    
    *snapshot_data = snapshot;
    *snapshot_size = total_size;
    return 0;

error:
    if (array_data) kvs_free(array_data);
    if (hash_data) kvs_free(hash_data);
    if (rbtree_data) kvs_free(rbtree_data);
    if (btree_data) kvs_free(btree_data);
    if (skiplist_data) kvs_free(skiplist_data);
    return -1;
}

// 从快照恢复存储引擎
int storage_restore_snapshot(const char* snapshot_data, size_t snapshot_size) {
    if (!snapshot_data || snapshot_size < sizeof(uint32_t) + sizeof(uint64_t)) {
        return -1;
    }
    
    const char* ptr = snapshot_data;
    const char* end = snapshot_data + snapshot_size;
    
    // 读取版本号和时间戳
    uint32_t version;
    uint64_t timestamp;
    if (ptr + sizeof(version) + sizeof(timestamp) > end) return -1;
    
    memcpy(&version, ptr, sizeof(version));
    ptr += sizeof(version);
    memcpy(&timestamp, ptr, sizeof(timestamp));
    ptr += sizeof(timestamp);
    
    // 验证版本号
    if (version != 1) {
        return -1; // 不支持的版本
    }
    
    // 读取各个数据结构的大小
    size_t array_size, hash_size, rbtree_size, btree_size, skiplist_size;
    if (ptr + 5 * sizeof(size_t) > end) return -1;
    
    memcpy(&array_size, ptr, sizeof(array_size));
    ptr += sizeof(array_size);
    memcpy(&hash_size, ptr, sizeof(hash_size));
    ptr += sizeof(hash_size);
    memcpy(&rbtree_size, ptr, sizeof(rbtree_size));
    ptr += sizeof(rbtree_size);
    memcpy(&btree_size, ptr, sizeof(btree_size));
    ptr += sizeof(btree_size);
    memcpy(&skiplist_size, ptr, sizeof(skiplist_size));
    ptr += sizeof(skiplist_size);
    
    // 验证数据完整性
    if (ptr + array_size + hash_size + rbtree_size + btree_size + skiplist_size > end) {
        return -1;
    }
    
    // 恢复各个数据结构
    int ret = 0;
    
    // 先清空现有数据
    dest_array();
    dest_hashtable();
    dest_rbtree();
    dest_btree();
    dest_skiplist();
    
    // 重新初始化数据结构
    init_array();
    init_hashtable();
    init_rbtree();
    init_btree(5);
    init_skipTable();
    
    // 恢复数据（注意顺序）
    if (array_size > 0) {
        ret |= array_restore(ptr, array_size);
        storage_log("array_restore result: ret=%d, size=%zu", LOG_DEBUG, ret, array_size);
        ptr += array_size;
    }
    if (hash_size > 0) {
        ret |= hash_restore(ptr, hash_size);
        storage_log("hash_restore result: ret=%d, size=%zu", LOG_DEBUG, ret, hash_size);
        ptr += hash_size;
    }
    if (rbtree_size > 0) {
        ret |= rbtree_restore(ptr, rbtree_size);
        storage_log("rbtree_restore result: ret=%d, size=%zu", LOG_DEBUG, ret, rbtree_size);
        ptr += rbtree_size;
    }
    if (btree_size > 0) {
        ret |= btree_restore(ptr, btree_size);
        storage_log("btree_restore result: ret=%d, size=%zu", LOG_DEBUG, ret, btree_size);
        ptr += btree_size;
    }
    if (skiplist_size > 0) {
        ret |= skiplist_restore(ptr, skiplist_size);
        storage_log("skiplist_restore result: ret=%d, size=%zu", LOG_DEBUG, ret, skiplist_size);
        ptr += skiplist_size;
    }
    
    return ret;
}

void storage_free_snapshot(char* snapshot_data) {
    if (snapshot_data) {
        kvs_free(snapshot_data);
    }
}