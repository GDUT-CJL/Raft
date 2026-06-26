// gcc -fPIC -c *.c && gcc -shared *.o -o libstorage.so && rm *.o
#include "storage.h"

#define JL_MEMPOOL_SIZE (1 << 12)

static int active_storage_engine = STORAGE_ENGINE_ROCKSDB;

int initPool() {
    p = jl_create_mempool(JL_MEMPOOL_SIZE);
    if (p == NULL) {
        return -1;
    }
    return 0;
}

void* kvs_malloc(size_t size) {
    return jl_alloc(p, size);
}

void kvs_free(void* ptr) {
    if (ptr != NULL) {
        jl_free(p, ptr);
    }
}

int storage_set_active_engine(int engine_type) {
    if (engine_type != STORAGE_ENGINE_LEGACY && engine_type != STORAGE_ENGINE_ROCKSDB) {
        return -1;
    }
    active_storage_engine = engine_type;
    return 0;
}

int storage_get_active_engine(void) {
    return active_storage_engine;
}

static int create_legacy_snapshot_payload(char** payload_data, size_t* payload_size) {
    char* array_data = NULL;
    char* hash_data = NULL;
    char* rbtree_data = NULL;
    char* btree_data = NULL;
    char* skiplist_data = NULL;
    size_t array_size = 0;
    size_t hash_size = 0;
    size_t rbtree_size = 0;
    size_t btree_size = 0;
    size_t skiplist_size = 0;
    int ret = 0;

    if (!payload_data || !payload_size) {
        return -1;
    }

    ret |= array_snapshot(&array_data, &array_size);
    ret |= hash_snapshot(&hash_data, &hash_size);
    ret |= rbtree_snapshot(&rbtree_data, &rbtree_size);
    ret |= btree_snapshot(&btree_data, &btree_size);
    ret |= skiplist_snapshot(&skiplist_data, &skiplist_size);
    if (ret != 0) {
        goto error;
    }

    size_t total_size = (5 * sizeof(size_t)) +
                        array_size + hash_size + rbtree_size + btree_size + skiplist_size;
    char* snapshot = (char*)kvs_malloc(total_size);
    char* ptr = snapshot;
    if (!snapshot) {
        goto error;
    }

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

    if (array_size > 0 && array_data) {
        memcpy(ptr, array_data, array_size);
        ptr += array_size;
    }
    if (hash_size > 0 && hash_data) {
        memcpy(ptr, hash_data, hash_size);
        ptr += hash_size;
    }
    if (rbtree_size > 0 && rbtree_data) {
        memcpy(ptr, rbtree_data, rbtree_size);
        ptr += rbtree_size;
    }
    if (btree_size > 0 && btree_data) {
        memcpy(ptr, btree_data, btree_size);
        ptr += btree_size;
    }
    if (skiplist_size > 0 && skiplist_data) {
        memcpy(ptr, skiplist_data, skiplist_size);
    }

    if (array_data) kvs_free(array_data);
    if (hash_data) kvs_free(hash_data);
    if (rbtree_data) kvs_free(rbtree_data);
    if (btree_data) kvs_free(btree_data);
    if (skiplist_data) kvs_free(skiplist_data);

    *payload_data = snapshot;
    *payload_size = total_size;
    return 0;

error:
    if (array_data) kvs_free(array_data);
    if (hash_data) kvs_free(hash_data);
    if (rbtree_data) kvs_free(rbtree_data);
    if (btree_data) kvs_free(btree_data);
    if (skiplist_data) kvs_free(skiplist_data);
    return -1;
}

static int restore_legacy_snapshot_payload(const char* snapshot_data, size_t snapshot_size) {
    const char* ptr = snapshot_data;
    const char* end = snapshot_data + snapshot_size;
    size_t array_size = 0;
    size_t hash_size = 0;
    size_t rbtree_size = 0;
    size_t btree_size = 0;
    size_t skiplist_size = 0;
    int ret = 0;

    if (!snapshot_data) {
        return -1;
    }
    if (ptr + 5 * sizeof(size_t) > end) {
        return -1;
    }

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

    if (ptr + array_size + hash_size + rbtree_size + btree_size + skiplist_size > end) {
        return -1;
    }

    dest_array();
    dest_hashtable();
    dest_rbtree();
    dest_btree();
    dest_skiplist();

    init_array();
    init_hashtable();
    init_rbtree();
    init_btree(5);
    init_skipTable();

    if (array_size > 0) {
        ret |= array_restore(ptr, array_size);
        ptr += array_size;
    }
    if (hash_size > 0) {
        ret |= hash_restore(ptr, hash_size);
        ptr += hash_size;
    }
    if (rbtree_size > 0) {
        ret |= rbtree_restore(ptr, rbtree_size);
        ptr += rbtree_size;
    }
    if (btree_size > 0) {
        ret |= btree_restore(ptr, btree_size);
        ptr += btree_size;
    }
    if (skiplist_size > 0) {
        ret |= skiplist_restore(ptr, skiplist_size);
    }

    return ret;
}

int storage_create_snapshot(char** snapshot_data, size_t* snapshot_size) {
    char* payload = NULL;
    size_t payload_size = 0;
    size_t total_size = 0;
    char* snapshot = NULL;
    storage_snapshot_t header;
    int ret = -1;

    if (!snapshot_data || !snapshot_size) {
        storage_log("snapshot_data or snapshot_size = null", LOG_DEBUG);
        return -1;
    }

    if (active_storage_engine == STORAGE_ENGINE_ROCKSDB) {
        ret = rocksdb_snapshot(&payload, &payload_size);
    } else {
        ret = create_legacy_snapshot_payload(&payload, &payload_size);
    }
    if (ret != 0 || payload == NULL) {
        return -1;
    }

    total_size = sizeof(storage_snapshot_t) + payload_size;
    snapshot = (char*)kvs_malloc(total_size);
    if (!snapshot) {
        kvs_free(payload);
        return -1;
    }

    header.version = 2;
    header.engine_type = (uint32_t)active_storage_engine;
    header.timestamp = (uint64_t)time(NULL);

    memcpy(snapshot, &header, sizeof(header));
    memcpy(snapshot + sizeof(header), payload, payload_size);

    kvs_free(payload);
    *snapshot_data = snapshot;
    *snapshot_size = total_size;
    return 0;
}

int storage_restore_snapshot(const char* snapshot_data, size_t snapshot_size) {
    uint32_t version = 0;

    if (!snapshot_data || snapshot_size < sizeof(uint32_t) + sizeof(uint64_t)) {
        return -1;
    }

    memcpy(&version, snapshot_data, sizeof(version));
    if (version == 1) {
        const char* legacy_payload = snapshot_data + sizeof(uint32_t) + sizeof(uint64_t);
        size_t legacy_payload_size = snapshot_size - (sizeof(uint32_t) + sizeof(uint64_t));
        storage_set_active_engine(STORAGE_ENGINE_LEGACY);
        return restore_legacy_snapshot_payload(legacy_payload, legacy_payload_size);
    }

    if (snapshot_size < sizeof(storage_snapshot_t)) {
        return -1;
    }

    storage_snapshot_t header;
    memcpy(&header, snapshot_data, sizeof(header));
    if (header.version != 2) {
        return -1;
    }

    const char* payload = snapshot_data + sizeof(storage_snapshot_t);
    size_t payload_size = snapshot_size - sizeof(storage_snapshot_t);
    storage_set_active_engine((int)header.engine_type);

    if (header.engine_type == STORAGE_ENGINE_ROCKSDB) {
        return rocksdb_restore(payload, payload_size);
    }
    if (header.engine_type == STORAGE_ENGINE_LEGACY) {
        return restore_legacy_snapshot_payload(payload, payload_size);
    }
    return -1;
}

void storage_free_snapshot(char* snapshot_data) {
    if (snapshot_data) {
        kvs_free(snapshot_data);
    }
}
