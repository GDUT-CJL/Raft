#include "storage.h"

// ---------------------------------------Array----------------------------------------- //
static void _clean_expired_task(){
#if 1
	struct timeval tv;
	gettimeofday(&tv,NULL);
	long long cur_time = (tv.tv_sec * 1000LL) + (tv.tv_usec / 1000);
	for(int i = 0; i < MAX_ARRAY_NUMS; ++i){
		kvs_array_item_t* enter = &array_table->array[i];
		if(enter->key != NULL && enter->value != NULL){
			if(enter->expired != 0 && cur_time > enter->expired){
				kvs_free(enter->key);
				kvs_free(enter->value);
				enter->key = NULL;
				enter->expired = 0;
				enter->value = NULL;
				array_table->array_count--;
			}
		}
	}
#endif
}

int init_array(){
	array_table = (kvs_array_t*)kvs_malloc(sizeof(kvs_array_t));
    if(!array_table)   return -1;

    if (pthread_mutex_init(&array_table->array_mutex, NULL) != 0) {  
        kvs_free(array_table);  
        return -1; // 处理mutex初始化错误  
    }  

	array_table->array = (kvs_array_item_t*)calloc(MAX_ARRAY_NUMS, sizeof(kvs_array_item_t));  
	if(!array_table->array){
		pthread_mutex_destroy(&array_table->array_mutex);
		kvs_free(array_table); // 释放之前分配的内存  
		return -1;
	}
	array_table->array_count = 0;
    return 0;
}

void dest_array(){
	if (!array_table){
		return;
	}
	for(int i = 0; i < array_table->array_count;i++){
		if(array_table->array){
			kvs_free(array_table->array[i].key);
			kvs_free(array_table->array[i].value);
		}
	}

	pthread_mutex_destroy(&array_table->array_mutex);
	kvs_free(array_table->array); // 释放数组内存  
    pthread_mutex_destroy(&array_table->array_mutex);  
    kvs_free(array_table); // 释放结构体内存  
    array_table = NULL; // 避免悬挂指针 
}

kvs_array_item_t* array_search_item(const char* key){
	if(!key) return NULL;
	_clean_expired_task();
	for(int idx = 0; idx < MAX_ARRAY_NUMS;idx++){
		if (array_table->array[idx].key == NULL) {
			continue;
		}
		if((strcmp(array_table->array[idx].key,key) == 0)){
			return &array_table->array[idx];
		}			
	}
	return NULL;
}

int kvs_array_insert_ttl(char* key, char* value, long long expired_time) {
    // 1. 检查输入有效性
    if (key == NULL || value == NULL) {
        fprintf(stderr, "Error: NULL key or value\n");
        return -1;
    }

    // 2. 复制key和value（避免外部修改影响数据）
    char* kcopy = (char*)kvs_malloc(strlen(key) + 1);
    if (!kcopy) {
        fprintf(stderr, "Error: Failed to allocate key\n");
        return -1;
    }
    char* vcopy = (char*)kvs_malloc(strlen(value) + 1);
    if (!vcopy) {
        kvs_free(kcopy);
        fprintf(stderr, "Error: Failed to allocate value\n");
        return -1;
    }
    strncpy(kcopy, key, strlen(key) + 1);
    strncpy(vcopy, value, strlen(value) + 1);

    // 3. 加锁并插入数据
    //pthread_mutex_lock(&array_table->array_mutex);

    // 3.1 检查数组容量
    if (array_table->array_count >= MAX_ARRAY_NUMS) {
        //pthread_mutex_unlock(&array_table->array_mutex);
        kvs_free(kcopy);
        kvs_free(vcopy);
        return -1;
    }

    // 3.2 执行插入
    int insert_index = array_table->array_count;
    array_table->array[insert_index].key = kcopy;
    array_table->array[insert_index].value = vcopy;
    array_table->array[insert_index].expired = expired_time;
    array_table->array_count++;
    //pthread_mutex_unlock(&array_table->array_mutex);
    return 0;
}

// int kvs_array_insert_ttl(char* key, char* value, long long expired_time) {
//     if (key == NULL || value == NULL || array_table->array_count >= MAX_ARRAY_NUMS)
//         return -1;

//     // 先进行非共享资源操作（内存申请和复制）
//     char* kcopy = (char*)kvs_malloc(strlen(key) + 1);
//     if (!kcopy) {
//         return -1;
//     }
//     char* vcopy = (char*)kvs_malloc(strlen(value) + 1);
//     if (!vcopy) {
//         kvs_free(kcopy);
//         return -1;
//     }
//     // 复制字符串
//     strncpy(kcopy, key, strlen(key) + 1);
//     strncpy(vcopy, value, strlen(value) + 1);

//     // 先清理过期任务（不涉及共享结构，不加锁）
//     //_clean_expired_task();

//     int insert_index = -1;
//     // 只在修改共享数据结构时加锁
//     pthread_mutex_lock(&array_table->array_mutex);
//     // 找空位
//     for (int i = 0; i < MAX_ARRAY_NUMS; ++i) {
//         if (array_table->array[i].key == NULL && array_table->array[i].value == NULL) {
//             insert_index = i;
//             break;
//         }
//     }

//     // 检查空间是否足够
//     if (insert_index == -1) {
//         pthread_mutex_unlock(&array_table->array_mutex);
//         kvs_free(kcopy);
//         kvs_free(vcopy);
//         return -1; // 数组已满
//     }

//     // 插入数据
//     array_table->array[insert_index].key = kcopy;
//     array_table->array[insert_index].value = vcopy;
//     array_table->array[insert_index].expired = expired_time;
//     array_table->array_count++;
//     pthread_mutex_unlock(&array_table->array_mutex);

//     return 0;
// }

// array set 
int set(char* key,char* value){
	if(array_search_item(key) != NULL)	return -1;
	return kvs_array_insert_ttl(key,value,0);
}

// array get 
char* get(const char* key){
	kvs_array_item_t* get = array_search_item(key);
	if(get){
		return get->value;
	}
	return NULL;
}

// array delete
int delete(const char* key){  
    if (!key) return -1; // 检查 key 是否为空  
	
    _clean_expired_task();  
    for (int i = 0; i < MAX_ARRAY_NUMS; i++) {  
        // 检查 array_table[i].key 是否为 NULL，只有在不为 NULL 的情况下才进行比较  
        if (array_table->array[i].key != NULL && strcmp(array_table->array[i].key, key) == 0) {  
            // 释放内存
            kvs_free(array_table->array[i].key);  
            array_table->array[i].key = NULL;  

            if (array_table->array[i].value != NULL) {  
                kvs_free(array_table->array[i].value);  
                array_table->array[i].value = NULL;    
            }  
            array_table->array_count--;  // 减少元素计数  
            return 0;  // 成功删除  
        }  
    }  
    return -1; // 未找到要删除的 key  
}  

// array count
int count(){
    return array_table->array_count;
}

kvs_array_item_t* kvs_array_search_item(const char* key){
	if(!key) return NULL;
	_clean_expired_task();
	for(int idx = 0; idx < MAX_ARRAY_NUMS;idx++){
		if (array_table->array[idx].key == NULL) {
			continue;
		}
		if((strcmp(array_table->array[idx].key,key) == 0)){
			return &array_table->array[idx];
		}			
	}
	return NULL;
}

// array exist 存在：0 不存在-1
int exist(const char* key){
	kvs_array_item_t* get = kvs_array_search_item(key);
	if(get){
		return 0;
	}
	return -1;
}
