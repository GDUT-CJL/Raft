#include "storage.h"

int init_array(){
	array_table = (kvs_array_t*)kvs_malloc(sizeof(kvs_array_t));
    if(!array_table)   return -1;

	array_table->array = (kvs_array_item_t*)kvs_malloc(sizeof(kvs_array_item_t)*MAX_ARRAY_NUMS);
	if(array_table->array){
		return -1;
	}

	array_table->array->key = (bstring_t*)kvs_malloc(sizeof(bstring_t));
	if(!array_table->array->key){
		kvs_free(array_table->array);
		return -1;
	}

	array_table->array->value = (bstring_t*)kvs_malloc(sizeof(bstring_t));
	if(!array_table->array->value){
		kvs_free(array_table->array);
		bstring_free(array_table->array->key);
		return -1;
	}
	
	array_table->array_count = 0;

    return 0;
}

kvs_array_item_t* kvs_array_search_item(const bstring_t* key){
	if(!key) return NULL;
	
	for(int idx = 0; idx < MAX_ARRAY_NUMS;idx++){
		if (array_table->array[idx].key == NULL) {
			continue;
		}
		if((bstring_equal(array_table->array[idx].key,key))){
			return &array_table->array[idx];
		}			
	}
	return NULL;
}

// array exist
int exist(const char* key,size_t keylen){
	bstring_t* bkey = bstring_new_from_data(key,keylen);
	kvs_array_item_t* get = kvs_array_search_item(bkey);
	if(get){
		return 0;
	}
	return -1;
}

int kvs_array_insert_ttl(char* key,size_t klen,char* value,size_t vlen,long long expired_time){
	if(key == NULL || value == NULL || array_table->array_count == MAX_ARRAY_NUMS - 1 || klen < 0 || vlen < 0) return -1;
	bstring_t* key1 = bstring_new_from_data(key,klen);
    bstring_t* value1 = bstring_new_from_data(value,vlen);
	int i = 0;
	for(i = 0; i < MAX_ARRAY_NUMS;++i){
		if(array_table->array[i].key == NULL && array_table->array[i].value == NULL)
			break;
		if(bstring_equal(array_table->array[i].key, key1))
			break;
	}
	if(kvs_array_search_item(key1) == NULL){
		array_table->array_count++;	
	}
	array_table->array[i].key = key1;
	array_table->array[i].value = value1;
	array_table->array[i].expired = expired_time;
	return 0;
}

// array set 
int set(char* key,size_t klen,char* value,size_t vlen){
	return kvs_array_insert_ttl(key,klen,value,vlen,0);
}

int kvs_set_array_expired(char* key,char* value,char* cmd,int expired){
	// if(key == NULL && value == NULL && cmd== NULL && expired <= 0)	return -1;
	// long long time;
	// if(strcasecmp(cmd,"px") == 0){
	// 	time = expired;
	// }else if(strcasecmp(cmd,"ex") == 0){
	// 	time = (long long)expired * 1000;
	// }else{
	// 	return -1;
	// }

	// struct timeval cur_time;
	// gettimeofday(&cur_time,NULL);
	// long long alltime = (cur_time.tv_sec * 1000LL) + (cur_time.tv_usec / 1000) + time;
	// return kvs_array_insert_ttl(key,value,alltime);
}

// array get 
// C 函数：同时返回数据指针和长度
uint8_t* get(const char* key, size_t klen, size_t* out_vlen) {
    bstring_t* bkey = bstring_new_from_data(key, klen);
    kvs_array_item_t* item = kvs_array_search_item(bkey);
    bstring_free(bkey); // 别忘了释放临时 key

    if (item && item->value) {
        *out_vlen = item->value->len;  // 👈 关键：返回真实长度
        return item->value->data;
    } else {
        *out_vlen = 0;
        return NULL;
    }
}
// array delete
int delete(const char* key,size_t klen){  
    if (!key) return -1; // 检查 key 是否为空  
	bstring_t* bkey = bstring_new_from_data(key,klen);
    for (int i = 0; i < MAX_ARRAY_NUMS; i++) {  
        // 检查 array_table[i].key 是否为 NULL，只有在不为 NULL 的情况下才进行比较  
        if (array_table->array[i].key != NULL && bstring_compare(array_table->array[i].key, bkey) == 0) {  
            // 释放内存
            bstring_free(array_table->array[i].key);  
            array_table->array[i].key = NULL;  

            if (array_table->array[i].value != NULL) {  
                bstring_free(array_table->array[i].value);  
                array_table->array[i].value = NULL;    
            }  
            array_table->array_count--;  // 减少元素计数  
            return 0;  // 成功删除  
        }  
    }  
    return -1; // 未找到要删除的 key  
}  
int count(){
    return array_table->array_count;
}