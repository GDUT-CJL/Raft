#include "storage.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>
static void _clean_expired_task(){
#if 1
	struct timeval tv;
	gettimeofday(&tv,NULL);
	long long cur_time = (tv.tv_sec * 1000LL) + (tv.tv_usec / 1000);
	for(int i = 0; i < MAX_ARRAY_NUMS; ++i){
		kvs_array_item_t* enter = &array_table->array[i];
		if(enter->key != NULL && enter->value != NULL){
			if(enter->expired != 0 && cur_time > enter->expired){
				free(enter->key);
				free(enter->value);
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
	array_table = (kvs_array_t*)malloc(sizeof(kvs_array_t));
    if(!array_table)   return -1;

	pthread_mutex_init(&array_table->array_mutex,NULL);

	array_table->array = (kvs_array_item_t*)malloc(sizeof(kvs_array_item_t)*MAX_ARRAY_NUMS);
	if(!array_table->array){
		pthread_mutex_destroy(&array_table->array_mutex);
		return -1;
	}

	array_table->array->key = (char*)malloc(sizeof(char));
	if(!array_table->array->key){
		pthread_mutex_destroy(&array_table->array_mutex);
		free(array_table->array);
		return -1;
	}

	array_table->array->value = (char*)malloc(sizeof(char));
	if(!array_table->array->value){
		pthread_mutex_destroy(&array_table->array_mutex);
		free(array_table->array);
		free(array_table->array->key);
		return -1;
	}

    return 0;
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

int kvs_array_insert_ttl(char* key,char* value,long long expired_time){
	if(key == NULL || value == NULL || array_table->array_count == MAX_ARRAY_NUMS - 1) return -1;
	_clean_expired_task();
	pthread_mutex_lock(&array_table->array_mutex);
	char* kcopy = (char*)malloc(strlen(key)+1);
	if(!kcopy){
		pthread_mutex_unlock(&array_table->array_mutex);
		return -1;
	} 

	char* vcopy = (char*)malloc(strlen(value)+1);
	if(!vcopy){
		pthread_mutex_unlock(&array_table->array_mutex);
		free(kcopy);
		return -1;
	}
	strncpy(kcopy,key,strlen(key)+1);
	strncpy(vcopy,value,strlen(value)+1);

	//int* time_copy = (int*)malloc(sizeof(int));
	//*time_copy = expired_time;
#if 0
	// 有问题，不能和delete配合，会发现delete完后count--数据会被覆盖
	array_table[array_count].key = kcopy;
	array_table[array_count].value = vcopy;
	array_count++;
#endif
	int i = 0;
	for(i = 0; i < MAX_ARRAY_NUMS;++i){
		if(array_table->array[i].key == NULL && array_table->array[i].value == NULL)
			break;
		if(strcmp(array_table->array[i].key, kcopy) == 0)
			break;
	}
	array_table->array[i].key = kcopy;
	array_table->array[i].value = vcopy;
	array_table->array[i].expired = expired_time;
	array_table->array_count++;
	pthread_mutex_unlock(&array_table->array_mutex);
	return 0;
}

// array set 
int set(char* key,char* value){
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

// int main(){

// 	int ret = init_array();
//     if(ret == -1){
//         printf("初始化失败\n");
//         return -1;
//     }

//     set("k1","v1");
//     set("k2","v2");
//     set("k3","v3");
//     set("k4","v4");

//     char* v1 = get("k1");
//     if(v1!= NULL){
//         printf("k1:%s\n",v1);
//     }
//     char* v2 = get("k2");
//     if(v2!= NULL){
//         printf("k2:%s\n",v2);
//     }
//     char* v3 = get("k3");
//     if(v3!= NULL){
//         printf("k3:%s\n",v3);
//     }

//     char* v4 = get("k4");
//     if(v4!= NULL){
//         printf("k4:%s\n",v4);
//     }
// }