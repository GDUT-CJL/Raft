#ifndef STORAGE_H  
#define STORAGE_H  

#ifdef __cplusplus  
extern "C" {  
#endif  

// 定义你的函数和结构体  
#include <pthread.h>  
#define MAX_MSGBUFFER_LENGTH	1024
#define MAX_ARRAY_NUMS	102400
typedef struct kvs_array_item_s{
	char* key;
	char* value;
    long long expired;
}kvs_array_item_t;

typedef struct kvs_array_s{
	kvs_array_item_t* array;
	int array_count;
	pthread_mutex_t array_mutex;
}kvs_array_t;

struct kvs_array_s* array_table;
// array
int init_array();
int set(char* key,char* value);
char* get(const char* key);
int delete(const char* key);
int count();
int exist(const char* key);


#ifdef __cplusplus  
}  
#endif  

#endif // STORAGE_H  