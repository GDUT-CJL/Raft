// gcc -fPIC -c *.c
// gcc -shared *.o -o libstorage.so
#include "storage.h"
void* kvs_malloc(size_t size){
	return malloc(size);
}

void kvs_free(void* ptr){
	if(ptr != NULL){
		free(ptr);
	}
}