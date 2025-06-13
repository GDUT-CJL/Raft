// gcc -fPIC -c *.c
// gcc -shared *.o -o libstorage.so
#include "storage.h"
#define JL_MEMPOOL_SIZE		1 << 12
int initPool(){
	p = jl_create_mempool(JL_MEMPOOL_SIZE);
	if(p == NULL){
		return -1;
	}
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