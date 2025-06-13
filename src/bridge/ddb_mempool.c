#include "storage.h"

// 仿照nginx内存池写的


struct jl_pool_s* 
jl_create_mempool(size_t size)
{
    struct jl_pool_s* p;
    int ret = posix_memalign((void **)&p,JL_MP_ALIGNMENT,size);
    p->d.last = (unsigned char*)p + sizeof(struct jl_pool_s);
    p->d.end = (unsigned char*)p + size;
    p->d.next = NULL;
    p->d.failed = 0;
    p->d.data_size = 0;

    size = size - sizeof(jl_pool_t);
    p->max = (size > JL_MAX_ALLOC_FROM_POOL) ? JL_MAX_ALLOC_FROM_POOL : size;
    p->current = p;
    p->large = NULL;

    return p;

}

void jl_destory_mempool(struct jl_pool_s* pool) {
    struct jl_large_s* l, *next_l;
    struct jl_pool_s* p, *n;

    for (l = pool->large; l; l = next_l) {
        next_l = l->next;
        if (l->alloc) free(l->alloc);
        free(l);
    }

    for (p = pool, n = p->d.next; ; p = n, n = n->d.next) {
        free(p);
        if (!n) break;
    }
}

void jl_reset_pool(struct jl_pool_s *pool)
{
    struct jl_large_s *l;
    struct jl_pool_s *p;
    for(l = pool->large; l ;l = l->next)
    {
        if(l->alloc)
            free(l->alloc);
    }

    for(p = pool;p;p = p->d.next)
    {
        p->d.last = (unsigned char*)p + sizeof(struct jl_pool_s);
        p->d.failed = 0;
    }

    pool->current = pool;
    pool->large = NULL;
}

void* jl_alloc_block(jl_pool_t* pool,int size)
{
    unsigned char* m;
    size_t psize;
    jl_pool_t *p,*new;

    psize = (size_t)(pool->d.end - (unsigned char*)pool);
    int ret = posix_memalign((void**)&m,JL_MP_ALIGNMENT,psize);
    
    if(ret)
    {
        printf("posix_memalign fail\n");
        return NULL;
    }

    new = (jl_pool_t*)m;

    new->d.end = m + psize;
    new->d.failed = 0;
    new->d.next = NULL;

    m += sizeof(jl_data_t);
    m = jl_align_ptr(m,JL_MP_ALIGNMENT);
    new->d.last = m + size;

    for (p = pool->current; p->d.next; p = p->d.next) {
        if (p->d.failed++ > 4) {
            pool->current = p->d.next;
        }
    }

    p->d.next = new;
    return m;
}


void* jl_alloc_large(jl_pool_t* pool,int size)
{
    unsigned char* p;
    jl_large_t* large;
    uint32_t n = 0;
    p = malloc(size);
    for(large = pool->large;large;large = large->next)
    {
        if(large->alloc == NULL)
        {
            large->alloc = p;
            return p;
        }

        if (n++ > 3) {
            break;
        }
    }

    large = jl_alloc(pool,sizeof(large));
    if(large == NULL)
    {
        free(large);
        return NULL;
    }
    large->alloc = p;
    large->next = NULL;
    pool->large = large;
    return p;

}

void* jl_alloc(jl_pool_t* pool,int size)
{
    jl_pool_t* p;
    unsigned char* m;
    p = pool->current;       
    if(size <= pool->max)
    {
        do{
            m = jl_align_ptr(p->d.last,JL_MP_ALIGNMENT);
            //m = jl_align_ptr_fast(p->d.last);
            if((size_t)(p->d.end - m) >= size)
            {
                p->d.last = m + size;
                p->d.data_size += size; // 更新有效数据大小
                return m;
            }
            p = p->d.next;
        }while(p);

        return jl_alloc_block(pool,size);
    }
    return jl_alloc_large(pool,size);
    
}

int jl_flush_to_disk(jl_pool_t* pool, const char* filename) {
    FILE* file = fopen(filename, "w");
    if (!file) {
        perror("Failed to open file");
        return -1;
    }

    // 遍历内存池中的所有数据块
    for (jl_pool_t* p = pool; p; p = p->d.next) {
        unsigned char* start = (unsigned char*)p + sizeof(struct jl_pool_s);
        unsigned char* end = p->d.last;

        // 写入有效数据
        if (p->d.data_size > 0) {
            fwrite(start, 1, p->d.data_size, file);
            //fprintf(file, "%.*s", p->d.data_size, start);
        }
    }

    // 遍历大内存块
    for (jl_large_t* l = pool->large; l; l = l->next) {
        if (l->alloc) {
            // 假设大内存块的大小为 size（需要额外记录大小）
            fwrite(l->alloc, 1, JL_MAX_POOLSIZE, file); // 需要记录大内存块的大小
            //fprintf(file, "%.*s", JL_MAX_POOLSIZE, l->alloc);
        }
    }

    fclose(file);
    return 0;
}



void* jl_calloc(jl_pool_t* pool,int size)
{
    void* p;
    p = jl_alloc(pool,size);
    if(p){
        memset(p,0,size);
    }
    return p;
}

void jl_free(jl_pool_t* pool,void* p)
{
    struct jl_large_s *l;
	for (l = pool->large; l; l = l->next) {
		if (p == l->alloc) {
			free(l->alloc);
			l->alloc = NULL;
			return ;
		}
	}
}

#if 0
int main()
{
    int size = 1 << 12;

	struct jl_pool_s *p = jl_create_pool(size);

    char* key = (char*)jl_alloc(p,sizeof(char*));
    strcpy(key, "Hello, World!");

    char* value = (char*)jl_alloc(p,sizeof(char*));
    strcpy(key, "This is a test");

    jl_flush_to_disk(p, "memory_pool_data.bin");

    jl_destory_pool(p);
    // int size = 1 << 12;

	// struct jl_pool_s *p = jl_create_pool(size);

	// int i = 0;
	// for (i = 0;i < 20;i ++) {
	// 	void *mp = jl_alloc(p, 512);
	// }
	// printf("mp_align(123, 32): %d, mp_align(17, 32): %d\n", jl_align(123, 32), jl_align(17, 32));
	
	// int j = 0;
	// for (i = 0;i < 5;i ++) {
	// 	char *pp = jl_calloc(p, 32);
	// 	for (j = 0;j < 32;j ++) {
	// 		if (pp[j]) {
	// 			//printf("calloc wrong\n");
	// 		}
	// 		//printf("calloc success\n");
	// 	}
	// }    

	// for (i = 0;i < 5;i ++) {
	// 	void *l = jl_alloc(p, 8192);
	// 	jl_free(p, l);
	// }

    // printf("jl_reset_pool\n");
    // jl_reset_pool(p);

    // for (i = 0;i < 58;i ++) {
	// 	jl_alloc(p, 256);
	// }


	// printf("jl_destory_pool\n");
	// jl_destory_pool(p);

	// return 0;
}

#endif

