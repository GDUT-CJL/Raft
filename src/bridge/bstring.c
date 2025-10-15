#include "storage.h"
#define MIN_CAPACITY 16
#define GROWTH_FACTOR 2

// 内部函数声明
static bool bstring_ensure_capacity(bstring_t *bs, size_t needed);
static size_t calculate_new_capacity(size_t current, size_t needed);

static bool bstring_ensure_capacity(bstring_t *bs, size_t needed) 
{
    if (!bs || !(bs->flags & BSTRING_OWNED)) return false;
    
    if (needed >= bs->cap) {
        size_t new_cap = calculate_new_capacity(bs->cap,needed);
        uint8_t *new_data = (uint8_t*)realloc(bs->data, new_cap);
        if(!new_data)   return false;
        bs->data = new_data;
        bs->cap = new_cap;
    }
    return true;
}

static size_t calculate_new_capacity(size_t current, size_t needed){
    size_t new_cap = current;
    while(new_cap <= needed){
        new_cap = (new_cap == 0) ? MIN_CAPACITY : new_cap * GROWTH_FACTOR;
        // 防止溢出
        if (new_cap < current) return SIZE_MAX;
    }
    return new_cap;
}

bstring_t* bstring_new(void)
{
    bstring_t* bs = (bstring_t*)malloc(sizeof(bstring_t));
    if(!bs) return NULL;
    
    bs->data = (uint8_t*)malloc(MIN_CAPACITY);
    if(!bs->data){
        free(bs);
        return NULL;
    }
    bs->cap = MIN_CAPACITY;
    bs->len = 0;
    bs->flags = BSTRING_OWNED;
    bs->data[0] = '\0'; // 空字符终止，便于调试
    
    return bs;
}

bstring_t* bstring_new_with_capacity(size_t capacity)
{
    bstring_t *bs = (bstring_t*)malloc(sizeof(bstring_t));
    if (!bs) return NULL;
    
    size_t actual_cap = (capacity < MIN_CAPACITY) ? MIN_CAPACITY : capacity;
    bs->data = (uint8_t*)malloc(actual_cap);
    if (!bs->data) {
        free(bs);
        return NULL;
    }
    
    bs->len = 0;
    bs->cap = actual_cap;
    bs->flags = BSTRING_OWNED;
    bs->data[0] = '\0';
    
    return bs;
}

bstring_t* bstring_new_from_data(const void *data, size_t len)
{
    if(!data && len > 0)    return NULL;
    bstring_t *bs = bstring_new_with_capacity(len + 1);
    
    if(data && len > 0){
        memcpy(bs->data, data, len);// 使用memcpy
        bs->len = len;
    }
    bs->data[bs->len] = '\0';
    return bs;
}

bstring_t* bstring_new_from_cstr(const char *cstr)
{
    if (!cstr) return bstring_new();
    
    size_t len = strlen(cstr);
    return bstring_new_from_data(cstr,len);
}

void bstring_free(bstring_t *bs)
{
    if (!bs) return;
    
    if ((bs->flags & BSTRING_OWNED) && bs->data) {
        free(bs->data);
    }
    free(bs);
}

size_t bstring_len(const bstring_t *bs) {
    return bs ? bs->len : 0;
}

const uint8_t* bstring_data(const bstring_t *bs) {
    return bs ? bs->data : NULL;
}

bool bstring_empty(const bstring_t *bs) {
    return !bs || bs->len == 0;
}

// 修改操作
int bstring_append(bstring_t *bs, const void *data, size_t len)
{
    if(!bs || !data || len == 0)    return -1;
    if (!(bs->flags & BSTRING_OWNED)) return -1;  // 只读数据不能追加
    if(!bstring_ensure_capacity(bs,bs->len+len+1))  return -1;
    
    memcpy(bs->data + bs->len, data, len);
    bs->len += len;
    bs->data[bs->len] = '\0';  // 保持C字符串兼容
    return 0;
    
}

int bstring_append_cstr(bstring_t *bs, const char *cstr)
{
    if (!cstr) return -1;
    size_t len = strlen(cstr);
    return bstring_append(bs,cstr,len);
}

int bstring_append_bstring(bstring_t *bs, const bstring_t *other)
{
    if(!other || other->len == 0) return -1; 
    bstring_append(bs,other->data,other->len);
}

// 清空和重置
void bstring_clear(bstring_t *bs) {
    if (bs && (bs->flags & BSTRING_OWNED)) {
        bs->len = 0;
        if (bs->data) {
            bs->data[0] = '\0';
        }
    }
}

void bstring_reset(bstring_t *bs) {
    if (bs && (bs->flags & BSTRING_OWNED)) {
        if (bs->data) {
            free(bs->data);
            bs->data = (uint8_t*)malloc(MIN_CAPACITY);
            if (bs->data) {
                bs->data[0] = '\0';
                bs->cap = MIN_CAPACITY;
            }
        }
        bs->len = 0;
    }
}
// 比较操作
int bstring_compare(const bstring_t *bs1, const bstring_t *bs2)
{
    if (!bs1 && !bs2) return 0;
    if (!bs1) return -1;
    if (!bs2) return 1;
    
    size_t min_len = (bs1->len < bs2->len) ? bs1->len : bs2->len;
    int result = memcmp(bs1->data, bs2->data, min_len);
    
    if (result != 0) return result;
    return (bs1->len == bs2->len) ? 0 : (bs1->len < bs2->len) ? -1 : 1;
}

bool bstring_equal(const bstring_t *bs1, const bstring_t *bs2) 
{
    if (!bs1 || !bs2) return bs1 == bs2;
    if (bs1->len != bs2->len) return false;
    return memcmp(bs1->data, bs2->data, bs1->len) == 0;
}

// 十六进制格式打印
void bstring_print_hex(const bstring_t *bs) {
    if (!bs || bstring_empty(bs)) {
        printf("(empty)\n");
        return;
    }
    
    const uint8_t *data = bstring_data(bs);
    size_t len = bstring_len(bs);
    
    for (size_t i = 0; i < len; i++) {
        printf("%02x", data[i]);
        if (i < len - 1) printf(" ");
        if ((i + 1) % 16 == 0) printf("\n");  // 每16字节换行
    }
    if (len % 16 != 0) printf("\n");
}


// 可读文本格式打印（转义特殊字符）
void bstring_print_text(const bstring_t *bs) {
    if (!bs || bstring_empty(bs)) {
        printf("(empty)\n");
        return;
    }
    
    const uint8_t *data = bstring_data(bs);
    size_t len = bstring_len(bs);
    
    printf("\"");
    for (size_t i = 0; i < len; i++) {
        uint8_t byte = data[i];
        
        // 转义特殊字符
        switch (byte) {
            case '\0': printf("\\0"); break;
            case '\a': printf("\\a"); break;
            case '\b': printf("\\b"); break;
            case '\t': printf("\\t"); break;
            case '\n': printf("\\n"); break;
            case '\v': printf("\\v"); break;
            case '\f': printf("\\f"); break;
            case '\r': printf("\\r"); break;
            case '\\': printf("\\\\"); break;
            case '\"': printf("\\\""); break;
            default:
                if (isprint(byte)) {
                    printf("%c", byte);
                } else {
                    printf("\\x%02x", byte);
                }
                break;
        }
    }
    printf("\"\n");
}

// 详细信息打印
void bstring_print_detailed(const bstring_t *bs) {
    if (!bs) {
        printf("bstring: NULL\n");
        return;
    }
    
    printf("=== bstring详细信息 ===\n");
    printf("长度: %zu 字节\n", bstring_len(bs));
    printf("容量: %zu 字节\n", bs->cap);
    printf("标志: 0x%x\n", bs->flags);
    
    printf("十六进制: ");
    bstring_print_hex(bs);
    
    printf("可读格式: ");
    bstring_print_text(bs);
    
    // 显示ASCII预览
    printf("ASCII预览: ");
    const uint8_t *data = bstring_data(bs);
    size_t len = bstring_len(bs);
    for (size_t i = 0; i < len && i < 32; i++) {  // 最多显示32字符
        if (isprint(data[i])) {
            printf("%c", data[i]);
        } else {
            printf(".");
        }
    }
    if (len > 32) printf("...");
    printf("\n");
    
    printf("========================\n");
}

char* bstring_to_cstr(const bstring_t* bs)
{
    if (!bs) return strdup("");
    
    char *cstr = (char*)malloc(bs->len + 1);
    if (!cstr) return NULL;
    
    if (bs->len > 0) {
        memcpy(cstr, bs->data, bs->len);
    }
    cstr[bs->len] = '\0';
    return cstr;
}