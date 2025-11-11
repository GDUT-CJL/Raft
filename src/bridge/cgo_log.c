#include "storage.h"
static LogCallback g_log_callback = NULL;

void set_storage_log_callback(LogCallback callback) {
    g_log_callback = callback;
}
// 内部日志函数
void storage_log(const char* format, int level, ...) {
    if (!g_log_callback) return;
    
    char buffer[1024];
    va_list args;
    va_start(args, level);
    vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);
    
    g_log_callback(buffer, level);
}
