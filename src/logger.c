#include "logger.h"

void logger__emit(struct logger *l, int level, const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    l->emit(l->data, level, fmt, args);
    va_end(args);
}
