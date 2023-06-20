#ifndef DQLITE_LIB_ALLOC_H
#define DQLITE_LIB_ALLOC_H

#include <stddef.h>

void oomAbort(void);

void *mallocChecked(size_t n);

void *callocChecked(size_t nmemb, size_t size);

char *strdupChecked(const char *s);

char *strndupChecked(const char *s, size_t n);

#endif
