/* Internal heap APIs. */

#ifndef HEAP_H_
#define HEAP_H_

#include <stddef.h>

void *RaftHeapMalloc(size_t size);

void *RaftHeapCalloc(size_t nmemb, size_t size);

void *RaftHeapRealloc(void *ptr, size_t size);

void RaftHeapFree(void *ptr);

#endif /* HEAP_H_ */
