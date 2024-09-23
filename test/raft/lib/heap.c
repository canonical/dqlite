#include "heap.h"

#include <stdlib.h>

#include "fault.h"
#include "munit.h"

struct heap
{
    size_t alignment;   /* Value of last aligned alloc */
    struct Fault fault; /* Fault trigger. */
};

static void heapInit(struct heap *h)
{
    h->alignment = 0;
    FaultInit(&h->fault);
}

static void *heapMalloc(void *data, size_t size)
{
    struct heap *h = data;
    if (FaultTick(&h->fault)) {
        return NULL;
    }
    return munit_malloc(size);
}

static void heapFree(void *data, void *ptr)
{
    (void)data;
    free(ptr);
}

static void *heapCalloc(void *data, size_t nmemb, size_t size)
{
    struct heap *h = data;
    if (FaultTick(&h->fault)) {
        return NULL;
    }
    return munit_calloc(nmemb, size);
}

static void *heapRealloc(void *data, void *ptr, size_t size)
{
    struct heap *h = data;

    if (FaultTick(&h->fault)) {
        return NULL;
    }

    ptr = realloc(ptr, size);

    if (size != 0) {
        munit_assert_ptr_not_null(ptr);
    }

    return ptr;
}

static void *heapAlignedAlloc(void *data, size_t alignment, size_t size)
{
    struct heap *h = data;
    void *p;

    if (FaultTick(&h->fault)) {
        return NULL;
    }

    p = aligned_alloc(alignment, size);
    munit_assert_ptr_not_null(p);

    h->alignment = alignment;

    return p;
}

static void heapAlignedFree(void *data, size_t alignment, void *ptr)
{
    struct heap *h = data;
    munit_assert_ulong(alignment, ==, h->alignment);
    heapFree(data, ptr);
}

static int getIntParam(const MunitParameter params[], const char *name)
{
    const char *value = munit_parameters_get(params, name);
    return value != NULL ? atoi(value) : 0;
}

void HeapSetUp(const MunitParameter params[], struct raft_heap *h)
{
    struct heap *heap = munit_malloc(sizeof *heap);
    int delay = getIntParam(params, TEST_HEAP_FAULT_DELAY);
    int repeat = getIntParam(params, TEST_HEAP_FAULT_REPEAT);

    munit_assert_ptr_not_null(h);

    heapInit(heap);

    FaultConfig(&heap->fault, delay, repeat);

    h->data = heap;
    h->malloc = heapMalloc;
    h->free = heapFree;
    h->calloc = heapCalloc;
    h->realloc = heapRealloc;
    h->aligned_alloc = heapAlignedAlloc;
    h->aligned_free = heapAlignedFree;

    raft_heap_set(h);
    FaultPause(&heap->fault);
}

void HeapTearDown(struct raft_heap *h)
{
    struct heap *heap = h->data;
    free(heap);
    raft_heap_set_default();
}

void HeapFaultConfig(struct raft_heap *h, int delay, int repeat)
{
    struct heap *heap = h->data;
    FaultConfig(&heap->fault, delay, repeat);
}

void HeapFaultEnable(struct raft_heap *h)
{
    struct heap *heap = h->data;
    FaultResume(&heap->fault);
}
