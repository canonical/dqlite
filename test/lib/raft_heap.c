#include <raft.h>

#include "fault.h"
#include "raft_heap.h"

struct heapFault
{
	struct test_fault fault;
	const struct raft_heap *orig_heap;
};

static struct heapFault faulty;

static void *faultyMalloc(void *data, size_t size)
{
	(void)data;
	if (test_fault_tick(&faulty.fault)) {
		return NULL;
	} else {
		return faulty.orig_heap->malloc(faulty.orig_heap->data, size);
	}
}

static void faultyFree(void *data, void *ptr)
{
	(void)data;
	faulty.orig_heap->free(faulty.orig_heap->data, ptr);
}

static void *faultyCalloc(void *data, size_t nmemb, size_t size)
{
	(void)data;
	if (test_fault_tick(&faulty.fault)) {
		return NULL;
	} else {
		return faulty.orig_heap->calloc(faulty.orig_heap->data, nmemb,
						size);
	}
}

static void *faultyRealloc(void *data, void *ptr, size_t size)
{
	(void)data;
	if (test_fault_tick(&faulty.fault)) {
		return NULL;
	} else {
		return faulty.orig_heap->realloc(faulty.orig_heap->data, ptr,
						 size);
	}
}

static void *faultyAlignedAlloc(void *data, size_t alignment, size_t size)
{
	(void)data;
	if (test_fault_tick(&faulty.fault)) {
		return NULL;
	} else {
		return faulty.orig_heap->aligned_alloc(faulty.orig_heap->data,
						       alignment, size);
	}
}

static void faultyAlignedFree(void *data, size_t alignment, void *ptr)
{
	(void)data;
	(void)alignment;
	faulty.orig_heap->aligned_free(faulty.orig_heap->data, alignment, ptr);
}

void test_raft_heap_setup(const MunitParameter params[], void *user_data)
{
	(void)params;
	(void)user_data;
	struct raft_heap *heap = munit_malloc(sizeof(*heap));
	test_fault_init(&faulty.fault);
	faulty.orig_heap = raft_heap_get();
	heap->data = NULL;
	heap->malloc = faultyMalloc;
	heap->free = faultyFree;
	heap->calloc = faultyCalloc;
	heap->realloc = faultyRealloc;
	heap->aligned_alloc = faultyAlignedAlloc;
	heap->aligned_free = faultyAlignedFree;
	raft_heap_set(heap);
}

void test_raft_heap_tear_down(void *data)
{
	struct raft_heap *heap = (struct raft_heap *)raft_heap_get();
	(void)data;
	raft_heap_set((struct raft_heap *)faulty.orig_heap);
	faulty.orig_heap = NULL;
	free(heap);
}

void test_raft_heap_fault_config(int delay, int repeat)
{
	test_fault_config(&faulty.fault, delay, repeat);
}

void test_raft_heap_fault_enable(void)
{
	test_fault_enable(&faulty.fault);
}
