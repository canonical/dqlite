#include "callbacks.h"
#include "heap.h"

int raftInitCallbacks(struct raft *r)
{
	r->callbacks = 0;
	struct raft_callbacks *cbs = RaftHeapCalloc(1, sizeof(*cbs));
	if (cbs == NULL) {
		return RAFT_NOMEM;
	}
	r->callbacks = (uint64_t)(uintptr_t)cbs;
	return 0;
}

void raftDestroyCallbacks(struct raft *r)
{
	RaftHeapFree((void *)(uintptr_t)r->callbacks);
	r->callbacks = 0;
}

struct raft_callbacks *raftGetCallbacks(struct raft *r)
{
	return (void *)(uintptr_t)r->callbacks;
}
