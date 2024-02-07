#ifndef CALLBACKS_H_
#define CALLBACKS_H_

#include "../raft.h"

struct raft_callbacks
{
	raft_state_cb state_cb;
};

int raftInitCallbacks(struct raft *r);
void raftDestroyCallbacks(struct raft *r);
struct raft_callbacks *raftGetCallbacks(struct raft *r);

#endif
