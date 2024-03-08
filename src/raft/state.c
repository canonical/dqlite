#include "assert.h"
#include "configuration.h"
#include "election.h"
#include "log.h"
#include "../lib/queue.h"

int raft_state(struct raft *r)
{
	return r->state;
}

void raft_leader(struct raft *r, raft_id *id, const char **address)
{
	switch (r->state) {
		case RAFT_UNAVAILABLE:
		case RAFT_CANDIDATE:
			*id = 0;
			*address = NULL;
			return;
		case RAFT_FOLLOWER:
			*id = r->follower_state.current_leader.id;
			*address = r->follower_state.current_leader.address;
			return;
		case RAFT_LEADER:
			if (r->transfer != NULL) {
				*id = 0;
				*address = NULL;
				return;
			}
			*id = r->id;
			*address = r->address;
			return;
	}
}

raft_index raft_last_index(struct raft *r)
{
	return logLastIndex(r->log);
}

raft_index raft_last_applied(struct raft *r)
{
	return r->last_applied;
}

int raft_role(struct raft *r)
{
	const struct raft_server *local =
	    configurationGet(&r->configuration, r->id);
	if (local == NULL) {
		return -1;
	}
	return local->role;
}
