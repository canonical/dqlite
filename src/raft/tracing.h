#ifndef RAFT_TRACE_H_
#define RAFT_TRACE_H_
#include "../raft.h"
#include "../tracing.h"

inline static void raft_emit_trace_event(struct raft *r) {
    if (r->trace_cb == NULL) {
        return;
    }

    raft_id known_leader_id;
    if (r->state == RAFT_LEADER) {
        known_leader_id = r->id;
    } else if (r->state == RAFT_FOLLOWER) {
        known_leader_id = r->follower_state.current_leader.id;
    } else {
        known_leader_id = 0;
    }
    const struct raft_trace trace = {
        .state = (enum raft_state)r->state,
        .term = r->current_term,
        .last_stored = r->last_stored,
        .commit_index = r->commit_index,
        .last_applied = r->last_applied,
        .known_leader_id = known_leader_id,
        .voted_for = r->voted_for,
    };
    r->trace_cb(r->trace_data, &trace);
}

#endif
