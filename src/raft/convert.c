#include "convert.h"
#include "../lib/assert.h"
#include "../lib/queue.h"
#include "../raft.h"
#include "../tracing.h"
#include "callbacks.h"
#include "configuration.h"
#include "election.h"
#include "log.h"
#include "membership.h"
#include "progress.h"
#include "replication.h"
#include "request.h"

static const char *stateToStr(unsigned short state)
{
	switch (state) {
		case RAFT_UNAVAILABLE:
			return "UNAVAILABLE";
		case RAFT_FOLLOWER:
			return "FOLLOWER";
		case RAFT_CANDIDATE:
			return "CANDIDATE";
		case RAFT_LEADER:
			return "LEADER";
		default:
			return "UNKNOWN";
	}
}

/* Clear follower state. */
static void convertClearFollower(const struct raft *r)
{
	tracef("clear follower state");
	raft_free(r->follower_state.current_leader.address);
}

/* Clear candidate state. */
static void convertClearCandidate(const struct raft *r)
{
	tracef("clear candidate state");
	raft_free(r->candidate_state.votes);
}

static void convertFailApply(struct raft_apply *req)
{
	PRE(req != NULL);
	if (req->cb != NULL) {
		req->cb(req, RAFT_LEADERSHIPLOST);
	}
}

static void convertFailBarrier(struct raft_barrier *req)
{
	PRE(req != NULL);
	while (req != NULL) {
		struct raft_barrier *next = req->next;
		if (req->cb != NULL) {
			req->cb(req, RAFT_LEADERSHIPLOST);
		}
		req = next;
	}
}

static void convertFailChange(struct raft_change *req)
{
	PRE(req != NULL);
	if (req->cb != NULL) {
		req->cb(req, RAFT_LEADERSHIPLOST);
	}
}

/* Clear leader state. */
static void convertClearLeader(const struct raft *r)
{
	tracef("clear leader state");
	if (r->leader_state.progress != NULL) {
		raft_free(r->leader_state.progress);
	}

	/* Fail all outstanding requests */
	while (!queue_empty(&r->leader_state.requests)) {
		struct request *req;
		queue *head;
		head = queue_head(&r->leader_state.requests);
		queue_remove(head);
		req = QUEUE_DATA(head, struct request, queue);
		dqlite_assert(IN(req->type, RAFT_COMMAND, RAFT_BARRIER));
		switch (req->type) {
			case RAFT_COMMAND:
				convertFailApply((struct raft_apply *)req);
				break;
			case RAFT_BARRIER:
				convertFailBarrier((struct raft_barrier *)req);
				break;
		};
	}

	/* Fail any promote request that is still outstanding because the server
	 * is still catching up and no entry was submitted. */
	if (r->leader_state.change != NULL) {
		convertFailChange(r->leader_state.change);
	}
}

/* Convenience for setting a new state value and asserting that the transition
 * is valid. */
static void convertSetState(struct raft *r, unsigned short new_state)
{
	/* Check that the transition is legal, see Figure 3.3. Note that with
	 * respect to the paper we have an additional "unavailable" state, which
	 * is the initial or final state. */
	unsigned short old_state = r->state;
	tracef("old_state: %s new_state: %s", stateToStr(old_state),
	       stateToStr(new_state));

	dqlite_assert(
	    ERGO(r->state == RAFT_UNAVAILABLE, IN(new_state, RAFT_FOLLOWER)) &&
	    ERGO(r->state == RAFT_FOLLOWER,
		 IN(new_state, RAFT_CANDIDATE, RAFT_UNAVAILABLE)) &&
	    ERGO(r->state == RAFT_CANDIDATE,
		 IN(new_state, RAFT_UNAVAILABLE, RAFT_FOLLOWER, RAFT_LEADER)) &&
	    ERGO(r->state == RAFT_LEADER,
		 IN(new_state, RAFT_UNAVAILABLE, RAFT_FOLLOWER)));

	switch (old_state) {
		case RAFT_FOLLOWER:
			convertClearFollower(r);
			break;
		case RAFT_CANDIDATE:
			convertClearCandidate(r);
			break;
		case RAFT_LEADER:
			convertClearLeader(r);
			break;
		case RAFT_UNAVAILABLE:
			break;
		default:
			IMPOSSIBLE("unknown state");
			break;
	}

	r->state = new_state;
	switch (r->state) {
		case RAFT_FOLLOWER:
			r->follower_state = (struct raft_follower_state){};
			break;
		case RAFT_CANDIDATE:
			r->candidate_state = (struct raft_candidate_state){};
			break;
		case RAFT_LEADER:
			r->leader_state =
			    (struct raft_leader_state){ .voter_contacts = 1 };
			break;
		case RAFT_UNAVAILABLE:
			break;
		default:
			IMPOSSIBLE("unknown state");
			break;
	}

	struct raft_callbacks *cbs = raftGetCallbacks(r);
	if (cbs != NULL && cbs->state_cb != NULL) {
		cbs->state_cb(r, old_state, new_state);
	}
}

void convertToFollower(struct raft *r)
{
	convertSetState(r, RAFT_FOLLOWER);

	/* Reset election timer. */
	electionResetTimer(r);
}

int convertToCandidate(struct raft *r, bool disrupt_leader)
{
	const struct raft_server *server;
	size_t n_voters = configurationVoterCount(&r->configuration);
	int rv;

	(void)server; /* Only used for assertions. */

	convertSetState(r, RAFT_CANDIDATE);

	/* Allocate the votes array. */
	r->candidate_state.votes = raft_malloc(n_voters * sizeof(bool));
	if (r->candidate_state.votes == NULL) {
		return RAFT_NOMEM;
	}
	r->candidate_state.disrupt_leader = disrupt_leader;
	r->candidate_state.in_pre_vote = disrupt_leader ? false : r->pre_vote;

	/* Fast-forward to leader if we're the only voting server in the
	 * configuration. */
	server = configurationGet(&r->configuration, r->id);
	dqlite_assert(server != NULL);
	dqlite_assert(server->role == RAFT_VOTER);

	if (n_voters == 1) {
		tracef("self elect and convert to leader");
		return convertToLeader(r);
	}

	/* Start a new election round */
	rv = electionStart(r);
	if (rv != 0) {
		convertSetState(r, RAFT_FOLLOWER);
		return rv;
	}

	return 0;
}

void convertInitialBarrierCb(struct raft_barrier *req, int status)
{
	(void)status;
	raft_free(req);
}

int convertToLeader(struct raft *r)
{
	tracef("become leader for term %" PRIu64, r->current_term);

	convertSetState(r, RAFT_LEADER);

	/* Reset timers */
	r->election_timer_start = r->io->time(r->io);

	/* Reset apply requests queue */
	queue_init(&r->leader_state.requests);

	/* Allocate and initialize the progress array. */
	int rv = progressBuildArray(r);
	if (rv != 0) {
		return rv;
	}

	/* By definition, all entries until the last_stored entry will be
	 * committed if we are the only voter around. */
	size_t n_voters = configurationVoterCount(&r->configuration);
	if (n_voters == 1 && (r->last_stored > r->commit_index)) {
		tracef("apply log entries after self election %" PRIu64
		       " %" PRIu64,
		       r->last_stored, r->commit_index);
		r->commit_index = r->last_stored;
		rv = replicationApply(r);
	} else if (n_voters > 1) {
		/* Raft Dissertation, paragraph 6.4:
		 * The Leader Completeness Property guarantees that a leader has
		 * all committed entries, but at the start of its term, it may
		 * not know which those are. To find out, it needs to commit an
		 * entry from its term. Raft handles this by having each leader
		 * commit a blank no-op entry into the log at the start of its
		 * term. */
		struct raft_barrier *req = raft_malloc(sizeof(*req));
		if (req == NULL) {
			return RAFT_NOMEM;
		}
		rv = raft_barrier(r, req, convertInitialBarrierCb);
		if (rv != 0) {
			tracef(
			    "failed to send no-op barrier entry after leader "
			    "conversion: "
			    "%d",
			    rv);
		}
	}

	return rv;
}

void convertToUnavailable(struct raft *r)
{
	/* Abort any pending leadership transfer request. */
	if (r->transfer != NULL) {
		membershipLeadershipTransferClose(r);
	}
	convertSetState(r, RAFT_UNAVAILABLE);
}
