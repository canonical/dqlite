#include <string.h>

#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "entry.h"
#ifdef __GLIBC__
#include "error.h"
#endif
#include "../lib/queue.h"
#include "../tracing.h"
#include "err.h"
#include "flags.h"
#include "heap.h"
#include "log.h"
#include "membership.h"
#include "progress.h"
#include "replication.h"
#include "request.h"
#include "snapshot.h"

#ifndef max
#define max(a, b) ((a) < (b) ? (b) : (a))
#endif

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

/* Context of a RAFT_IO_APPEND_ENTRIES request that was submitted with
 * raft_io_>send(). */
struct sendAppendEntries
{
	struct raft *raft;        /* Instance sending the entries. */
	struct raft_io_send send; /* Underlying I/O send request. */
	raft_index index;         /* Index of the first entry in the request. */
	struct raft_entry *entries; /* Entries referenced in the request. */
	unsigned n;                 /* Length of the entries array. */
	raft_id server_id;          /* Destination server. */
};

/* Callback invoked after request to send an AppendEntries RPC has completed. */
static void sendAppendEntriesCb(struct raft_io_send *send, const int status)
{
	struct sendAppendEntries *req = send->data;
	struct raft *r = req->raft;
	unsigned i = configurationIndexOf(&r->configuration, req->server_id);

	if (r->state == RAFT_LEADER && i < r->configuration.n) {
		if (status != 0) {
			tracef(
			    "failed to send append entries to server %llu: %s",
			    req->server_id, raft_strerror(status));
			/* Go back to probe mode. */
			progressToProbe(r, i);
		}
	}

	/* Tell the log that we're done referencing these entries. */
	logRelease(r->log, req->index, req->entries, req->n);
	raft_free(req);
}

/* Send an AppendEntries message to the i'th server, including all log entries
 * from the given point onwards. */
static int sendAppendEntries(struct raft *r,
			     const unsigned i,
			     const raft_index prev_index,
			     const raft_term prev_term)
{
	struct raft_server *server = &r->configuration.servers[i];
	struct raft_message message;
	struct raft_append_entries *args = &message.append_entries;
	struct sendAppendEntries *req;
	raft_index next_index = prev_index + 1;
	int rv;

	args->term = r->current_term;
	args->prev_log_index = prev_index;
	args->prev_log_term = prev_term;

	/* TODO: implement a limit to the total size of the entries being sent
	 */
	rv = logAcquire(r->log, next_index, &args->entries, &args->n_entries);
	if (rv != 0) {
		goto err;
	}

	/* From Section 3.5:
	 *
	 *   The leader keeps track of the highest index it knows to be
	 * committed, and it includes that index in future AppendEntries RPCs
	 * (including heartbeats) so that the other servers eventually find out.
	 * Once a follower learns that a log entry is committed, it applies the
	 * entry to its local state machine (in log order)
	 */
	args->leader_commit = r->commit_index;

	tracef(
	    "send %u entries starting at %llu to server %llu (last index %llu)",
	    args->n_entries, args->prev_log_index, server->id,
	    logLastIndex(r->log));

	message.type = RAFT_IO_APPEND_ENTRIES;
	message.server_id = server->id;
	message.server_address = server->address;

	req = raft_malloc(sizeof *req);
	if (req == NULL) {
		rv = RAFT_NOMEM;
		goto err_after_entries_acquired;
	}
	req->raft = r;
	req->index = args->prev_log_index + 1;
	req->entries = args->entries;
	req->n = args->n_entries;
	req->server_id = server->id;

	req->send.data = req;
	rv = r->io->send(r->io, &req->send, &message, sendAppendEntriesCb);
	if (rv != 0) {
		goto err_after_req_alloc;
	}

	if (progressState(r, i) == PROGRESS__PIPELINE) {
		/* Optimistically update progress. */
		progressOptimisticNextIndex(r, i, req->index + req->n);
	}

	progressUpdateLastSend(r, i);
	return 0;

err_after_req_alloc:
	raft_free(req);
err_after_entries_acquired:
	logRelease(r->log, next_index, args->entries, args->n_entries);
err:
	assert(rv != 0);
	return rv;
}

/* Context of a RAFT_IO_INSTALL_SNAPSHOT request that was submitted with
 * raft_io_>send(). */
struct sendInstallSnapshot
{
	struct raft *raft;               /* Instance sending the snapshot. */
	struct raft_io_snapshot_get get; /* Snapshot get request. */
	struct raft_io_send send;        /* Underlying I/O send request. */
	struct raft_snapshot *snapshot;  /* Snapshot to send. */
	raft_id server_id;               /* Destination server. */
};

static void sendInstallSnapshotCb(struct raft_io_send *send, int status)
{
	struct sendInstallSnapshot *req = send->data;
	struct raft *r = req->raft;
	const struct raft_server *server;

	server = configurationGet(&r->configuration, req->server_id);

	if (status != 0) {
		tracef("send install snapshot: %s", raft_strerror(status));
		if (r->state == RAFT_LEADER && server != NULL) {
			unsigned i;
			i = configurationIndexOf(&r->configuration,
						 req->server_id);
			progressAbortSnapshot(r, i);
		}
	}

	snapshotClose(req->snapshot);
	raft_free(req->snapshot);
	raft_free(req);
}

static void sendSnapshotGetCb(struct raft_io_snapshot_get *get,
			      struct raft_snapshot *snapshot,
			      int status)
{
	struct sendInstallSnapshot *req = get->data;
	struct raft *r = req->raft;
	struct raft_message message;
	struct raft_install_snapshot *args = &message.install_snapshot;
	const struct raft_server *server = NULL;
	bool progress_state_is_snapshot = false;
	unsigned i = 0;
	int rv;

	if (status != 0) {
		tracef("get snapshot %s", raft_strerror(status));
		goto abort;
	}
	if (r->state != RAFT_LEADER) {
		goto abort_with_snapshot;
	}

	server = configurationGet(&r->configuration, req->server_id);

	if (server == NULL) {
		/* Probably the server was removed in the meantime. */
		goto abort_with_snapshot;
	}

	i = configurationIndexOf(&r->configuration, req->server_id);
	progress_state_is_snapshot = progressState(r, i) == PROGRESS__SNAPSHOT;

	if (!progress_state_is_snapshot) {
		/* Something happened in the meantime. */
		goto abort_with_snapshot;
	}

	assert(snapshot->n_bufs == 1);

	message.type = RAFT_IO_INSTALL_SNAPSHOT;
	message.server_id = server->id;
	message.server_address = server->address;

	args->term = r->current_term;
	args->last_index = snapshot->index;
	args->last_term = snapshot->term;
	args->conf_index = snapshot->configuration_index;
	args->conf = snapshot->configuration;
	args->data = snapshot->bufs[0];

	req->snapshot = snapshot;
	req->send.data = req;

	tracef("sending snapshot with last index %llu to %llu", snapshot->index,
	       server->id);

	rv = r->io->send(r->io, &req->send, &message, sendInstallSnapshotCb);
	if (rv != 0) {
		goto abort_with_snapshot;
	}

	goto out;

abort_with_snapshot:
	snapshotClose(snapshot);
	raft_free(snapshot);
abort:
	if (r->state == RAFT_LEADER && server != NULL &&
	    progress_state_is_snapshot) {
		progressAbortSnapshot(r, i);
	}
	raft_free(req);
out:
	return;
}

/* Send the latest snapshot to the i'th server */
static int sendSnapshot(struct raft *r, const unsigned i)
{
	struct raft_server *server = &r->configuration.servers[i];
	struct sendInstallSnapshot *request;
	int rv;

	progressToSnapshot(r, i);

	request = raft_malloc(sizeof *request);
	if (request == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}
	request->raft = r;
	request->server_id = server->id;
	request->get.data = request;

	/* TODO: make sure that the I/O implementation really returns the latest
	 * snapshot *at this time* and not any snapshot that might be stored at
	 * a later point. Otherwise the progress snapshot_index would be wrong.
	 */
	rv = r->io->snapshot_get(r->io, &request->get, sendSnapshotGetCb);
	if (rv != 0) {
		goto err_after_req_alloc;
	}

	progressUpdateSnapshotLastSend(r, i);
	return 0;

err_after_req_alloc:
	raft_free(request);
err:
	progressAbortSnapshot(r, i);
	assert(rv != 0);
	return rv;
}

int replicationProgress(struct raft *r, unsigned i)
{
	struct raft_server *server = &r->configuration.servers[i];
	bool progress_state_is_snapshot =
	    progressState(r, i) == PROGRESS__SNAPSHOT;
	raft_index snapshot_index = logSnapshotIndex(r->log);
	raft_index next_index = progressNextIndex(r, i);
	raft_index prev_index;
	raft_term prev_term;

	assert(r->state == RAFT_LEADER);
	assert(server->id != r->id);
	assert(next_index >= 1);

	if (!progressShouldReplicate(r, i)) {
		return 0;
	}

	/* From Section 3.5:
	 *
	 *   When sending an AppendEntries RPC, the leader includes the index
	 * and term of the entry in its log that immediately precedes the new
	 *   entries. If the follower does not find an entry in its log with the
	 *   same index and term, then it refuses the new entries. The
	 * consistency check acts as an induction step: the initial empty state
	 * of the logs satisfies the Log Matching Property, and the consistency
	 * check preserves the Log Matching Property whenever logs are extended.
	 * As a result, whenever AppendEntries returns successfully, the leader
	 * knows that the follower's log is identical to its own log up through
	 * the new entries (Log Matching Property in Figure 3.2).
	 */
	if (next_index == 1) {
		/* We're including the very first entry, so prevIndex and
		 * prevTerm are null. If the first entry is not available
		 * anymore, send the last snapshot if we're not already sending
		 * one. */
		if (snapshot_index > 0 && !progress_state_is_snapshot) {
			raft_index last_index = logLastIndex(r->log);
			assert(last_index > 0); /* The log can't be empty */
			goto send_snapshot;
		}
		prev_index = 0;
		prev_term = 0;
	} else {
		/* Set prevIndex and prevTerm to the index and term of the entry
		 * at next_index - 1. */
		prev_index = next_index - 1;
		prev_term = logTermOf(r->log, prev_index);
		/* If the entry is not anymore in our log, send the last
		 * snapshot if we're not doing so already. */
		if (prev_term == 0 && !progress_state_is_snapshot) {
			assert(prev_index < snapshot_index);
			tracef("missing entry at index %lld -> send snapshot",
			       prev_index);
			goto send_snapshot;
		}
	}

	/* Send empty AppendEntries RPC when installing a snaphot */
	if (progress_state_is_snapshot) {
		prev_index = logLastIndex(r->log);
		prev_term = logLastTerm(r->log);
	}

	return sendAppendEntries(r, i, prev_index, prev_term);

send_snapshot:
	if (progressGetRecentRecv(r, i)) {
		/* Only send a snapshot when we have heard from the server */
		return sendSnapshot(r, i);
	} else {
		/* Send empty AppendEntries RPC when we haven't heard from the
		 * server */
		prev_index = logLastIndex(r->log);
		prev_term = logLastTerm(r->log);
		return sendAppendEntries(r, i, prev_index, prev_term);
	}
}

/* Possibly trigger I/O requests for newly appended log entries or heartbeats.
 *
 * This function loops through all followers and triggers replication on them.
 *
 * It must be called only by leaders. */
static int triggerAll(struct raft *r)
{
	unsigned i;
	int rv;

	assert(r->state == RAFT_LEADER);

	/* Trigger replication for servers we didn't hear from recently. */
	for (i = 0; i < r->configuration.n; i++) {
		struct raft_server *server = &r->configuration.servers[i];
		if (server->id == r->id) {
			continue;
		}
		/* Skip spare servers, unless they're being promoted. */
		if (server->role == RAFT_SPARE &&
		    server->id != r->leader_state.promotee_id) {
			continue;
		}
		rv = replicationProgress(r, i);
		if (rv != 0 && rv != RAFT_NOCONNECTION) {
			/* This is not a critical failure, let's just log it. */
			tracef(
			    "failed to send append entries to server %llu: %s "
			    "(%d)",
			    server->id, raft_strerror(rv), rv);
		}
	}

	return 0;
}

int replicationHeartbeat(struct raft *r)
{
	return triggerAll(r);
}

/* Context for a write log entries request that was submitted by a leader. */
struct appendLeader
{
	struct raft *raft; /* Instance that has submitted the request */
	raft_index index;  /* Index of the first entry in the request. */
	struct raft_entry *entries; /* Entries referenced in the request. */
	unsigned n;                 /* Length of the entries array. */
	struct raft_io_append req;
};

/* Called after a successful append entries I/O request to update the index of
 * the last entry stored on disk. Return how many new entries that are still
 * present in our in-memory log were stored. */
static size_t updateLastStored(struct raft *r,
			       raft_index first_index,
			       struct raft_entry *entries,
			       size_t n_entries)
{
	size_t i;

	/* Check which of these entries is still in our in-memory log */
	for (i = 0; i < n_entries; i++) {
		struct raft_entry *entry = &entries[i];
		raft_index index = first_index + i;
		raft_term local_term = logTermOf(r->log, index);

		/* If we have no entry at this index, or if the entry we have
		 * now has a different term, it means that this entry got
		 * truncated, so let's stop here. */
		if (local_term == 0 ||
		    (local_term > 0 && local_term != entry->term)) {
			break;
		}

		/* If we do have an entry at this index, its term must match the
		 * one of the entry we wrote on disk. */
		assert(local_term != 0 && local_term == entry->term);
	}

	r->last_stored += i;
	return i;
}

/* Get the request matching the given @index and @type, if any.
 * The type check is skipped when @type == -1. */
static struct request *getRequest(struct raft *r,
				  const raft_index index,
				  int type)
{
	queue *head;
	struct request *req;

	if (r->state != RAFT_LEADER) {
		return NULL;
	}
	QUEUE_FOREACH(head, &r->leader_state.requests)
	{
		req = QUEUE_DATA(head, struct request, queue);
		if (req->index == index) {
			if (type != -1) {
				assert(req->type == type);
			}
			return req;
		}
	}
	return NULL;
}

/* Invoked once a disk write request for new entries has been completed. */
static void appendLeaderCb(struct raft_io_append *append, int status)
{
	struct appendLeader *request = append->data;
	struct raft *r = request->raft;
	size_t server_index;
	raft_index index;
	int rv;

	tracef("leader: written %u entries starting at %lld: status %d",
	       request->n, request->index, status);

	/* In case of a failed disk write, if we were the leader creating these
	 * entries in the first place, truncate our log too (since we have
	 * appended these entries to it) and fire the request callbacks.
	 *
	 * Afterward, convert immediately to follower state, giving the cluster
	 * a chance to elect another leader that doesn't have a full disk (or
	 * whatever caused our write error). */
	if (status != 0) {
		ErrMsgTransfer(r->io->errmsg, r->errmsg, "io");
		for (unsigned i = 0; i < request->n; i++) {
			struct request *req =
			    getRequest(r, request->index + i, -1);
			if (!req) {
				tracef("no request found at index %llu",
				       request->index + i);
				continue;
			}
			queue_remove(&req->queue);
			switch (req->type) {
				case RAFT_COMMAND: {
					sm_fail(&req->sm, REQUEST_FAILED, status);
					sm_fini(&req->sm);
					struct raft_apply *apply =
					    (struct raft_apply *)req;
					if (apply->cb) {
						apply->cb(apply, status, NULL);
					}
					break;
				}
				case RAFT_BARRIER: {
					struct raft_barrier *barrier =
					    (struct raft_barrier *)req;
					if (barrier->cb) {
						barrier->cb(barrier, status);
					}
					break;
				}
				case RAFT_CHANGE: {
					struct raft_change *change =
					    (struct raft_change *)req;
					if (change->cb) {
						change->cb(change, status);
					}
					break;
				}
				default:
					tracef(
					    "unknown request type, shutdown.");
					assert(false);
					break;
			}
		}
		goto out;
	}

	updateLastStored(r, request->index, request->entries, request->n);

	/* If we are not leader anymore, just discard the result. */
	if (r->state != RAFT_LEADER) {
		tracef("local server is not leader -> ignore write log result");
		goto out;
	}

	/* Only update the next index if we are part of the current
	 * configuration. The only case where this is not true is when we were
	 * asked to remove ourselves from the cluster.
	 *
	 * From Section 4.2.2:
	 *
	 *   there will be a period of time (while it is committing Cnew) when a
	 *   leader can manage a cluster that does not include itself; it
	 *   replicates log entries but does not count itself in majorities.
	 */
	server_index = configurationIndexOf(&r->configuration, r->id);
	if (server_index < r->configuration.n) {
		r->leader_state.progress[server_index].match_index =
		    r->last_stored;
	}

	/* Check if we can commit some new entries. */
	replicationQuorum(r, r->last_stored);

	rv = replicationApply(r);
	if (rv != 0) {
		/* TODO: just log the error? */
	}

out:
	/* Tell the log that we're done referencing these entries. */
	logRelease(r->log, request->index, request->entries, request->n);
	index = request->index;
	raft_free(request);
	if (status != 0) {
		if (index <= logLastIndex(r->log)) {
			logTruncate(r->log, index);
		}
		if (r->state == RAFT_LEADER) {
			convertToFollower(r);
		}
	}
}

/* Submit a disk write for all entries from the given index onward. */
static int appendLeader(struct raft *r, raft_index index)
{
	struct raft_entry *entries = NULL;
	unsigned n;
	struct appendLeader *request;
	const struct sm *entry_sm;
	int rv;

	assert(r->state == RAFT_LEADER);
	assert(index > 0);
	assert(index > r->last_stored);

	/* Acquire all the entries from the given index onwards. */
	rv = logAcquire(r->log, index, &entries, &n);
	if (rv != 0) {
		goto err;
	}

	/* We expect this function to be called only when there are actually
	 * some entries to write. */
	if (n == 0) {
		assert(false);
		tracef("No log entries found at index %llu", index);
		ErrMsgPrintf(r->errmsg, "No log entries found at index %llu",
			     index);
		rv = RAFT_SHUTDOWN;
		goto err_after_entries_acquired;
	}

	/* Allocate a new request. */
	request = raft_malloc(sizeof *request);
	if (request == NULL) {
		rv = RAFT_NOMEM;
		goto err_after_entries_acquired;
	}

	request->raft = r;
	request->index = index;
	request->entries = entries;
	request->n = n;
	request->req.data = request;

	rv = r->io->append(r->io, &request->req, entries, n, appendLeaderCb);
	for (unsigned i = 0; i < n; i++) {
		entry_sm = log_get_entry_sm(r->log, entries[i].term, index + i);
		sm_relate(entry_sm, &request->req.sm);
	}
	if (rv != 0) {
		ErrMsgTransfer(r->io->errmsg, r->errmsg, "io");
		goto err_in_append;
	}

	return 0;

err_in_append:
	sm_fini(&request->req.sm);
	raft_free(request);
err_after_entries_acquired:
	logRelease(r->log, index, entries, n);
err:
	assert(rv != 0);
	return rv;
}

int replicationTrigger(struct raft *r, raft_index index)
{
	int rv;

	rv = appendLeader(r, index);
	if (rv != 0) {
		return rv;
	}

	return triggerAll(r);
}

/* Helper to be invoked after a promotion of a non-voting server has been
 * requested via @raft_assign and that server has caught up with logs.
 *
 * This function changes the local configuration marking the server being
 * promoted as actually voting, appends the a RAFT_CHANGE entry with the new
 * configuration to the local log and triggers its replication. */
static int triggerActualPromotion(struct raft *r)
{
	raft_index index;
	raft_term term = r->current_term;
	size_t server_index;
	struct raft_server *server;
	int old_role;
	int rv;

	assert(r->state == RAFT_LEADER);
	assert(r->leader_state.promotee_id != 0);

	server_index = configurationIndexOf(&r->configuration,
					    r->leader_state.promotee_id);
	assert(server_index < r->configuration.n);

	server = &r->configuration.servers[server_index];

	assert(server->role != RAFT_VOTER);

	/* Update our current configuration. */
	old_role = server->role;
	server->role = RAFT_VOTER;

	/* Index of the entry being appended. */
	index = logLastIndex(r->log) + 1;

	/* Encode the new configuration and append it to the log. */
	rv = logAppendConfiguration(r->log, term, &r->configuration);
	if (rv != 0) {
		goto err;
	}

	/* Start writing the new log entry to disk and send it to the followers.
	 */
	rv = replicationTrigger(r, index);
	if (rv != 0) {
		goto err_after_log_append;
	}

	r->leader_state.promotee_id = 0;
	r->configuration_uncommitted_index = logLastIndex(r->log);

	return 0;

err_after_log_append:
	logTruncate(r->log, index);

err:
	server->role = old_role;

	assert(rv != 0);
	return rv;
}

int replicationUpdate(struct raft *r,
		      const struct raft_server *server,
		      const struct raft_append_entries_result *result)
{
	bool is_being_promoted;
	raft_index last_index;
	unsigned i;
	int rv;

	i = configurationIndexOf(&r->configuration, server->id);

	assert(r->state == RAFT_LEADER);
	assert(i < r->configuration.n);

	progressMarkRecentRecv(r, i);

	progressSetFeatures(r, i, result->features);

	/* If the RPC failed because of a log mismatch, retry.
	 *
	 * From Figure 3.1:
	 *
	 *   [Rules for servers] Leaders:
	 *
	 *   - If AppendEntries fails because of log inconsistency:
	 *     decrement nextIndex and retry.
	 */
	if (result->rejected > 0) {
		bool retry;
		retry = progressMaybeDecrement(r, i, result->rejected,
					       result->last_log_index);
		if (retry) {
			/* Retry, ignoring errors. */
			tracef("log mismatch -> send old entries to %llu",
			       server->id);
			replicationProgress(r, i);
		}
		return 0;
	}

	/* In case of success the remote server is expected to send us back the
	 * value of prevLogIndex + len(entriesToAppend). If it has a longer log,
	 * it might be a leftover from previous terms. */
	last_index = result->last_log_index;
	if (last_index > logLastIndex(r->log)) {
		last_index = logLastIndex(r->log);
	}

	/* If the RPC succeeded, update our counters for this server.
	 *
	 * From Figure 3.1:
	 *
	 *   [Rules for servers] Leaders:
	 *
	 *   If successful update nextIndex and matchIndex for follower.
	 */
	if (!progressMaybeUpdate(r, i, last_index)) {
		return 0;
	}

	switch (progressState(r, i)) {
		case PROGRESS__SNAPSHOT:
			/* If a snapshot has been installed, transition back to
			 * probe */
			if (progressSnapshotDone(r, i)) {
				progressToProbe(r, i);
			}
			break;
		case PROGRESS__PROBE:
			/* Transition to pipeline */
			progressToPipeline(r, i);
	}

	/* If the server is currently being promoted and is catching with logs,
	 * update the information about the current catch-up round, and possibly
	 * proceed with the promotion. */
	is_being_promoted = r->leader_state.promotee_id != 0 &&
			    r->leader_state.promotee_id == server->id;
	if (is_being_promoted) {
		bool is_up_to_date = membershipUpdateCatchUpRound(r);
		if (is_up_to_date) {
			rv = triggerActualPromotion(r);
			if (rv != 0) {
				return rv;
			}
		}
	}

	/* Check if we can commit some new entries. */
	replicationQuorum(r, last_index);

	rv = replicationApply(r);
	if (rv != 0) {
		/* TODO: just log the error? */
	}

	/* Abort here we have been removed and we are not leaders anymore. */
	if (r->state != RAFT_LEADER) {
		goto out;
	}

	/* Get again the server index since it might have been removed from the
	 * configuration. */
	i = configurationIndexOf(&r->configuration, server->id);

	if (i < r->configuration.n) {
		/* If we are transferring leadership to this follower, check if
		 * its log is now up-to-date and, if so, send it a TimeoutNow
		 * RPC (unless we already did). */
		if (r->transfer != NULL && r->transfer->id == server->id) {
			if (progressPersistedIsUpToDate(r, i) &&
			    r->transfer->send.data == NULL) {
				rv = membershipLeadershipTransferStart(r);
				if (rv != 0) {
					membershipLeadershipTransferClose(r);
				}
			}
		}
		/* If this follower is in pipeline mode, send it more entries.
		 */
		if (progressState(r, i) == PROGRESS__PIPELINE) {
			replicationProgress(r, i);
		}
	}

out:
	return 0;
}

static void sendAppendEntriesResultCb(struct raft_io_send *req, int status)
{
	(void)status;
	RaftHeapFree(req);
}

static void sendAppendEntriesResult(
    struct raft *r,
    const struct raft_append_entries_result *result)
{
	struct raft_message message;
	struct raft_io_send *req;
	int rv;

	assert(r->state == RAFT_FOLLOWER);
	message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
	message.server_id = r->follower_state.current_leader.id;
	message.server_address = r->follower_state.current_leader.address;
	message.append_entries_result = *result;

	req = raft_malloc(sizeof *req);
	if (req == NULL) {
		return;
	}
	req->data = r;

	rv = r->io->send(r->io, req, &message, sendAppendEntriesResultCb);
	if (rv != 0) {
		raft_free(req);
	}
}

/**
 * State machine for handling of AppendEntries on the follower side.
 */
enum {
	AF_START,
	AF_DONE,
	AF_FAILED,
	AF_NR,
};

static struct sm_conf af_states[AF_NR] = {
	[AF_START] = {
		.name = "start",
		.allowed = BITS(AF_DONE)|BITS(AF_FAILED),
		.flags = SM_INITIAL,
	},
	[AF_DONE] = {
		.name = "done",
		.flags = SM_FINAL,
	},
	[AF_FAILED] = {
		.name = "failed",
		.flags = SM_FAILURE|SM_FINAL,
	},
};

static bool af_invariant(const struct sm *sm, int prev)
{
	(void)sm;
	(void)prev;
	return true;
}

/* Context for a write log entries request that was submitted by a follower. */
struct appendFollower
{
	struct raft *raft; /* Instance that has submitted the request */
	raft_index index;  /* Index of the first entry in the request. */
	struct raft_append_entries args;
	struct raft_io_append req;
	struct sm sm;
};

static void append_follower_done(struct appendFollower *req, int status)
{
	if (status == 0) {
		sm_move(&req->sm, AF_DONE);
	} else {
		sm_fail(&req->sm, AF_FAILED, status);
	}
	sm_fini(&req->sm);
	raft_free(req);
}

static void appendFollowerCb(struct raft_io_append *req, int status)
{
	struct appendFollower *request = req->data;
	struct raft *r = request->raft;
	struct raft_append_entries *args = &request->args;
	struct raft_append_entries_result result;
	size_t i;
	size_t j;
	int rv;

	tracef("I/O completed on follower: status %d", status);

	assert(args->entries != NULL);
	assert(args->n_entries > 0);

	assert(r->state == RAFT_FOLLOWER || r->state == RAFT_UNAVAILABLE);
	if (r->state == RAFT_UNAVAILABLE) {
		goto out;
	}
	assert(r->follower_state.append_in_flight_count > 0);
	r->follower_state.append_in_flight_count -= 1;

	result.term = r->current_term;
	result.version = RAFT_APPEND_ENTRIES_RESULT_VERSION;
	result.features = RAFT_DEFAULT_FEATURE_FLAGS;
	if (status != 0) {
		ErrMsgTransfer(r->io->errmsg, r->errmsg, "io");
		result.rejected = args->prev_log_index + 1;
		goto respond;
	}

	/* We received an InstallSnapshot RPC while these entries were being
	 * persisted to disk */
	if (replicationInstallSnapshotBusy(r)) {
		goto out;
	}

	i = updateLastStored(r, request->index, args->entries, args->n_entries);

	/* If none of the entries that we persisted is present anymore in our
	 * in-memory log, there's nothing to report or to do. We just discard
	 * them. */
	if (i == 0) {
		goto out;
	}

	/* Possibly apply configuration changes as uncommitted. */
	for (j = 0; j < i; j++) {
		struct raft_entry *entry = &args->entries[j];
		raft_index index = request->index + j;
		raft_term local_term = logTermOf(r->log, index);

		assert(local_term != 0 && local_term == entry->term);

		if (entry->type == RAFT_CHANGE) {
			rv = membershipUncommittedChange(r, index, entry);
			if (rv != 0) {
				goto out;
			}
		}
	}

	/* From Figure 3.1:
	 *
	 *   AppendEntries RPC: Receiver implementation: If leaderCommit >
	 *   commitIndex, set commitIndex = min(leaderCommit, index of last new
	 *   entry).
	 */
	if (args->leader_commit > r->commit_index &&
	    r->last_stored >= r->commit_index) {
		r->commit_index = min(args->leader_commit, r->last_stored);
		rv = replicationApply(r);
		if (rv != 0) {
			goto out;
		}
	}

	/* If our term number has changed since receiving these entries,
	 * our current_leader may have changed as well, so don't send a response
	 * to that server. */
	if (r->current_term != args->term) {
		tracef(
		    "new role or term since receiving entries -> don't "
		    "respond");
		goto out;
	}

	result.rejected = 0;

respond:
	result.last_log_index = r->last_stored;
	sendAppendEntriesResult(r, &result);

out:
	logRelease(r->log, request->index, request->args.entries,
		   request->args.n_entries);

	/* If the write failed, we need to truncate the log. */
	if (status != 0) {
		if (request->index <= logLastIndex(r->log)) {
			logTruncate(r->log, request->index);
		}
	}

	append_follower_done(request, status);
}

/* Check the log matching property against an incoming AppendEntries request.
 *
 * From Figure 3.1:
 *
 *   [AppendEntries RPC] Receiver implementation:
 *
 *   2. Reply false if log doesn't contain an entry at prevLogIndex whose
 *   term matches prevLogTerm.
 *
 * Return 0 if the check passed.
 *
 * Return 1 if the check did not pass and the request needs to be rejected.
 *
 * Return -1 if there's a conflict and we need to shutdown. */
static int checkLogMatchingProperty(struct raft *r,
				    const struct raft_append_entries *args)
{
	raft_term local_prev_term;

	/* If this is the very first entry, there's nothing to check. */
	if (args->prev_log_index == 0) {
		return 0;
	}

	local_prev_term = logTermOf(r->log, args->prev_log_index);
	if (local_prev_term == 0) {
		tracef("no entry at index %llu -> reject",
		       args->prev_log_index);
		return 1;
	}

	if (local_prev_term != args->prev_log_term) {
		if (args->prev_log_index <= r->commit_index) {
			/* Should never happen; something is seriously wrong! */
			tracef(
			    "conflicting terms %llu and %llu for entry %llu "
			    "(commit "
			    "index %llu) -> shutdown",
			    local_prev_term, args->prev_log_term,
			    args->prev_log_index, r->commit_index);
			return -1;
		}
		tracef("previous term mismatch -> reject");
		return 1;
	}

	return 0;
}

/* Delete from our log all entries that conflict with the ones in the given
 * AppendEntries request.
 *
 * From Figure 3.1:
 *
 *   [AppendEntries RPC] Receiver implementation:
 *
 *   3. If an existing entry conflicts with a new one (same index but
 *   different terms), delete the existing entry and all that follow it.
 *
 * The i output parameter will be set to the array index of the first new log
 * entry that we don't have yet in our log, among the ones included in the given
 * AppendEntries request. */
static int deleteConflictingEntries(struct raft *r,
				    const struct raft_append_entries *args,
				    const struct sm *append_sm,
				    size_t *i)
{
	size_t j;
	int rv;

	for (j = 0; j < args->n_entries; j++) {
		struct raft_entry *entry = &args->entries[j];
		raft_index entry_index = args->prev_log_index + 1 + j;
		raft_term local_term = logTermOf(r->log, entry_index);

		if (local_term > 0 && local_term != entry->term) {
			if (entry_index <= r->commit_index) {
				/* Should never happen; something is seriously
				 * wrong! */
				tracef(
				    "new index conflicts with committed entry "
				    "-> shutdown");
				return RAFT_SHUTDOWN;
			}

			tracef("log mismatch -> truncate (%llu)", entry_index);

			/* Possibly discard uncommitted configuration changes.
			 */
			if (r->configuration_uncommitted_index >= entry_index) {
				rv = membershipRollback(r);
				if (rv != 0) {
					return rv;
				}
			}

			/* Delete all entries from this index on because they
			 * don't match. */
			struct raft_io_truncate *trunc = raft_malloc(sizeof(*trunc));
			if (trunc == NULL) {
				return RAFT_NOMEM;
			}
			rv = r->io->truncate(r->io, trunc, entry_index);
			sm_relate(append_sm, &trunc->sm);
			if (rv != 0) {
				sm_fini(&trunc->sm);
				raft_free(trunc);
				return rv;
			}
			for (raft_index x = entry_index; x <= logLastIndex(r->log); x++) {
				const struct raft_entry *e = logGet(r->log, x);
				assert(e != NULL);
				const struct sm *entry_sm = log_get_entry_sm(r->log, e->term, x);
				assert(entry_sm != NULL);
				sm_relate(append_sm, entry_sm);
			}
			logTruncate(r->log, entry_index);

			/* Drop information about previously stored entries that
			 * have just been discarded. */
			if (r->last_stored >= entry_index) {
				r->last_stored = entry_index - 1;
			}

			/* We want to append all entries from here on, replacing
			 * anything that we had before. */
			break;
		} else if (local_term == 0) {
			/* We don't have an entry at this index, so we want to
			 * append this new one and all the subsequent ones. */
			break;
		}
	}

	*i = j;

	return 0;
}

int replicationAppend(struct raft *r,
		      const struct raft_append_entries *args,
		      raft_index *rejected,
		      bool *async)
{
	struct appendFollower *request;
	int match;
	size_t n;
	size_t i;
	size_t j;
	bool reinstated;
	const struct sm *entry_sm;
	int rv;

	assert(r != NULL);
	assert(args != NULL);
	assert(rejected != NULL);
	assert(async != NULL);

	assert(r->state == RAFT_FOLLOWER);

	request = raft_malloc(sizeof *request);
	if (request == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}
	sm_init(&request->sm, af_invariant, NULL, af_states, "append-follower",
		AF_START);

	*rejected = args->prev_log_index;
	*async = false;

	/* Check the log matching property. */
	match = checkLogMatchingProperty(r, args);
	if (match == 1) {
		append_follower_done(request, 0);
		return 0;
	} else if (match != 0) {
		assert(match == -1);
		rv = RAFT_SHUTDOWN;
		goto err_after_request_alloc;
	}

	/* Delete conflicting entries. */
	rv = deleteConflictingEntries(r, args, &request->sm, &i);
	if (rv != 0) {
		goto err_after_request_alloc;
	}

	*rejected = 0;

	n = args->n_entries - i; /* Number of new entries */
	sm_attr(&request->sm, "n", "%zu", n);

	/* If this is an empty AppendEntries, there's nothing to write. However
	 * we still want to check if we can commit some entry. However, don't
	 * commit anything while a snapshot install is busy, r->last_stored will
	 * be 0 in that case.
	 *
	 * From Figure 3.1:
	 *
	 *   AppendEntries RPC: Receiver implementation: If leaderCommit >
	 *   commitIndex, set commitIndex = min(leaderCommit, index of last new
	 *   entry).
	 */
	if (n == 0) {
		if ((args->leader_commit > r->commit_index) &&
		    r->last_stored >= r->commit_index &&
		    !replicationInstallSnapshotBusy(r)) {
			r->commit_index =
			    min(args->leader_commit, r->last_stored);
			rv = replicationApply(r);
			if (rv != 0) {
				goto err_after_request_alloc;
			}
		}

		append_follower_done(request, 0);
		return 0;
	}

	*async = true;

	request->raft = r;
	request->args = *args;
	/* Index of first new entry */
	request->index = args->prev_log_index + 1 + i;

	/* Update our in-memory log to reflect that we received these entries.
	 * We'll notify the leader of a successful append once the write entries
	 * request that we issue below actually completes.  */
	for (j = 0; j < n; j++) {
		struct raft_entry *entry = &args->entries[i + j];
		struct raft_entry copy;

		/* We are trying to append an entry at index X with term T to
		 * our in-memory log. If we've gotten this far, we know that the
		 * log *logically* has no entry at this index. However, it's
		 * possible that we're still hanging on to such an entry,
		 * because we previously tried to append and replicate it, and
		 * the associated disk write failed, but some send requests are
		 * still pending that refer to it. Since the log is not capable
		 * of tracking multiple independent entries that share an index
		 * and term, we just piggyback on the already-stored entry in
		 * this case. */
		rv =
		    logReinstate(r->log, entry->term, entry->type, &reinstated);
		if (rv != 0) {
			goto err_after_log_append;
		}
		if (!reinstated) {
			/* TODO This copy should not strictly be necessary, as
			 * the batch logic will take care of freeing the batch
			 * buffer in which the entries are received. However,
			 * this would lead to memory spikes in certain edge
			 * cases.
			 * https://github.com/canonical/dqlite/issues/276
			 */
			rv = entryCopy(entry, &copy);
			if (rv != 0) {
				goto err_after_log_append;
			}

			rv = logAppend(r->log, copy.term, copy.type, copy.buf,
				       (struct raft_entry_local_data){}, false, NULL);
			if (rv != 0) {
				goto err_after_log_append;
			}
		}

		entry_sm = log_get_entry_sm(r->log, entry->term, request->index + j);
		assert(entry_sm != NULL);
		sm_relate(&request->sm, entry_sm);
	}

	/* Acquire the relevant entries from the log. */
	rv = logAcquire(r->log, request->index, &request->args.entries,
			&request->args.n_entries);
	if (rv != 0) {
		goto err_after_log_append;
	}

	assert(request->args.n_entries == n);
	if (request->args.n_entries == 0) {
		tracef("No log entries found at index %llu", request->index);
		ErrMsgPrintf(r->errmsg, "No log entries found at index %llu",
			     request->index);
		rv = RAFT_SHUTDOWN;
		goto err_after_acquire_entries;
	}

	request->req.data = request;
	rv = r->io->append(r->io, &request->req, request->args.entries,
			   request->args.n_entries, appendFollowerCb);
	/* FIXME this relates the sm of the appendFollower request to that of
	 * the UvAppend request. Ideally we would instead relate the sms of
	 * each involved log entry to the UvAppend request, but this seems to
	 * work poorly with chronoscope's chart visualization, causing it to
	 * draw the same UvAppend request many times. */
	sm_relate(&request->sm, &request->req.sm);
	if (rv != 0) {
		ErrMsgTransfer(r->io->errmsg, r->errmsg, "io");
		goto err_in_append;
	}
	r->follower_state.append_in_flight_count += 1;

	entryBatchesDestroy(args->entries, args->n_entries);
	return 0;

err_in_append:
	sm_fini(&request->req.sm);
err_after_acquire_entries:
	/* Release the entries related to the IO request */
	logRelease(r->log, request->index, request->args.entries,
		   request->args.n_entries);
err_after_log_append:
	/* Release all entries added to the in-memory log, making
	 * sure the in-memory log and disk don't diverge, leading
	 * to future log entries not being persisted to disk.
	 */
	if (j != 0) {
		logTruncate(r->log, request->index);
	}
err_after_request_alloc:
	append_follower_done(request, rv);

err:
	assert(rv != 0);
	return rv;
}

struct recvInstallSnapshot
{
	struct raft *raft;
	struct raft_snapshot snapshot;
	raft_term term; /* Used to check for state transitions. */
};

static void installSnapshotCb(struct raft_io_snapshot_put *req, int status)
{
	struct recvInstallSnapshot *request = req->data;
	struct raft *r = request->raft;
	struct raft_snapshot *snapshot = &request->snapshot;
	struct raft_append_entries_result result;
	bool should_respond = true;
	int rv;

	/* We avoid converting to candidate state while installing a snapshot.
	 */
	assert(r->state == RAFT_FOLLOWER || r->state == RAFT_UNAVAILABLE);

	r->snapshot.put.data = NULL;

	result.term = r->current_term;
	result.version = RAFT_APPEND_ENTRIES_RESULT_VERSION;
	result.features = RAFT_DEFAULT_FEATURE_FLAGS;
	result.rejected = 0;

	/* If we are shutting down, let's discard the result. */
	if (r->state == RAFT_UNAVAILABLE) {
		tracef(
		    "shutting down -> discard result of snapshot installation");
		should_respond = false;
		goto discard;
	}
	/* If the request is from a previous term, it means that someone else
	 * became a candidate while we were installing the snapshot. In that
	 * case, we want to install the snapshot anyway, but our "current
	 * leader" may no longer be the same as the server that sent the install
	 * request, so we shouldn't send a response to that server. */
	if (request->term != r->current_term) {
		tracef(
		    "new term since receiving snapshot -> install but don't "
		    "respond");
		should_respond = false;
	}

	if (status != 0) {
		tracef("save snapshot %llu: %s", snapshot->index,
		       raft_strerror(status));
		goto discard;
	}

	/* From Figure 5.3:
	 *
	 *   7. Discard the entire log
	 *   8. Reset state machine using snapshot contents (and load lastConfig
	 *      as cluster configuration).
	 */
	rv = snapshotRestore(r, snapshot);
	if (rv != 0) {
		tracef("restore snapshot %llu: %s", snapshot->index,
		       raft_strerror(status));
		goto discard;
	}

	tracef("restored snapshot with last index %llu", snapshot->index);

	goto respond;

discard:
	/* In case of error we must also free the snapshot data buffer and free
	 * the configuration. */
	result.rejected = snapshot->index;
	raft_free(snapshot->bufs[0].base);
	raft_free(snapshot->bufs);
	raft_configuration_close(&snapshot->configuration);

respond:
	if (should_respond) {
		result.last_log_index = r->last_stored;
		sendAppendEntriesResult(r, &result);
	}

	raft_free(request);
}

int replicationInstallSnapshot(struct raft *r,
			       const struct raft_install_snapshot *args,
			       raft_index *rejected,
			       bool *async)
{
	struct recvInstallSnapshot *request;
	struct raft_snapshot *snapshot;
	raft_term local_term;
	int rv;

	assert(r->state == RAFT_FOLLOWER);

	*rejected = args->last_index;
	*async = false;

	/* If we are taking a snapshot ourselves or installing a snapshot,
	 * ignore the request, the leader will eventually retry. TODO: we should
	 * do something smarter. */
	if (r->snapshot.pending.term != 0 || r->snapshot.put.data != NULL) {
		*async = true;
		tracef("already taking or installing snapshot");
		return RAFT_BUSY;
	}

	/* If our last snapshot is more up-to-date, this is a no-op */
	if (r->log->snapshot.last_index >= args->last_index) {
		tracef("have more recent snapshot");
		*rejected = 0;
		return 0;
	}

	/* If we already have all entries in the snapshot, this is a no-op */
	local_term = logTermOf(r->log, args->last_index);
	if (local_term != 0 && local_term >= args->last_term) {
		tracef("have all entries");
		*rejected = 0;
		return 0;
	}

	*async = true;

	/* Preemptively update our in-memory state. */
	logRestore(r->log, args->last_index, args->last_term);

	r->last_stored = 0;

	request = raft_malloc(sizeof *request);
	if (request == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}
	request->raft = r;
	request->term = r->current_term;

	snapshot = &request->snapshot;
	snapshot->term = args->last_term;
	snapshot->index = args->last_index;
	snapshot->configuration_index = args->conf_index;
	snapshot->configuration = args->conf;

	snapshot->bufs = raft_malloc(sizeof *snapshot->bufs);
	if (snapshot->bufs == NULL) {
		rv = RAFT_NOMEM;
		goto err_after_request_alloc;
	}
	snapshot->bufs[0] = args->data;
	snapshot->n_bufs = 1;

	assert(r->snapshot.put.data == NULL);
	r->snapshot.put.data = request;
	rv = r->io->snapshot_put(r->io,
				 0 /* zero trailing means replace everything */,
				 &r->snapshot.put, snapshot, installSnapshotCb);
	if (rv != 0) {
		tracef("snapshot_put failed %d", rv);
		goto err_after_bufs_alloc;
	}

	return 0;

err_after_bufs_alloc:
	raft_free(snapshot->bufs);
	r->snapshot.put.data = NULL;
err_after_request_alloc:
	raft_free(request);
err:
	assert(rv != 0);
	return rv;
}

/* Apply a RAFT_COMMAND entry that has been committed. */
static int applyCommand(struct raft *r,
			const raft_index index,
			const struct raft_buffer *buf)
{
	struct raft_apply *req;
	void *result;
	int rv;

	rv = r->fsm->apply(r->fsm, buf, &result);
	if (rv != 0) {
		return rv;
	}

	r->last_applied = index;

	req = (struct raft_apply *)getRequest(r, index, RAFT_COMMAND);
	if (req == NULL) {
		return 0;
	}
	queue_remove(&req->queue);
	sm_move(&req->sm, REQUEST_COMPLETE);
	sm_fini(&req->sm);
	if (req->cb != NULL) {
		req->cb(req, 0, result);
	}
	return 0;
}

/* Fire the callback of a barrier request whose entry has been committed. */
static void applyBarrier(struct raft *r, const raft_index index)
{
	r->last_applied = index;

	struct raft_barrier *req;
	req = (struct raft_barrier *)getRequest(r, index, RAFT_BARRIER);
	if (req == NULL) {
		return;
	}
	queue_remove(&req->queue);
	if (req->cb != NULL) {
		req->cb(req, 0);
	}
}

/* Apply a RAFT_CHANGE entry that has been committed. */
static void applyChange(struct raft *r, const raft_index index)
{
	struct raft_change *req;

	assert(index > 0);

	/* If this is an uncommitted configuration that we had already applied
	 * when submitting the configuration change (for leaders) or upon
	 * receiving it via an AppendEntries RPC (for followers), then reset the
	 * uncommitted index, since that uncommitted configuration is now
	 * committed. */
	if (r->configuration_uncommitted_index == index) {
		tracef("configuration at index:%llu is committed.", index);
		r->configuration_uncommitted_index = 0;
	}

	r->configuration_committed_index = index;
	r->last_applied = index;

	if (r->state == RAFT_LEADER) {
		const struct raft_server *server;
		req = r->leader_state.change;
		r->leader_state.change = NULL;

		/* If we are leader but not part of this new configuration, step
		 * down.
		 *
		 * From Section 4.2.2:
		 *
		 *   In this approach, a leader that is removed from the
		 * configuration steps down once the Cnew entry is committed.
		 */
		server = configurationGet(&r->configuration, r->id);
		if (server == NULL || server->role != RAFT_VOTER) {
			tracef(
			    "leader removed from config or no longer voter "
			    "server: %p",
			    (void *)server);
			convertToFollower(r);
		}

		if (req != NULL && req->cb != NULL) {
			req->cb(req, 0);
		}
	}
}

static bool shouldTakeSnapshot(struct raft *r)
{
	/* If we are shutting down, let's not do anything. */
	if (r->state == RAFT_UNAVAILABLE) {
		return false;
	}

	/* If a snapshot is already in progress or we're installing a snapshot,
	 * we don't want to start another one. */
	if (r->snapshot.pending.term != 0 || r->snapshot.put.data != NULL) {
		return false;
	};

	/* If we didn't reach the threshold yet, do nothing. */
	if (r->last_applied - r->log->snapshot.last_index <
	    r->snapshot.threshold) {
		return false;
	}

	return true;
}

/*
 * When taking a snapshot, ownership of the snapshot data is with raft if
 * `snapshot_finalize` is NULL.
 */
static void takeSnapshotClose(struct raft *r, struct raft_snapshot *s)
{
	if (r->fsm->version == 1 ||
	    (r->fsm->version > 1 && r->fsm->snapshot_finalize == NULL)) {
		snapshotClose(s);
		return;
	}

	configurationClose(&s->configuration);
	r->fsm->snapshot_finalize(r->fsm, &s->bufs, &s->n_bufs);
}

static void takeSnapshotCb(struct raft_io_snapshot_put *req, int status)
{
	struct raft *r = req->data;
	struct raft_snapshot *snapshot;
	int rv;

	r->snapshot.put.data = NULL;
	snapshot = &r->snapshot.pending;

	if (status != 0) {
		tracef("snapshot %lld at term %lld: %s", snapshot->index,
		       snapshot->term, raft_strerror(status));
		goto out;
	}

	/* Cache the configuration contained in the snapshot. While the snapshot
	 * was written, new configuration changes could have been committed,
	 * these changes will not be purged from the log by this snapshot.
	 * However we still cache the configuration for consistency. */
	configurationClose(&r->configuration_last_snapshot);
	rv = configurationCopy(&snapshot->configuration,
			       &r->configuration_last_snapshot);
	if (rv != 0) {
		/* TODO: make this a hard fault, because if we have no backup
		 * and the log was truncated it will be impossible to rollback
		 * an aborted configuration change. */
		tracef("failed to backup last committed configuration.");
	}
	logSnapshot(r->log, snapshot->index, r->snapshot.trailing);
out:
	takeSnapshotClose(r, snapshot);
	r->snapshot.pending.term = 0;
}

static int putSnapshot(struct raft *r,
		       struct raft_snapshot *snapshot,
		       raft_io_snapshot_put_cb cb)
{
	int rv;
	assert(r->snapshot.put.data == NULL);
	r->snapshot.put.data = r;
	rv = r->io->snapshot_put(r->io, r->snapshot.trailing, &r->snapshot.put,
				 snapshot, cb);
	if (rv != 0) {
		takeSnapshotClose(r, snapshot);
		r->snapshot.pending.term = 0;
		r->snapshot.put.data = NULL;
	}

	return rv;
}

static void takeSnapshotDoneCb(struct raft_io_async_work *take, int status)
{
	struct raft *r = take->data;
	struct raft_snapshot *snapshot = &r->snapshot.pending;
	int rv;

	raft_free(take);

	if (status != 0) {
		tracef("take snapshot failed %s", raft_strerror(status));
		takeSnapshotClose(r, snapshot);
		r->snapshot.pending.term = 0;
		r->snapshot.put.data = NULL;
		return;
	}

	rv = putSnapshot(r, snapshot, takeSnapshotCb);
	if (rv != 0) {
		tracef("put snapshot failed %d", rv);
	}
}

static int takeSnapshotAsync(struct raft_io_async_work *take)
{
	struct raft *r = take->data;
	tracef("take snapshot async at %lld", r->snapshot.pending.index);
	struct raft_snapshot *snapshot = &r->snapshot.pending;
	return r->fsm->snapshot_async(r->fsm, &snapshot->bufs,
				      &snapshot->n_bufs);
}

static int takeSnapshot(struct raft *r)
{
	struct raft_snapshot *snapshot;
	int rv;

	tracef("take snapshot at %lld", r->last_applied);

	snapshot = &r->snapshot.pending;
	snapshot->index = r->last_applied;
	snapshot->term = logTermOf(r->log, r->last_applied);
	snapshot->bufs = NULL;
	snapshot->n_bufs = 0;

	rv = membershipFetchLastCommittedConfiguration(
	    r, &snapshot->configuration);
	if (rv != 0) {
		goto abort;
	}
	snapshot->configuration_index = r->configuration_committed_index;

	rv = r->fsm->snapshot(r->fsm, &snapshot->bufs, &snapshot->n_bufs);
	if (rv != 0) {
		/* Ignore transient errors. We'll retry next time. */
		if (rv == RAFT_BUSY) {
			rv = 0;
		}
		raft_configuration_close(&snapshot->configuration);
		goto abort;
	}

	bool sync_snapshot =
	    r->fsm->version < 3 || r->fsm->snapshot_async == NULL;
	if (sync_snapshot) {
		/* putSnapshot will clean up config and buffers in case of error
		 */
		return putSnapshot(r, snapshot, takeSnapshotCb);
	} else {
		struct raft_io_async_work *take = raft_malloc(sizeof(*take));
		if (take == NULL) {
			rv = RAFT_NOMEM;
			goto abort_after_snapshot;
		}
		take->data = r;
		take->work = takeSnapshotAsync;
		rv = r->io->async_work(r->io, take, takeSnapshotDoneCb);
		if (rv != 0) {
			raft_free(take);
			goto abort_after_snapshot;
		}
	}

	return 0;

abort_after_snapshot:
	/* Closes config and finalizes snapshot */
	takeSnapshotClose(r, snapshot);
abort:
	r->snapshot.pending.term = 0;
	return rv;
}

int replicationApply(struct raft *r)
{
	raft_index index;
	int rv = 0;

	assert(r->state == RAFT_LEADER || r->state == RAFT_FOLLOWER);
	assert(r->last_applied <= r->commit_index);

	if (r->last_applied == r->commit_index) {
		/* Nothing to do. */
		return 0;
	}

	for (index = r->last_applied + 1; index <= r->commit_index; index++) {
		const struct raft_entry *entry = logGet(r->log, index);
		if (entry == NULL) {
			/* This can happen while installing a snapshot */
			tracef("replicationApply - ENTRY NULL");
			return 0;
		}
		struct sm *entry_sm = log_get_entry_sm(r->log, entry->term, index);
		assert(entry_sm != NULL);

		assert(entry->type == RAFT_COMMAND ||
		       entry->type == RAFT_BARRIER ||
		       entry->type == RAFT_CHANGE);

		sm_move(entry_sm, ENTRY_COMMITTED);
		switch (entry->type) {
			case RAFT_COMMAND:
				rv = applyCommand(r, index, &entry->buf);
				break;
			case RAFT_BARRIER:
				applyBarrier(r, index);
				rv = 0;
				break;
			case RAFT_CHANGE:
				applyChange(r, index);
				rv = 0;
				break;
			default:
				rv = 0; /* For coverity. This case can't be
					   taken. */
				break;
		}

		if (rv == 0) {
			sm_move(entry_sm, ENTRY_APPLIED);
		} else {
			break;
		}
	}

	if (shouldTakeSnapshot(r)) {
		rv = takeSnapshot(r);
	}

	return rv;
}

void replicationQuorum(struct raft *r, const raft_index index)
{
	size_t votes = 0;
	size_t i;
	raft_term term;

	assert(r->state == RAFT_LEADER);

	if (index <= r->commit_index) {
		return;
	}

	term = logTermOf(r->log, index);

	/* TODO: fuzzy-test --seed 0x8db5fccc replication/entries/partitioned
	 * fails the assertion below. */
	if (term == 0) {
		return;
	}
	// assert(logTermOf(r->log, index) > 0);
	assert(!(term > r->current_term));

	/* Don't commit entries from previous terms by counting replicas. */
	if (term < r->current_term) {
		return;
	}

	for (i = 0; i < r->configuration.n; i++) {
		struct raft_server *server = &r->configuration.servers[i];
		if (server->role != RAFT_VOTER) {
			continue;
		}
		if (r->leader_state.progress[i].match_index >= index) {
			votes++;
		}
	}

	if (votes > configurationVoterCount(&r->configuration) / 2) {
		r->commit_index = index;
		tracef("new commit index %llu", r->commit_index);
	}

	return;
}

inline bool replicationInstallSnapshotBusy(struct raft *r)
{
	return r->last_stored == 0 && r->snapshot.put.data != NULL;
}

#undef tracef
