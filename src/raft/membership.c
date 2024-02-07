#include "membership.h"

#include "../raft.h"
#include "../tracing.h"
#include "assert.h"
#include "configuration.h"
#include "err.h"
#include "heap.h"
#include "log.h"
#include "progress.h"

int membershipCanChangeConfiguration(struct raft *r)
{
	int rv;

	if (r->state != RAFT_LEADER || r->transfer != NULL) {
		tracef("NOT LEADER");
		rv = RAFT_NOTLEADER;
		goto err;
	}

	if (r->configuration_uncommitted_index != 0) {
		tracef("r->configuration_uncommitted_index %llu",
		       r->configuration_uncommitted_index);
		rv = RAFT_CANTCHANGE;
		goto err;
	}

	if (r->leader_state.promotee_id != 0) {
		tracef("r->leader_state.promotee_id %llu",
		       r->leader_state.promotee_id);
		rv = RAFT_CANTCHANGE;
		goto err;
	}

	/* In order to become leader at all we are supposed to have committed at
	 * least the initial configuration at index 1. */
	assert(r->configuration_committed_index > 0);

	/* The index of the last committed configuration can't be greater than
	 * the last log index. */
	assert(logLastIndex(r->log) >= r->configuration_committed_index);

	/* No catch-up round should be in progress. */
	assert(r->leader_state.round_number == 0);
	assert(r->leader_state.round_index == 0);
	assert(r->leader_state.round_start == 0);

	return 0;

err:
	assert(rv != 0);
	ErrMsgFromCode(r->errmsg, rv);
	return rv;
}

int membershipFetchLastCommittedConfiguration(struct raft *r,
					      struct raft_configuration *conf)
{
	const struct raft_entry *entry;
	int rv;

	/* Try to get the entry at r->configuration_committed_index from the
	 * log. If the entry is not present in the log anymore because the log
	 * was truncated after a snapshot, we can just use
	 * configuration_last_snapshot, which we cached when we took or restored
	 * the snapshot and is guaranteed to match the content that the entry at
	 * r->configuration_committed_index had. */
	entry = logGet(r->log, r->configuration_committed_index);
	if (entry != NULL) {
		rv = configurationDecode(&entry->buf, conf);
	} else {
		assert(r->configuration_last_snapshot.n > 0);
		rv = configurationCopy(&r->configuration_last_snapshot, conf);
	}
	if (rv != 0) {
		return rv;
	}

	return 0;
}

bool membershipUpdateCatchUpRound(struct raft *r)
{
	unsigned server_index;
	raft_index match_index;
	raft_index last_index;
	raft_time now = r->io->time(r->io);
	raft_time round_duration;
	bool is_up_to_date;
	bool is_fast_enough;

	assert(r->state == RAFT_LEADER);
	assert(r->leader_state.promotee_id != 0);

	server_index = configurationIndexOf(&r->configuration,
					    r->leader_state.promotee_id);
	assert(server_index < r->configuration.n);

	match_index = progressMatchIndex(r, server_index);

	/* If the server did not reach the target index for this round, it did
	 * not catch up. */
	if (match_index < r->leader_state.round_index) {
		tracef(
		    "member (index: %u) not yet caught up match_index:%llu "
		    "round_index:%llu",
		    server_index, match_index, r->leader_state.round_index);
		return false;
	}

	last_index = logLastIndex(r->log);
	round_duration = now - r->leader_state.round_start;

	is_up_to_date = match_index == last_index;
	is_fast_enough = round_duration < r->election_timeout;

	tracef("member is_up_to_date:%d is_fast_enough:%d", is_up_to_date,
	       is_fast_enough);

	/* If the server's log is fully up-to-date or the round that just
	 * terminated was fast enough, then the server as caught up. */
	if (is_up_to_date || is_fast_enough) {
		r->leader_state.round_number = 0;
		r->leader_state.round_index = 0;
		r->leader_state.round_start = 0;

		return true;
	}

	/* If we get here it means that this catch-up round is complete, but
	 * there are more entries to replicate, or it was not fast enough. Let's
	 * start a new round. */
	r->leader_state.round_number++;
	r->leader_state.round_index = last_index;
	r->leader_state.round_start = now;

	return false;
}

int membershipUncommittedChange(struct raft *r,
				const raft_index index,
				const struct raft_entry *entry)
{
	struct raft_configuration configuration;
	int rv;
	char msg[128];

	assert(r != NULL);
	assert(r->state == RAFT_FOLLOWER);
	assert(entry != NULL);
	assert(entry->type == RAFT_CHANGE);

	rv = configurationDecode(&entry->buf, &configuration);
	if (rv != 0) {
		tracef("failed to decode configuration at index:%llu", index);
		goto err;
	}

	/* ignore errors */
	snprintf(msg, sizeof(msg), "uncommitted config change at index:%llu",
		 index);
	configurationTrace(r, &configuration, msg);

	raft_configuration_close(&r->configuration);

	r->configuration = configuration;
	r->configuration_uncommitted_index = index;

	return 0;

err:
	assert(rv != 0);
	return rv;
}

int membershipRollback(struct raft *r)
{
	int rv;

	assert(r != NULL);
	assert(r->state == RAFT_FOLLOWER);
	assert(r->configuration_uncommitted_index > 0);
	tracef("roll back membership");

	/* Fetch the last committed configuration entry. */
	assert(r->configuration_committed_index != 0);

	/* Replace the current configuration with the last committed one. */
	configurationClose(&r->configuration);
	rv = membershipFetchLastCommittedConfiguration(r, &r->configuration);
	if (rv != 0) {
		return rv;
	}

	configurationTrace(r, &r->configuration, "roll back config");
	r->configuration_uncommitted_index = 0;
	return 0;
}

void membershipLeadershipTransferInit(struct raft *r,
				      struct raft_transfer *req,
				      raft_id id,
				      raft_transfer_cb cb)
{
	req->cb = cb;
	req->id = id;
	req->start = r->io->time(r->io);
	req->send.data = NULL;
	r->transfer = req;
}

static void membershipLeadershipSendCb(struct raft_io_send *send, int status)
{
	(void)status;
	RaftHeapFree(send);
}

int membershipLeadershipTransferStart(struct raft *r)
{
	const struct raft_server *server;
	struct raft_message message;
	struct raft_io_send *send;
	int rv;
	assert(r->transfer->send.data == NULL);
	server = configurationGet(&r->configuration, r->transfer->id);
	assert(server != NULL);
	if (server == NULL) {
		tracef("transferee server not found in configuration");
		return -1;
	}

	/* Don't use the raft_io_send object embedded in struct raft_transfer,
	 * since the two objects must have different lifetimes. For example
	 * raft_io_send might live longer than raft_transfer, see #396.
	 *
	 * Ideally we should remove the embedded struct raft_io_send send field
	 * from struct raft_transfer, and replace it with a raft_io_send *send
	 * pointer, that we set to the raft_io_send object allocated in this
	 * function. This would break ABI compatibility though. */
	send = RaftHeapMalloc(sizeof *send);
	if (send == NULL) {
		return RAFT_NOMEM;
	}

	message.type = RAFT_IO_TIMEOUT_NOW;
	message.server_id = server->id;
	message.server_address = server->address;
	message.timeout_now.term = r->current_term;
	message.timeout_now.last_log_index = logLastIndex(r->log);
	message.timeout_now.last_log_term = logLastTerm(r->log);

	/* Set the data attribute of the raft_io_send object embedded in
	 * raft_transfer. This is needed because we historically used it as a
	 * flag to indicate that a transfer request was sent. See the
	 * replicationUpdate function. */
	r->transfer->send.data = r;

	send->data = r;

	rv = r->io->send(r->io, send, &message, membershipLeadershipSendCb);
	if (rv != 0) {
		RaftHeapFree(send);
		ErrMsgTransferf(r->io->errmsg, r->errmsg,
				"send timeout now to %llu", server->id);
		return rv;
	}
	return 0;
}

void membershipLeadershipTransferClose(struct raft *r)
{
	struct raft_transfer *req = r->transfer;
	raft_transfer_cb cb = req->cb;
	r->transfer = NULL;
	if (cb != NULL) {
		cb(req);
	}
}
