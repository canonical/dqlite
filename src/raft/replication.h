/* Log replication logic and helpers. */

#ifndef REPLICATION_H_
#define REPLICATION_H_

#include "../raft.h"

/* Send AppendEntries RPC messages to all followers to which no AppendEntries
 * was sent in the last heartbeat interval. */
int replicationHeartbeat(struct raft *r);

/* Start a local disk write for entries from the given index onwards, and
 * trigger replication against all followers, typically sending AppendEntries
 * RPC messages with outstanding log entries. */
int replicationTrigger(struct raft *r, raft_index index);

/* Possibly send an AppendEntries or an InstallSnapshot RPC message to the
 * server with the given index.
 *
 * The rules to decide whether or not to send a message are:
 *
 * - If we have sent an InstallSnapshot RPC recently and we haven't yet received
 *   a response, then don't send any new message.
 *
 * - If we are probing the follower (i.e. we haven't received a successful
 *   response during the last heartbeat interval), then send a message only if
 *   haven't sent any during the last heartbeat interval.
 *
 * - If we are pipelining entries to the follower, then send any new entries
 *   haven't yet sent.
 *
 * If a message should be sent, the rules to decide what type of message to send
 * and what it should contain are:
 *
 * - If we don't have anymore the first entry that should be sent to the
 *   follower, then send an InstallSnapshot RPC with the last snapshot.
 *
 * - If we still have the first entry to send, then send all entries from that
     index onward (possibly zero).
 *
 * This function must be called only by leaders. */
int replicationProgress(struct raft *r, unsigned i);

/* Update the replication state (match and next indexes) for the given server
 * using the given AppendEntries RPC result.
 *
 * Possibly send to the server a new set of entries or a snapshot if the result
 * was unsuccessful because of missing entries or if new entries were added to
 * our log in the meantime.
 *
 * It must be called only by leaders. */
int replicationUpdate(struct raft *r,
		      const struct raft_server *server,
		      const struct raft_append_entries_result *result);

/* Append the log entries in the given request if the Log Matching Property is
 * satisfied.
 *
 * The rejected output parameter will be set to 0 if the Log Matching Property
 * was satisfied, or to args->prev_log_index if not.
 *
 * The async output parameter will be set to true if some of the entries in the
 * request were not present in our log, and a disk write was started to persist
 * them to disk. The entries will still be appended immediately to our in-memory
 * copy of the log, but an AppendEntries result message will be sent only once
 * the disk write completes and the I/O callback is invoked.
 *
 * It must be called only by followers. */
int replicationAppend(struct raft *r,
		      struct raft_append_entries *args,
		      raft_index *rejected,
		      bool *async);

int replicationInstallSnapshot(struct raft *r,
			       const struct raft_install_snapshot *args,
			       raft_index *rejected,
			       bool *async);

/* Returns `true` if the raft instance is currently installing a snapshot */
bool replicationInstallSnapshotBusy(struct raft *r);

/* Apply any committed entry that was not applied yet.
 *
 * It must be called by leaders or followers. */
int replicationApply(struct raft *r);

/* Check if a quorum has been reached for the given log index, and update the
 * commit index accordingly if so.
 *
 * From Figure 3.1:
 *
 *   [Rules for servers] Leaders:
 *
 *   If there exists an N such that N > commitIndex, a majority of
 *   matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N */
void replicationQuorum(struct raft *r, const raft_index index);

#endif /* REPLICATION_H_ */
