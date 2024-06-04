#include "recv_install_snapshot.h"

#include "../tracing.h"
#include "assert.h"
#include "convert.h"
#include "flags.h"
#include "log.h"
#include "recv.h"
#include "replication.h"

#include "../lib/sm.h"

/**
 * =Overview
 *
 * This detailed level design is based on PL018 and describes
 * significant implementation details of data structures, RPCs
 * introduced in it; provides model of operation and failure handling
 * based on Leader's and Follower's states.
 *
 * =Data structures
 *
 * Among other structures it's needed to introduce a (persistent) container
 * `HT` to efficiently store and map checksums to their page numbers on both
 * the leader's and  follower's side. HT is implemented on top of sqlite3
 * database with unix VFS. Every database corresponds to a raft-related
 * database and maintains the following schema:
 *
 * CREATE TABLE "map" ("checksum" INTEGER NOT NULL, "pageno" INTEGER NOT NULL UNIQUE)
 * CREATE INDEX map_idx on map(checksum);
 *
 * Each database stores a mapping from checksum to page number. This
 * provides an efficient way to insert and lookup records
 * corresponding to the checksums and page numbers.
 */

typedef unsigned int checksum_t;
typedef unsigned long int pageno_t;

struct page_checksum_t {
	pageno_t   page_no;
	checksum_t checksum;
};

struct page_from_to {
	pageno_t from;
	pageno_t to;
};

enum result {
	OK = 0,
	FAILED = 1,
	DONE = 2,
};

struct raft_signature {
	int version;

	const char *db;
	struct page_from_to page_from_to;
	unsigned int cs_page_no;
};

struct raft_signature_result {
	int version;

	const char *db;
	struct page_checksum_t *cs;
	unsigned int cs_nr;
	unsigned int cs_page_no;
	enum result result;
};

struct raft_install_snapshot_mv {
	int version;

	const char *db;
	struct page_from_to *mv;
	unsigned int mv_nr;
};

struct raft_install_snapshot_mv_result {
	int version;

	const char *db;
	pageno_t last_known_page_no; /* used for retries and message losses */
	enum result result;
};

struct raft_install_snapshot_cp {
	int version;

	const char *db;
	pageno_t page_no;
	struct raft_buffer page_data;
	enum result result;
};

struct raft_install_snapshot_cp_result {
	int version;

	pageno_t last_known_page_no; /* used for retries and message losses */
	enum result result;
};

/**
 * =Operation
 *
 * 0. Leader creates one state machine per Follower to track of their states
 * and moves it to FOLLOWER_ONLINE state. Follower creates a state machine to
 * keep track of its states and moves it to NORMAL state.
 *
 * 1. The Leader learns the Follower’s follower.lastLogIndex during receiving
 * replies on AppendEntries() RPC, fails to find follower.lastLogIndex in its
 * RAFT log or tries and fails to construct an AppendEntries() message because
 * of the WAL that contained some necessary frames has been rotated out, and
 * understands that the snapshot installation procedure is required.
 *
 * Leader calls leader_tick() putting struct raft_message as a parameter which
 * logic moves it from FOLLOWER_ONLINE to FOLLOWER_NEEDS_SNAPSHOT state.
 *
 * 2. The Leader initiates the snapshot installation by sending
 * InstallSnapshot() message.
 *
 * 3. Upon receiving this message on the Follower's side, Follower calls
 * follower_tick() putting struct raft_message as a parameter which logic moves
 * it from NORMAL to SIGNATURES_CALC_STARTED state. The Follower then creates
 * its HT and starts calculating checksums and recording them. Once finished it
 * sends the leader the InstallSnapshotResult() message and the Leader moves to
 * SIGNATURES_CALC_STARTED and creates its HT.
 *
 * 3. The Leader sends Signature() messages to the Follower containing the page
 * range for which we want to get the checksums.
 *
 * The Follower sends the requested checksums in a SignatureResult() message
 * back to the Leader and the leader puts incomming payloads of Signature()
 * message into the HT.
 *
 * 4. When the follower sends the checksum of its highest numbered page to the
 * Leader, it sends the SignatureResult() message using the done=true flag,
 * upon receiving it the Leader moves into SNAPSHOT_INSTALLATION_STARTED state.
 *
 * 5. In SNAPSHOT_INSTALLATION_STARTED state, the Leader starts iterating over
 * the local persistent state, and calculates the checksum for each page the
 * state has. Then, it tries to find the checksum it calculated in HT. Based on
 * the result of this calculation, the Leader sends InstallShapshot(CP..) or
 * InstallShapshot(MV..) to the Follower.
 *
 * Upon receving these messages, the Follower moves into
 * SNAPSHOT_CHUNCK_RECEIVED state. The Leader moves into SNAPSHOT_CHUNCK_SENT
 * state after receiving first reply from the Follower.
 *
 * 6. When the iteration has finished the Leader sends
 * InstallShapshot(..., done=true) message to the Follower. It moves the
 * Follower back to NORMAL state and the state machine corresponding to the
 * Follower on the Leader is moved to SNAPSHOT_DONE_SENT state.
 *
 * 7. The Leader sends AppendEntries() RPC to the Follower and restarts the
 * algorithm from (1). The Leader's state machine is being moved to
 * FOLLOWER_ONLINE state.
 *
 * =Failure model
 *
 * ==Unavailability of the Leader and Follower.
 *
 * To handle use-cases when any party of the communication becomes
 * unavailable for a while without crash the following assumtions are
 * made:
 *
 * - Signature() or InstallSnapshot(MV/CP) messages are idempotent and
 *   can be applied to the persistent state many times resulting the
 *   same transition.
 *
 * - Each message with data chuncks has an information about the
 *   "chunk index". Chunk indexes come in monotonically increasing
 *   order.
 *
 * - Each reply message acknowledges that the data received (or
 *   ignored) by sending `result` field back to the counter part along
 *   with last known chunk index as a confirmation that the receiver
 *   "knows everything up to the given chunck index".
 *
 * - If a party notices that last known chunk index sent back to it
 *   doesn't match it's own, the communication get's restarted from
 *   the lowest known index.
 *
 * If a reply is not received the Leader will eventually timeout and retry
 * sending the same message.
 *
 * ==Crashes of the Leader and Follower.
 *
 * Crashes of the Leader are handled by Raft when a new leader is elected
 * and the snapshot process is restarted.
 *
 * If the Follower crashes, it will restart its state machine into the NORMAL
 * state. If the Leader then sends a message which assumes the Follower is at
 * the state prior to the crash, the Follower will reply using the message's
 * result RPC using the failed=true flag. Upong receiving the message the
 * Leader will restart the snapshot installation procedure.
 *
 * =State model
 *
 * Definitions:
 *
 * Rf -- raft index sent in AppendEntriesResult() from Follower to Leader
 * Tf -- Follower's term sent in AppendEntriesResult() from Follower to Leader
 *
 * Tl -- Leader's term
 * Rl -- raft index of the Leader
 *
  * Leader's state machine:
 *
 * +---------------------------------+
 * |                                 |     AppendEntriesResult() received
 * |        *Result(failed=true)     |     raft_log.find(Rf) == "FOUND"
 * |        received                 |      +------------+
 * |        +---------------> FOLLOWER_ONLINE <----------+
 * |        |                        |
 * |        |                        | AppendEntriesResult() received
 * |        |                        V Rf << Rl && raft_log.find(Rf) == "ENOENTRY"
 * |        +------------- FOLLOWER_NEEDS_SNAPSHOT
 * |        |                        |
 * |        |                        | InstallSnapshotResult() received
 * |        |                        V
 * |        +------------ SIGNATURES_CALC_STARTED  -------+ SignatureResult() received
 * |        |                        |             <------+
 * |        |                        | SignatureResult(done=true) received
 * |        |                        V
 * |        +----------- SNAPSHOT_INSTALLATION_STARTED
 * |        |                        |
 * |        |                        | CP_Result()/MV_Result() received
 * |        |                        V
 * |        +--------------- SNAPSHOT_CHUNCK_SENT -------+ CP_Result()/MV_Result()
 * |        |                        |            <------+  received
 * |        |                        | Raw snapshot iteration done
 * |        |                        V
 * |        +--------------- SNAPSHOT_DONE_SENT
 * |                                 | InstallSnapshotResult() received
 * +---------------------------------+
 *
 * Follower's state machine:
 *
 *                         +---------+
 *                         |         | Signature/CP_Result/MV_Result received
 *                         |         | *Result(failed=true) sent
 * +-------------------> NORMAL <----+
 * |       +----------->   |
 * |   (@) |               | InstallSnapshot() received
 * |       |               V
 * |       +--- SIGNATURES_CALC_STARTED
 * |       |               |
 * |       |               | Signatures for all db pages have been calculated.
 * |       |               | InstallSnapshotResult() sent.
 * |       |               V
 * |       +---- SIGNATURES_CALC_DONE
 * |       |               |
 * |       |               | First SignatureResult() sent
 * |       |               V
 * |       +---- SIGNATURES_PART_SENT ---------+ Signature() received
 * |       |               |          <--------+ SignatureResult() sent
 * |       |               |
 * |       |               | CP()/MV() received
 * |       |               V
 * |       +--- SNAPSHOT_CHUNCK_RECEIVED -------+ CP()/MV() received
 * |                       |             <------+
 * |                       | InstallSnapshot(done=true) received
 * +-----------------------+
 *
 * (@) -- AppendEntries() received && Tf < Tl
 */

enum follower_states {
	FS_NORMAL,
	FS_SIGNATURES_CALC_STARTED,
	FS_SIGNATURES_CALC_DONE,
	FS_SIGNATURES_PART_SENT,
	FS_SNAPSHOT_CHUNCK_RECEIVED,
	FS_NR,
};

static const struct sm_conf follower_states[FS_NR] = {
	[FS_NORMAL] = {
		.flags = SM_INITIAL | SM_FINAL,
		.name = "normal",
		.allowed = BITS(FS_SIGNATURES_CALC_STARTED),
	},
/*	[PS_DRAINING] = {
	    .name = "draining",
	    .allowed = BITS(PS_DRAINING)
		     | BITS(PS_NOTHING)
		     | BITS(PS_BARRIER),
	},
*/
};

 __attribute__((unused)) static void follower_tick(struct sm *follower, const struct raft_message *msg)
{
	(void) follower;
	(void) msg;
	(void) follower_states;
	//switch (sm_state(follower)) {
	//}
}

__attribute__((unused)) static bool follower_invariant(const struct sm *m, int prev_state)
{
	(void) m;
	(void) prev_state;
	//pool_impl_t *pi = CONTAINER_OF(m, pool_impl_t, planner_sm);
	return true;
}

enum leader_states {
	LS_FOLLOWER_ONLINE,
	LS_FOLLOWER_NEEDS_SNAPSHOT,
	LS_SIGNATURES_CALC_STARTED,
	LS_SNAPSHOT_INSTALLATION_STARTED,
	LS_SNAPSHOT_CHUNCK_SENT,
	LS_SNAPSHOT_DONE_SENT,
	LS_NR,
};

static const struct sm_conf leader_states[LS_NR] = {
	[LS_FOLLOWER_ONLINE] = {
		.flags = SM_INITIAL | SM_FINAL,
		.name = "online",
		.allowed = BITS(LS_FOLLOWER_NEEDS_SNAPSHOT),
	},
/*	[PS_DRAINING] = {
	    .name = "draining",
	    .allowed = BITS(PS_DRAINING)
		     | BITS(PS_NOTHING)
		     | BITS(PS_BARRIER),
	},
*/
};

__attribute__((unused)) static void leader_tick(struct sm *leader, const struct raft_message *msg)
{
	(void) leader;
	(void) msg;
	(void) leader_states;
	//switch (sm_state(leader)) {
	//}
}

__attribute__((unused)) static bool leader_invariant(const struct sm *m, int prev_state)
{
	(void) m;
	(void) prev_state;
	//pool_impl_t *pi = CONTAINER_OF(m, pool_impl_t, planner_sm);
	return true;
}

static void installSnapshotSendCb(struct raft_io_send *req, int status)
{
	(void)status;
	raft_free(req);
}

int recvInstallSnapshot(struct raft *r,
			const raft_id id,
			const char *address,
			struct raft_install_snapshot *args)
{
	struct raft_io_send *req;
	struct raft_message message;
	struct raft_append_entries_result *result =
	    &message.append_entries_result;
	int rv;
	int match;
	bool async;

	assert(address != NULL);
	tracef(
	    "self:%llu from:%llu@%s conf_index:%llu last_index:%llu "
	    "last_term:%llu "
	    "term:%llu",
	    r->id, id, address, args->conf_index, args->last_index,
	    args->last_term, args->term);

	result->rejected = args->last_index;
	result->last_log_index = logLastIndex(r->log);
	result->version = RAFT_APPEND_ENTRIES_RESULT_VERSION;
	result->features = RAFT_DEFAULT_FEATURE_FLAGS;

	rv = recvEnsureMatchingTerms(r, args->term, &match);
	if (rv != 0) {
		return rv;
	}

	if (match < 0) {
		tracef("local term is higher -> reject ");
		goto reply;
	}

	/* TODO: this logic duplicates the one in the AppendEntries handler */
	assert(r->state == RAFT_FOLLOWER || r->state == RAFT_CANDIDATE);
	assert(r->current_term == args->term);
	if (r->state == RAFT_CANDIDATE) {
		assert(match == 0);
		tracef("discovered leader -> step down ");
		convertToFollower(r);
	}

	rv = recvUpdateLeader(r, id, address);
	if (rv != 0) {
		return rv;
	}
	r->election_timer_start = r->io->time(r->io);

	rv = replicationInstallSnapshot(r, args, &result->rejected, &async);
	if (rv != 0) {
		tracef("replicationInstallSnapshot failed %d", rv);
		return rv;
	}

	if (async) {
		return 0;
	}

	if (result->rejected == 0) {
		/* Echo back to the leader the point that we reached. */
		result->last_log_index = args->last_index;
	}

reply:
	result->term = r->current_term;

	/* Free the snapshot data. */
	raft_configuration_close(&args->conf);
	raft_free(args->data.base);

	message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
	message.server_id = id;
	message.server_address = address;

	req = raft_malloc(sizeof *req);
	if (req == NULL) {
		return RAFT_NOMEM;
	}
	req->data = r;

	rv = r->io->send(r->io, req, &message, installSnapshotSendCb);
	if (rv != 0) {
		raft_free(req);
		return rv;
	}

	return 0;
}

#undef tracef
