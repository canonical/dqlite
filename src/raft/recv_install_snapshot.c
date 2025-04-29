#include <assert.h>
#include <stdint.h>
#include <stdio.h>

#include "../tracing.h"
#include "assert.h"
#include "convert.h"
#include "flags.h"
#include "log.h"
#include "recv.h"
#include "replication.h"

#include "../lib/sm.h"
#include "../raft.h"
#include "../raft/recv_install_snapshot.h"
#include "../utils.h"
#include "../lib/threadpool.h"

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

/**
 * =Operation
 *
 * 0. Leader creates one state machine per Follower to track of their states
 * and moves it to F_ONLINE state. Follower creates a state machine to keep
 * track of its states and moves it to NORMAL state.
 *
 * 1. The Leader learns the Follower’s follower.lastLogIndex during receiving
 * replies on AppendEntries() RPC, fails to find follower.lastLogIndex in its
 * RAFT log or tries and fails to construct an AppendEntries() message because
 * of the WAL that contained some necessary frames has been rotated out, and
 * understands that the snapshot installation procedure is required.
 *
 * Leader calls leader_tick() putting struct raft_message as a parameter which
 * logic moves it from F_ONLINE to F_NEEDS_SNAP state.
 *
 * 2. The Leader triggers the creation of its HT and initiates the snapshot
 * installation by sending InstallSnapshot() message as soon as the HT is
 * created.
 *
 * 3. Upon receiving this message on the Follower's side, Follower calls
 * follower_tick() putting struct raft_message as a parameter which triggers the
 * creation of the HT on the follower side. Once HT is created follower moves
 * to SIGS_CALC_STARTED and triggers a background job to calculate the checksum
 * of its pages and inserting them in the HT.
 *
 * 4. The Leader probes the follower sending Signature(calculated?) messages
 * and the Follower replies with either SignatureResult(calculated=false) if it
 * is still calculating the chechsums or SignatureResult(calculated=true) if it
 * has finished. If the process finishes, Follower moves into SIG_RECEIVING and
 * Leader moves into REQ_SIG_LOOP.
 *
 * 5. The Leader sends Signature() messages to the Follower containing the page
 * range for which we want to get the checksums.
 *
 * The Follower sends the requested checksums in a SignatureResult() message
 * back to the Leader and the leader puts incomming payloads of Signature()
 * message into the HT.
 *
 * 6. When the follower sends the checksum of its highest numbered page to the
 * Leader, it sends the SignatureResult() message using the done=true flag,
 * upon receiving it the Leader moves into READ_PAGES_LOOP state and the
 * Follower moves into CHUNK_RECEIVING.
 *
 * 7. In READ_PAGES_LOOP state, the Leader starts iterating over
 * the local persistent state, and calculates the checksum for each page the
 * state has. Then, it tries to find the checksum it calculated in HT. Based on
 * the result of this calculation, the Leader sends CP() or MV() to the
 * Follower.
 *
 * The Follower receives the message and persists the page using a background
 * job. Once the background job is finished, the Follower replies with
 * CPResult() or MVResult().
 *
 * 8. When the iteration has finished the Leader moves into SNAP_DONE state and
 * sends InstallShapshot(..., done=true) message to the Follower. When Follower
 * replies, both state machines move to their FINAL states.
 *
 * 9. The Leader sends AppendEntries() RPC to the Follower and restarts the
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
 * If the Follower receives an message which is not expected in the Follower's
 * current state, the Follower will reply using the message's result RPC
 * setting the unexpected=true flag. This response suggests the Leader to
 * restart the snapshot installation procedure.
 *
 * In particular, if the follower crashes it will restart its state machine to
 * the NORMAL state and reply using the unexpected=true flag for any messages
 * not expected in the NORMAL state, suggesting the Leader to restart the
 * procedure.
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
 * +-----------------------------+
 * |                             |     AppendEntriesResult() received
 * |    *Result(unexpected=true) |     raft_log.find(Rf) == "FOUND"
 * |    received                 V    +------------+
 * |        +------------->  F_ONLINE <------------+
 * |        |                    |
 * |        |                    | AppendEntriesResult() received
 * |        |                    | Rf << Rl && raft_log.find(Rf) == "ENOENTRY"
 * |        |                    V Trigger background job.
 * |        +---------------  HT_WAIT
 * |        |                    V HT creation finished,
 * |        +-------------  F_NEEDS_SNAP*
 * |        |                    | InstallSnapshot() sent,
 * |        |                    V InstallSnapshotResult() received.
 * |        +-----------  CHECK_F_HAS_SIGS* <-----------------------+ SignatureResult() had
 * |        |                    | Signature(calculated?) sent,     | calculated=false and
 * |        |                    V SignatureResult() received.      | timeout reached.
 * |        +-------------   WAIT_SIGS -----------------------------+
 * |        |                    V SignatureResult() had calculated=true.
 * |        +-------------  REQ_SIG_LOOP* <-------------------------+
 * |        |                    | Signature() sent,                | Signature persisted in HT,
 * |        |                    V SignatureResult() received.      | there are some pending
 * |        +-------------  RECV_SIG_PART                           | signatures.
 * |        |                    V Background job triggered.        |
 * |        +----------  PRESISTED_SIG_PART ------------------------+
 * |        |                    | Signature persisted in HT,
 * |        |                    V all signatures have been persisted.
 * |        +-----------  READ_PAGES_LOOP <-------------------------+
 * |        |                    V Background job triggered.        | There are pending pages to
 * |        +--------------  PAGE_READ*                             | be sent.
 * |        |                    | Page read from disk,             |
 * |        |                    V CP()/MV() sent.                  |
 * |        +--------------  PAGE_SENT -----------------------------+
 * |        |                    V All pages sent and acked.
 * |        +--------------  SNAP_DONE
 * |        |                    | InstallSnapshot(done=true) sent,
 * |        |                    V and reply received.
 * |        +----------------  FINAL
 * |                             |
 * +-----------------------------+
 *
 * Note all states marked with (*) have an extra transition not represented in
 * the diagram above. When the leader sends a message there is always a timeout
 * sheduled. If the reply is not received and the timeout expires, we will stay
 * in the same state and re-send the message.
 *
 * Follower's state machine:
 *
 *                            +------+ (%)
 * +-------------------> NORMAL <----+
 * |       +----------->   |
 * |       |               | InstallSnapshot() received.
 * |       |               V
 * |       +---------  HT_CREATE
 * |       |               V Trigger background job.
 * |       +----------  HT_WAIT
 * |       |               | Background job finishes,
 * |       |               | InstallSnapshotResult() sent.
 * |       |               V
 * |       +------  SIGS_CALC_STARTED
 * |       |               V Trigger background job.
 * |       +------  SIGS_CALC_LOOP <--------------------------+
 * |       |               V Signature(calculated?) received. | SignatureResult(calculated=false) sent.
 * |       +---  SIGS_CALC_MSG_RECEIVED ----------------------+
 * |       |               | Signatures for all db pages have been calculated.
 * |       |               V SignatureResult(calculated=true) sent.
 * |       |        SIGS_CALC_DONE
 * |       |               V
 * |       +-------  SIG_RECEIVING <--------------------------+
 * |       |               V Signature() received.            |
 * |       +-------  SIG_PROCESSED                            |
 * |       |               V Background job triggered.        | Signature() had done=false,
 * |       +---------   SIG_READ                              | SignatureResult() sent.
 * |       |               V Checksum is read from HT.        |
 * |       +---------  SIG_REPLIED ---------------------------+
 * |       |               | Signature() had done=true,
 * |       |               V SignatureResult() sent.
 * |       +-------  CHUNK_RECEIVING <------------------------+
 * |       |               V CP()/MV() received.              |
 * |       +-------  CHUNK_PROCESSED                          |
 * |       |               V Background job triggered.        |
 * |       +-------  CHUNK_APPLIED                            |
 * |       |               V Chunk has been written to disk.  |
 * |       +-------  CHUNK_REPLIED ---------------------------+
 * |       |               | CP()/MV() had done=true.
 * |       |               V CPResult()/MVResult() sent.
 * |       +---------  SNAP_DONE
 * |    (@ || %)           | InstallSnapshot(done=true) received,
 * |                       V and reply sent.
 * |                     FINAL
 * |                       |
 * +-----------------------+
 *
 * (@) -- AppendEntries() received && Tf < Tl
 * (%) -- Signature()/CP()/MV() received and in the current state receving a
 *        message of such type is unexpected. *Result(unexpected=true) sent.
 */

/* TODO this uses several GNU extensions, do we use it?
#define RC(rc) ({ \
	typeof(rc) __rc = (rc); \
	printf("< rc=%d\n", __rc); \
	__rc; \
}) */

enum rpc_state {
	RPC_INIT,
	RPC_FILLED,
	RPC_SENT,
	RPC_TIMEDOUT,
	RPC_REPLIED,
	RPC_ERROR,
	RPC_END,
	RPC_NR,
};

/* clang-format off */
static const struct sm_conf rpc_sm_conf[RPC_NR] = {
	[RPC_INIT] = {
		.flags   = SM_INITIAL | SM_FINAL,
		.name    = "init",
		.allowed = BITS(RPC_FILLED)
		         | BITS(RPC_ERROR),
	},
	[RPC_FILLED] = {
		.name    = "filled",
		.allowed = BITS(RPC_SENT)
		         | BITS(RPC_ERROR),
	},
	[RPC_SENT] = {
		.name    = "sent",
		.allowed = BITS(RPC_TIMEDOUT)
		         | BITS(RPC_REPLIED)
		         | BITS(RPC_ERROR)
		         | BITS(RPC_END),
	},
	[RPC_TIMEDOUT] = {
		.name    = "timedout",
		.allowed = BITS(RPC_INIT),
	},
	[RPC_REPLIED] = {
		.name    = "replied",
		.allowed = BITS(RPC_INIT)
		         | BITS(RPC_END),
	},
	[RPC_ERROR] = {
		.name    = "error",
		.flags   = SM_FINAL,
	},
	[RPC_END] = {
		.name    = "end",
		.allowed = BITS(RPC_END),
		.flags   = SM_FINAL,
	},
};
/* clang-format on */

enum work_state {
	WORK_INIT,
	WORK_DONE,
	WORK_ERROR,
	WORK_NR,
};

static const struct sm_conf work_sm_conf[WORK_NR] = {
	[WORK_INIT] = {
		.flags   = SM_INITIAL | SM_FINAL,
		.name    = "w_init",
		.allowed = BITS(WORK_DONE) | BITS(WORK_ERROR),
	},
	[WORK_DONE] = {
		.flags   = SM_FINAL,
		.name    = "w_done",
	},
	[WORK_ERROR] = {
		.flags   = SM_FINAL,
		.name    = "w_error",
	},
};

enum to_state {
	TO_INIT,
	TO_STARTED,
	TO_EXPIRED,
	TO_CANCELED,
	TO_NR,
};

/* clang-format off */
static const struct sm_conf to_sm_conf[TO_NR] = {
	[TO_INIT] = {
		.flags   = SM_INITIAL | SM_FINAL,
		.name    = "init",
		.allowed = BITS(TO_STARTED),
	},
	[TO_STARTED] = {
		.flags   = SM_FINAL,
		.name    = "started",
		.allowed = BITS(TO_EXPIRED) | BITS(TO_CANCELED),
	},
	[TO_EXPIRED] = {
		.flags   = SM_FINAL,
		.name    = "expired",
	},
	[TO_CANCELED] = {
		.flags   = SM_FINAL,
		.name    = "canceled",
	},
};
/* clang-format on */

#define M_MSG_SENT   ((const struct raft_message *) 3)
#define M_TIMEOUT    ((const struct raft_message *) 2)
#define M_WORK_DONE  ((const struct raft_message *) 1)

static bool work_sm_invariant(const struct sm *sm, int prev_state)
{
	(void)sm;
	(void)prev_state;
	return true;
}

bool leader_sm_invariant(const struct sm *sm, int prev_state)
{
	(void)sm;
	(void)prev_state;
	return true;
}

bool follower_sm_invariant(const struct sm *sm, int prev_state)
{
	(void)sm;
	(void)prev_state;
	return true;
}

static bool rpc_sm_invariant(const struct sm *sm, int prev_state)
{
	(void)sm;
	(void)prev_state;
	return true;
}

static bool to_sm_invariant(const struct sm *sm, int prev_state)
{
	(void)sm;
	(void)prev_state;
	return true;
}

static void leader_work_done(pool_work_t *w)
{
	struct work *work = CONTAINER_OF(w, struct work, pool_work);
	struct leader *leader = CONTAINER_OF(work, struct leader, work);

	PRE(!leader->ops->is_pool_thread());
	sm_move(&work->sm, WORK_DONE);
	leader_tick(leader, M_WORK_DONE);
}

static void follower_work_done(pool_work_t *w)
{
	struct work *work = CONTAINER_OF(w, struct work, pool_work);
	struct follower *follower = CONTAINER_OF(work, struct follower, work);

	PRE(!follower->ops->is_pool_thread());
	sm_move(&work->sm, WORK_DONE);
	follower_tick(follower, M_WORK_DONE);
}

static void rpc_to_cb(uv_timer_t *handle)
{
	struct timeout *to = CONTAINER_OF(handle, struct timeout, handle);
	struct rpc *rpc = CONTAINER_OF(to, struct rpc, timeout);
	struct leader *leader = CONTAINER_OF(rpc, struct leader, rpc);

	PRE(!leader->ops->is_pool_thread());
	if (sm_state(&to->sm) == TO_CANCELED) {
		return;
	}
	sm_move(&to->sm, TO_EXPIRED);
	sm_move(&rpc->sm, RPC_TIMEDOUT);
	leader_tick(leader, M_TIMEOUT);
}

static void leader_to_cb(uv_timer_t *handle)
{
	struct timeout *to = CONTAINER_OF(handle, struct timeout, handle);
	struct leader *leader = CONTAINER_OF(to, struct leader, timeout);

	PRE(!leader->ops->is_pool_thread());
	if (sm_state(&to->sm) == TO_CANCELED) {
		return;
	}
	sm_move(&to->sm, TO_EXPIRED);
	leader_tick(leader, M_TIMEOUT);
}

static void leader_to_start(struct leader *leader,
		struct timeout *to,
		unsigned delay,
		to_cb_op to_cb)
{
	leader->ops->to_init(to);
	sm_init(&to->sm, to_sm_invariant, NULL, to_sm_conf, "to", TO_INIT);
	leader->ops->to_start(to, delay, to_cb);
	sm_relate(&leader->sm, &to->sm);
	sm_move(&to->sm, TO_STARTED);
}

static void leader_to_cancel(struct leader *leader, struct timeout *to)
{
	leader->ops->to_stop(to);
	sm_move(&to->sm, TO_CANCELED);
}

static void leader_sent_cb(struct sender *s, int rc)
{
	struct rpc *rpc = CONTAINER_OF(s, struct rpc, sender);
	struct leader *leader = CONTAINER_OF(rpc, struct leader, rpc);

	PRE(!leader->ops->is_pool_thread());
	if (UNLIKELY(rc != 0)) {
		sm_move(&rpc->sm, RPC_ERROR);
		return;
	}
	leader_tick(leader, M_MSG_SENT);
}

static void follower_sent_cb(struct sender *s, int rc)
{
	struct rpc *rpc = CONTAINER_OF(s, struct rpc, sender);
	struct follower *follower = CONTAINER_OF(rpc, struct follower, rpc);

	PRE(!follower->ops->is_pool_thread());
	if (UNLIKELY(rc != 0)) {
		sm_move(&rpc->sm, RPC_ERROR);
		return;
	}
	follower_tick(follower, M_MSG_SENT);
}

static bool is_a_trigger_leader(const struct leader *leader, const struct raft_message *incoming)
{
	int state = sm_state(&leader->sm);

	/* Special cases: */
	if (incoming == M_WORK_DONE) {
		return IN(state, LS_HT_WAIT, LS_PAGE_SENT, LS_PERSISTED_SIG_PART,
				LS_PAGE_READ);
	} else if (incoming == M_MSG_SENT) {
		if (sm_state(&leader->rpc.sm) != RPC_FILLED) {
			return false;
		}
		return IN(state, LS_PAGE_READ, LS_SNAP_DONE, LS_F_NEEDS_SNAP,
				LS_REQ_SIG_LOOP, LS_CHECK_F_HAS_SIGS);
	} else if (incoming == M_TIMEOUT) {
		return IN(state, LS_PAGE_READ, LS_SNAP_DONE, LS_F_NEEDS_SNAP,
				LS_REQ_SIG_LOOP, LS_CHECK_F_HAS_SIGS, LS_WAIT_SIGS);
	}

	/* From now on, the message pointer is a valid pointer. */
	PRE(incoming != M_MSG_SENT && incoming != M_TIMEOUT &&
			incoming != M_WORK_DONE);

	/* Leader has not send the AppendEntries message but reacts on its
	 * reply. */
	if (state == LS_F_ONLINE) {
		// TODO check if raft entry is present, else it is not a trigger.
		return incoming->type == RAFT_IO_APPEND_ENTRIES_RESULT;
	}

	/* Leader is waiting for follower reply. */
	if (sm_state(&leader->rpc.sm) != RPC_SENT) {
		/* We are not expecting a reply. */
		return false;
	};
	switch (state) {
	case LS_PAGE_READ:
		return IN(incoming->type, RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT,
				RAFT_IO_INSTALL_SNAPSHOT_MV_RESULT);
	case LS_SNAP_DONE:
	case LS_F_NEEDS_SNAP:
		return incoming->type == RAFT_IO_INSTALL_SNAPSHOT_RESULT;
	case LS_REQ_SIG_LOOP:
	case LS_CHECK_F_HAS_SIGS:
		return incoming->type == RAFT_IO_SIGNATURE_RESULT;
	}
	return false;
}

static bool is_a_trigger_follower(const struct follower *follower,
		const struct raft_message *incoming)
{
	int state = sm_state(&follower->sm);

	/* Special cases: */
	if (incoming == M_WORK_DONE) {
		return IN(state, FS_SIG_PROCESSED, FS_CHUNCK_PROCESSED,
				FS_SIG_PROCESSED, FS_CHUNCK_PROCESSED, FS_CHUNCK_REPLIED,
				FS_HT_WAIT);
	} else if (incoming == M_MSG_SENT) {
		if (sm_state(&follower->rpc.sm) != RPC_FILLED) {
			return false;
		}
		return IN(state, FS_NORMAL, FS_SIGS_CALC_LOOP, FS_SIG_READ,
				FS_CHUNCK_APPLIED, FS_SNAP_DONE);
	} else if (incoming == M_TIMEOUT) {
		/* No timeouts in follower. */
		return false;
	}

	/* From now on, the message pointer is a valid pointer. */
	PRE(incoming != M_MSG_SENT && incoming != M_TIMEOUT &&
			incoming != M_WORK_DONE);

	switch (state) {
	case FS_NORMAL:
	case FS_SNAP_DONE:
		return incoming->type == RAFT_IO_INSTALL_SNAPSHOT;
	case FS_SIGS_CALC_LOOP:
		return incoming->type == RAFT_IO_SIGNATURE;
	case FS_SIG_RECEIVING:
		return incoming->type == RAFT_IO_SIGNATURE;
	case FS_CHUNCK_RECEIVING:
		return IN(incoming->type, RAFT_IO_INSTALL_SNAPSHOT_CP,
				RAFT_IO_INSTALL_SNAPSHOT_MV);
	}
	return false;
}

static bool is_a_duplicate(const void *state,
		const struct raft_message *incoming)
{
	(void)state;
	(void)incoming;
	return false;
}

static void work_init(struct work *w)
{
	sm_init(&w->sm, work_sm_invariant, NULL, work_sm_conf, "work", WORK_INIT);
}

static void rpc_init(struct rpc *rpc)
{
	sm_init(&rpc->sm, rpc_sm_invariant, NULL, rpc_sm_conf, "rpc", RPC_INIT);
}

static void rpc_fini(struct rpc *rpc)
{
	sm_move(&rpc->sm, RPC_END);
}


static void work_fill_leader(struct leader *leader)
{
	leader->work_cb = leader->ops->ht_create;
	work_init(&leader->work);
	sm_relate(&leader->sm, &leader->work.sm);
}

static void work_fill_follower(struct follower *follower)
{
	switch (sm_state(&follower->sm)) {
	case FS_HT_CREATE:
		follower->work_cb = follower->ops->ht_create;
		break;
	case FS_SIGS_CALC_STARTED:
		follower->work_cb = follower->ops->fill_ht;
		break;
	case FS_SIG_RECEIVING:
		follower->work_cb = follower->ops->read_sig;
		break;
	case FS_CHUNCK_RECEIVING:
		follower->work_cb = follower->ops->write_chunk;
		break;
	}
	work_init(&follower->work);
	sm_relate(&follower->sm, &follower->work.sm);
}

static void rpc_fill_leader(struct leader *leader)
{
	struct rpc *rpc = &leader->rpc;

	rpc_init(rpc);
	sm_relate(&leader->sm, &rpc->sm);

	switch (sm_state(&leader->sm)) {
	case LS_PAGE_READ:
		rpc->message = (struct raft_message) {
			.type = RAFT_IO_INSTALL_SNAPSHOT_CP, // TODO CP OR MV.
		};
		break;
	case LS_SNAP_DONE:
		rpc->message = (struct raft_message) {
			.type = RAFT_IO_INSTALL_SNAPSHOT,
			.install_snapshot = (struct raft_install_snapshot) {
				.result = RAFT_SNAPSHOT_DONE,
			},
		};
		break;
	case LS_F_NEEDS_SNAP:
		rpc->message = (struct raft_message) {
			.type = RAFT_IO_INSTALL_SNAPSHOT,
		};
		break;
	case LS_REQ_SIG_LOOP:
		rpc->message = (struct raft_message) {
			.type = RAFT_IO_SIGNATURE,
		};
		break;
	case LS_CHECK_F_HAS_SIGS:
		rpc->message = (struct raft_message) {
			.type = RAFT_IO_SIGNATURE,
			.signature = (struct raft_signature) {
				.ask_calculated = true,
			},
		};
		break;
	}

	sm_move(&rpc->sm, RPC_FILLED);
}

static void rpc_fill_follower(struct follower *follower)
{
	struct rpc *rpc = &follower->rpc;

	rpc_init(rpc);
	sm_relate(&follower->sm, &rpc->sm);

	switch (sm_state(&follower->sm)) {
	case FS_SIGS_CALC_LOOP:
		rpc->message = (struct raft_message) {
			.type = RAFT_IO_SIGNATURE_RESULT,
			.signature_result = (struct raft_signature_result) {
				.calculated = follower->sigs_calculated,
			},
		};
		break;
	case FS_SIG_READ:
		rpc->message = (struct raft_message) {
			.type = RAFT_IO_SIGNATURE_RESULT,
		};
		break;
	case FS_CHUNCK_APPLIED:
		rpc->message = (struct raft_message) {
			.type = RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT,
		};
		break;
	case FS_NORMAL:
	case FS_SNAP_DONE:
		rpc->message = (struct raft_message) {
			.type = RAFT_IO_INSTALL_SNAPSHOT_RESULT,
		};
		break;
	}

	sm_move(&rpc->sm, RPC_FILLED);
}

static int rpc_send(struct rpc *rpc, sender_send_op op, sender_cb_op sent_cb)
{
	int rc = op(&rpc->sender, &rpc->message, sent_cb);
	return rc;
}

static void follower_rpc_tick(struct rpc *rpc)
{
	switch(sm_state(&rpc->sm)) {
	case RPC_INIT:
		break;
	case RPC_FILLED:
		sm_move(&rpc->sm, RPC_SENT);
		break;
	case RPC_SENT:
	case RPC_TIMEDOUT:
	case RPC_REPLIED:
	case RPC_ERROR:
	case RPC_END:
	default:
		break;
	}
}

static void leader_rpc_tick(struct rpc *rpc)
{
	switch(sm_state(&rpc->sm)) {
	case RPC_INIT:
		break;
	case RPC_FILLED:
		sm_move(&rpc->sm, RPC_SENT);
		break;
	case RPC_SENT:
		sm_move(&rpc->sm, RPC_REPLIED);
		break;
	case RPC_TIMEDOUT:
	case RPC_REPLIED:
	case RPC_ERROR:
	case RPC_END:
	default:
		break;
	}
}

static void leader_reset(struct leader *leader)
{
	(void)leader;
}

static bool is_an_unexpected_trigger(const struct leader *leader,
				     const struct raft_message *msg)
{
	(void)leader;

	if (msg == M_MSG_SENT || msg == M_TIMEOUT || msg == M_WORK_DONE) {
		return false;
	}

	enum raft_snapshot_result res = RAFT_SNAPSHOT_UNEXPECTED;
	switch (msg->type) {
	case RAFT_IO_APPEND_ENTRIES_RESULT:
		res = RAFT_SNAPSHOT_OK;
		break;
	case RAFT_IO_INSTALL_SNAPSHOT:
		res = msg->install_snapshot.result;
		break;
	case RAFT_IO_INSTALL_SNAPSHOT_RESULT:
		res = msg->install_snapshot_result.result;
		break;
	case RAFT_IO_INSTALL_SNAPSHOT_CP:
		res = msg->install_snapshot_cp.result;
		break;
	case RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT:
		res = msg->install_snapshot_cp_result.result;
		break;
	case RAFT_IO_INSTALL_SNAPSHOT_MV:
		res = msg->install_snapshot_mv.result;
		break;
	case RAFT_IO_INSTALL_SNAPSHOT_MV_RESULT:
		res = msg->install_snapshot_mv_result.result;
		break;
	case RAFT_IO_SIGNATURE:
		res = msg->signature.result;
		break;
	case RAFT_IO_SIGNATURE_RESULT:
		res = msg->signature_result.result;
		break;
	}
	return res == RAFT_SNAPSHOT_UNEXPECTED;
}

static int follower_next_state(struct sm *sm)
{
	struct follower *follower = CONTAINER_OF(sm, struct follower, sm);

	switch (sm_state(sm)) {
	case FS_SIGS_CALC_LOOP:
		return follower->sigs_calculated ? FS_SIGS_CALC_DONE : FS_SIGS_CALC_MSG_RECEIVED;
	case FS_SIGS_CALC_MSG_RECEIVED:
		return FS_SIGS_CALC_LOOP;
	case FS_SIG_REPLIED:
		return FS_CHUNCK_RECEIVING;
	case FS_FINAL:
		return FS_NORMAL;
	}
	return sm_state(sm) + 1;
}

static int leader_next_state(struct sm *sm)
{
	struct leader *leader = CONTAINER_OF(sm, struct leader, sm);

	switch (sm_state(sm)) {
	case LS_WAIT_SIGS:
		return sm_state(sm) + (leader->sigs_calculated ? +1 : -1);
	case LS_FINAL:
		return LS_F_ONLINE;
	}

	return sm_state(sm) + 1;
}

__attribute__((unused)) void leader_tick(struct leader *leader, const struct raft_message *incoming)
{
	(void)leader_sm_conf;
	(void)leader_sm_invariant;
	int rc;
	struct sm *sm = &leader->sm;
	const struct leader_ops *ops = leader->ops;

	PRE(!ops->is_pool_thread());

	if (!is_a_trigger_leader(leader, incoming) ||
	    is_a_duplicate(leader, incoming))
		return;

	if (is_an_unexpected_trigger(leader, incoming)) {
		leader_reset(leader);
		return;
	}

again:
	switch(sm_state(sm)) {
	case LS_F_ONLINE:
	case LS_RECV_SIG_PART:
	case LS_READ_PAGES_LOOP:
		work_fill_leader(leader);
		ops->work_queue(&leader->work, leader->work_cb, leader_work_done);
		sm_move(sm, leader_next_state(sm));
		break;
	case LS_HT_WAIT:
	case LS_PAGE_SENT:
	case LS_PERSISTED_SIG_PART:
		sm_move(sm, leader_next_state(sm));
		goto again;
	case LS_FINAL:
		sm_move(sm, leader_next_state(sm));
		break;
	case LS_PAGE_READ:
	case LS_SNAP_DONE:
	case LS_F_NEEDS_SNAP:
	case LS_REQ_SIG_LOOP:
	case LS_CHECK_F_HAS_SIGS:
		leader_rpc_tick(&leader->rpc);
		switch (sm_state(&leader->rpc.sm)) {
		case RPC_SENT:
			leader_to_start(leader, &leader->rpc.timeout, 10000, rpc_to_cb);
			return;
		case RPC_REPLIED:
			leader_to_cancel(leader, &leader->rpc.timeout);
			rpc_fini(&leader->rpc);
			sm_move(sm, leader_next_state(sm));
			goto again;
		}

		rpc_fill_leader(leader);
		rc = rpc_send(&leader->rpc, ops->sender_send, leader_sent_cb);
		if (rc != 0) {
			goto again;
		}
		break;
	case LS_WAIT_SIGS:
		if (leader_next_state(sm) > sm_state(sm)) {
			sm_move(sm, leader_next_state(sm));
			goto again;
		}

		leader_to_start(leader, &leader->timeout, 10000, leader_to_cb);
		sm_move(sm, leader_next_state(sm));
		break;
	default:
		IMPOSSIBLE("");
	}
}

__attribute__((unused)) void follower_tick(struct follower *follower, const struct raft_message *incoming)
{
	(void)follower_sm_conf;
	(void)follower_sm_invariant;
	int rc;
	struct sm *sm = &follower->sm;
	const struct follower_ops *ops = follower->ops;

	if (!is_a_trigger_follower(follower, incoming) ||
	    is_a_duplicate(follower, incoming))
		return;

	PRE(!ops->is_pool_thread());

again:
	switch (sm_state(&follower->sm)) {
	case FS_NORMAL:
	case FS_SIGS_CALC_LOOP:
	case FS_SIG_READ:
	case FS_CHUNCK_APPLIED:
	case FS_SNAP_DONE:
		follower_rpc_tick(&follower->rpc);
		if (sm_state(&follower->rpc.sm) == RPC_SENT) {
			rpc_fini(&follower->rpc);
			sm_move(sm, follower_next_state(sm));
			goto again;
		}
		rpc_fill_follower(follower);
		rc = rpc_send(&follower->rpc, ops->sender_send, follower_sent_cb);
		if (rc != 0) {
			goto again;
		}
		break;
	case FS_SIG_PROCESSED:
	case FS_CHUNCK_PROCESSED:
	case FS_HT_WAIT:
		sm_move(sm, follower_next_state(sm));
		goto again;
	case FS_HT_CREATE:
	case FS_SIGS_CALC_STARTED:
	case FS_SIG_RECEIVING:
	case FS_CHUNCK_RECEIVING:
		work_fill_follower(follower);
		ops->work_queue(&follower->work, follower->work_cb, follower_work_done);
		sm_move(sm, follower_next_state(sm));
		break;
	case FS_SIG_REPLIED:
	case FS_SIGS_CALC_DONE:
	case FS_SIGS_CALC_MSG_RECEIVED:
	case FS_CHUNCK_REPLIED:
	case FS_FINAL:
		sm_move(sm, follower_next_state(sm));
		break;
	default:
		IMPOSSIBLE("");
	}

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
