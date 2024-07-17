/* InstallSnapshot RPC handlers. */
#ifndef RECV_INSTALL_SNAPSHOT_H_
#define RECV_INSTALL_SNAPSHOT_H_

#include <sqlite3.h>
#include <stdint.h>

#include "../raft.h"
#include "../lib/threadpool.h"

struct work;
struct sender;
struct timeout;

typedef void (*to_cb_op)(uv_timer_t *handle);
typedef void (*work_op)(pool_work_t *w);
typedef void (*sender_cb_op)(struct sender *s, int rc);

struct work {
	struct sm sm;
	work_op work_cb;
	work_op after_cb;

	pool_work_t pool_work;
};

struct sender {
	// TODO embbed the uv req here.
	sender_cb_op cb;
};

struct timeout {
	struct sm sm;
	to_cb_op cb;

	uv_timer_t handle;
};

struct rpc {
	struct sm sm;
	struct sender sender;
	struct raft_message message;
	struct timeout timeout;
};

typedef int (*sender_send_op)(struct sender *s,
		struct raft_message *payload,
		sender_cb_op cb);

struct leader_ops {
	work_op ht_create;

	void (*to_init)(struct timeout *to);
	void (*to_stop)(struct timeout *to);
	void (*to_start)(struct timeout *to, unsigned delay, to_cb_op cb);

	sender_send_op sender_send;

	void (*work_queue)(struct work *w,
			    work_op work, work_op after_cb);
	bool (*is_main_thread)(void);
};

struct follower_ops {
	work_op ht_create;
	work_op fill_ht;
	work_op read_sig;
	work_op write_chunk;

	sender_send_op sender_send;
	void (*work_queue)(struct work *w,
			    work_op work, work_op after_cb);
	bool (*is_main_thread)(void);
};

struct leader {
	struct sm sm;
	struct rpc rpc;
	struct work work;
	work_op work_cb;
	struct timeout timeout;
	const struct leader_ops *ops;

	/* TODO dummy flags */
	bool sigs_calculated;
	bool sigs_more;
	bool pages_more;
};

struct follower {
	struct sm sm;
	struct rpc rpc;
	struct work work;
	work_op work_cb;
	const struct follower_ops *ops;

	/* TODO dummy flags */
	bool sigs_calculated;
};

void leader_tick(struct leader *leader, const struct raft_message *incoming);
void follower_tick(struct follower *follower, const struct raft_message *incoming);

/* TODO make all of these private and static once we can write tests without
 * depending on the states. */
bool leader_sm_invariant(const struct sm *sm, int prev_state);
bool follower_sm_invariant(const struct sm *sm, int prev_state);

enum leader_states {
	LS_F_ONLINE,
	LS_HT_WAIT,
	LS_F_NEEDS_SNAP,
	LS_CHECK_F_HAS_SIGS,
	LS_WAIT_SIGS,

	LS_REQ_SIG_LOOP,
	LS_RECV_SIG_PART,
	LS_PERSISTED_SIG_PART,

	LS_READ_PAGES_LOOP,
	LS_PAGE_READ,
	LS_PAGE_SENT,

	LS_SNAP_DONE,
	LS_FINAL,

	LS_NR,
};

/* clang-format off */
static const struct sm_conf leader_sm_conf[LS_NR] = {
	[LS_F_ONLINE] = {
		.flags   = SM_INITIAL | SM_FINAL,
		.name    = "online",
		.allowed = BITS(LS_HT_WAIT)
		         | BITS(LS_F_ONLINE),
	},
	[LS_HT_WAIT] = {
		.name    = "ht-wait",
		.allowed = BITS(LS_F_NEEDS_SNAP),
	},
	[LS_F_NEEDS_SNAP] = {
		.name    = "needs-snapshot",
		.allowed = BITS(LS_CHECK_F_HAS_SIGS)
		         | BITS(LS_F_NEEDS_SNAP)
		         | BITS(LS_F_ONLINE),
	},
	[LS_CHECK_F_HAS_SIGS] = {
		.name    = "check-f-has-sigs",
		.allowed = BITS(LS_CHECK_F_HAS_SIGS)
		         | BITS(LS_WAIT_SIGS)
		         | BITS(LS_F_ONLINE),
	},
	[LS_WAIT_SIGS] = {
		.name    = "wait-sigs",
		.allowed = BITS(LS_CHECK_F_HAS_SIGS)
		         | BITS(LS_REQ_SIG_LOOP)
		         | BITS(LS_F_ONLINE),
	},
	[LS_REQ_SIG_LOOP] = {
		.name    = "req-sig-loop",
		.allowed = BITS(LS_RECV_SIG_PART)
		         | BITS(LS_F_ONLINE),
	},
	[LS_RECV_SIG_PART] = {
		.name    = "recv-sig",
		.allowed = BITS(LS_PERSISTED_SIG_PART)
		         | BITS(LS_REQ_SIG_LOOP)
		         | BITS(LS_F_ONLINE),
	},
	[LS_PERSISTED_SIG_PART] = {
		.name    = "pers-sig",
		.allowed = BITS(LS_READ_PAGES_LOOP)
		         | BITS(LS_REQ_SIG_LOOP)
		         | BITS(LS_F_ONLINE),
	},
	[LS_READ_PAGES_LOOP] = {
		.name    = "read-pages-loop",
		.allowed = BITS(LS_PAGE_READ)
		         | BITS(LS_F_ONLINE),
	},
	[LS_PAGE_READ] = {
		.name    = "page-read",
		.allowed = BITS(LS_PAGE_SENT)
		         | BITS(LS_F_ONLINE),
	},
	[LS_PAGE_SENT] = {
		.name    = "page-sent",
		.allowed = BITS(LS_READ_PAGES_LOOP)
		         | BITS(LS_SNAP_DONE)
		         | BITS(LS_F_ONLINE),
	},
	[LS_SNAP_DONE] = {
		.name    = "snap-done",
		.allowed = BITS(LS_SNAP_DONE)
		         | BITS(LS_FINAL),
	},
	[LS_FINAL] = {
		.name    = "final",
		.allowed = BITS(LS_F_ONLINE),
	},
};
/* clang-format on */

enum follower_states {
	FS_NORMAL,

	FS_HT_CREATE,
	FS_HT_WAIT,

	FS_SIGS_CALC_STARTED,
	FS_SIGS_CALC_LOOP,
	FS_SIGS_CALC_MSG_RECEIVED,
	FS_SIGS_CALC_DONE,

	FS_SIG_RECEIVING,
	FS_SIG_PROCESSED,
	FS_SIG_READ,
	FS_SIG_REPLIED,

	FS_CHUNCK_RECEIVING,
	FS_CHUNCK_PROCESSED,
	FS_CHUNCK_APPLIED,
	FS_CHUNCK_REPLIED,

	FS_SNAP_DONE,
	FS_FINAL,

	FS_NR,
};

/* clang-format off */
static const struct sm_conf follower_sm_conf[FS_NR] = {
	[FS_NORMAL] = {
		.flags = SM_INITIAL | SM_FINAL,
		.name = "normal",
		.allowed = BITS(FS_HT_CREATE)
		         | BITS(FS_NORMAL),
	},
	[FS_HT_CREATE] = {
		.name = "ht_create",
		.allowed = BITS(FS_HT_WAIT)
		         | BITS(FS_NORMAL),
	},
	[FS_HT_WAIT] = {
		.name = "ht_waiting",
		.allowed = BITS(FS_SIGS_CALC_STARTED)
		         | BITS(FS_NORMAL),
	},
	[FS_SIGS_CALC_STARTED] = {
		.name = "signatures_calc_started",
		.allowed = BITS(FS_SIGS_CALC_LOOP)
		         | BITS(FS_NORMAL),
	},
	[FS_SIGS_CALC_LOOP] = {
		.name = "signatures_calc_loop",
		.allowed = BITS(FS_SIGS_CALC_MSG_RECEIVED)
		         | BITS(FS_SIGS_CALC_DONE)
		         | BITS(FS_NORMAL),
	},
	[FS_SIGS_CALC_MSG_RECEIVED] = {
		.name = "signatures_msg_received",
		.allowed = BITS(FS_SIGS_CALC_LOOP)
		         | BITS(FS_NORMAL),
	},
	[FS_SIGS_CALC_DONE] = {
		.name = "signatures_calc_done",
		.allowed = BITS(FS_SIG_RECEIVING)
		         | BITS(FS_NORMAL),
	},
	[FS_SIG_RECEIVING] = {
		.name = "signature_received",
		.allowed = BITS(FS_SIG_PROCESSED)
		         | BITS(FS_NORMAL),
	},
	[FS_SIG_PROCESSED] = {
		.name = "signature_processed",
		.allowed = BITS(FS_SIG_READ)
		         | BITS(FS_NORMAL),
	},
	[FS_SIG_READ] = {
		.name = "signature_read",
		.allowed = BITS(FS_SIG_REPLIED)
		         | BITS(FS_NORMAL),
	},
	[FS_SIG_REPLIED] = {
		.name = "signature_sent",
		.allowed = BITS(FS_CHUNCK_RECEIVING)
		         | BITS(FS_SIG_RECEIVING)
		         | BITS(FS_NORMAL),
	},
	[FS_CHUNCK_RECEIVING] = {
		.name = "chunk_received",
		.allowed = BITS(FS_CHUNCK_PROCESSED)
		         | BITS(FS_NORMAL),
	},
	[FS_CHUNCK_PROCESSED] = {
		.name = "chunk_processed",
		.allowed = BITS(FS_CHUNCK_APPLIED)
		         | BITS(FS_NORMAL),
	},
	[FS_CHUNCK_APPLIED] = {
		.name = "chunk_applied",
		.allowed = BITS(FS_CHUNCK_REPLIED)
		         | BITS(FS_NORMAL),
	},
	[FS_CHUNCK_REPLIED] = {
		.name = "chunk_replied",
		.allowed = BITS(FS_CHUNCK_PROCESSED)
		         | BITS(FS_SNAP_DONE)
		         | BITS(FS_NORMAL),
	},
	[FS_SNAP_DONE] = {
		.name = "snap_done",
		.allowed = BITS(FS_FINAL)
		         | BITS(FS_NORMAL),
	},
	[FS_FINAL] = {
		.name = "final",
		.allowed = BITS(FS_NORMAL),
	},
};
/* clang-format on */
/* end of TODO make this private */

/* Process an InstallSnapshot RPC from the given server. */
int recvInstallSnapshot(struct raft *r,
			raft_id id,
			const char *address,
			struct raft_install_snapshot *args);

#endif /* RECV_INSTALL_SNAPSHOT_H_ */
