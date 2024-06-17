#include <stdio.h>
#include <stddef.h>
#include "sm.h"


// SM -------------------------------------------------------------------------

enum leader_states {
	L_F_ONLINE,
	L_HT_WAIT,
	L_F_NEEDS_SNAP,
	L_CHECK_F_HAS_SIGS,
	L_WAIT_SIGS,

	L_REQ_SIG_LOOP,
	L_RECV_SIG_PART,
	L_PERSISTED_SIG_PART,

	L_READ_PAGES_LOOP,
	L_PAGE_READ,
	L_PAGE_SENT,

	L_SNAP_DONE,
	L_FINAL,

	L_NR,
};

/*
L_F_ONLINE
|
|
V
L_HT_WAIT
|
|
V
L_F_NEEDS_SNAP
|
|
V
L_CHECK_F_HAS_SIGS <-+
|                    |
|                    |
V                    |
L_WAIT_SIGS ---------+
|
|
V
L_REQ_SIG_LOOP <-----------+
|                          |
|                          |
V                          |
L_RECV_SIG_PART            |
|                          |
|                          |
V                          |
L_PERSISTED_SIG_PART ------+
|
|
V
L_READ_PAGES_LOOP <--------+
|                          |
|                          |
V                          |
L_PAGE_READ                |
|                          |
|                          |
V                          |
L_PAGE_SENT ----------------
|
|
V
L_SNAP_DONE
|
|
V
L_FINAL
|
|
V


 */

static const struct sm_conf leader_sm_conf[L_NR] = {
	[L_F_ONLINE] = {
		.flags   = SM_INITIAL | SM_FINAL,
		.name    = "online",
		.allowed = BITS(L_HT_WAIT)
			 | BITS(L_F_ONLINE),
	},
	[L_HT_WAIT] = {
		.name    = "ht-wait",
		.allowed = BITS(L_F_NEEDS_SNAP),
	},
	[L_F_NEEDS_SNAP] = {
		.name    = "needs-snapshot",
		.allowed = BITS(L_CHECK_F_HAS_SIGS)
			 | BITS(L_F_NEEDS_SNAP)
		         | BITS(L_F_ONLINE),
	},
	[L_CHECK_F_HAS_SIGS] = {
		.name    = "check-f-has-sigs",
		.allowed = BITS(L_CHECK_F_HAS_SIGS)
		         | BITS(L_WAIT_SIGS)
		         | BITS(L_F_ONLINE),
	},
	[L_WAIT_SIGS] = {
		.name    = "wait-sigs",
		.allowed = BITS(L_CHECK_F_HAS_SIGS)
			 | BITS(L_REQ_SIG_LOOP)
		         | BITS(L_F_ONLINE),
	},
	[L_REQ_SIG_LOOP] = {
		.name    = "req-sig-loop",
		.allowed = BITS(L_RECV_SIG_PART)
		         | BITS(L_F_ONLINE),
	},
	[L_RECV_SIG_PART] = {
		.name    = "recv-sig",
		.allowed = BITS(L_PERSISTED_SIG_PART)
			 | BITS(L_REQ_SIG_LOOP)
		         | BITS(L_F_ONLINE),
	},
	[L_PERSISTED_SIG_PART] = {
		.name    = "pers-sig",
		.allowed = BITS(L_READ_PAGES_LOOP)
			 | BITS(L_REQ_SIG_LOOP)
		         | BITS(L_F_ONLINE),
	},
	[L_READ_PAGES_LOOP] = {
		.name    = "read-pages-loop",
		.allowed = BITS(L_PAGE_READ)
		         | BITS(L_F_ONLINE),
	},
	[L_PAGE_READ] = {
		.name    = "page-read",
		.allowed = BITS(L_PAGE_SENT)
		         | BITS(L_F_ONLINE),
	},
	[L_PAGE_SENT] = {
		.name    = "page-sent",
		.allowed = BITS(L_READ_PAGES_LOOP)
			 | BITS(L_SNAP_DONE)
		         | BITS(L_F_ONLINE),
	},
	[L_SNAP_DONE] = {
		.name    = "snap-done",
		.allowed = BITS(L_SNAP_DONE)
		         | BITS(L_FINAL),
	},
	[L_FINAL] = {
		.name    = "final",
		.allowed = BITS(L_F_ONLINE),
	},
};

enum rpc_state {
	RPC_INIT,
	RPC_SENT,
	RPC_TIMEDOUT,
	RPC_REPLIED,
	RPC_ERROR,
	RPC_NR,
};

static const struct sm_conf rpc_sm_conf[RPC_NR] = {
	[RPC_INIT] = {
		.flags   = SM_INITIAL | SM_FINAL,
		.name    = "init",
		.allowed = BITS(RPC_SENT) | BITS(RPC_ERROR),
	},
	[RPC_SENT] = {
		.name    = "sent",
		.allowed = BITS(RPC_TIMEDOUT)
		         | BITS(RPC_REPLIED)
		         | BITS(RPC_ERROR),
	},
	[RPC_TIMEDOUT] = {
		.name    = "timedout",
		.allowed = BITS(RPC_INIT),
	},
	[RPC_REPLIED] = {
		.name    = "replied",
		.allowed = BITS(RPC_INIT),
	},
	[RPC_ERROR] = {
		.name    = "error",
		.allowed = BITS(RPC_INIT),
	},
};

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

static const struct sm_conf to_sm_conf[TO_NR] = {
	[TO_INIT] = {
		.flags   = SM_INITIAL | SM_FINAL,
		.name    = "to_init",
		.allowed = BITS(TO_STARTED),
	},
	[TO_STARTED] = {
		.flags   = SM_FINAL,
		.name    = "to_started",
		.allowed = BITS(TO_EXPIRED) | BITS(TO_CANCELED),
	},
	[TO_EXPIRED] = {
		.flags   = SM_FINAL,
		.name    = "to_expired",
	},
	[TO_CANCELED] = {
		.flags   = SM_FINAL,
		.name    = "w_canceled",
	},
};

// DATA ------------------------------------------------------------------------

enum message_type {
	M_APPEND_ENTRIES,
	M_APPEND_ENTRIES_REPLY,

	M_IS_INSTALL_SNAPSHOT,
	M_IS_INSTALL_SNAPSHOT_RESULT,
	M_IS_SIGNATURE_GET,
	M_IS_SIGNATURE_RESULT,
	M_IS_PAGES_GET,
	M_IS_PAGES_REPLY,
};

struct message {
	enum message_type type;
	int result;
};

#define M_TIMEOUT    ((const struct message *) 2)
#define M_WORK_DONE  ((const struct message *) 1)

struct pages {
	const char *db;
	unsigned    nr;
	unsigned    off;
	void      **pages;
};

struct signatures {
	const char *db;
	unsigned    nr;
	int         signatures;
};

// IPC -------------------------------------------------------------------------

struct work;
struct sender;
struct timeout;

typedef void (*to__cb)(struct timeout *t, int rc);
typedef void (*work__cb)(struct work *w);
typedef void (*sender__cb)(struct sender *s, int rc);

struct work {
	work__cb work_cb;
	work__cb after_cb;
	struct sm sm;
};

struct sender {
	sender__cb cb;
};

struct timeout {
	to__cb cb;
	struct sm sm;
};

struct rpc {
	struct sm sm;
	struct sender sender;
	struct message message;
	struct timeout timeout;
	const struct leader *leader;
};

struct leader_ops {
	work__cb ht__create;

	void (*to__stop)(struct timeout *to);
	void (*to__start)(struct timeout *to, unsigned delay, to__cb cb);

	int  (*sender__send)(struct sender *s, struct message *payload,
			     sender__cb cb);

	void (*work__queue)(struct work *w,
			    work__cb work_cb, work__cb after_cb);
};

struct leader {
	struct sm sm;
	struct rpc rpc;
	struct work work;
	work__cb work_cb;
	struct timeout timeout;
	const struct leader_ops *ops;

	/* dummy flags */
	bool sigs_calculated;
	bool sigs_more;
	bool pages_more;
};

// ----------------------------------------------------------------------------

static void leader_tick(struct leader *leader, const struct message *incoming);

static bool work_sm_invariant(const struct sm *sm, int prev_state)
{
	return true;
}

static bool leader_sm_invariant(const struct sm *sm, int prev_state)
{
	return true;
}

static bool rpc_sm_invariant(const struct sm *sm, int prev_state)
{
	return true;
}

static bool to_sm_invariant(const struct sm *sm, int prev_state)
{
	return true;
}

static void work_done(struct work *w)
{
	struct leader *leader = container_of(w, struct leader, work);
	sm_move(&w->sm, WORK_DONE);
	leader_tick(leader, M_WORK_DONE);
}

static void to_init(struct timeout *to)
{
	static const char *to_sm_name = "to";
	to->sm = (struct sm){ .name = to_sm_name };
	sm_init(&to->sm, to_sm_invariant, NULL, to_sm_conf, TO_INIT);
}

static void to_cb(struct timeout *t, int rc)
{
	struct leader *leader = container_of(t, struct leader, timeout);
	sm_move(&t->sm, TO_EXPIRED);
	leader_tick(leader, M_TIMEOUT);
}

static void to_start(struct timeout *to, unsigned delay, to__cb to_cb)
{
	struct leader *leader = container_of(to, struct leader, timeout);

	to_init(to);
	leader->ops->to__start(&leader->timeout, 10000, to_cb);
	sm_to_sm_obs(&leader->sm, &to->sm);
	sm_move(&to->sm, TO_STARTED);
}

static void sent_cb(struct sender *s, int rc)
{
	struct rpc *rpc = container_of(s, struct rpc, sender);

	if (unlikely(rc != 0)) {
		sm_move(&rpc->sm, RPC_ERROR);
		return;
	}

	sm_move(&rpc->sm, RPC_SENT);
	rpc->leader->ops->to__start(&rpc->timeout, 10000, to_cb);
}

static bool is_a_trigger(const struct leader  *leader,
			 const struct message *incoming)
{
	return true;
}

static bool is_a_duplicate(const struct leader  *leader,
			   const struct message *incoming)
{
	return false;
}

static void work_init(struct work *w)
{
	static const char *work_sm_name = "work";
	w->sm = (struct sm){ .name = work_sm_name };
	sm_init(&w->sm, work_sm_invariant, NULL, work_sm_conf, WORK_INIT);
}

static void rpc_init(struct rpc *rpc)
{
	static const char *rpc_sm_name = "rpc";
	rpc->sm = (struct sm){ .name = rpc_sm_name };
	sm_init(&rpc->sm, rpc_sm_invariant, NULL, rpc_sm_conf, RPC_INIT);
}

static void work_fill(struct leader *leader)
{
	leader->work_cb = leader->ops->ht__create;
	work_init(&leader->work);
	sm_to_sm_obs(&leader->sm, &leader->work.sm);
}

static void rpc_fill(struct leader *leader)
{
	//switch(sm_state()) {
	// L_F_ONLINE:
	//     fill_online_message()...
	//}

	leader->rpc.leader = leader;
	rpc_init(&leader->rpc);
	sm_to_sm_obs(&leader->sm, &leader->rpc.sm);
}

static int rpc_send(struct rpc *rpc, to__cb to_cb, sender__cb sent_cb)
{
	int rc = rpc->leader->ops->sender__send(&rpc->sender, &rpc->message,
						sent_cb);
	return RC(rc);
}

static void rpc_tick(struct rpc *rpc)
{
	switch(sm_state(&rpc->sm)) {
	case RPC_INIT:
		break;
	case RPC_SENT:
		sm_move(&rpc->sm, RPC_REPLIED);
		break;
	case RPC_TIMEDOUT:
	case RPC_REPLIED:
	case RPC_ERROR:
	default:
	}
}

static void leader_reset(struct leader *leader)
{
}

static bool is_an_unexpected_trigger(const struct leader  *leader,
				     const struct message *incoming)
{
	return false;
}

static enum leader_states next_state(struct sm *sm)
{
	struct leader *leader = container_of(sm, struct leader, sm);

	switch (sm_state(sm)) {
	case L_WAIT_SIGS:
		return sm_state(sm) + (leader->sigs_calculated ? +1 : -1);
	case L_FINAL:
		return L_F_ONLINE;
	}

	return sm_state(sm) + 1;
}

static void leader_tick(struct leader *leader, const struct message *incoming)
{
	int rc;
	struct sm *sm = &leader->sm;
	const struct leader_ops *ops = leader->ops;

	if (!is_a_trigger(leader, incoming) ||
	    is_a_duplicate(leader, incoming))
		return;

	if (is_an_unexpected_trigger(leader, incoming)) {
		leader_reset(leader);
		return;
	}

again:
	switch(sm_state(sm)) {
	case L_F_ONLINE:
	case L_RECV_SIG_PART:
	case L_READ_PAGES_LOOP:
		work_fill(leader);
		ops->work__queue(&leader->work, leader->work_cb, work_done);
		sm_move(sm, next_state(sm));
		break;
	case L_HT_WAIT:
	case L_PAGE_SENT:
	case L_PERSISTED_SIG_PART:
		sm_move(sm, next_state(sm));
		goto again;
	case L_FINAL:
		sm_move(sm, next_state(sm));
		break;
	case L_PAGE_READ:
	case L_SNAP_DONE:
	case L_F_NEEDS_SNAP:
	case L_REQ_SIG_LOOP:
	case L_CHECK_F_HAS_SIGS:
		rpc_tick(&leader->rpc);
		if (sm_state(&leader->rpc.sm) == RPC_REPLIED) {
			rpc_init(&leader->rpc);
			sm_move(sm, next_state(sm));
			goto again;
		}

		rpc_fill(leader);
		rc = rpc_send(&leader->rpc, to_cb, sent_cb);
		if (rc == 0)
			return;
		goto again;
	case L_WAIT_SIGS:
		if (next_state(sm) > sm_state(sm)) {
			sm_move(sm, next_state(sm));
			goto again;
		}

		to_start(&leader->timeout, 10000, to_cb);
		sm_move(sm, next_state(sm));
		break;
	default:
		IMPOSSIBLE("");
	}
}

// UT -------------------------------------------------------------------------

static void ut_message_received(struct leader *leader,
				const struct message *incoming)
{
	leader_tick(leader, incoming);
}

static void ut_ht_create_op(struct work *w)
{
}

static void ut_disk_io(struct leader *leader)
{
	leader->work.work_cb(&leader->work);
}

static void ut_disk_io_done(struct leader *leader)
{
	leader->work.after_cb(&leader->work);
}

static void ut_to_expired(struct leader *leader)
{
	leader->timeout.cb(&leader->timeout, 0);
}

static void ut_rpc_sent(struct leader *leader)
{
	leader->rpc.sender.cb(&leader->rpc.sender, 0);
}

static const struct message *append_entries(void)
{
	static struct message append_entries = {
		.type = M_APPEND_ENTRIES,
	};

	return &append_entries;
}

static const struct message *ut_install_snapshot_result(void)
{
	static struct message ut_install_snapshot_result = {
		.type = M_IS_INSTALL_SNAPSHOT_RESULT,
	};

	return &ut_install_snapshot_result;
}

static const struct message *ut_sign_result(void)
{
	static struct message ut_sign_result = {
		.type = M_IS_SIGNATURE_RESULT,
	};

	return &ut_sign_result;
}

static const struct message *ut_page_result(void)
{
	static struct message ut_sign_result = {
		.type = M_IS_PAGES_REPLY,
	};

	return &ut_sign_result;
}

void ut_work_queue_op(struct work *w, work__cb work_cb, work__cb after_cb)
{
	w->work_cb = work_cb;
	w->after_cb = after_cb;
}

void ut_to_start_op(struct timeout *to, unsigned delay, to__cb cb)
{
	to->cb = cb;
}

void ut_to_stop_op(struct timeout *to)
{
}

int ut_sender_send_op(struct sender *s, struct message *payload, sender__cb cb)
{
	s->cb = cb;
	return 0;
}

int main(void)
{
	struct leader_ops ops = {
		.to__stop = ut_to_stop_op,
		.to__start = ut_to_start_op,
		.ht__create = ut_ht_create_op,
		.work__queue = ut_work_queue_op,
		.sender__send = ut_sender_send_op,
	};

	struct leader leader = {
		.ops = &ops,
		.sm = { .name = "leader" },

		.sigs_more = false,
		.pages_more = false,
		.sigs_calculated = false,
	};

	sm_init(&leader.sm, leader_sm_invariant,
		NULL, leader_sm_conf, L_F_ONLINE);

	//PRE(sm_state(&leader.sm) == L_F_ONLINE);
	ut_message_received(&leader, append_entries());

	//PRE(sm_state(&leader.sm) == L_HT_WAIT);
	ut_disk_io(&leader);
	ut_disk_io_done(&leader);

	//PRE(sm_state(&leader.sm) == L_F_NEEDS_SNAP);
	ut_rpc_sent(&leader);
	ut_message_received(&leader, ut_install_snapshot_result());

	//PRE(sm_state(&leader.sm) == L_CHECK_F_HAS_SIGS);
	ut_rpc_sent(&leader);
	ut_message_received(&leader, ut_sign_result());
	ut_to_expired(&leader);
	leader.sigs_calculated = true;
	ut_rpc_sent(&leader);
	ut_message_received(&leader, ut_sign_result());

	//PRE(sm_state(&leader.sm) == L_REQ_SIG_LOOP);
	ut_rpc_sent(&leader);
	ut_message_received(&leader, ut_sign_result());
	ut_disk_io(&leader);
	ut_disk_io_done(&leader);
	ut_disk_io(&leader);
	ut_disk_io_done(&leader);

	//PRE(sm_state(&leader.sm) == L_PAGE_READ);
	ut_rpc_sent(&leader);
	ut_message_received(&leader, ut_page_result());

	//PRE(sm_state(&leader.sm) == L_SNAP_DONE);
	ut_rpc_sent(&leader);
	ut_message_received(&leader, ut_install_snapshot_result());

	sm_fini(&leader.sm);
	return 0;
}
