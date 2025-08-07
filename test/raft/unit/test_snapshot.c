#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <uv.h>
#include "../../../src/lib/sm.h"
#include "../../../src/lib/threadpool.h"
#include "../../../src/raft.h"
#include "../../../src/raft/recv_install_snapshot.h"
#include "../../../src/utils.h"
#include "../lib/runner.h"

struct fixture {};

static void *set_up(MUNIT_UNUSED const MunitParameter params[],
		    MUNIT_UNUSED void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	return f;
}

static void tear_down(void *data)
{
	free(data);
}

SUITE(snapshot_leader)
SUITE(snapshot_follower)

static void ut_leader_message_received(struct leader *leader,
				       const struct raft_message *incoming)
{
	leader_tick(leader, incoming);
}

static void ut_follower_message_received(struct follower *follower,
					 const struct raft_message *incoming)
{
	follower_tick(follower, incoming);
}

static void ut_ht_create_op(pool_work_t *w)
{
	(void)w;
}

static void ut_fill_ht_op(pool_work_t *w)
{
	(void)w;
}

static void ut_write_chunk_op(pool_work_t *w)
{
	(void)w;
}

static void ut_read_sig_op(pool_work_t *w)
{
	(void)w;
}

static void ut_disk_io(struct work *work)
{
	work->work_cb(&work->pool_work);
}

static void ut_disk_io_done(struct work *work)
{
	work->after_cb(&work->pool_work);
}

static void ut_to_expired(struct leader *leader)
{
	leader->timeout.cb(&leader->timeout.handle);
}

static void ut_rpc_sent(struct rpc *rpc)
{
	rpc->sender.cb(&rpc->sender, 0);
}

static void ut_rpc_to_expired(struct rpc *rpc)
{
	rpc->timeout.cb(&rpc->timeout.handle);
}

static const struct raft_message *append_entries_result(void)
{
	static struct raft_message append_entries = {
		.type = RAFT_IO_APPEND_ENTRIES_RESULT,
	};

	return &append_entries;
}

static const struct raft_message *ut_install_snapshot(void)
{
	static struct raft_message ut_install_snapshot = {
		.type = RAFT_IO_INSTALL_SNAPSHOT,
	};

	return &ut_install_snapshot;
}

static const struct raft_message *ut_install_snapshot_result(void)
{
	static struct raft_message ut_install_snapshot_result = {
		.type = RAFT_IO_INSTALL_SNAPSHOT_RESULT,
	};

	return &ut_install_snapshot_result;
}

static const struct raft_message *ut_sign(void)
{
	static struct raft_message ut_sign = {
		.type = RAFT_IO_SIGNATURE,
	};

	return &ut_sign;
}

static const struct raft_message *ut_sign_result(void)
{
	static struct raft_message ut_sign_result = {
		.type = RAFT_IO_SIGNATURE_RESULT,
	};

	return &ut_sign_result;
}

static const struct raft_message *ut_page(void)
{
	static struct raft_message ut_page = {
		.type = RAFT_IO_INSTALL_SNAPSHOT_CP,
	};

	return &ut_page;
}

static const struct raft_message *ut_page_result(void)
{
	static struct raft_message ut_page_result = {
		.type = RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT,
	};

	return &ut_page_result;
}

static void ut_work_queue_op(struct work *w, work_op work_cb, work_op after_cb)
{
	w->work_cb = work_cb;
	w->after_cb = after_cb;
}

static void ut_to_init_op(struct timeout *to)
{
	(void)to;
}

static void ut_to_start_op(struct timeout *to, unsigned delay, to_cb_op cb)
{
	(void)delay;
	to->cb = cb;
}

static void ut_to_stop_op(struct timeout *to)
{
	(void)to;
}

static bool ut_msg_consumed = false;
static struct raft_message ut_last_msg_sent;

struct raft_message ut_get_msg_sent(void)
{
	munit_assert(!ut_msg_consumed);
	ut_msg_consumed = true;
	return ut_last_msg_sent;
}

int ut_sender_send_op(struct sender *s,
		      struct raft_message *payload,
		      sender_cb_op cb)
{
	ut_last_msg_sent = *payload;
	ut_msg_consumed = false;
	s->cb = cb;
	return 0;
}

static bool ut_is_pool_thread_op(void)
{
	return false;
}

TEST(snapshot_follower, basic, set_up, tear_down, 0, NULL)
{
	struct follower_ops ops = {
		.ht_create = ut_ht_create_op,
		.work_queue = ut_work_queue_op,
		.sender_send = ut_sender_send_op,
		.read_sig = ut_read_sig_op,
		.write_chunk = ut_write_chunk_op,
		.fill_ht = ut_fill_ht_op,
		.is_pool_thread = ut_is_pool_thread_op,
	};

	struct follower follower = {
		.ops = &ops,
	};

	sm_init(&follower.sm, follower_sm_invariant, NULL, follower_sm_conf,
		"follower", FS_NORMAL);

	PRE(sm_state(&follower.sm) == FS_NORMAL);
	ut_follower_message_received(&follower, ut_install_snapshot());
	ut_rpc_sent(&follower.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==,
			 RAFT_IO_INSTALL_SNAPSHOT_RESULT);
	ut_disk_io(&follower.work);

	PRE(sm_state(&follower.sm) == FS_HT_WAIT);
	ut_disk_io_done(&follower.work);

	PRE(sm_state(&follower.sm) == FS_SIGS_CALC_LOOP);
	ut_follower_message_received(&follower, ut_sign());
	ut_rpc_sent(&follower.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE_RESULT);

	PRE(sm_state(&follower.sm) == FS_SIGS_CALC_LOOP);
	ut_disk_io(&follower.work);
	follower.sigs_calculated = true;
	ut_disk_io_done(&follower.work);

	PRE(sm_state(&follower.sm) == FS_SIGS_CALC_LOOP);
	ut_follower_message_received(&follower, ut_sign());
	ut_rpc_sent(&follower.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE_RESULT);

	PRE(sm_state(&follower.sm) == FS_SIG_RECEIVING);
	ut_follower_message_received(&follower, ut_sign());

	PRE(sm_state(&follower.sm) == FS_SIG_PROCESSED);
	ut_disk_io(&follower.work);
	ut_disk_io_done(&follower.work);

	PRE(sm_state(&follower.sm) == FS_SIG_READ);
	ut_rpc_sent(&follower.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE_RESULT);

	PRE(sm_state(&follower.sm) == FS_CHUNCK_RECEIVING);
	ut_follower_message_received(&follower, ut_page());
	ut_disk_io(&follower.work);
	ut_disk_io_done(&follower.work);

	PRE(sm_state(&follower.sm) == FS_CHUNCK_APPLIED);
	ut_rpc_sent(&follower.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==,
			 RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT);

	PRE(sm_state(&follower.sm) == FS_SNAP_DONE);
	ut_follower_message_received(&follower, ut_install_snapshot());
	ut_rpc_sent(&follower.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==,
			 RAFT_IO_INSTALL_SNAPSHOT_RESULT);

	sm_fini(&follower.sm);
	return MUNIT_OK;
}

TEST(snapshot_leader, basic, set_up, tear_down, 0, NULL)
{
	struct leader_ops ops = {
		.to_init = ut_to_init_op,
		.to_stop = ut_to_stop_op,
		.to_start = ut_to_start_op,
		.ht_create = ut_ht_create_op,
		.work_queue = ut_work_queue_op,
		.sender_send = ut_sender_send_op,
		.is_pool_thread = ut_is_pool_thread_op,
	};

	struct leader leader = {
		.ops = &ops,

		.sigs_more = false,
		.pages_more = false,
		.sigs_calculated = false,
	};

	sm_init(&leader.sm, leader_sm_invariant, NULL, leader_sm_conf, "leader",
		LS_F_ONLINE);

	PRE(sm_state(&leader.sm) == LS_F_ONLINE);
	ut_leader_message_received(&leader, append_entries_result());

	PRE(sm_state(&leader.sm) == LS_HT_WAIT);
	ut_disk_io(&leader.work);
	ut_disk_io_done(&leader.work);

	PRE(sm_state(&leader.sm) == LS_F_NEEDS_SNAP);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT);
	ut_leader_message_received(&leader, ut_install_snapshot_result());

	PRE(sm_state(&leader.sm) == LS_CHECK_F_HAS_SIGS);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	ut_leader_message_received(&leader, ut_sign_result());
	ut_to_expired(&leader);
	leader.sigs_calculated = true;
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	ut_leader_message_received(&leader, ut_sign_result());

	PRE(sm_state(&leader.sm) == LS_REQ_SIG_LOOP);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	ut_rpc_sent(&leader.rpc);
	PRE(sm_state(&leader.sm) == LS_REQ_SIG_LOOP);
	ut_leader_message_received(&leader, ut_sign_result());
	ut_disk_io(&leader.work);
	ut_disk_io_done(&leader.work);
	ut_disk_io(&leader.work);
	ut_disk_io_done(&leader.work);

	PRE(sm_state(&leader.sm) == LS_PAGE_READ);
	munit_assert_int(ut_get_msg_sent().type, ==,
			 RAFT_IO_INSTALL_SNAPSHOT_CP);
	ut_rpc_sent(&leader.rpc);
	ut_leader_message_received(&leader, ut_page_result());

	PRE(sm_state(&leader.sm) == LS_SNAP_DONE);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT);
	ut_rpc_sent(&leader.rpc);
	ut_leader_message_received(&leader, ut_install_snapshot_result());

	sm_fini(&leader.sm);
	return MUNIT_OK;
}

TEST(snapshot_leader, timeouts, set_up, tear_down, 0, NULL)
{
	struct leader_ops ops = {
		.to_init = ut_to_init_op,
		.to_stop = ut_to_stop_op,
		.to_start = ut_to_start_op,
		.ht_create = ut_ht_create_op,
		.work_queue = ut_work_queue_op,
		.sender_send = ut_sender_send_op,
		.is_pool_thread = ut_is_pool_thread_op,
	};

	struct leader leader = {
		.ops = &ops,

		.sigs_more = false,
		.pages_more = false,
		.sigs_calculated = false,
	};

	sm_init(&leader.sm, leader_sm_invariant, NULL, leader_sm_conf, "leader",
		LS_F_ONLINE);

	PRE(sm_state(&leader.sm) == LS_F_ONLINE);
	ut_leader_message_received(&leader, append_entries_result());

	PRE(sm_state(&leader.sm) == LS_HT_WAIT);
	ut_disk_io(&leader.work);
	ut_disk_io_done(&leader.work);

	PRE(sm_state(&leader.sm) == LS_F_NEEDS_SNAP);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT);
	ut_rpc_to_expired(&leader.rpc);

	PRE(sm_state(&leader.sm) == LS_F_NEEDS_SNAP);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT);
	ut_leader_message_received(&leader, ut_install_snapshot_result());

	PRE(sm_state(&leader.sm) == LS_CHECK_F_HAS_SIGS);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	ut_leader_message_received(&leader, ut_sign_result());
	ut_to_expired(&leader);

	PRE(sm_state(&leader.sm) == LS_CHECK_F_HAS_SIGS);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	ut_rpc_to_expired(&leader.rpc);

	PRE(sm_state(&leader.sm) == LS_CHECK_F_HAS_SIGS);
	leader.sigs_calculated = true;
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	ut_leader_message_received(&leader, ut_sign_result());

	PRE(sm_state(&leader.sm) == LS_REQ_SIG_LOOP);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	PRE(sm_state(&leader.sm) == LS_REQ_SIG_LOOP);
	ut_leader_message_received(&leader, ut_sign_result());
	ut_disk_io(&leader.work);
	ut_disk_io_done(&leader.work);
	ut_disk_io(&leader.work);
	ut_disk_io_done(&leader.work);

	PRE(sm_state(&leader.sm) == LS_PAGE_READ);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==,
			 RAFT_IO_INSTALL_SNAPSHOT_CP);
	ut_rpc_to_expired(&leader.rpc);

	PRE(sm_state(&leader.sm) == LS_PAGE_READ);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==,
			 RAFT_IO_INSTALL_SNAPSHOT_CP);
	ut_leader_message_received(&leader, ut_page_result());

	PRE(sm_state(&leader.sm) == LS_SNAP_DONE);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT);
	ut_leader_message_received(&leader, ut_install_snapshot_result());

	sm_fini(&leader.sm);
	return MUNIT_OK;
}

struct test_fixture {
	uv_loop_t loop;
	pool_t pool;

	/* true when union contains leader, false when it contains follower. */
	bool is_leader;
	union {
		struct {
			struct leader leader;
			struct leader_ops leader_ops;
		};
		struct {
			struct follower follower;
			struct follower_ops follower_ops;
		};
	};

	/* We only expect one message to be in-flight. */
	struct raft_message last_msg_sent;
	/* Message was sent and has not been consumed, see uv_get_msg_sent(). */
	bool msg_valid;

	/* TODO: should accomodate several background jobs in the future.
	 * Probably by scheduling a barrier in the pool after all the works that
	 * toggles this flag. */
	work_op orig_work_cb;
	bool work_done;

	/* Work request */
	uv_work_t req;
};

static int fixture_progress(struct test_fixture *f, int n)
{
	int rv;
	for (int i = 0; i < n; i++) {
		rv = uv_run(&f->loop, UV_RUN_NOWAIT);
		if (rv == 0) {
			break;
		}
	}
	return rv;
}

static struct test_fixture *pool_set_up(void)
{
	/* Prevent hangs. */
	alarm(2);

	struct test_fixture *f = munit_malloc(sizeof(struct test_fixture));
	*f = (struct test_fixture){};
	uv_loop_init(&f->loop);
	pool_init(&f->pool, &f->loop, 4, POOL_QOS_PRIO_FAIR);
	f->pool.flags |= POOL_FOR_UT;

	return f;
}

static void close_cb(uv_handle_t *handle)
{
	(void)handle;
	munit_logf(MUNIT_LOG_INFO, "closed timer %p", handle);
	switch (uv_handle_get_type(handle)) {
#define RESET(UPPER, lower)                                     \
	case UV_##UPPER:                                        \
		*(uv_##lower##_t *)handle = (uv_##lower##_t){}; \
		break;
		UV_HANDLE_TYPE_MAP(RESET)
#undef RESET
		default:
			break;
	}
}

static void walk_cb(uv_handle_t *handle, void *arg)
{
	(void)arg;
	uv_handle_type type = uv_handle_get_type(handle);
	munit_errorf("handle alive: %p %s (%d)", handle,
		     uv_handle_type_name(type), type);
}

static void pool_tear_down(struct test_fixture *f)
{
	pool_close(&f->pool);
	munit_log(MUNIT_LOG_DEBUG, "running uv loop to the end");
	int rv = fixture_progress(f, 100);
	munit_assert_int(rv, ==, 0);
	pool_fini(&f->pool);

	rv = uv_loop_close(&f->loop);
	if (rv != 0) {
		uv_walk(&f->loop, walk_cb, NULL);
	}
	munit_assert_int(rv, ==, 0);
	alarm(0);
}

/* Advances libuv in the main thread until the in-flight background work is
 * finished.
 *
 * This function is designed with the constaint that there can only be one
 * request in-flight. It will hang until the work is finished. */
static void wait_work(struct test_fixture *f)
{
	PRE(!pool_is_pool_thread());

	// FIXME: don't wait forever.
	while (!f->work_done) {
		uv_run(&f->loop, UV_RUN_NOWAIT);
	}
}

/* Advances libuv in the main thread until the in-flight message that was queued
 * is sent.
 *
 * This function is designed with the constaint that there can only be one
 * message in-flight. It will hang until the message is sent. */
static void wait_msg_sent(struct test_fixture *f)
{
	PRE(!pool_is_pool_thread());

	// FIXME: don't wait forever.
	while (!f->msg_valid) {
		uv_run(&f->loop, UV_RUN_NOWAIT);
	}
}

/* Decorates the callback used when the pool work is done to set the test
 * fixture flag to true, then calls the original callback.*/
static void test_fixture_work_cb(pool_work_t *w)
{
	struct test_fixture *f =
	    CONTAINER_OF(w->pool, struct test_fixture, pool);
	f->work_done = true;
	f->orig_work_cb(w);
}

static void pool_to_start_op(struct timeout *to, unsigned delay, to_cb_op cb)
{
	to->cb = cb;
	uv_timer_start(&to->handle, cb, delay, 0);
}

static void pool_to_stop_op(struct timeout *to)
{
	uv_timer_stop(&to->handle);
}

static void pool_work_queue_op_leader(struct work *w,
				      work_op work_cb,
				      work_op after_cb)
{
	struct leader *leader = CONTAINER_OF(w, struct leader, work);
	struct test_fixture *f =
	    CONTAINER_OF(leader, struct test_fixture, leader);
	w->pool_work = (pool_work_t){};
	f->orig_work_cb = after_cb;
	f->work_done = false;
	pool_queue_work(&f->pool, &w->pool_work, 0, WT_UNORD, work_cb,
			test_fixture_work_cb);
}

static void pool_work_queue_op_follower(struct work *w,
					work_op work_cb,
					work_op after_cb)
{
	struct follower *follower = CONTAINER_OF(w, struct follower, work);
	struct test_fixture *f =
	    CONTAINER_OF(follower, struct test_fixture, follower);
	w->pool_work = (pool_work_t){};
	f->orig_work_cb = after_cb;
	f->work_done = false;
	pool_queue_work(&f->pool, &w->pool_work, 0, WT_UNORD, work_cb,
			test_fixture_work_cb);
}

static void pool_to_expired(struct leader *leader)
{
	uv_timer_start(&leader->timeout.handle, leader->timeout.cb, 0, 0);

	uv_loop_t *loop =
	    uv_handle_get_loop((uv_handle_t *)&leader->timeout.handle);
	struct test_fixture *f = CONTAINER_OF(loop, struct test_fixture, loop);
	fixture_progress(f, 20);
}

static void pool_rpc_to_expired(struct rpc *rpc)
{
	uv_timer_start(&rpc->timeout.handle, rpc->timeout.cb, 0, 0);

	uv_loop_t *loop =
	    uv_handle_get_loop((uv_handle_t *)&rpc->timeout.handle);
	struct test_fixture *f = CONTAINER_OF(loop, struct test_fixture, loop);
	fixture_progress(f, 20);
}

static void pool_ht_create_op(pool_work_t *w)
{
	struct test_fixture *f =
	    CONTAINER_OF(w->pool, struct test_fixture, pool);
	if (f->is_leader) {
		PRE(f->leader.ops->is_pool_thread());
	} else {
		PRE(f->follower.ops->is_pool_thread());
	}
}

static void pool_fill_ht_op(pool_work_t *w)
{
	struct test_fixture *f =
	    CONTAINER_OF(w->pool, struct test_fixture, pool);
	if (f->is_leader) {
		PRE(f->leader.ops->is_pool_thread());
	} else {
		PRE(f->follower.ops->is_pool_thread());
	}
}

static void pool_write_chunk_op(pool_work_t *w)
{
	struct work *work = CONTAINER_OF(w, struct work, pool_work);
	struct follower *follower = CONTAINER_OF(work, struct follower, work);
	PRE(follower->ops->is_pool_thread());
}

static void pool_read_sig_op(pool_work_t *w)
{
	struct work *work = CONTAINER_OF(w, struct work, pool_work);
	struct follower *follower = CONTAINER_OF(work, struct follower, work);
	PRE(follower->ops->is_pool_thread());
}

static void uv_sender_send_cb(uv_work_t *req)
{
	(void)req;
}

static void uv_sender_send_after_cb(uv_work_t *req, int status)
{
	struct test_fixture *f = CONTAINER_OF(req, struct test_fixture, req);
	f->msg_valid = true;
	struct sender *s = req->data;
	s->cb(s, status);
}

static int uv_sender_send_op(struct sender *s,
			     struct raft_message *payload,
			     sender_cb_op cb)
{
	/* This is a bit messy, but so is this whole interface. */
	struct rpc *rpc = CONTAINER_OF(s, struct rpc, sender);
	struct leader *leader = CONTAINER_OF(rpc, struct leader, rpc);
	struct test_fixture *f =
	    CONTAINER_OF(leader, struct test_fixture, leader);

	f->last_msg_sent = *payload;
	/* Flag is only toggled when the after_cb is called, emulating the
	 * message being sent. */
	f->msg_valid = false;
	s->cb = cb;
	f->req.data = s;

	uv_queue_work(&f->loop, &f->req, uv_sender_send_cb,
		      uv_sender_send_after_cb);
	return RAFT_OK;
}

struct raft_message uv_get_msg_sent(struct test_fixture *f)
{
	munit_assert(f->msg_valid);
	f->msg_valid = false;
	return f->last_msg_sent;
}

static void *setUpLeader(MUNIT_UNUSED const MunitParameter params[],
			 MUNIT_UNUSED void *user_data)
{
	struct test_fixture *f = pool_set_up();
	f->is_leader = true;
	f->leader_ops = (struct leader_ops){
		.to_init = ut_to_init_op,
		.to_stop = pool_to_stop_op,
		.to_start = pool_to_start_op,
		.ht_create = pool_ht_create_op,
		.work_queue = pool_work_queue_op_leader,
		.sender_send = uv_sender_send_op,
		.is_pool_thread = pool_is_pool_thread,
	};
	f->leader = (struct leader){
		.ops = &f->leader_ops,

		.sigs_more = false,
		.pages_more = false,
		.sigs_calculated = false,
	};
	sm_init(&f->leader.sm, leader_sm_invariant, NULL, leader_sm_conf,
		"leader", LS_F_ONLINE);
	uv_timer_init(&f->loop, &f->leader.timeout.handle);
	uv_timer_init(&f->loop, &f->leader.rpc.timeout.handle);

	return f;
}

static void tearDownLeader(void *data)
{
	struct test_fixture *f = data;

	uv_timer_stop(&f->leader.rpc.timeout.handle);
	uv_close((uv_handle_t *)&f->leader.rpc.timeout.handle, close_cb);

	uv_timer_stop(&f->leader.timeout.handle);
	uv_close((uv_handle_t *)&f->leader.timeout.handle, close_cb);
	pool_tear_down(f);
	free(data);
}

TEST(snapshot_leader, pool_timeouts, setUpLeader, tearDownLeader, 0, NULL)
{
	struct test_fixture *f = data;
	struct leader *leader = &f->leader;

	munit_log(MUNIT_LOG_DEBUG, "here 1");
	PRE(sm_state(&leader->sm) == LS_F_ONLINE);
	ut_leader_message_received(leader, append_entries_result());
	wait_work(f);

	munit_log(MUNIT_LOG_DEBUG, "here 2");
	PRE(sm_state(&leader->sm) == LS_F_NEEDS_SNAP);
	wait_msg_sent(f);
	munit_assert_int(uv_get_msg_sent(f).type, ==, RAFT_IO_INSTALL_SNAPSHOT);
	pool_rpc_to_expired(&leader->rpc);

	munit_log(MUNIT_LOG_DEBUG, "here 3");
	PRE(sm_state(&leader->sm) == LS_F_NEEDS_SNAP);
	wait_msg_sent(f);
	munit_assert_int(uv_get_msg_sent(f).type, ==, RAFT_IO_INSTALL_SNAPSHOT);
	ut_leader_message_received(leader, ut_install_snapshot_result());

	munit_log(MUNIT_LOG_DEBUG, "here 4");
	PRE(sm_state(&leader->sm) == LS_CHECK_F_HAS_SIGS);
	wait_msg_sent(f);
	munit_assert_int(uv_get_msg_sent(f).type, ==, RAFT_IO_SIGNATURE);
	ut_leader_message_received(leader, ut_sign_result());
	pool_to_expired(leader);

	munit_log(MUNIT_LOG_DEBUG, "here 5");
	PRE(sm_state(&leader->sm) == LS_CHECK_F_HAS_SIGS);
	wait_msg_sent(f);
	munit_assert_int(uv_get_msg_sent(f).type, ==, RAFT_IO_SIGNATURE);
	pool_rpc_to_expired(&leader->rpc);

	munit_log(MUNIT_LOG_DEBUG, "here 6");
	PRE(sm_state(&leader->sm) == LS_CHECK_F_HAS_SIGS);
	leader->sigs_calculated = true;
	wait_msg_sent(f);
	munit_assert_int(uv_get_msg_sent(f).type, ==, RAFT_IO_SIGNATURE);
	ut_leader_message_received(leader, ut_sign_result());

	munit_log(MUNIT_LOG_DEBUG, "here 7");
	PRE(sm_state(&leader->sm) == LS_REQ_SIG_LOOP);
	wait_msg_sent(f);
	munit_assert_int(uv_get_msg_sent(f).type, ==, RAFT_IO_SIGNATURE);
	PRE(sm_state(&leader->sm) == LS_REQ_SIG_LOOP);
	ut_leader_message_received(leader, ut_sign_result());

	munit_log(MUNIT_LOG_DEBUG, "here 8");
	wait_work(f);
	munit_log(MUNIT_LOG_DEBUG, "here 9");
	wait_work(f);

	munit_log(MUNIT_LOG_DEBUG, "here 10");
	PRE(sm_state(&leader->sm) == LS_PAGE_READ);
	wait_msg_sent(f);
	munit_assert_int(uv_get_msg_sent(f).type, ==,
			 RAFT_IO_INSTALL_SNAPSHOT_CP);
	pool_rpc_to_expired(&leader->rpc);

	munit_log(MUNIT_LOG_DEBUG, "here 11");
	PRE(sm_state(&leader->sm) == LS_PAGE_READ);
	wait_msg_sent(f);
	munit_assert_int(uv_get_msg_sent(f).type, ==,
			 RAFT_IO_INSTALL_SNAPSHOT_CP);
	ut_leader_message_received(leader, ut_page_result());

	munit_log(MUNIT_LOG_DEBUG, "here 12");
	PRE(sm_state(&leader->sm) == LS_SNAP_DONE);
	wait_msg_sent(f);
	munit_assert_int(uv_get_msg_sent(f).type, ==, RAFT_IO_INSTALL_SNAPSHOT);
	ut_leader_message_received(leader, ut_install_snapshot_result());

	sm_fini(&leader->sm);

	munit_log(MUNIT_LOG_DEBUG, "ended");

	return MUNIT_OK;
}

static void *setUpFollower(MUNIT_UNUSED const MunitParameter params[],
			   MUNIT_UNUSED void *user_data)
{
	struct test_fixture *f = pool_set_up();
	f->is_leader = false;
	f->follower_ops = (struct follower_ops){
		.ht_create = pool_ht_create_op,
		.work_queue = pool_work_queue_op_follower,
		.sender_send = uv_sender_send_op,
		.read_sig = pool_read_sig_op,
		.write_chunk = pool_write_chunk_op,
		.fill_ht = pool_fill_ht_op,
		.is_pool_thread = pool_is_pool_thread,

	};
	f->follower = (struct follower){
		.ops = &f->follower_ops,
	};
	sm_init(&f->follower.sm, follower_sm_invariant, NULL, follower_sm_conf,
		"follower", FS_NORMAL);

	return f;
}

static void tearDownFollower(void *data)
{
	pool_tear_down(data);
	free(data);
}

TEST(snapshot_follower, pool, setUpFollower, tearDownFollower, 0, NULL)
{
	struct test_fixture *f = data;
	struct follower *follower = &f->follower;

	PRE(sm_state(&follower->sm) == FS_NORMAL);
	ut_follower_message_received(follower, ut_install_snapshot());
	wait_msg_sent(f);
	munit_assert_int(uv_get_msg_sent(f).type, ==,
			 RAFT_IO_INSTALL_SNAPSHOT_RESULT);

	wait_work(f);

	PRE(sm_state(&follower->sm) == FS_SIGS_CALC_LOOP);
	ut_follower_message_received(follower, ut_sign());
	wait_msg_sent(f);
	munit_assert_int(uv_get_msg_sent(f).type, ==, RAFT_IO_SIGNATURE_RESULT);

	PRE(sm_state(&follower->sm) == FS_SIGS_CALC_LOOP);

	follower->sigs_calculated = true;
	wait_work(f);

	PRE(sm_state(&follower->sm) == FS_SIGS_CALC_LOOP);
	ut_follower_message_received(follower, ut_sign());
	wait_msg_sent(f);
	munit_assert_int(uv_get_msg_sent(f).type, ==, RAFT_IO_SIGNATURE_RESULT);

	PRE(sm_state(&follower->sm) == FS_SIG_RECEIVING);
	ut_follower_message_received(follower, ut_sign());

	PRE(sm_state(&follower->sm) == FS_SIG_PROCESSED);

	wait_work(f);

	PRE(sm_state(&follower->sm) == FS_SIG_READ);
	wait_msg_sent(f);
	munit_assert_int(uv_get_msg_sent(f).type, ==, RAFT_IO_SIGNATURE_RESULT);

	PRE(sm_state(&follower->sm) == FS_CHUNCK_RECEIVING);
	ut_follower_message_received(follower, ut_page());

	wait_work(f);

	PRE(sm_state(&follower->sm) == FS_CHUNCK_APPLIED);
	wait_msg_sent(f);
	munit_assert_int(uv_get_msg_sent(f).type, ==,
			 RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT);

	PRE(sm_state(&follower->sm) == FS_SNAP_DONE);
	ut_follower_message_received(follower, ut_install_snapshot());
	wait_msg_sent(f);
	munit_assert_int(uv_get_msg_sent(f).type, ==,
			 RAFT_IO_INSTALL_SNAPSHOT_RESULT);

	sm_fini(&follower->sm);
	return MUNIT_OK;
}
