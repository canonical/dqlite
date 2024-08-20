#include "threadpool.h"
#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <uv.h>
#include <uv/unix.h>
#include "../../src/lib/queue.h"
#include "../../src/lib/sm.h"
#include "../../src/utils.h"

/**
 *  Planner thread state machine.
 *
 *     signal() &&
 *     empty(o) &&                 signal() && exiting
 *     empty(u) && +-----> NOTHING ----------------> EXITED
 *     !exiting    +-------  ^ |
 *                           | |
 *               empty(o) && | | signal()
 *               empty(u)    | | !empty(o) || !empty(u)
 *                           | |
 *                           | |
 *                           | V
 *    !empty(o) && +-----> DRAINING
 *    !empty(u) && +-------  ^ |
 * type(head(o)) != BAR      | |
 *                           | | type(head(o)) == BAR
 *        ord_in_flight == 0 | |
 *                           | V
 *                         BARRIER --------+ signal()
 *                           ^ |   <-------+ ord_in_flight == 0
 *                           | |
 *                  empty(u) | | !empty(u)
 *                           | V
 *                      DRAINING_UNORD
 */

enum planner_states {
	PS_NOTHING,
	PS_DRAINING,
	PS_BARRIER,
	PS_DRAINING_UNORD,
	PS_EXITED,
	PS_NR,
};

static const struct sm_conf planner_states[PS_NR] = {
	[PS_NOTHING] = {
	    .flags = SM_INITIAL,
	    .name = "nothing",
	    .allowed = BITS(PS_DRAINING) | BITS(PS_EXITED),
	},
	[PS_DRAINING] = {
	    .name = "draining",
	    .allowed = BITS(PS_DRAINING)
		     | BITS(PS_NOTHING)
		     | BITS(PS_BARRIER),
	},
	[PS_BARRIER] = {
	    .name = "barrier",
	    .allowed = BITS(PS_DRAINING_UNORD)
		     | BITS(PS_DRAINING)
		     | BITS(PS_BARRIER),
	},
	[PS_DRAINING_UNORD] = {
	    .name = "draining-unord",
	    .allowed = BITS(PS_BARRIER)
	},
	[PS_EXITED] = {
	    .flags = SM_FINAL,
	    .name = "exited",
	    .allowed = 0,
	},
};

static const uintptr_t pool_thread_magic = 0xf344e2;
static uv_key_t thread_identifier_key;

enum {
	THREADPOOL_SIZE_MAX = 1024,
};

typedef struct pool_thread pool_thread_t;
typedef struct pool_impl pool_impl_t;

struct targs {
	pool_impl_t *pi;
	uv_sem_t *sem;
	uint32_t idx; /* Thread's index */
};

/* Worker thread of the pool */
struct pool_thread {
	queue inq;          /* Thread's input queue */
	uv_cond_t cond;     /* Signalled when work item appears in @inq */
	uv_thread_t thread; /* Pool's worker thread */
	struct targs arg;
};

/* clang-format off */
struct pool_impl {
	uv_mutex_t     mutex;          /* Input queue, planner_sm,
					  worker and planner threads lock */
	uint32_t       threads_nr;
	pool_thread_t *threads;

	queue          outq;           /* Output queue used by libuv part */
	uv_mutex_t     outq_mutex;     /* Output queue lock */
	uv_async_t     outq_async;     /* Signalled when output queue is not
				          empty and libuv loop has to process
				          items from it */
	uint64_t       active_ws;      /* Number of all work items in flight,
				          accessed from the main thread only */

	queue          ordered;        /* Queue of WT_ORD{N} items */
	queue          unordered;      /* Queue of WT_UNORD items */
	struct sm      planner_sm;     /* State machine of the scheduler */
	uv_cond_t      planner_cond;
	uv_thread_t    planner_thread; /* Scheduler's thread */

	uint32_t       ord_in_flight;  /* Number of WT_ORD{N} in flight */
	bool           exiting;        /* True when the pool is being stopped */
	enum pool_work_type            /* Type of the previous work item, */
	               ord_prev;       /* used in WT_ORD{N} ivariants */
	uint32_t       qos;            /* QoS token */
	uint32_t       qos_prio;       /* QoS prio */
};
/* clang-format on */

/* Callback does not allow passing data, we use a static variable to report
 * errors back. */
static int thread_key_create_err = 0;
static void thread_key_create(void) {
	PRE(thread_key_create_err == 0);
	thread_key_create_err = uv_key_create(&thread_identifier_key);
}

static inline bool pool_is_inited(const pool_t *pool)
{
	return pool->pi != NULL;
}

static inline bool has_active_ws(pool_t *pool)
{
	return pool->pi->active_ws > 0;
}

static inline void w_register(pool_t *pool, pool_work_t *w)
{
	if (w->type != WT_BAR) {
		pool->pi->active_ws++;
	}
}

static inline void w_unregister(pool_t *pool, pool_work_t *w)
{
	(void)w;
	PRE(has_active_ws(pool));
	pool->pi->active_ws--;
}

static bool empty(const queue *q)
{
	return queue_empty(q);
}

static queue *head(const queue *q)
{
	return queue_head(q);
}

static void push(queue *to, queue *what)
{
	queue_insert_tail(to, what);
}

static queue *pop(queue *from)
{
	queue *q = queue_head(from);
	PRE(q != NULL);
	queue_remove(q);
	queue_init(q);
	return q;
}

static queue *qos_pop(pool_impl_t *pi, queue *first, queue *second)
{
	PRE(!empty(first) || !empty(second));

	if (empty(first)) {
		return pop(second);
	} else if (empty(second)) {
		return pop(first);
	}

	return pop(pi->qos++ % pi->qos_prio ? first : second);
}

static pool_work_t *q_to_w(const queue *q)
{
	return QUEUE_DATA(q, pool_work_t, link);
}

static enum pool_work_type q_type(const queue *q)
{
	return q_to_w(q)->type;
}

static uint32_t q_tid(const queue *q)
{
	return q_to_w(q)->thread_id;
}

static bool planner_invariant(const struct sm *m, int prev_state)
{
	pool_impl_t *pi = CONTAINER_OF(m, pool_impl_t, planner_sm);
	queue *o = &pi->ordered;
	queue *u = &pi->unordered;

	/* clang-format off */
	return ERGO(sm_state(m) == PS_NOTHING, empty(o) && empty(u)) &&
		ERGO(sm_state(m) == PS_DRAINING,
		     ERGO(prev_state == PS_BARRIER,
			  pi->ord_in_flight == 0 && empty(u)) &&
		     ERGO(prev_state == PS_NOTHING,
			  !empty(u) || !empty(o))) &&
		ERGO(sm_state(m) == PS_EXITED,
		     pi->exiting && empty(o) && empty(u)) &&
		ERGO(sm_state(m) == PS_BARRIER,
		     ERGO(prev_state == PS_DRAINING,
			  q_type(head(o)) == WT_BAR) &&
		     ERGO(prev_state == PS_DRAINING_UNORD, empty(u))) &&
		ERGO(sm_state(m) == PS_DRAINING_UNORD, !empty(u));
	/* clang-format on */
}

static void planner(void *arg)
{
	struct targs *ta = arg;
	uv_sem_t *sem = ta->sem;
	pool_impl_t *pi = ta->pi;
	uv_mutex_t *mutex = &pi->mutex;
	pool_thread_t *ts = pi->threads;
	struct sm *planner_sm = &pi->planner_sm;
	queue *o = &pi->ordered;
	queue *u = &pi->unordered;
	queue *q;

	sm_init(planner_sm, planner_invariant, NULL, planner_states, "ps",
		PS_NOTHING);
	uv_sem_post(sem);
	uv_mutex_lock(mutex);
	for (;;) {
		switch (sm_state(planner_sm)) {
			case PS_NOTHING:
				while (empty(o) && empty(u) && !pi->exiting) {
					uv_cond_wait(&pi->planner_cond, mutex);
				}
				sm_move(planner_sm,
					pi->exiting && empty(o) && empty(u)
					    ? PS_EXITED
					    : PS_DRAINING);
				break;
			case PS_DRAINING:
				while (!(empty(o) && empty(u))) {
					sm_move(planner_sm, PS_DRAINING);
					if (!empty(o) &&
					    q_type(head(o)) == WT_BAR) {
						sm_move(planner_sm, PS_BARRIER);
						goto ps_barrier;
					}
					q = qos_pop(pi, o, u);
					push(&ts[q_tid(q)].inq, q);
					uv_cond_signal(&ts[q_tid(q)].cond);
					if (q_type(q) >= WT_ORD1) {
						pi->ord_in_flight++;
					}
				}
				sm_move(planner_sm, PS_NOTHING);
			ps_barrier:
				break;
			case PS_BARRIER:
				if (!empty(u)) {
					sm_move(planner_sm, PS_DRAINING_UNORD);
					break;
				}
				if (pi->ord_in_flight == 0) {
					q = pop(o);
					PRE(q_to_w(q)->type == WT_BAR);
					free(q_to_w(q));
					sm_move(planner_sm, PS_DRAINING);
					break;
				}
				uv_cond_wait(&pi->planner_cond, mutex);
				sm_move(planner_sm, PS_BARRIER);
				break;
			case PS_DRAINING_UNORD:
				while (!empty(u)) {
					q = pop(u);
					push(&ts[q_tid(q)].inq, q);
					uv_cond_signal(&ts[q_tid(q)].cond);
				}
				sm_move(planner_sm, PS_BARRIER);
				break;
			case PS_EXITED:
				sm_fini(planner_sm);
				uv_mutex_unlock(mutex);
				return;
			default:
				POST(false && "Impossible!");
		}
	}
}

static void queue_work(pool_work_t *w)
{
	w->work_cb(w);
}

static void queue_done(pool_work_t *w)
{
	w_unregister(w->pool, w);
	if (w->after_work_cb != NULL) {
		w->after_work_cb(w);
	}
}

static void worker(void *arg)
{
	struct targs *ta = arg;
	pool_impl_t *pi = ta->pi;
	uv_mutex_t *mutex = &pi->mutex;
	pool_thread_t *ts = pi->threads;
	enum pool_work_type wtype;
	pool_work_t *w;
	queue *q;

	uv_key_set(&thread_identifier_key, (void*)pool_thread_magic);
	uv_sem_post(ta->sem);
	uv_mutex_lock(mutex);
	for (;;) {
		while (empty(&ts[ta->idx].inq)) {
			if (pi->exiting) {
				uv_mutex_unlock(mutex);
				return;
			}
			uv_cond_wait(&ts[ta->idx].cond, mutex);
		}

		q = pop(&ts[ta->idx].inq);
		uv_mutex_unlock(mutex);

		w = q_to_w(q);
		wtype = w->type;
		queue_work(w);

		uv_mutex_lock(&pi->outq_mutex);
		push(&pi->outq, &w->link);
		uv_async_send(&pi->outq_async);
		uv_mutex_unlock(&pi->outq_mutex);

		uv_mutex_lock(mutex);
		if (wtype > WT_BAR) {
			assert(pi->ord_in_flight > 0);
			if (--pi->ord_in_flight == 0) {
				uv_cond_signal(&pi->planner_cond);
			}
		}
	}
}

static void pool_cleanup(pool_t *pool)
{
	pool_impl_t *pi = pool->pi;
	pool_thread_t *ts = pi->threads;
	uint32_t i;

	if (pi->threads_nr == 0) {
		return;
	}

	uv_cond_signal(&pi->planner_cond);

	if (uv_thread_join(&pi->planner_thread)) {
		abort();
	}
	uv_cond_destroy(&pi->planner_cond);
	POST(empty(&pi->ordered) && empty(&pi->unordered));

	for (i = 0; i < pi->threads_nr; i++) {
		uv_cond_signal(&ts[i].cond);
		if (uv_thread_join(&ts[i].thread)) {
			abort();
		}
		POST(empty(&ts[i].inq));
		uv_cond_destroy(&ts[i].cond);
	}

	free(pi->threads);
	uv_mutex_destroy(&pi->mutex);
	pi->threads_nr = 0;
}

static void pool_threads_init(pool_t *pool)
{
	uint32_t i;
	uv_sem_t sem;
	pool_impl_t *pi = pool->pi;
	pool_thread_t *ts;
	struct targs pa = {
		.sem = &sem,
		.pi = pi,
	};
	uv_thread_options_t config = {
		.flags = UV_THREAD_HAS_STACK_SIZE,
		.stack_size = 8u << 20,
	};

	if (uv_mutex_init(&pi->mutex)) {
		abort();
	}
	if (uv_sem_init(&sem, 0)) {
		abort();
	}

	pi->threads = calloc(pi->threads_nr, sizeof(pi->threads[0]));
	if (pi->threads == NULL) {
		abort();
	}

	for (i = 0, ts = pi->threads; i < pi->threads_nr; i++) {
		ts[i].arg = (struct targs){
			.pi = pi,
			.sem = &sem,
			.idx = i,
		};

		queue_init(&ts[i].inq);
		if (uv_cond_init(&ts[i].cond)) {
			abort();
		}
		if (uv_thread_create_ex(&ts[i].thread, &config, worker,
					&ts[i].arg)) {
			abort();
		}
	}

	if (uv_cond_init(&pi->planner_cond)) {
		abort();
	}
	if (uv_thread_create_ex(&pi->planner_thread, &config, planner, &pa)) {
		abort();
	}
	for (i = 0; i < pi->threads_nr + 1 /* +planner */; i++) {
		uv_sem_wait(&sem);
	}

	uv_sem_destroy(&sem);
}

static void pool_work_submit(pool_t *pool, pool_work_t *w)
{
	pool_impl_t *pi = pool->pi;
	queue *o = &pi->ordered;
	queue *u = &pi->unordered;

	if (w->type > WT_UNORD) {
		/* Make sure that elements in the ordered queue come in order.
		 */
		PRE(ERGO(pi->ord_prev != WT_BAR && w->type != WT_BAR,
			 pi->ord_prev == w->type));
		pi->ord_prev = w->type;
	}

	uv_mutex_lock(&pi->mutex);
	POST(!pi->exiting);
	push(w->type == WT_UNORD ? u : o, &w->link);
	uv_cond_signal(&pi->planner_cond);
	uv_mutex_unlock(&pi->mutex);
}

void work_done(uv_async_t *handle)
{
	queue q = {};
	pool_impl_t *pi = CONTAINER_OF(handle, pool_impl_t, outq_async);

	uv_mutex_lock(&pi->outq_mutex);
	queue_move(&pi->outq, &q);
	uv_mutex_unlock(&pi->outq_mutex);

	while (!empty(&q)) {
		queue_done(q_to_w(pop(&q)));
	}
}

void pool_queue_work(pool_t *pool,
		     pool_work_t *w,
		     uint32_t cookie,
		     enum pool_work_type type,
		     void (*work_cb)(pool_work_t *w),
		     void (*after_work_cb)(pool_work_t *w))
{
	PRE(memcmp(w, &(pool_work_t){}, sizeof *w) == 0);
	PRE(work_cb != NULL && type < WT_NR);

	if (!!(pool->flags & POOL_FOR_UT_NOT_ASYNC)) {
		work_cb(w);
		after_work_cb(w);
		return;
	}

	PRE(pool_is_inited(pool));
	*w = (pool_work_t){
		.pool = pool,
		.type = type,
		.thread_id = cookie % pool->pi->threads_nr,
		.work_cb = work_cb,
		.after_work_cb = after_work_cb,
	};

	w_register(pool, w);
	pool_work_submit(pool, w);
}

int pool_init(pool_t *pool,
	      uv_loop_t *loop,
	      uint32_t threads_nr,
	      uint32_t qos_prio)
{
	int rc;
	pool_impl_t *pi = pool->pi;

	PRE(threads_nr <= THREADPOOL_SIZE_MAX);

	pool->flags = 0x0;
	pi = pool->pi = calloc(1, sizeof(*pool->pi));
	if (pi == NULL) {
		return UV_ENOMEM;
	}

	*pi = (pool_impl_t){
		.qos = 0,
		.qos_prio = qos_prio,
		.exiting = false,
		.ord_prev = WT_BAR,
		.threads_nr = threads_nr,
		.ord_in_flight = 0,
	};
	queue_init(&pi->outq);
	queue_init(&pi->ordered);
	queue_init(&pi->unordered);

	rc = uv_mutex_init(&pi->outq_mutex);
	if (rc != 0) {
		free(pi);
		return rc;
	}

	static uv_once_t once = UV_ONCE_INIT;
	uv_once(&once, thread_key_create);
	if (thread_key_create_err != 0) {
		uv_mutex_destroy(&pi->outq_mutex);
		free(pi);
		return thread_key_create_err;
	}

	rc = uv_async_init(loop, &pi->outq_async, work_done);
	if (rc != 0) {
		uv_mutex_destroy(&pi->outq_mutex);
		free(pi);
		return rc;
	}

	pool_threads_init(pool);

	return 0;
}

void pool_fini(pool_t *pool)
{
	pool_impl_t *pi = pool->pi;

	pool_cleanup(pool);

	uv_mutex_lock(&pi->outq_mutex);
	POST(!!(pool->flags & POOL_FOR_UT_NON_CLEAN_FINI) ||
	     (empty(&pi->outq) && !has_active_ws(pool)));
	uv_mutex_unlock(&pi->outq_mutex);

	uv_mutex_destroy(&pi->outq_mutex);
	free(pi);
}

void pool_close(pool_t *pool)
{
	pool_impl_t *pi = pool->pi;

	uv_close((uv_handle_t *)&pi->outq_async, NULL);
	uv_mutex_lock(&pi->mutex);
	pi->exiting = true;
	uv_mutex_unlock(&pi->mutex);
}

bool pool_is_pool_thread(void) {
	return uv_key_get(&thread_identifier_key) == (void*)pool_thread_magic;
}

pool_t *pool_ut_fallback(void)
{
	static pool_t pool;
	return &pool;
}
