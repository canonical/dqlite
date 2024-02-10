#include "src/lib/threadpool.h"
#include "src/lib/sm.h"
#include <assert.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>
#include "src/lib/queue.h"
#include "src/utils.h"


/**
 *  Planner thread state machine.
 *
 * signal() &&
 * empty(o) &&                     signal() && exiting
 * empty(u) &&     +-----> NOTHING ----------------> EXITED
 * !exiting        +-------  ^ |
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
 *            in_flight == 0 | |
 *                           | V
 *                         BARRIER --------+ signal()
 *                           ^ |   <-------+
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
};

static const struct sm_conf planner_sm_states[SM_STATES_MAX] = {
       [PS_NOTHING] = {
               .flags   = SM_INITIAL,
               .name    = "nothing",
               .allowed = BITS(PS_DRAINING)
			| BITS(PS_EXITED),
       },
       [PS_DRAINING] = {
               .name    = "draining",
               .allowed = BITS(PS_DRAINING)
			| BITS(PS_NOTHING)
			| BITS(PS_BARRIER),
       },
       [PS_BARRIER] = {
               .name    = "barrier",
               .allowed = BITS(PS_DRAINING_UNORD)
			| BITS(PS_DRAINING)
			| BITS(PS_BARRIER),
       },
       [PS_DRAINING_UNORD] = {
               .name    = "unord-draining",
               .allowed = BITS(PS_BARRIER)
       },
       [PS_EXITED] = {
               .flags   = SM_FINAL,
               .name    = "exited",
               .allowed = 0,
       },
};

enum {
	POOL_THREADPOOL_SIZE = 4,
	MAX_THREADPOOL_SIZE  = 1024,
	POOL_LOOP_MAGIC      = 0x00ba5e1e55ba5500, /* baseless bass */
};

typedef struct pool_thread pool_thread_t;
typedef struct pool_impl pool_impl_t;

struct targs {
	pool_impl_t *impl;
	uv_sem_t    *sem;
	uint32_t     idx;
};

struct pool_thread {
	queue        inq;
	uv_cond_t    cond;
	uv_thread_t  thread;
	struct targs arg;
};

struct pool_impl {
	uint32_t       nthreads;
	uv_mutex_t     mutex;
	pool_thread_t *threads;

	queue          outq;
	uv_mutex_t     outq_mutex;
	uv_async_t     outq_async;
	uint64_t       active_reqs;

	queue 	       ordered;
	queue 	       unordered;
	struct sm      planner_sm;
	uv_cond_t      planner_cond;
	uv_thread_t    planner_thread;

	uv_key_t       thread_key;
	uint32_t       in_flight;
	bool           exiting;
	uint32_t       o_prev;
	uint32_t       qos;
};

static inline bool has_active_reqs(pool_t *loop)
{
	return loop->impl->active_reqs > 0;
}

static inline void req_register(pool_t *loop, pool_work_t *)
{
	loop->impl->active_reqs++;
}

static inline void req_unregister(pool_t *loop, pool_work_t *)
{
	assert(has_active_reqs(loop));
	loop->impl->active_reqs--;
}

static bool empty(const queue *q)
{
	return QUEUE__IS_EMPTY(q);
}

static queue *head(const queue *q)
{
	return QUEUE__HEAD(q);
}

static void push(queue *to, queue *what)
{
	QUEUE__INSERT_TAIL(to, what);
}

static queue *pop(queue *from)
{
	queue *q = QUEUE__HEAD(from);
	POST(q != NULL);
	QUEUE__REMOVE(q);
	QUEUE__INIT(q);
	return q;
}

static queue *qos_pop(uint32_t *qos, queue *first, queue *second)
{
	PRE(!empty(first) || !empty(second));

	if (empty(first))
		return pop(second);
	else if (empty(second))
		return pop(first);

	return pop((*qos)++ % 2 ? first : second);
}

static void wt_free(queue *q)
{
	struct pool_work *w = QUEUE__DATA(q, struct pool_work, qlink);
	assert(w->type == WT_BAR);
	free(container_of(w, pool_work_t, work_req));
}

static enum pool_work_type wt_type(const queue *q)
{
	struct pool_work *w = QUEUE__DATA(q, struct pool_work, qlink);
	return w->type;
}

static uint32_t wt_idx(const queue *q)
{
	struct pool_work *w = QUEUE__DATA(q, struct pool_work, qlink);
	return w->thread_idx;
}

static bool planner_invariant(const struct sm *m, int prev_state)
{
	pool_impl_t *impl = container_of(m, pool_impl_t, planner_sm);
	queue *o = &impl->ordered;
	queue *u = &impl->unordered;

	return ERGO(sm_state(m) == PS_NOTHING, empty(o) && empty(u)) &&
		ERGO(sm_state(m) == PS_DRAINING,
		     ERGO(prev_state == PS_BARRIER,
			  impl->in_flight == 0 && empty(u)) &&
		     ERGO(prev_state == PS_NOTHING,
			  !empty(u) || !empty(o))) &&
		ERGO(sm_state(m) == PS_EXITED,
		     impl->exiting && empty(o) && empty(u)) &&
		ERGO(sm_state(m) == PS_BARRIER,
		     ERGO(prev_state == PS_DRAINING,
			  wt_type(head(o)) == WT_BAR) &&
		     ERGO(prev_state == PS_DRAINING_UNORD, empty(u))) &&
		ERGO(sm_state(m) == PS_DRAINING_UNORD, !empty(u));
}

static void planner(void *arg)
{
	struct targs  *ta = arg;
	uv_sem_t      *sem = ta->sem;
	pool_impl_t   *impl = ta->impl;
	uv_mutex_t    *mutex = &impl->mutex;
	pool_thread_t *ts = impl->threads;
	struct sm     *planner_sm = &impl->planner_sm;
	queue 	      *o = &impl->ordered;
	queue 	      *u = &impl->unordered;
	queue 	      *q;

	sm_init(planner_sm, planner_invariant, NULL,
		planner_sm_states, PS_NOTHING);
	uv_sem_post(sem);
	uv_mutex_lock(mutex);
	for (;;) {
		switch(sm_state(planner_sm)) {
		case PS_NOTHING:
			while (empty(o) && empty(u) && !impl->exiting)
				uv_cond_wait(&impl->planner_cond, mutex);
			sm_move(planner_sm,
				impl->exiting ? PS_EXITED : PS_DRAINING);
			break;
		case PS_DRAINING:
			while (!(empty(o) && empty(u))) {
				sm_move(planner_sm, PS_DRAINING);
				if (!empty(o) && wt_type(head(o)) == WT_BAR) {
					sm_move(planner_sm, PS_BARRIER);
					goto ps_barrier;
				}
				q = qos_pop(&impl->qos, o, u);
				push(&ts[wt_idx(q)].inq, q);
				uv_cond_signal(&ts[wt_idx(q)].cond);
				if (wt_type(q) >= WT_ORD1)
					impl->in_flight++;
			}
			sm_move(planner_sm, PS_NOTHING);
		ps_barrier:
			break;
		case PS_BARRIER:
			if (!empty(u)) {
				sm_move(planner_sm, PS_DRAINING_UNORD);
				break;
			}
			if (impl->in_flight == 0) {
				q = pop(o);
				wt_free(q); /* remove BAR */
				sm_move(planner_sm, PS_DRAINING);
				break;
			}
			uv_cond_wait(&impl->planner_cond, mutex);
			sm_move(planner_sm, PS_BARRIER);
			break;
		case PS_DRAINING_UNORD:
			while (!empty(u)) {
				q = pop(u);
				push(&ts[wt_idx(q)].inq, q);
				uv_cond_signal(&ts[wt_idx(q)].cond);
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


static void worker(void *arg)
{
	struct targs        *ta = arg;
	pool_impl_t         *impl = ta->impl;
	uv_mutex_t          *mutex = &impl->mutex;
	pool_thread_t       *ts = impl->threads;
	enum pool_work_type  wtype;
	struct pool_work    *w;
	queue               *q;

	uv_key_set(&impl->thread_key, &ta->idx);
	uv_sem_post(ta->sem);
	uv_mutex_lock(mutex);
	for (;;) {
		while (QUEUE__IS_EMPTY(&ts[ta->idx].inq)) {
		    	if (impl->exiting) {
		    		uv_mutex_unlock(mutex);
		    		return;
		    	}
			uv_cond_wait(&ts[ta->idx].cond, mutex);
		}

		q = QUEUE__HEAD(&ts[ta->idx].inq);
		QUEUE__REMOVE(q);
		QUEUE__INIT(q);

		uv_mutex_unlock(mutex);

		w = QUEUE__DATA(q, struct pool_work, qlink);
		wtype = w->type;
		w->work(w);

		uv_mutex_lock(&impl->outq_mutex);
		w->work = NULL;

		QUEUE__INSERT_TAIL(&impl->outq, &w->qlink);

		uv_async_send(&impl->outq_async);
		uv_mutex_unlock(&impl->outq_mutex);

		uv_mutex_lock(mutex);
		if (wtype > WT_BAR) {
		    assert(impl->in_flight > 0);
		    impl->in_flight--;
		    if (impl->in_flight == 0)
			    uv_cond_signal(&impl->planner_cond);
		}
	}
}

static void threadpool_cleanup(pool_t *loop)
{
	pool_impl_t   *impl = loop->impl;
	pool_thread_t *ts = impl->threads;
	uint32_t i;

	if (impl->nthreads == 0)
		return;

	impl->exiting = true;
	uv_cond_signal(&impl->planner_cond);

	if (uv_thread_join(&impl->planner_thread))
		abort();
	uv_cond_destroy(&impl->planner_cond);
	POST(empty(&impl->ordered) && empty(&impl->unordered));

	for (i = 0; i < impl->nthreads; i++) {
	    	uv_cond_signal(&ts[i].cond);
		if (uv_thread_join(&ts[i].thread))
			abort();
		POST(QUEUE__IS_EMPTY(&ts[i].inq));
		uv_cond_destroy(&ts[i].cond);
	}

	free(impl->threads);
	uv_mutex_destroy(&impl->mutex);
	uv_key_delete(&impl->thread_key);
	impl->nthreads = 0;
}

static void init_threads(pool_t *loop)
{
	uv_thread_options_t config;
	const char *val;
	uv_sem_t sem;
	uint32_t i;
	pool_impl_t *impl = loop->impl;
	struct targs planner_args = (struct targs) {
		.sem = &sem,
		.impl = impl,
	};

	impl->qos = 0;
	impl->o_prev = WT_BAR;
	impl->exiting = false;
	impl->in_flight = 0;
	impl->nthreads = POOL_THREADPOOL_SIZE;

	val = getenv("POOL_THREADPOOL_SIZE");
	if (val != NULL)
		impl->nthreads = (uint32_t)atoi(val);
	if (impl->nthreads == 0)
		impl->nthreads = 1;
	if (impl->nthreads > MAX_THREADPOOL_SIZE)
		impl->nthreads = MAX_THREADPOOL_SIZE;
	if (uv_key_create(&impl->thread_key))
		abort();

	impl->threads = calloc(impl->nthreads, sizeof(impl->threads[0]));
	if (impl->threads == NULL)
		abort();

	if (uv_mutex_init(&impl->mutex))
		abort();

	QUEUE__INIT(&impl->ordered);
	QUEUE__INIT(&impl->unordered);

	if (uv_sem_init(&sem, 0))
		abort();

	config.flags = UV_THREAD_HAS_STACK_SIZE;
	config.stack_size = 8u << 20; /* 8 MB */

	for (i = 0; i < impl->nthreads; i++) {
		impl->threads[i].arg = (struct targs) {
			.impl = impl,
			.sem = &sem,
			.idx = i,
		};

		QUEUE__INIT(&impl->threads[i].inq);
	    	if (uv_cond_init(&impl->threads[i].cond))
			abort();

		if (uv_thread_create_ex(&impl->threads[i].thread, &config,
					worker, &impl->threads[i].arg))
			abort();
	}

	if (uv_cond_init(&impl->planner_cond))
		abort();

	if (uv_thread_create_ex(&impl->planner_thread, &config, planner,
				&planner_args))
		abort();

	for (i = 0; i < impl->nthreads + 1; i++)
		uv_sem_wait(&sem);

	uv_sem_destroy(&sem);
}

static void pool_work_submit(uv_loop_t *loop,
			     struct pool_work *w,
			     void (*work)(struct pool_work *w),
			     void (*done)(struct pool_work *w))
{
	pool_impl_t *impl = uv_loop_to_pool(loop)->impl;

	/* Make sure that elements in ordered queue come in order. */
	if (w->type > WT_UNORD) {
		PRE(ERGO(impl->o_prev != WT_BAR && w->type != WT_BAR,
			 impl->o_prev == w->type));
		impl->o_prev = w->type;
	}

	w->loop = loop;
	w->work = work;
	w->done = done;

	uv_mutex_lock(&impl->mutex);
	QUEUE__INSERT_TAIL(w->type == WT_UNORD
			   ? &impl->unordered
			   : &impl->ordered, &w->qlink);
	uv_cond_signal(&impl->planner_cond);
	uv_mutex_unlock(&impl->mutex);
}

void work_done(uv_async_t *handle)
{
	pool_impl_t *impl;
	struct pool_work *w;
	queue *q;
	queue wq = {};

	impl = container_of(handle, pool_impl_t, outq_async);
	uv_mutex_lock(&impl->outq_mutex);
	QUEUE__MOVE(&impl->outq, &wq);
	uv_mutex_unlock(&impl->outq_mutex);

	while (!QUEUE__IS_EMPTY(&wq)) {
		q = QUEUE__HEAD(&wq);
		QUEUE__REMOVE(q);

		w = container_of(q, struct pool_work, qlink);
		w->done(w);
	}
}

static void queue_work(struct pool_work *w)
{
	pool_work_t *req = container_of(w, pool_work_t, work_req);
	req->work_cb(req);
}

static void queue_done(struct pool_work *w)
{
	pool_work_t *req = container_of(w, pool_work_t, work_req);

	req_unregister(uv_loop_to_pool(req->loop), req);
	if (req->after_work_cb == NULL)
		return;

	req->after_work_cb(req);
}

int pool_queue_work(pool_t *pool,
		    pool_work_t *req,
		    uint32_t cookie,
		    void (*work_cb)(pool_work_t *req),
		    void (*after_work_cb)(pool_work_t *req))
{
	pool_impl_t *impl = pool->impl;

	if (work_cb == NULL)
		return UV_EINVAL;

	if (req->work_req.type != WT_BAR)
		req_register(pool, req);

	req->loop = &pool->loop;
	req->work_cb = work_cb;
	req->after_work_cb = after_work_cb;
	req->work_req.thread_idx = cookie % impl->nthreads;
	pool_work_submit(&pool->loop, &req->work_req, queue_work, queue_done);
	return 0;
}

int pool_init(pool_t *loop)
{
	int rc;

	loop->magic = POOL_LOOP_MAGIC;
	loop->impl = calloc(1, sizeof(*loop->impl));
	if (loop->impl == NULL)
		return UV_ENOMEM;

	rc = uv_mutex_init(&loop->impl->outq_mutex);
	if (rc != 0) {
		free(loop->impl);
		return rc;
	}

	rc = uv_async_init(&loop->loop, &loop->impl->outq_async, work_done);
	if (rc != 0) {
		free(loop->impl);
		uv_mutex_destroy(&loop->impl->outq_mutex);
		return rc;
	}

	QUEUE__INIT(&loop->impl->outq);
	init_threads(loop);
	return 0;
}

void pool_fini(pool_t *loop)
{
	threadpool_cleanup(loop);

	uv_mutex_lock(&loop->impl->outq_mutex);
	POST(QUEUE__IS_EMPTY(&loop->impl->outq) &&
	     "thread pool output queue not empty!");
	POST(!has_active_reqs(loop));
	uv_mutex_unlock(&loop->impl->outq_mutex);

	uv_mutex_destroy(&loop->impl->outq_mutex);
	free(loop->impl);
}

void pool_close(pool_t *pool)
{
	uv_close((uv_handle_t *)&pool->impl->outq_async, NULL);
}

uint32_t pool_thread_id(const pool_t *loop)
{
	return *(uint32_t *)uv_key_get(&loop->impl->thread_key);
}

pool_t *uv_loop_to_pool(const uv_loop_t *loop)
{
	pool_t *pl = container_of(loop, pool_t, loop);
	PRE(pl->magic == POOL_LOOP_MAGIC);
	return pl;
}
