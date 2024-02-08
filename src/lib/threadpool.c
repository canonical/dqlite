#include "src/lib/threadpool.h"
#include "src/lib/sm.h"
#include <assert.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>
#include "src/lib/queue.h"
#include "src/utils.h"

static struct xx_loop_s *xx_loop(struct uv_loop_s *loop)
{
	return (struct xx_loop_s *)loop;
}

static inline bool xx__has_active_reqs(xx_loop_t *loop)
{
    return loop->active_reqs > 0;
}

static inline void xx__req_register(xx_loop_t *loop, xx_work_t *req UNUSED)
{
    loop->active_reqs++;
}

static inline void xx__req_unregister(xx_loop_t *loop, xx_work_t *req UNUSED)
{
	assert(xx__has_active_reqs(loop));
	loop->active_reqs--;
}

enum {
	MAX_THREADPOOL_SIZE = 1024,
	XX_THREADPOOL_SIZE = 4,
};

struct thread_args
{
	uv_sem_t *sem;
	unsigned int idx;
};

static uv_cond_t *cond;
static uv_mutex_t mutex;
static unsigned int nthreads;
static uv_thread_t *threads;
static struct thread_args *thread_args;
static uv_key_t thread_key;
static queue *thread_queues;
static queue exit_message;
static queue wq;

static struct sm planner_sm;
static uv_cond_t planner_cond;
static uv_thread_t planner_thread;
static queue ordered;
static queue unordered;
static unsigned int in_flight = 0;
static bool exiting = false;

enum planner_states {
	PS_NOTHING,
	PS_DRAINING,
	PS_BARRIER,
	PS_DRAINING_UNORD,
	PS_EXITED,
};

/**
 *  Planner thread state machine.
 *
 *                         NOTHING
 *                           ^ |
 *               empty(o) && | | signal()
 *               empty(u)    | |
 *                           | |        empty(o) &&
 *                           | |        empty(u) &&
 *                           | V        exiting
 *    !empty(o) && +-----> DRAINING ---------------> EXITED
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

static struct sm_conf planner_sm_states[SM_STATES_MAX] = {
       [PS_NOTHING] = {
               .flags   = SM_INITIAL,
               .name    = "nothing",
               .allowed = BITS(PS_DRAINING),
       },
       [PS_DRAINING] = {
               .name    = "draining",
               .allowed = BITS(PS_DRAINING) |
			  BITS(PS_NOTHING) |
			  BITS(PS_BARRIER) |
			  BITS(PS_EXITED),
       },
       [PS_BARRIER] = {
               .name    = "barrier",
               .allowed = BITS(PS_DRAINING_UNORD) |
			  BITS(PS_DRAINING) |
			  BITS(PS_BARRIER),
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

static bool empty(queue *q)
{
	return QUEUE__IS_EMPTY(q);
}

static queue *head(queue *q)
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

static queue *qos_pop(queue *first, queue *second)
{
	static unsigned int qos = 0;

	PRE(!empty(first) || !empty(second));

	if (empty(first))
		return pop(second);
	else if (empty(second))
		return pop(first);

	return pop(qos++ % 2 ? first : second);
}

static unsigned int wt_type(queue *q)
{
	struct xx__work *w = QUEUE__DATA(q, struct xx__work, wq);
	return w->type;
}
static unsigned int wt_idx(queue *q)
{
	struct xx__work *w = QUEUE__DATA(q, struct xx__work, wq);
	return w->thread_idx;
}

static bool planner_invariant(const struct sm *m, int prev_state)
{
	queue *o = &ordered;
	queue *u = &unordered;

	return ERGO(sm_state(m) == PS_NOTHING,
		    empty(o) && empty(u)) &&
		ERGO(sm_state(m) == PS_DRAINING,
		     ERGO(prev_state == PS_BARRIER, in_flight == 0) ||
		     !empty(o) || !empty(u)) &&
		ERGO(sm_state(m) == PS_EXITED,
		     exiting && empty(o) && empty(u)) &&
		ERGO(sm_state(m) == PS_BARRIER,
		     ERGO(prev_state == PS_DRAINING,
			  wt_type(head(o)) == WT_BAR) &&
		     ERGO(prev_state == PS_DRAINING_UNORD, empty(o))) &&
		ERGO(sm_state(m) == PS_DRAINING_UNORD, !empty(u));
}

static void planner(void *arg)
{
	uv_sem_t *sem = arg;
	queue *o  = &ordered;
	queue *u  = &unordered;
	queue *tq = thread_queues;
	queue *q;

	sm_init(&planner_sm, planner_invariant, planner_sm_states, PS_NOTHING);
	uv_sem_post(sem);
	uv_mutex_lock(&mutex);
	for (;;) {
		switch(sm_state(&planner_sm)) {
		case PS_NOTHING:
			uv_cond_wait(&planner_cond, &mutex);
			sm_move(&planner_sm, PS_DRAINING);
			break;
		case PS_DRAINING:
			if (!empty(o) && wt_type(head(o)) == WT_BAR) {
				sm_move(&planner_sm, PS_BARRIER);
				break;
			}
			while (!(empty(o) && empty(u))) {
				q = qos_pop(o, u);
				push(&tq[wt_idx(q)], q);
				uv_cond_signal(&cond[wt_idx(q)]);
				sm_move(&planner_sm, PS_DRAINING);
			}
			sm_move(&planner_sm, exiting ? PS_EXITED : PS_NOTHING);
			break;
		case PS_BARRIER:
			if (!empty(u)) {
				sm_move(&planner_sm, PS_DRAINING_UNORD);
				break;
			}
			if (in_flight == 0) {
				free(pop(o)); /* remove BAR */
				sm_move(&planner_sm, PS_DRAINING);
				break;
			}
			uv_cond_wait(&planner_cond, &mutex);
			sm_move(&planner_sm, PS_BARRIER);
			break;
		case PS_DRAINING_UNORD:
			while (!empty(u)) {
				q = pop(u);
				push(&tq[wt_idx(q)], q);
				uv_cond_signal(&cond[wt_idx(q)]);
			}
			sm_move(&planner_sm, PS_BARRIER);
			break;
		case PS_EXITED:
			sm_fini(&planner_sm);
			uv_mutex_unlock(&mutex);
			return;
		default:
			POST(false && "Impossible!");
		}
	}
}


static void worker(void *arg)
{
	struct xx__work *w;
	queue *q;
	struct thread_args *ta = arg;
	unsigned int wtype;

	uv_key_set(&thread_key, &ta->idx);
	uv_sem_post(ta->sem);
	uv_mutex_lock(&mutex);
	for (;;) {
		while (QUEUE__IS_EMPTY(&thread_queues[ta->idx]))
			uv_cond_wait(&cond[ta->idx], &mutex);

		q = QUEUE__HEAD(&thread_queues[ta->idx]);
		QUEUE__REMOVE(q);
		QUEUE__INIT(q);

		uv_mutex_unlock(&mutex);

		w = QUEUE__DATA(q, struct xx__work, wq);
		wtype = w->type;
		w->work(w);

		uv_mutex_lock(&xx_loop(w->loop)->wq_mutex);
		w->work = NULL;

		QUEUE__INSERT_TAIL(&xx_loop(w->loop)->wq, &w->wq);

		uv_async_send(&xx_loop(w->loop)->wq_async);
		uv_mutex_unlock(&xx_loop(w->loop)->wq_mutex);

		uv_mutex_lock(&mutex);
		if (wtype > WT_BAR) {
		    assert(in_flight > 0);
		    in_flight--;
		}
	}
}

static void post(queue *q, unsigned int idx)
{
	uv_mutex_lock(&mutex);

	QUEUE__INSERT_TAIL(idx == ~0u ? &ordered : &unordered, q);
	uv_cond_signal(&planner_cond);

	if (wt_type(q) >= WT_ORD1)
	    in_flight++;

	uv_mutex_unlock(&mutex);
}

static void xx__threadpool_cleanup(void)
{
	unsigned int i;

	if (nthreads == 0)
		return;

	assert(0);
	post(&exit_message, ~0u);

	for (i = 0; i < nthreads; i++) {
		if (uv_thread_join(threads + i))
			abort();
		POST(QUEUE__IS_EMPTY(&thread_queues[i]));
		uv_cond_destroy(&cond[i]);
	}

	if (uv_thread_join(&planner_thread))
	    abort();
	uv_cond_destroy(&planner_cond);

	free(cond);
	free(threads);
	free(thread_args);
	free(thread_queues);

	uv_mutex_destroy(&mutex);
	uv_key_delete(&thread_key);

	threads = NULL;
	thread_args = NULL;
	thread_queues = NULL;
	nthreads = 0;
}

static void init_threads(void)
{
	uv_thread_options_t config;
	unsigned int i;
	const char *val;
	uv_sem_t sem;

	nthreads = XX_THREADPOOL_SIZE;
	val = getenv("XX_THREADPOOL_SIZE");
	if (val != NULL)
		nthreads = (unsigned int)atoi(val);
	if (nthreads == 0)
		nthreads = 1;
	if (nthreads > MAX_THREADPOOL_SIZE)
		nthreads = MAX_THREADPOOL_SIZE;

	if (uv_key_create(&thread_key))
		abort();

	cond = calloc(nthreads, sizeof(cond[0]));
	threads = calloc(nthreads, sizeof(threads[0]));
	thread_args = calloc(nthreads, sizeof(thread_args[0]));
	thread_queues = calloc(nthreads, sizeof(thread_queues[0]));
	if (cond == NULL || threads == NULL || thread_args == NULL ||
	    thread_queues == NULL)
		abort();

	if (uv_mutex_init(&mutex))
		abort();

	QUEUE__INIT(&wq);
	QUEUE__INIT(&ordered);
	QUEUE__INIT(&unordered);


	if (uv_sem_init(&sem, 0))
		abort();

	config.flags = UV_THREAD_HAS_STACK_SIZE;
	config.stack_size = 8u << 20; /* 8 MB */

	for (i = 0; i < nthreads; i++) {
	    	if (uv_cond_init(&cond[i]))
		    abort();
		QUEUE__INIT(&thread_queues[i]);
		thread_args[i] = (struct thread_args){
		    .sem = &sem,
		    .idx = i,
		};
		if (uv_thread_create_ex(threads + i, &config, worker,
					&thread_args[i]))
			abort();
	}

	if (uv_cond_init(&planner_cond))
	    abort();

	if (uv_thread_create_ex(&planner_thread, &config, planner, &sem))
	    abort();

	for (i = 0; i < nthreads + 1; i++)
		uv_sem_wait(&sem);

	uv_sem_destroy(&sem);
}

static UNUSED bool threads__invariant(void)
{
	return nthreads > 0 && threads != NULL && thread_args != NULL &&
	       thread_queues != NULL && !IS0(&wq) && !ARE0(threads, nthreads) &&
	       !ARE0(thread_args, nthreads) && !ARE0(thread_queues, nthreads);
}

void xx__work_submit(uv_loop_t *loop,
		     struct xx__work *w,
		     void (*work)(struct xx__work *w),
		     void (*done)(struct xx__work *w, int status))
{
    //PRE(threads__invariant());
	w->loop = loop;
	w->work = work;
	w->done = done;
	post(&w->wq, w->type == WT_UNORD ? w->thread_idx: ~0u);
}

void xx__work_done(uv_async_t *handle)
{
	struct xx__work *w;
	uv_loop_t *loop;
	struct xx_loop_s *xxloop;
	queue *q;
	queue wq_ = {};

	xxloop = container_of(handle, struct xx_loop_s, wq_async);
	loop = &xxloop->loop;

	uv_mutex_lock(&xx_loop(loop)->wq_mutex);
	QUEUE__MOVE(&xx_loop(loop)->wq, &wq_);
	uv_mutex_unlock(&xx_loop(loop)->wq_mutex);

	while (!QUEUE__IS_EMPTY(&wq_)) {
		q = QUEUE__HEAD(&wq_);
		QUEUE__REMOVE(q);

		w = container_of(q, struct xx__work, wq);
		w->done(w, 0);
	}
}

static void xx__queue_work(struct xx__work *w)
{
	xx_work_t *req = container_of(w, xx_work_t, work_req);

	req->work_cb(req);
}

static void xx__queue_done(struct xx__work *w, int err)
{
	xx_work_t *req = container_of(w, xx_work_t, work_req);

	xx__req_unregister(xx_loop(req->loop), req);
	if (req->after_work_cb == NULL)
		return;

	req->after_work_cb(req, err);
}

int xx_queue_work(uv_loop_t *loop,
		  xx_work_t *req,
		  unsigned int cookie,
		  xx_work_cb work_cb,
		  xx_after_work_cb after_work_cb)
{
	if (work_cb == NULL)
		return UV_EINVAL;

	if (req->work_req.type != WT_BAR)
	    xx__req_register(xx_loop(loop), req);
	req->loop = loop;
	req->work_cb = work_cb;
	req->after_work_cb = after_work_cb;
	req->work_req.thread_idx = cookie % nthreads;
	xx__work_submit(loop, &req->work_req, xx__queue_work, xx__queue_done);
	return 0;
}

int xx_loop_init(struct xx_loop_s *loop)
{
	int err;

	QUEUE__INIT(&loop->wq);

	err = uv_mutex_init(&loop->wq_mutex);
	if (err != 0)
		return err;

	err = uv_async_init(&loop->loop, &loop->wq_async, xx__work_done);
	if (err != 0) {
		uv_mutex_destroy(&loop->wq_mutex);
		return err;
	}

	init_threads();
	return 0;
}

void xx_loop_close(struct xx_loop_s *loop)
{
	xx__threadpool_cleanup();

	uv_mutex_lock(&loop->wq_mutex);
	assert(QUEUE__IS_EMPTY(&loop->wq) &&
	       "thread pool work queue not empty!");
	assert(!xx__has_active_reqs(loop));
	uv_mutex_unlock(&loop->wq_mutex);

	uv_mutex_destroy(&loop->wq_mutex);
}

void xx_loop_async_close(uv_loop_t *loop)
{
	uv_close((uv_handle_t *)&xx_loop(loop)->wq_async, NULL);
}

unsigned int xx__thread_id(void)
{
	return *(unsigned int *)uv_key_get(&thread_key);
}
