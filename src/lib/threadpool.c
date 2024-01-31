#include "src/lib/threadpool.h"
#include <assert.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>
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
	NOT_AFFINED = ~0u,
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

static void xx__cancelled(struct xx__work *w UNUSED)
{
	abort();
}

unsigned int xx__thread_id(void)
{
	return *(unsigned int *)uv_key_get(&thread_key);
}

/* To avoid deadlock with uv_cancel() it's crucial that the worker
 * never holds the global mutex and the loop-local mutex at the same time.
 */
static void worker(void *arg)
{
	struct xx__work *w;
	queue *q;
	struct thread_args *ta = arg;

	uv_key_set(&thread_key, &ta->idx);
	uv_sem_post(ta->sem);
	uv_mutex_lock(&mutex);
	for (;;) {
		if (QUEUE__IS_EMPTY(&thread_queues[ta->idx])) {
		    uv_cond_wait(&cond[ta->idx], &mutex);
		}

		if (!QUEUE__IS_EMPTY(&wq)) {
		    assert(QUEUE__HEAD(&wq) == &exit_message);
		    uv_mutex_unlock(&mutex);
		    break;
		}

		if (QUEUE__IS_EMPTY(&thread_queues[ta->idx])) {
		    continue;
		}

		q = QUEUE__HEAD(&thread_queues[ta->idx]);
		QUEUE__REMOVE(q);
		QUEUE__INIT(q);

		uv_mutex_unlock(&mutex);

		w = QUEUE__DATA(q, struct xx__work, wq);
		w->work(w);

		uv_mutex_lock(&xx_loop(w->loop)->wq_mutex);
		w->work = NULL;

		QUEUE__INSERT_TAIL(&xx_loop(w->loop)->wq, &w->wq);

		uv_async_send(&xx_loop(w->loop)->wq_async);
		uv_mutex_unlock(&xx_loop(w->loop)->wq_mutex);

		/* Lock `mutex` since that is expected at the start of the next
		 * iteration. */
		uv_mutex_lock(&mutex);
	}
}

static void post(queue *q, unsigned int idx)
{
	uv_mutex_lock(&mutex);

	/* Assign work item thread affinity */
	QUEUE__INSERT_TAIL(idx == NOT_AFFINED ? &wq : &thread_queues[idx], q);

	for (idx = 0; idx < nthreads; ++idx)
	    uv_cond_signal(&cond[idx]);

	uv_mutex_unlock(&mutex);
}

static void xx__threadpool_cleanup(void)
{
	unsigned int i;

	if (nthreads == 0)
		return;

	post(&exit_message, NOT_AFFINED);

	for (i = 0; i < nthreads; i++) {
		if (uv_thread_join(threads + i))
			abort();
		POST(QUEUE__IS_EMPTY(&thread_queues[i]));
		uv_cond_destroy(&cond[i]);
	}

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
	if (cond == NULL ||threads == NULL || thread_args == NULL ||
	    thread_queues == NULL)
		abort();

	if (uv_mutex_init(&mutex))
		abort();

	QUEUE__INIT(&wq);

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

	for (i = 0; i < nthreads; i++)
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
	post(&w->wq, w->thread_idx);
}

static int xx__work_cancel(uv_loop_t *loop, struct xx__work *w)
{
	int cancelled;

	//PRE(threads__invariant());
	uv_mutex_lock(&mutex);
	uv_mutex_lock(&xx_loop(w->loop)->wq_mutex);

	cancelled = !QUEUE__IS_EMPTY(&w->wq) && w->work != NULL;
	if (cancelled)
		QUEUE__REMOVE(&w->wq);

	uv_mutex_unlock(&xx_loop(w->loop)->wq_mutex);
	uv_mutex_unlock(&mutex);

	if (!cancelled)
		return UV_EBUSY;

	w->work = xx__cancelled;
	uv_mutex_lock(&xx_loop(loop)->wq_mutex);
	QUEUE__INSERT_TAIL(&xx_loop(loop)->wq, &w->wq);
	uv_async_send(&xx_loop(loop)->wq_async);
	uv_mutex_unlock(&xx_loop(loop)->wq_mutex);

	return 0;
}

void xx__work_done(uv_async_t *handle)
{
	struct xx__work *w;
	uv_loop_t *loop;
	struct xx_loop_s *xxloop;
	queue *q;
	queue wq_ = {};
	int err;

	xxloop = container_of(handle, struct xx_loop_s, wq_async);
	loop = &xxloop->loop;

	uv_mutex_lock(&xx_loop(loop)->wq_mutex);
	QUEUE__MOVE(&xx_loop(loop)->wq, &wq_);
	uv_mutex_unlock(&xx_loop(loop)->wq_mutex);

	while (!QUEUE__IS_EMPTY(&wq_)) {
		q = QUEUE__HEAD(&wq_);
		QUEUE__REMOVE(q);

		w = container_of(q, struct xx__work, wq);
		err = (w->work == xx__cancelled) ? UV_ECANCELED : 0;
		w->done(w, err);
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

	xx__req_register(xx_loop(loop), req);
	req->loop = loop;
	req->work_cb = work_cb;
	req->after_work_cb = after_work_cb;
	req->work_req.thread_idx = cookie % nthreads;
	xx__work_submit(loop, &req->work_req, xx__queue_work, xx__queue_done);
	return 0;
}

int xx_cancel(xx_work_t *req)
{
	return xx__work_cancel(req->loop, &req->work_req);
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
