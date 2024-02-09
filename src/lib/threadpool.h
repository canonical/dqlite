#ifndef __THREAD_POOL__
#define __THREAD_POOL__

#include <uv.h>
#include "queue.h"


typedef struct pool_loop_s pool_loop_t;
typedef struct pool_work_s pool_work_t;
typedef void (*pool_work_cb)(pool_work_t *req);
typedef void (*pool_after_work_cb)(pool_work_t *req, int status);

enum pool__work_type {
	WT_UNORD,
	WT_BAR,
	WT_ORD1,
	WT_ORD2,
};

struct pool__work {
	queue qlink;
	struct uv_loop_s *loop;
	unsigned int thread_idx;
	enum pool__work_type type;

	void (*work)(struct pool__work *w);
	void (*done)(struct pool__work *w, int status);
};

struct pool_work_s {
	uv_loop_t *loop;
	pool_work_cb work_cb;
	pool_after_work_cb after_work_cb;
	struct pool__work work_req;
};

struct pool_loop_s {
	struct uv_loop_s loop;
	uint64_t	 magic;

	queue      outq;
	uv_mutex_t outq_mutex;
	uv_async_t outq_async;
	uint64_t   active_reqs;
};

int  pool_loop_init(pool_loop_t *loop);
void pool_loop_close(pool_loop_t *loop);
int  pool_queue_work(uv_loop_t *loop,
		     pool_work_t *req,
		     unsigned int cookie,
		     pool_work_cb work_cb,
		     pool_after_work_cb after_work_cb);
void pool_loop_async_close(uv_loop_t *loop);
pool_loop_t *pool_loop(struct uv_loop_s *loop);

/* For tests */
unsigned int pool_thread_id(void);
#endif  // __THREAD_POOL__
