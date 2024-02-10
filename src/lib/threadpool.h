#ifndef __THREAD_POOL__
#define __THREAD_POOL__

#include <uv.h>
#include "queue.h"


struct pool_impl;
typedef struct pool_s pool_t;
typedef struct pool_work_s pool_work_t;

enum pool_work_type {
	WT_UNORD,
	WT_BAR,
	WT_ORD1,
	WT_ORD2,
};

struct pool_work_s {
	queue      qlink; /* a link into ordered, unordered and outq */
	uint32_t   thread_id;
	uv_loop_t *loop;
	enum pool_work_type type;

	void (*work_cb)(pool_work_t *w);
	void (*after_work_cb)(pool_work_t *w);
};

struct pool_s {
	uint64_t	  magic;
	uv_loop_t         loop;
	struct pool_impl *pi;
};

int  pool_init(pool_t *pool);
void pool_fini(pool_t *pool);
void pool_close(pool_t *pool);
void pool_queue_work(pool_t *pool,
		     pool_work_t *w,
		     uint32_t cookie,
		     void (*work_cb)(pool_work_t *w),
		     void (*after_work_cb)(pool_work_t *w));
pool_t *uv_loop_to_pool(const uv_loop_t *loop);

/* For tests */
uint32_t pool_thread_id(const pool_t *pool);
#endif  // __THREAD_POOL__
