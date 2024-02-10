#ifndef __THREAD_POOL__
#define __THREAD_POOL__

#include <uv.h>
#include "queue.h"


typedef struct pool_s pool_t;
typedef struct pool_work_s pool_work_t;

enum pool_work_type {
	WT_UNORD,
	WT_BAR,
	WT_ORD1,
	WT_ORD2,
};

struct pool_work {
	queue qlink; /* link into ordered, unordered and outq */
	uv_loop_t *loop;
	uint32_t thread_idx;
	enum pool_work_type type;

	void (*work)(struct pool_work *w);
	void (*done)(struct pool_work *w);
};

struct pool_work_s {
	uv_loop_t *loop;
	void (*work_cb)(pool_work_t *req);
	void (*after_work_cb)(pool_work_t *req);
	struct pool_work work_req;
};

struct pool_impl;
struct pool_s {
	uint64_t	  magic;
	uv_loop_t         loop;
	struct pool_impl *impl;
};


int  pool_init(pool_t *pool);
void pool_fini(pool_t *pool);
void pool_close(pool_t *pool);
int  pool_queue_work(pool_t *pool,
		     pool_work_t *req,
		     uint32_t cookie,
		     void (*work_cb)(pool_work_t *req),
		     void (*after_work_cb)(pool_work_t *req));
pool_t *uv_loop_to_pool(const uv_loop_t *loop);

/* For tests */
uint32_t pool_thread_id(const pool_t *pool);
#endif  // __THREAD_POOL__
