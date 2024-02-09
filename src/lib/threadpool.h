#ifndef __THREAD_POOL__
#define __THREAD_POOL__

#include <uv.h>
#include "queue.h"


typedef struct pool_loop_s pool_loop_t;
typedef struct pool_work_s pool_work_t;

enum pool_work_type {
	WT_UNORD,
	WT_BAR,
	WT_ORD1,
	WT_ORD2,
};

struct pool_work {
	queue qlink; /* link into ordered, unordered and outq */
	struct uv_loop_s *loop;
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

struct pool_loop_impl;
struct pool_loop_s {
	uint64_t	       magic;
	struct uv_loop_s       loop;
	struct pool_loop_impl *impl;
};

int  pool_loop_init(pool_loop_t *loop);
void pool_loop_fini(pool_loop_t *loop);
void pool_loop_close(uv_loop_t *loop);
int  pool_queue_work(uv_loop_t *loop,
		     pool_work_t *req,
		     uint32_t cookie,
		     void (*work_cb)(pool_work_t *req),
		     void (*after_work_cb)(pool_work_t *req));
pool_loop_t *uv_to_pool_loop(uv_loop_t *loop);

/* For tests */
uint32_t pool_loop_thread_id(const pool_loop_t *loop);
#endif  // __THREAD_POOL__
