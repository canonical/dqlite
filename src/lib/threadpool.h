#ifndef __XX_THREAD_POOL__
#define __XX_THREAD_POOL__

#include <uv.h>
#include "queue.h"

struct xx__work
{
	void (*work)(struct xx__work *w);
	void (*done)(struct xx__work *w, int status);
	struct uv_loop_s *loop;
	queue wq;
	unsigned int thread_idx;
};

typedef struct xx_work_s xx_work_t;
typedef void (*xx_work_cb)(xx_work_t *req);
typedef void (*xx_after_work_cb)(xx_work_t *req, int status);

struct xx_work_s
{
	uv_loop_t *loop;
	xx_work_cb work_cb;
	xx_after_work_cb after_work_cb;
	struct xx__work work_req;
};

typedef struct xx_loop_s xx_loop_t;
struct xx_loop_s
{
	struct uv_loop_s loop;

	queue wq;
	uv_mutex_t wq_mutex;
	uv_async_t wq_async;
	uint64_t active_reqs;
};

void xx_loop_close(struct xx_loop_s *loop);
int xx_loop_init(struct xx_loop_s *loop);
int xx_queue_work(uv_loop_t *loop,
		  xx_work_t *req,
		  unsigned int cookie,
		  xx_work_cb work_cb,
		  xx_after_work_cb after_work_cb);
int xx_cancel(xx_work_t *req);
void xx_loop_async_close(uv_loop_t *loop);

/* For tests */
unsigned int xx__thread_id(void);
#endif  // __XX_THREAD_POOL__
