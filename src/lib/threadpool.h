#ifndef __THREAD_POOL__
#define __THREAD_POOL__

#include <uv.h>
#include "queue.h"

/**
   Thread pool

   - Use-cases:

     - Move sqlite3-, IO- related blocking operations from libuv
       loop's thread to pool's threads in order to unblock serving
       incoming dqlite requests during sqlite3 IO.
       Multiple sqlite3_step()-s can be in flight and executed
       concurrently, while thread's loop is not IO blocked.

     - Introduced pool's work item thread affinity to serve sqlite3-
       related items of each database in a "dedicated" thread which
       allows not to make any assumption on sqlite3 threading model.
       @see https://www.sqlite.org/threadsafe.html

     - The pool supports servicing of the following types of work items:

       - WT_UNORD - items, which can be processed by the pool in any
         order, concurrency assumptions of this type of work are
         guaranteed by other layers of the application. Read and write
         transactions executed by sqlite3_step() are good examples for
         such work item type.

       - WT_ORD_N - items, which can NOT be processed by the pool in
         any order. The pool's logic shall guarantee that servicing
         all WT_ORD_{N}s happens before WT_ORD_{N + 1}s. WT_ORD_{N}s
         and WT_ORD_{N + 1}s operations can't be put into the pool
         interleaved. Sqlite3 checkpoints is an example of WT_ORD_{N}
         and InstallSnapshot(CP(), MV()) is an example of WT_ORD_{N + 1}.

       - WT_BAR - special purpose item, barrier. Delimits WT_ORD_{N}s
         from WT_ORD_{N + 1}s.

     - The pool supports servicing of work items with a given quality
       of service (QoS) considerations. For example, the priority of
       serving read/write sqlite3 transactions (WT_UNORD) can be set
       higher then snapshot installation (WT_ORD{N}).
 */

struct pool_impl;
typedef struct pool_s pool_t;
typedef struct pool_work_s pool_work_t;

enum pool_work_type {
	WT_UNORD,
	WT_BAR,
	WT_ORD1,
	WT_ORD2,
	WT_NR,
};

struct pool_work_s {
	queue     link;       /* Link into ordered, unordered and outq */
	uint32_t  thread_id;  /* Identifier of the thread the item is affined */
	pool_t   *pool;       /* The pool, item is being associated with */
	enum pool_work_type type;

	void (*work_cb)(pool_work_t *w);
	void (*after_work_cb)(pool_work_t *w);
};

struct pool_s {
	struct pool_impl *pi;
};

int  pool_init(pool_t *pool, uv_loop_t *loop, uint32_t threads_nr);
void pool_fini(pool_t *pool);
void pool_close(pool_t *pool);
void pool_queue_work(pool_t *pool,
		     pool_work_t *w,
		     uint32_t cookie,
		     enum pool_work_type type,
		     void (*work_cb)(pool_work_t *w),
		     void (*after_work_cb)(pool_work_t *w));

#endif  /* __THREAD_POOL__ */
