#include <uv.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

// ---------------------------------------------------------------------------

struct xx__queue {
  struct xx__queue* next;
  struct xx__queue* prev;
};

#define xx__queue_data(pointer, type, field)		\
  ((type*) ((char*) (pointer) - offsetof(type, field)))

#define xx__queue_foreach(q, h)				\
  for ((q) = (h)->next; (q) != (h); (q) = (q)->next)

static inline void xx__queue_init(struct xx__queue* q) {
  q->next = q;
  q->prev = q;
}

static inline int xx__queue_empty(const struct xx__queue* q) {
  return q == q->next;
}

static inline struct xx__queue* xx__queue_head(const struct xx__queue* q) {
  return q->next;
}

static inline struct xx__queue* xx__queue_next(const struct xx__queue* q) {
  return q->next;
}

static inline void xx__queue_add(struct xx__queue* h, struct xx__queue* n) {
  h->prev->next = n->next;
  n->next->prev = h->prev;
  h->prev = n->prev;
  h->prev->next = h;
}

static inline void xx__queue_split(struct xx__queue* h,
                                   struct xx__queue* q,
                                   struct xx__queue* n) {
  n->prev = h->prev;
  n->prev->next = n;
  n->next = q;
  h->prev = q->prev;
  h->prev->next = h;
  q->prev = n;
}

static inline void xx__queue_move(struct xx__queue* h, struct xx__queue* n) {
  if (xx__queue_empty(h))
    xx__queue_init(n);
  else
    xx__queue_split(h, h->next, n);
}

static inline void xx__queue_insert_head(struct xx__queue* h,
                                         struct xx__queue* q) {
  q->next = h->next;
  q->prev = h;
  q->next->prev = q;
  h->next = q;
}

static inline void xx__queue_insert_tail(struct xx__queue* h,
                                         struct xx__queue* q) {
  q->next = h;
  q->prev = h->prev;
  q->prev->next = q;
  h->prev = q;
}

static inline void xx__queue_remove(struct xx__queue* q) {
  q->prev->next = q->next;
  q->next->prev = q->prev;
}

// ---------------------------------------------------------------------------

struct xx__work {
  void (*work)(struct xx__work *w);
  void (*done)(struct xx__work *w, int status);
  struct uv_loop_s* loop;
  struct xx__queue wq;
};


typedef struct xx_work_s xx_work_t;
typedef void (*xx_work_cb)(xx_work_t* req);
typedef void (*xx_after_work_cb)(xx_work_t* req, int status);

struct xx_work_s {
  uv_loop_t* loop;
  xx_work_cb work_cb;
  xx_after_work_cb after_work_cb;
  struct xx__work work_req;
};

struct xx_loop_s {
    struct uv_loop_s loop;

    struct xx__queue wq;
    uv_mutex_t wq_mutex;
    uv_async_t wq_async;
    uint64_t   active_reqs;
};

static struct xx_loop_s *xx_loop(struct uv_loop_s *loop) {
    return (struct xx_loop_s *) loop;
}

#define xx__has_active_reqs(loop)		\
    ((loop)->active_reqs > 0)

#define xx__req_register(loop, req)		\
    do {					\
	(loop)->active_reqs++;			\
    }						\
    while (0)

#define xx__req_unregister(loop, req)		\
    do {					\
	assert(xx__has_active_reqs(loop));	\
	(loop)->active_reqs--;			\
    }						\
    while (0)

#define container_of(ptr, type, member)		\
	((type *)((char *)(ptr)-offsetof(type, member)))

#define ARRAY_SIZE(a) (sizeof(a) / sizeof((a)[0]))

#define MAX_THREADPOOL_SIZE 1024

static uv_once_t once = UV_ONCE_INIT;
static uv_cond_t cond;
static uv_mutex_t mutex;
static unsigned int idle_threads;
static unsigned int nthreads;
static uv_thread_t* threads;
static uv_thread_t default_threads[4];
static struct xx__queue exit_message;
static struct xx__queue wq;


static void xx__cancelled(struct xx__work* w __attribute__((unused))) {
  abort();
}


/* To avoid deadlock with uv_cancel() it's crucial that the worker
 * never holds the global mutex and the loop-local mutex at the same time.
 */
static void worker(void* arg) {
  struct xx__work* w;
  struct xx__queue* q;

  uv_sem_post((uv_sem_t*) arg);
  arg = NULL;

  uv_mutex_lock(&mutex);
  for (;;) {
    /* `mutex` should always be locked at this point. */

    /* Keep waiting while either no work is present or only slow I/O
       and we're at the threshold for that. */
    while (xx__queue_empty(&wq)) {
      idle_threads += 1;
      uv_cond_wait(&cond, &mutex);
      idle_threads -= 1;
    }

    q = xx__queue_head(&wq);
    if (q == &exit_message) {
      uv_cond_signal(&cond);
      uv_mutex_unlock(&mutex);
      break;
    }

    xx__queue_remove(q);
    xx__queue_init(q);  /* Signal uv_cancel() that the work req is executing. */

    uv_mutex_unlock(&mutex);

    w = xx__queue_data(q, struct xx__work, wq);
    w->work(w);

    uv_mutex_lock(&xx_loop(w->loop)->wq_mutex);
    w->work = NULL;  /* Signal uv_cancel() that the work req is done
                        executing. */
    xx__queue_insert_tail(&xx_loop(w->loop)->wq, &w->wq);
    uv_async_send(&xx_loop(w->loop)->wq_async);
    uv_mutex_unlock(&xx_loop(w->loop)->wq_mutex);

    /* Lock `mutex` since that is expected at the start of the next
     * iteration. */
    uv_mutex_lock(&mutex);
  }
}


static void post(struct xx__queue* q) {
  uv_mutex_lock(&mutex);
  xx__queue_insert_tail(&wq, q);
  if (idle_threads > 0)
    uv_cond_signal(&cond);
  uv_mutex_unlock(&mutex);
}


void uv__threadpool_cleanup(void) {
  unsigned int i;

  if (nthreads == 0)
    return;

  post(&exit_message);

  for (i = 0; i < nthreads; i++)
    if (uv_thread_join(threads + i))
      abort();

  if (threads != default_threads)
    free(threads);

  uv_mutex_destroy(&mutex);
  uv_cond_destroy(&cond);

  threads = NULL;
  nthreads = 0;
}


static void init_threads(void) {
  uv_thread_options_t config;
  unsigned int i;
  const char* val;
  uv_sem_t sem;

  nthreads = ARRAY_SIZE(default_threads);
  val = getenv("UV_THREADPOOL_SIZE");
  if (val != NULL)
    nthreads = (unsigned int) atoi(val);
  if (nthreads == 0)
    nthreads = 1;
  if (nthreads > MAX_THREADPOOL_SIZE)
    nthreads = MAX_THREADPOOL_SIZE;

  threads = default_threads;
  if (nthreads > ARRAY_SIZE(default_threads)) {
    threads = malloc(nthreads * sizeof(threads[0]));
    if (threads == NULL) {
      nthreads = ARRAY_SIZE(default_threads);
      threads = default_threads;
    }
  }

  if (uv_cond_init(&cond))
    abort();

  if (uv_mutex_init(&mutex))
    abort();

  xx__queue_init(&wq);

  if (uv_sem_init(&sem, 0))
    abort();

  config.flags = UV_THREAD_HAS_STACK_SIZE;
  config.stack_size = 8u << 20;  /* 8 MB */

  for (i = 0; i < nthreads; i++)
    if (uv_thread_create_ex(threads + i, &config, worker, &sem))
      abort();

  for (i = 0; i < nthreads; i++)
    uv_sem_wait(&sem);

  uv_sem_destroy(&sem);
}


static void reset_once(void) {
  uv_once_t child_once = UV_ONCE_INIT;
  memcpy(&once, &child_once, sizeof(child_once));
}


static void init_once(void) {
  if (pthread_atfork(NULL, NULL, &reset_once))
    abort();
  init_threads();
}


void xx__work_submit(uv_loop_t* loop,
                     struct xx__work* w,
                     void (*work)(struct xx__work* w),
                     void (*done)(struct xx__work* w, int status)) {
  uv_once(&once, init_once);
  w->loop = loop;
  w->work = work;
  w->done = done;
  post(&w->wq);
}


static int xx__work_cancel(uv_loop_t* loop, struct xx__work* w) {
  int cancelled;

  uv_once(&once, init_once);  /* Ensure |mutex| is initialized. */
  uv_mutex_lock(&mutex);
  uv_mutex_lock(&xx_loop(w->loop)->wq_mutex);

  cancelled = !xx__queue_empty(&w->wq) && w->work != NULL;
  if (cancelled)
    xx__queue_remove(&w->wq);

  uv_mutex_unlock(&xx_loop(w->loop)->wq_mutex);
  uv_mutex_unlock(&mutex);

  if (!cancelled)
    return UV_EBUSY;

  w->work = xx__cancelled;
  uv_mutex_lock(&xx_loop(loop)->wq_mutex);
  xx__queue_insert_tail(&xx_loop(loop)->wq, &w->wq);
  uv_async_send(&xx_loop(loop)->wq_async);
  uv_mutex_unlock(&xx_loop(loop)->wq_mutex);

  return 0;
}


void xx__work_done(uv_async_t* handle) {
  struct xx__work* w;
  uv_loop_t* loop;
  struct xx_loop_s *xxloop;
  struct xx__queue* q;
  struct xx__queue wq_;
  int err;

  xxloop = container_of(handle, struct xx_loop_s, wq_async);
  loop = &xxloop->loop;
  uv_mutex_lock(&xx_loop(loop)->wq_mutex);
  xx__queue_move(&xx_loop(loop)->wq, &wq_);
  uv_mutex_unlock(&xx_loop(loop)->wq_mutex);

  while (!xx__queue_empty(&wq_)) {
    q = xx__queue_head(&wq_);
    xx__queue_remove(q);

    w = container_of(q, struct xx__work, wq);
    err = (w->work == xx__cancelled) ? UV_ECANCELED : 0;
    w->done(w, err);
  }
}


static void xx__queue_work(struct xx__work* w) {
  xx_work_t* req = container_of(w, xx_work_t, work_req);

  req->work_cb(req);
}


static void xx__queue_done(struct xx__work* w, int err) {
  xx_work_t* req;

  req = container_of(w, xx_work_t, work_req);
  xx__req_unregister(xx_loop(req->loop), req);

  if (req->after_work_cb == NULL)
    return;

  req->after_work_cb(req, err);
}


int xx_queue_work(uv_loop_t* loop,
                  xx_work_t* req,
                  xx_work_cb work_cb,
                  xx_after_work_cb after_work_cb) {
  if (work_cb == NULL)
    return UV_EINVAL;

  xx__req_register(xx_loop(loop), req);
  req->loop = loop;
  req->work_cb = work_cb;
  req->after_work_cb = after_work_cb;
  xx__work_submit(loop,
                  &req->work_req,
                  xx__queue_work,
                  xx__queue_done);
  return 0;
}


int xx_cancel(xx_work_t* req) {
  return xx__work_cancel(req->loop, &req->work_req);
}

int xx_loop_init(struct xx_loop_s *loop) {
    int err;

    xx__queue_init(&loop->wq);
    err = uv_mutex_init(&loop->wq_mutex);
    assert(err == 0);

    err = uv_async_init(&loop->loop, &loop->wq_async, xx__work_done);
    assert(err == 0);

    uv_unref((uv_handle_t *) &loop->wq_async);

    return 0;
}

void xx_loop_close(struct xx_loop_s *loop) {
    //loop->wq_async.async_sent = 0;
    //loop->wq_async.close_cb = NULL;
    //uv__handle_closing(&loop->wq_async);
    //uv__handle_close(&loop->wq_async);
    uv_close((uv_handle_t *) &loop->wq_async, NULL);

    uv_mutex_lock(&loop->wq_mutex);
    assert(xx__queue_empty(&loop->wq) && "thread pool work queue not empty!");
    assert(!xx__has_active_reqs(loop));
    uv_mutex_unlock(&loop->wq_mutex);
    uv_mutex_destroy(&loop->wq_mutex);
}
