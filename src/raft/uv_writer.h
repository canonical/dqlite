/* Asynchronous API to write a file. */

#ifndef UV_WRITER_H_
#define UV_WRITER_H_

#include <stdbool.h>
#include <uv.h>

#include "err.h"
#include "../lib/queue.h"

/* Perform asynchronous writes to a single file. */
struct UvWriter;
struct UvWriterReq;

/* Callback called after the memory associated with a file handle can be
 * released. */
typedef void (*UvWriterCloseCb)(struct UvWriter *w);

/* Callback called after a write request has been completed. */
typedef void (*UvWriterReqCb)(struct UvWriterReq *req, int status);

struct UvWriterOptions
{
	bool direct;                    /* Whether direct I/O is available. */
	bool async;                     /* Whether fully async I/O is available. */
	unsigned max_concurrent_writes; /* Maximum concurrent write requests. */
};

struct UvWriterBackend
{
	int (*init)(struct UvWriter *w,
		    struct uv_loop_s *loop,
		    uv_file fd,
		    const struct UvWriterOptions *options,
		    char *errmsg);
	void (*close)(struct UvWriter *w, UvWriterCloseCb cb);
	int (*submit)(struct UvWriter *w,
		      struct UvWriterReq *req,
		      const uv_buf_t bufs[],
		      unsigned n,
		      size_t offset,
		      UvWriterReqCb cb);
};

struct UvWriter
{
	void *data;                            /* User data */
	const struct UvWriterBackend *backend; /* Selected implementation */
	void *impl;                            /* Backend-private state */
	bool closing;                          /* Whether close has been requested */
	char *errmsg;                          /* Description of last error */
};

struct UvWriterReq
{
	void *data;              /* User data */
	struct UvWriter *writer; /* Originating writer */
	size_t len;              /* Total number of bytes to write */
	int status;              /* Request result code */
	UvWriterReqCb cb; /* Callback to invoke upon request completion */
	char errmsg[256]; /* Error description (for thread-safety) */
	queue queue;      /* Prev/next links in the inflight queue */
	union {
		uv_fs_t fs_req;
		uv_work_t work;
	} uv;
	void *impl;       /* Backend-private request state */
};

extern const struct UvWriterBackend UvWriterLibuvBackend;

#if defined(DQLITE_RAFT_IO_LINUX_KAIO)
extern const struct UvWriterBackend UvWriterLinuxKaioBackend;
#endif

/* Initialize a file writer. */
int UvWriterInit(struct UvWriter *w,
		 struct uv_loop_s *loop,
		 uv_file fd,
		 bool direct /* Whether to use direct I/O */,
		 bool async /* Whether async I/O is available */,
		 unsigned max_concurrent_writes,
		 char *errmsg);

/* Close the given file and release all associated resources. */
void UvWriterClose(struct UvWriter *w, UvWriterCloseCb cb);

/* Asynchronously write data to the underlying file. */
int UvWriterSubmit(struct UvWriter *w,
		   struct UvWriterReq *req,
		   const uv_buf_t bufs[],
		   unsigned n,
		   size_t offset,
		   UvWriterReqCb cb);

#endif /* UV_WRITER_H_ */
