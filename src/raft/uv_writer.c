#include "uv_writer.h"

#include <string.h>

#include "../lib/assert.h"

static const struct UvWriterBackend *writerDefaultBackend(void)
{
#if defined(DQLITE_RAFT_IO_LINUX_KAIO)
	return &UvWriterLinuxKaioBackend;
#else
	return &UvWriterLibuvBackend;
#endif
}

int UvWriterInit(struct UvWriter *writer,
		 struct uv_loop_s *loop,
		 uv_file fd,
		 bool direct,
		 bool async,
		 unsigned max_concurrent_writes,
		 char *errmsg)
{
	void *data = writer->data;
	struct UvWriterOptions options = {
	    .direct = direct,
	    .async = async,
	    .max_concurrent_writes = max_concurrent_writes,
	};

	memset(writer, 0, sizeof *writer);
	writer->data = data;
	writer->backend = writerDefaultBackend();
	writer->errmsg = errmsg;
	return writer->backend->init(writer, loop, fd, &options, errmsg);
}

void UvWriterClose(struct UvWriter *writer, UvWriterCloseCb cb)
{
	dqlite_assert(writer->backend != NULL);
	writer->backend->close(writer, cb);
}

int UvWriterSubmit(struct UvWriter *writer,
		   struct UvWriterReq *req,
		   const uv_buf_t bufs[],
		   unsigned n,
		   size_t offset,
		   UvWriterReqCb cb)
{
	dqlite_assert(writer->backend != NULL);
	return writer->backend->submit(writer, req, bufs, n, offset, cb);
}
