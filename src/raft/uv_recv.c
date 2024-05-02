#include <string.h>

#include "../raft.h"

#include "assert.h"
#include "byte.h"
#include "configuration.h"
#include "err.h"
#include "heap.h"
#include "uv.h"
#include "uv_encoding.h"

/* The happy path for a receiving an RPC message is:
 *
 * - When a peer server successfully establishes a new connection with us, the
 *   transport invokes our accept callback.
 *
 * - A new server object is created and added to the servers array. It starts
 *   reading from the stream handle of the new connection.
 *
 * - The RPC message preamble is read, which contains the message type and the
 *   message length.
 *
 * - The RPC message header is read, whose content depends on the message type.
 *
 * - Optionally, the RPC message payload is read (for AppendEntries requests).
 *
 * - The recv callback passed to raft_io->start() gets fired with the received
 *   message.
 *
 * Possible failure modes are:
 *
 * - The peer server disconnects. In this case the read callback will fire with
 *   UV_EOF, we'll close the stream handle and then release all memory
 *   associated with the server object.
 *
 * - The peer server sends us invalid data. In this case we close the stream
 *   handle and act like above.
 */

struct uvServer
{
	struct uv *uv;              /* libuv I/O implementation object */
	raft_id id;                 /* ID of the remote server */
	char *address;              /* Address of the other server */
	struct uv_stream_s *stream; /* Connection handle */
	uv_buf_t buf;         /* Sliding buffer for reading incoming data */
	uint64_t preamble[2]; /* Static buffer with the request preamble */
	uv_buf_t header;      /* Dynamic buffer with the request header */
	uv_buf_t payload;     /* Dynamic buffer with the request payload */
	struct raft_message message; /* The message being received */
	queue queue;                 /* Servers queue */
};

/* Initialize a new server object for reading requests from an incoming
 * connection. */
static int uvServerInit(struct uvServer *s,
			struct uv *uv,
			const raft_id id,
			const char *address,
			struct uv_stream_s *stream)
{
	s->uv = uv;
	s->id = id;
	s->address = RaftHeapMalloc(strlen(address) + 1);
	if (s->address == NULL) {
		return RAFT_NOMEM;
	}
	strcpy(s->address, address);
	s->stream = stream;
	s->stream->data = s;
	s->buf.base = NULL;
	s->buf.len = 0;
	s->preamble[0] = 0;
	s->preamble[1] = 0;
	s->header.base = NULL;
	s->header.len = 0;
	s->message.type = 0;
	s->payload.base = NULL;
	s->payload.len = 0;
	queue_insert_tail(&uv->servers, &s->queue);
	return 0;
}

static void uvServerDestroy(struct uvServer *s)
{
	queue_remove(&s->queue);

	if (s->header.base != NULL) {
		/* This means we were interrupted while reading the header. */
		RaftHeapFree(s->header.base);
		switch (s->message.type) {
			case RAFT_IO_APPEND_ENTRIES:
				RaftHeapFree(s->message.append_entries.entries);
				break;
			case RAFT_IO_INSTALL_SNAPSHOT:
				configurationClose(
				    &s->message.install_snapshot.conf);
				break;
		}
	}
	if (s->payload.base != NULL) {
		/* This means we were interrupted while reading the payload. */
		RaftHeapFree(s->payload.base);
	}
	RaftHeapFree(s->address);
	RaftHeapFree(s->stream);
}

/* Invoked to initialize the read buffer for the next asynchronous read on the
 * socket. */
static void uvServerAllocCb(uv_handle_t *handle,
			    size_t suggested_size,
			    uv_buf_t *buf)
{
	struct uvServer *s = handle->data;
	(void)suggested_size;

	assert(!s->uv->closing);

	/* If this is the first read of the preamble, or of the header, or of
	 * the payload, then initialize the read buffer, according to the chunk
	 * of data that we expect next. */
	if (s->buf.len == 0) {
		assert(s->buf.base == NULL);

		/* Check if we expect the preamble. */
		if (s->header.len == 0) {
			assert(s->preamble[0] == 0);
			assert(s->preamble[1] == 0);
			s->buf.base = (char *)s->preamble;
			s->buf.len = sizeof s->preamble;
			goto out;
		}

		/* Check if we expect the header. */
		if (s->payload.len == 0) {
			assert(s->header.len > 0);
			assert(s->header.base == NULL);
			s->header.base = RaftHeapMalloc(s->header.len);
			if (s->header.base == NULL) {
				/* Setting all buffer fields to 0 will make
				 * read_cb fail with ENOBUFS. */
				memset(buf, 0, sizeof *buf);
				return;
			}
			s->buf = s->header;
			goto out;
		}

		/* If we get here we should be expecting the payload. */
		assert(s->payload.len > 0);
		s->payload.base = RaftHeapMalloc(s->payload.len);
		if (s->payload.base == NULL) {
			/* Setting all buffer fields to 0 will make read_cb fail
			 * with ENOBUFS. */
			memset(buf, 0, sizeof *buf);
			return;
		}

		s->buf = s->payload;
	}

out:
	*buf = s->buf;
}

/* Callback invoked afer the stream handle of this server connection has been
 * closed. We can release all resources associated with the server object. */
static void uvServerStreamCloseCb(uv_handle_t *handle)
{
	struct uvServer *s = handle->data;
	struct uv *uv = s->uv;
	uvServerDestroy(s);
	RaftHeapFree(s);
	uvMaybeFireCloseCb(uv);
}

static void uvServerAbort(struct uvServer *s)
{
	struct uv *uv = s->uv;
	queue_remove(&s->queue);
	queue_insert_tail(&uv->aborting, &s->queue);
	uv_close((struct uv_handle_s *)s->stream, uvServerStreamCloseCb);
}

/* Invoke the receive callback. */
static void uvFireRecvCb(struct uvServer *s)
{
	s->uv->recv_cb(s->uv->io, &s->message);

	/* Reset our state as we'll start reading a new message. We don't need
	 * to release the payload buffer, since ownership was transferred to the
	 * user. */
	memset(s->preamble, 0, sizeof s->preamble);
	raft_free(s->header.base);
	s->message.type = 0;
	s->header.base = NULL;
	s->header.len = 0;
	s->payload.base = NULL;
	s->payload.len = 0;
}

/* Callback invoked when data has been read from the socket. */
static void uvServerReadCb(uv_stream_t *stream,
			   ssize_t nread,
			   const uv_buf_t *buf)
{
	struct uvServer *s = stream->data;
	int rv;

	(void)buf;

	assert(!s->uv->closing);

	/* If the read was successful, let's check if we have received all the
	 * data we expected. */
	if (nread > 0) {
		size_t n = (size_t)nread;

		/* We shouldn't have read more data than the pending amount. */
		assert(n <= s->buf.len);

		/* Advance the read window */
		s->buf.base += n;
		s->buf.len -= n;

		/* If there's more data to read in order to fill the current
		 * read buffer, just return, we'll be invoked again. */
		if (s->buf.len > 0) {
			return;
		}

		if (s->header.len == 0) {
			/* If the header buffer is not set, it means that we've
			 * just completed reading the preamble. */
			assert(s->header.base == NULL);

			s->header.len = (size_t)byteFlip64(s->preamble[1]);

			/* The length of the header must be greater than zero.
			 */
			if (s->header.len == 0) {
				tracef("message has zero length");
				goto abort;
			}
		} else if (s->payload.len == 0) {
			/* If the payload buffer is not set, it means we just
			 * completed reading the message header. */
			uint64_t type;

			assert(s->header.base != NULL);

			type = byteFlip64(s->preamble[0]);

			/* Only use first 2 bytes of the type. Normally we would
			 * check if type doesn't overflow UINT16_MAX, but we
			 * don't do this to allow future legacy nodes to still
			 * handle messages that include extra information in the
			 * 6 unused bytes of the type field of the preamble.
			 * TODO: This is preparation to add the version of the
			 * message in the raft preamble. Once this change has
			 * been active for sufficiently long time, we can start
			 * encoding the version in some of the remaining bytes
			 * of s->preamble[0]. */
			rv = uvDecodeMessage((uint16_t)type, &s->header,
					     &s->message, &s->payload.len);
			if (rv != 0) {
				tracef("decode message: %s",
				       errCodeToString(rv));
				goto abort;
			}

			s->message.server_id = s->id;
			s->message.server_address = s->address;

			/* If the message has no payload, we're done. */
			if (s->payload.len == 0) {
				uvFireRecvCb(s);
			}
		} else {
			/* If we get here it means that we've just completed
			 * reading the payload. TODO: avoid converting from
			 * uv_buf_t */
			struct raft_buffer payload;
			assert(s->payload.base != NULL);
			assert(s->payload.len > 0);

			switch (s->message.type) {
				case RAFT_IO_APPEND_ENTRIES:
					payload.base = s->payload.base;
					payload.len = s->payload.len;
					uvDecodeEntriesBatch(
					    payload.base, 0,
					    s->message.append_entries.entries,
					    s->message.append_entries
						.n_entries);
					break;
				case RAFT_IO_INSTALL_SNAPSHOT:
					s->message.install_snapshot.data.base =
					    s->payload.base;
					break;
				default:
					/* We should never have read a payload
					 * in the first place */
					assert(0);
			}

			uvFireRecvCb(s);
		}

		/* Mark that we're done with this chunk. When the alloc callback
		 * will trigger again it will notice that it needs to change the
		 * read buffer. */
		assert(s->buf.len == 0);
		s->buf.base = NULL;

		return;
	}

	/* The if nread>0 condition above should always exit the function with a
	 * goto abort or a return. */
	assert(nread <= 0);

	if (nread == 0) {
		/* Empty read */
		return;
	}
	if (nread != UV_EOF) {
		tracef("receive data: %s", uv_strerror((int)nread));
	}

abort:
	uvServerAbort(s);
}

/* Start reading incoming requests. */
static int uvServerStart(struct uvServer *s)
{
	int rv;
	rv = uv_read_start(s->stream, uvServerAllocCb, uvServerReadCb);
	if (rv != 0) {
		tracef("start reading: %s", uv_strerror(rv));
		return RAFT_IOERR;
	}
	return 0;
}

static int uvAddServer(struct uv *uv,
		       raft_id id,
		       const char *address,
		       struct uv_stream_s *stream)
{
	struct uvServer *server;
	int rv;

	/* Initialize the new connection */
	server = RaftHeapMalloc(sizeof *server);
	if (server == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}

	rv = uvServerInit(server, uv, id, address, stream);
	if (rv != 0) {
		goto err_after_server_alloc;
	}

	/* This will start reading requests. */
	rv = uvServerStart(server);
	if (rv != 0) {
		goto err_after_init_server;
	}

	return 0;

err_after_init_server:
	uvServerDestroy(server);
err_after_server_alloc:
	raft_free(server);
err:
	assert(rv != 0);
	return rv;
}

static void uvRecvAcceptCb(struct raft_uv_transport *transport,
			   raft_id id,
			   const char *address,
			   struct uv_stream_s *stream)
{
	struct uv *uv = transport->data;
	int rv;
	assert(!uv->closing);
	rv = uvAddServer(uv, id, address, stream);
	if (rv != 0) {
		tracef("add server: %s", errCodeToString(rv));
		uv_close((struct uv_handle_s *)stream,
			 (uv_close_cb)RaftHeapFree);
	}
}

int UvRecvStart(struct uv *uv)
{
	int rv;
	rv = uv->transport->listen(uv->transport, uvRecvAcceptCb);
	if (rv != 0) {
		return rv;
	}
	return 0;
}

void UvRecvClose(struct uv *uv)
{
	while (!queue_empty(&uv->servers)) {
		queue *head;
		struct uvServer *server;
		head = queue_head(&uv->servers);
		server = QUEUE_DATA(head, struct uvServer, queue);
		uvServerAbort(server);
	}
}

