#include <string.h>

#include "../raft.h"
#include "assert.h"
#include "heap.h"
#include "uv.h"
#include "uv_encoding.h"

/* The happy path for an raft_io_send request is:
 *
 * - Get the uvClient object whose address matches the one of target server.
 * - Encode the message and write it using the uvClient's TCP handle.
 * - Once the write completes, fire the send request callback.
 *
 * Possible failure modes are:
 *
 * - The uv->clients queue has no client object with a matching address. In this
 *   case add a new client object to the array, add the send request to the
 *   queue of pending requests and submit a connection request. Once the
 *   connection request succeeds, try to write the encoded request to the
 *   connected stream handle. If the connection request fails, schedule another
 *   attempt.
 *
 * - The uv->clients queue has a client object which is not connected. Add the
 *   send request to the pending queue, and, if there's no connection attempt
 *   already in progress, start a new one.
 *
 * - The write request fails (either synchronously or asynchronously). In this
 *   case we fire the request callback with an error, close the connection
 *   stream, and start a re-connection attempt.
 */

/* Maximum number of requests that can be buffered.  */
#define UV__CLIENT_MAX_PENDING 3

struct uvClient
{
	struct uv *uv;                  /* libuv I/O implementation object */
	struct uv_timer_s timer;        /* Schedule connection attempts */
	struct raft_uv_connect connect; /* Connection request */
	struct uv_stream_s *stream;     /* Current connection handle */
	struct uv_stream_s *old_stream; /* Connection handle being closed */
	unsigned n_connect_attempt;     /* Consecutive connection attempts */
	raft_id id;                     /* ID of the other server */
	char *address;                  /* Address of the other server */
	queue pending;                  /* Pending send message requests */
	queue queue;                    /* Clients queue */
	bool closing;                   /* True after calling uvClientAbort */
};

/* Hold state for a single send RPC message request. */
struct uvSend
{
	struct uvClient *client;  /* Client connected to the target server */
	struct raft_io_send *req; /* User request */
	uv_buf_t *bufs;           /* Encoded raft RPC message to send */
	unsigned n_bufs;          /* Number of buffers */
	uv_write_t write;         /* Stream write request */
	queue queue;              /* Pending send requests queue */
};

/* Free all memory used by the given send request object, including the object
 * itself. */
static void uvSendDestroy(struct uvSend *s)
{
	if (s->bufs != NULL) {
		/* Just release the first buffer. Further buffers are entry or
		 * snapshot payloads, which we were passed but we don't own. */
		RaftHeapFree(s->bufs[0].base);

		/* Release the buffers array. */
		RaftHeapFree(s->bufs);
	}
	RaftHeapFree(s);
}

/* Initialize a new client associated with the given server. */
static int uvClientInit(struct uvClient *c,
			struct uv *uv,
			raft_id id,
			const char *address)
{
	int rv;
	c->uv = uv;
	c->timer.data = c;
	c->connect.data = NULL; /* Set upon starting a connect request */
	c->stream = NULL;       /* Set upon successful connection */
	c->old_stream = NULL;   /* Set after closing the current connection */
	c->n_connect_attempt = 0;
	c->id = id;
	c->address = RaftHeapMalloc(strlen(address) + 1);
	if (c->address == NULL) {
		return RAFT_NOMEM;
	}
	rv = uv_timer_init(c->uv->loop, &c->timer);
	assert(rv == 0);
	strcpy(c->address, address);
	queue_init(&c->pending);
	c->closing = false;
	queue_insert_tail(&uv->clients, &c->queue);
	return 0;
}

/* If there's no more pending cleanup, remove the client from the abort queue
 * and destroy it. */
static void uvClientMaybeDestroy(struct uvClient *c)
{
	struct uv *uv = c->uv;

	assert(c->stream == NULL);

	if (c->connect.data != NULL) {
		return;
	}
	if (c->timer.data != NULL) {
		return;
	}
	if (c->old_stream != NULL) {
		return;
	}

	while (!queue_empty(&c->pending)) {
		queue *head;
		struct uvSend *send;
		struct raft_io_send *req;
		head = queue_head(&c->pending);
		send = QUEUE_DATA(head, struct uvSend, queue);
		queue_remove(head);
		req = send->req;
		uvSendDestroy(send);
		if (req->cb != NULL) {
			req->cb(req, RAFT_CANCELED);
		}
	}

	queue_remove(&c->queue);

	assert(c->address != NULL);
	RaftHeapFree(c->address);
	RaftHeapFree(c);

	uvMaybeFireCloseCb(uv);
}

/* Forward declaration. */
static void uvClientConnect(struct uvClient *c);

static void uvClientDisconnectCloseCb(struct uv_handle_s *handle)
{
	struct uvClient *c = handle->data;
	assert(c->old_stream != NULL);
	assert(c->stream == NULL);
	assert(handle == (struct uv_handle_s *)c->old_stream);
	RaftHeapFree(c->old_stream);
	c->old_stream = NULL;
	if (c->closing) {
		uvClientMaybeDestroy(c);
	} else {
		uvClientConnect(c); /* Trigger a new connection attempt. */
	}
}

/* Close the current connection. */
static void uvClientDisconnect(struct uvClient *c)
{
	assert(c->stream != NULL);
	assert(c->old_stream == NULL);
	c->old_stream = c->stream;
	c->stream = NULL;
	uv_close((struct uv_handle_s *)c->old_stream,
		 uvClientDisconnectCloseCb);
}

/* Invoked once an encoded RPC message has been written out. */
static void uvSendWriteCb(struct uv_write_s *write, const int status)
{
	struct uvSend *send = write->data;
	struct uvClient *c = send->client;
	struct raft_io_send *req = send->req;
	int cb_status = 0;

	/* If the write failed and we're not currently closing, let's consider
	 * the current stream handle as busted and start disconnecting (unless
	 * we're already doing so). We'll trigger a new connection attempt once
	 * the handle is closed. */
	if (status != 0) {
		cb_status = RAFT_IOERR;
		if (!c->closing) {
			if (c->stream != NULL) {
				uvClientDisconnect(c);
			}
		} else if (status == UV_ECANCELED) {
			cb_status = RAFT_CANCELED;
		}
	}

	uvSendDestroy(send);

	if (req->cb != NULL) {
		req->cb(req, cb_status);
	}
}

static int uvClientSend(struct uvClient *c, struct uvSend *send)
{
	int rv;
	assert(!c->closing);
	send->client = c;

	/* If there's no connection available, let's queue the request. */
	if (c->stream == NULL) {
		tracef("no connection available -> enqueue message");
		queue_insert_tail(&c->pending, &send->queue);
		return 0;
	}

	tracef("connection available -> write message");
	send->write.data = send;
	rv = uv_write(&send->write, c->stream, send->bufs, send->n_bufs,
		      uvSendWriteCb);
	if (rv != 0) {
		tracef("write message failed -> rv %d", rv);
		/* UNTESTED: what are the error conditions? perhaps ENOMEM */
		return RAFT_IOERR;
	}

	return 0;
}

/* Try to execute all send requests that were blocked in the queue waiting for a
 * connection. */
static void uvClientSendPending(struct uvClient *c)
{
	int rv;
	assert(c->stream != NULL);
	tracef("send pending messages");
	while (!queue_empty(&c->pending)) {
		queue *head;
		struct uvSend *send;
		head = queue_head(&c->pending);
		send = QUEUE_DATA(head, struct uvSend, queue);
		queue_remove(head);
		rv = uvClientSend(c, send);
		if (rv != 0) {
			if (send->req->cb != NULL) {
				send->req->cb(send->req, rv);
			}
			uvSendDestroy(send);
		}
	}
}

static void uvClientTimerCb(uv_timer_t *timer)
{
	struct uvClient *c = timer->data;
	tracef("timer expired -> attempt to reconnect");
	uvClientConnect(c); /* Retry to connect. */
}

/* Return the number of send requests that we have been parked in the send queue
 * because no connection is available yet. */
static unsigned uvClientPendingCount(struct uvClient *c)
{
	queue *head;
	unsigned n = 0;
	QUEUE_FOREACH(head, &c->pending)
	{
		n++;
	}
	return n;
}

static void uvClientConnectCb(struct raft_uv_connect *req,
			      struct uv_stream_s *stream,
			      int status)
{
	struct uvClient *c = req->data;
	unsigned n_pending;
	int rv;

	tracef("connect attempt completed -> status %s",
	       errCodeToString(status));

	assert(c->connect.data != NULL);
	assert(c->stream == NULL);
	assert(c->old_stream == NULL);
	assert(!uv_is_active((struct uv_handle_s *)&c->timer));

	c->connect.data = NULL;

	/* If we are closing, bail out, possibly discarding the new connection.
	 */
	if (c->closing) {
		if (status == 0) {
			assert(stream != NULL);
			c->stream = stream;
			c->stream->data = c;
			uvClientDisconnect(c);
		} else {
			uvClientMaybeDestroy(c);
		}
		return;
	}

	/* If, the connection attempt was successful, we're good. If we have
	 * pending requests, let's try to execute them. */
	if (status == 0) {
		assert(stream != NULL);
		c->stream = stream;
		c->n_connect_attempt = 0;
		c->stream->data = c;
		uvClientSendPending(c);
		return;
	}

	/* Shrink the queue of pending requests, by failing the oldest ones */
	n_pending = uvClientPendingCount(c);
	if (n_pending > UV__CLIENT_MAX_PENDING) {
		unsigned i;
		for (i = 0; i < n_pending - UV__CLIENT_MAX_PENDING; i++) {
			tracef("queue full -> evict oldest message");
			queue *head;
			struct uvSend *old_send;
			struct raft_io_send *old_req;
			head = queue_head(&c->pending);
			old_send = QUEUE_DATA(head, struct uvSend, queue);
			queue_remove(head);
			old_req = old_send->req;
			uvSendDestroy(old_send);
			if (old_req->cb != NULL) {
				old_req->cb(old_req, RAFT_NOCONNECTION);
			}
		}
	}

	/* Let's schedule another attempt. */
	rv = uv_timer_start(&c->timer, uvClientTimerCb,
			    c->uv->connect_retry_delay, 0);
	assert(rv == 0);
}

/* Perform a single connection attempt, scheduling a retry if it fails. */
static void uvClientConnect(struct uvClient *c)
{
	int rv;

	assert(!c->closing);
	assert(c->stream == NULL);
	assert(c->old_stream == NULL);
	assert(!uv_is_active((struct uv_handle_s *)&c->timer));
	assert(c->connect.data == NULL);

	c->n_connect_attempt++;

	c->connect.data = c;
	rv = c->uv->transport->connect(c->uv->transport, &c->connect, c->id,
				       c->address, uvClientConnectCb);
	if (rv != 0) {
		/* Restart the timer, so we can retry. */
		c->connect.data = NULL;
		rv = uv_timer_start(&c->timer, uvClientTimerCb,
				    c->uv->connect_retry_delay, 0);
		assert(rv == 0);
	}
}

/* Final callback in the close chain of an io_uv__client object */
static void uvClientTimerCloseCb(struct uv_handle_s *handle)
{
	struct uvClient *c = handle->data;
	assert(handle == (struct uv_handle_s *)&c->timer);
	c->timer.data = NULL;
	uvClientMaybeDestroy(c);
}

/* Start shutting down a client. This happens when the `raft_io` instance
 * has been closed or when the address of the client has changed. */
static void uvClientAbort(struct uvClient *c)
{
	struct uv *uv = c->uv;
	int rv;

	assert(c->stream != NULL || c->old_stream != NULL ||
	       uv_is_active((struct uv_handle_s *)&c->timer) ||
	       c->connect.data != NULL);

	queue_remove(&c->queue);
	queue_insert_tail(&uv->aborting, &c->queue);

	rv = uv_timer_stop(&c->timer);
	assert(rv == 0);

	/* If we are connected, let's close the outbound stream handle. This
	 * will eventually complete all inflight write requests, possibly with
	 * failing them with UV_ECANCELED. */
	if (c->stream != NULL) {
		uvClientDisconnect(c);
	}

	/* Closing the timer implicitly stop it, so the timeout callback won't
	 * be fired. */
	uv_close((struct uv_handle_s *)&c->timer, uvClientTimerCloseCb);
	c->closing = true;
}

/* Find the client object associated with the given server, or create one if
 * there's none yet. */
static int uvGetClient(struct uv *uv,
		       const raft_id id,
		       const char *address,
		       struct uvClient **client)
{
	queue *head;
	int rv;

	/* Check if we already have a client object for this peer server. */
	QUEUE_FOREACH(head, &uv->clients)
	{
		*client = QUEUE_DATA(head, struct uvClient, queue);
		if ((*client)->id != id) {
			continue;
		}

		/* Client address has changed, abort connection and create a new
		 * one. */
		if (strcmp((*client)->address, address) != 0) {
			uvClientAbort(*client);
			break;
		}

		return 0;
	}

	/* Initialize the new connection */
	*client = RaftHeapMalloc(sizeof **client);
	if (*client == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}

	rv = uvClientInit(*client, uv, id, address);
	if (rv != 0) {
		goto err_after_client_alloc;
	}

	/* Make a first connection attempt right away.. */
	uvClientConnect(*client);

	return 0;

err_after_client_alloc:
	RaftHeapFree(*client);
err:
	assert(rv != 0);
	return rv;
}

int UvSend(struct raft_io *io,
	   struct raft_io_send *req,
	   const struct raft_message *message,
	   raft_io_send_cb cb)
{
	struct uv *uv = io->impl;
	struct uvSend *send;
	struct uvClient *client;
	int rv;

	assert(!uv->closing);

	/* Allocate a new request object. */
	send = RaftHeapMalloc(sizeof *send);
	if (send == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}
	send->req = req;
	req->cb = cb;

	rv = uvEncodeMessage(message, &send->bufs, &send->n_bufs);
	if (rv != 0) {
		send->bufs = NULL;
		goto err_after_send_alloc;
	}

	/* Get a client object connected to the target server, creating it if it
	 * doesn't exist yet. */
	rv = uvGetClient(uv, message->server_id, message->server_address,
			 &client);
	if (rv != 0) {
		goto err_after_send_alloc;
	}

	rv = uvClientSend(client, send);
	if (rv != 0) {
		goto err_after_send_alloc;
	}

	return 0;

err_after_send_alloc:
	uvSendDestroy(send);
err:
	assert(rv != 0);
	return rv;
}

void UvSendClose(struct uv *uv)
{
	assert(uv->closing);
	while (!queue_empty(&uv->clients)) {
		queue *head;
		struct uvClient *client;
		head = queue_head(&uv->clients);
		client = QUEUE_DATA(head, struct uvClient, queue);
		uvClientAbort(client);
	}
}

