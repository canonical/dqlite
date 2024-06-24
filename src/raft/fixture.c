#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../raft.h"
#include "../tracing.h"
#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "entry.h"
#include "log.h"
#include "../lib/queue.h"
#include "snapshot.h"

/* Defaults */
#define HEARTBEAT_TIMEOUT 100
#define INSTALL_SNAPSHOT_TIMEOUT 30000
#define ELECTION_TIMEOUT 1000
#define NETWORK_LATENCY 15
#define DISK_LATENCY 10
#define WORK_DURATION 200
#define SEND_LATENCY 0

/* To keep in sync with raft.h */
#define N_MESSAGE_TYPES 6

/* Maximum number of peer stub instances connected to a certain stub
 * instance. This should be enough for testing purposes. */
#define MAX_PEERS 8

struct raft_fixture_server
{
	bool alive;                /* If false, the server is down. */
	raft_id id;                /* Server ID. */
	char address[16];          /* Server address (stringified ID). */
	struct raft_tracer tracer; /* Tracer. */
	struct raft_io io;         /* In-memory raft_io implementation. */
	struct raft raft;          /* Raft instance. */
};

struct raft_fixture_event
{
	unsigned server_index; /* Index of the server the event occurred on. */
	int type;              /* Type of the event. */
};

RAFT_API int raft_fixture_event_type(struct raft_fixture_event *event)
{
	assert(event != NULL);
	return event->type;
}

RAFT_API unsigned raft_fixture_event_server_index(
    struct raft_fixture_event *event)
{
	assert(event != NULL);
	return event->server_index;
}

/* Fields common across all request types. */
#define REQUEST                                                                \
	int type;                  /* Request code type. */                    \
	raft_time completion_time; /* When the request should be fulfilled. */ \
	queue queue                /* Link the I/O pending requests queue. */

/* Request type codes. */
enum { APPEND = 1, SEND, TRANSMIT, SNAPSHOT_PUT, SNAPSHOT_GET, ASYNC_WORK };

/* Abstract base type for an asynchronous request submitted to the stub I/o
 * implementation. */
struct ioRequest
{
	REQUEST;
};

/* Pending request to append entries to the log. */
struct append
{
	REQUEST;
	struct raft_io_append *req;
	const struct raft_entry *entries;
	unsigned n;
	unsigned start; /* Request timestamp. */
};

/* Pending request to send a message. */
struct send
{
	REQUEST;
	struct raft_io_send *req;
	struct raft_message message;
};

/* Pending request to store a snapshot. */
struct snapshot_put
{
	REQUEST;
	unsigned trailing;
	struct raft_io_snapshot_put *req;
	const struct raft_snapshot *snapshot;
};

/* Pending request to perform general work. */
struct async_work
{
	REQUEST;
	struct raft_io_async_work *req;
};

/* Pending request to load a snapshot. */
struct snapshot_get
{
	REQUEST;
	struct raft_io_snapshot_get *req;
};

/* Message that has been written to the network and is waiting to be delivered
 * (or discarded). */
struct transmit
{
	REQUEST;
	struct raft_message message; /* Message to deliver */
	int timer;                   /* Deliver after this n of msecs. */
};

/* Information about a peer server. */
struct peer
{
	struct io *io;  /* The peer's I/O backend. */
	bool connected; /* Whether a connection is established. */
	bool saturated; /* Whether the established connection is saturated. */
	unsigned send_latency;
};

/* Stub I/O implementation implementing all operations in-memory. */
struct io
{
	struct raft_io *io;  /* I/O object we're implementing. */
	unsigned index;      /* Fixture server index. */
	raft_time *time;     /* Global cluster time. */
	raft_time next_tick; /* Time the next tick should occurs. */

	/* Term and vote */
	raft_term term;
	raft_id voted_for;

	/* Log */
	struct raft_snapshot *snapshot; /* Latest snapshot */
	struct raft_entry *entries;     /* Array or persisted entries */
	size_t n; /* Size of the persisted entries array */

	/* Parameters passed via raft_io->init and raft_io->start */
	raft_id id;
	const char *address;
	unsigned tick_interval;
	raft_io_tick_cb tick_cb;
	raft_io_recv_cb recv_cb;

	/* Queue of pending asynchronous requests, whose callbacks still haven't
	 * been fired. */
	queue requests;

	/* Peers connected to us. */
	struct peer peers[MAX_PEERS];
	unsigned n_peers;

	unsigned
	    randomized_election_timeout; /* Value returned by io->random() */
	unsigned network_latency;        /* Milliseconds to deliver RPCs */
	unsigned disk_latency;           /* Milliseconds to perform disk I/O */
	unsigned work_duration;          /* Milliseconds to run async work */

	int append_fault_countdown;
	int vote_fault_countdown;
	int term_fault_countdown;
	int send_fault_countdown;

	/* If flag i is true, messages of type i will be silently dropped. */
	bool drop[N_MESSAGE_TYPES];

	/* Counters of events that happened so far. */
	unsigned n_send[N_MESSAGE_TYPES];
	unsigned n_recv[N_MESSAGE_TYPES];
	unsigned n_append;
};

static bool faultTick(int *countdown)
{
	bool trigger = *countdown == 0;
	if (*countdown >= 0) {
		*countdown -= 1;
	}
	return trigger;
}

static int ioMethodInit(struct raft_io *raft_io,
			raft_id id,
			const char *address)
{
	struct io *io = raft_io->impl;
	io->id = id;
	io->address = address;
	return 0;
}

static int ioMethodStart(struct raft_io *raft_io,
			 unsigned msecs,
			 raft_io_tick_cb tick_cb,
			 raft_io_recv_cb recv_cb)
{
	struct io *io = raft_io->impl;
	io->tick_interval = msecs;
	io->tick_cb = tick_cb;
	io->recv_cb = recv_cb;
	io->next_tick = *io->time + io->tick_interval;
	return 0;
}

/* Flush an append entries request, appending its entries to the local in-memory
 * log. */
static void ioFlushAppend(struct io *s, struct append *append)
{
	struct raft_entry *entries;
	unsigned i;
	int status = 0;

	/* Simulates a disk write failure. */
	if (faultTick(&s->append_fault_countdown)) {
		status = RAFT_IOERR;
		goto done;
	}

	/* Allocate an array for the old entries plus the new ones. */
	entries =
	    raft_realloc(s->entries, (s->n + append->n) * sizeof *s->entries);
	assert(entries != NULL);

	/* Copy new entries into the new array. */
	for (i = 0; i < append->n; i++) {
		const struct raft_entry *src = &append->entries[i];
		struct raft_entry *dst = &entries[s->n + i];
		int rv = entryCopy(src, dst);
		assert(rv == 0);
	}

	s->entries = entries;
	s->n += append->n;

done:
	if (append->req->cb != NULL) {
		append->req->cb(append->req, status);
	}
	raft_free(append);
}

/* Flush a snapshot put request, copying the snapshot data. */
static void ioFlushSnapshotPut(struct io *s, struct snapshot_put *r)
{
	int rv;

	if (s->snapshot == NULL) {
		s->snapshot = raft_malloc(sizeof *s->snapshot);
		assert(s->snapshot != NULL);
	} else {
		snapshotClose(s->snapshot);
	}

	rv = snapshotCopy(r->snapshot, s->snapshot);
	assert(rv == 0);

	if (r->trailing == 0) {
		rv = s->io->truncate(s->io, 1);
		assert(rv == 0);
	}

	if (r->req->cb != NULL) {
		r->req->cb(r->req, 0);
	}
	raft_free(r);
}

/* Flush a snapshot get request, returning to the client a copy of the local
 * snapshot (if any). */
static void ioFlushSnapshotGet(struct io *s, struct snapshot_get *r)
{
	struct raft_snapshot *snapshot;
	int rv;
	snapshot = raft_malloc(sizeof *snapshot);
	assert(snapshot != NULL);
	rv = snapshotCopy(s->snapshot, snapshot);
	assert(rv == 0);
	r->req->cb(r->req, snapshot, 0);
	raft_free(r);
}

/* Flush an async work request */
static void ioFlushAsyncWork(struct io *s, struct async_work *r)
{
	(void)s;
	int rv;
	rv = r->req->work(r->req);
	r->req->cb(r->req, rv);
	raft_free(r);
}

/* Search for the peer with the given ID. */
static struct peer *ioGetPeer(struct io *io, raft_id id)
{
	unsigned i;
	for (i = 0; i < io->n_peers; i++) {
		struct peer *peer = &io->peers[i];
		if (peer->io->id == id) {
			return peer;
		}
	}
	return NULL;
}

/* Copy the dynamically allocated memory of an AppendEntries message. */
static void copyAppendEntries(const struct raft_append_entries *src,
			      struct raft_append_entries *dst)
{
	int rv;
	rv = entryBatchCopy(src->entries, &dst->entries, src->n_entries);
	assert(rv == 0);
	dst->n_entries = src->n_entries;
}

/* Copy the dynamically allocated memory of an InstallSnapshot message. */
static void copyInstallSnapshot(const struct raft_install_snapshot *src,
				struct raft_install_snapshot *dst)
{
	int rv;
	rv = configurationCopy(&src->conf, &dst->conf);
	assert(rv == 0);
	dst->data.base = raft_malloc(dst->data.len);
	assert(dst->data.base != NULL);
	memcpy(dst->data.base, src->data.base, src->data.len);
}

/* Flush a raft_io_send request, copying the message content into a new struct
 * transmit object and invoking the user callback. */
static void ioFlushSend(struct io *io, struct send *send)
{
	struct peer *peer;
	struct transmit *transmit;
	struct raft_message *src;
	struct raft_message *dst;
	int status;

	/* If the peer doesn't exist or was disconnected, fail the request. */
	peer = ioGetPeer(io, send->message.server_id);
	if (peer == NULL || !peer->connected) {
		status = RAFT_NOCONNECTION;
		goto out;
	}

	transmit = raft_calloc(1, sizeof *transmit);
	assert(transmit != NULL);

	transmit->type = TRANSMIT;
	transmit->completion_time = *io->time + io->network_latency;

	src = &send->message;
	dst = &transmit->message;

	queue_insert_tail(&io->requests, &transmit->queue);

	*dst = *src;
	switch (dst->type) {
		case RAFT_IO_APPEND_ENTRIES:
			/* Make a copy of the entries being sent */
			copyAppendEntries(&src->append_entries,
					  &dst->append_entries);
			break;
		case RAFT_IO_INSTALL_SNAPSHOT:
			copyInstallSnapshot(&src->install_snapshot,
					    &dst->install_snapshot);
			break;
	}

	io->n_send[send->message.type]++;
	status = 0;

out:
	if (send->req->cb != NULL) {
		send->req->cb(send->req, status);
	}

	raft_free(send);
}

/* Release the memory used by the given message transmit object. */
static void ioDestroyTransmit(struct transmit *transmit)
{
	struct raft_message *message;
	message = &transmit->message;
	switch (message->type) {
		case RAFT_IO_APPEND_ENTRIES:
			if (message->append_entries.entries != NULL) {
				raft_free(
				    message->append_entries.entries[0].batch);
				raft_free(message->append_entries.entries);
			}
			break;
		case RAFT_IO_INSTALL_SNAPSHOT:
			raft_configuration_close(
			    &message->install_snapshot.conf);
			raft_free(message->install_snapshot.data.base);
			break;
	}
	raft_free(transmit);
}

/* Flush all requests in the queue. */
static void ioFlushAll(struct io *io)
{
	while (!queue_empty(&io->requests)) {
		queue *head;
		struct ioRequest *r;

		head = queue_head(&io->requests);
		queue_remove(head);

		r = QUEUE_DATA(head, struct ioRequest, queue);
		switch (r->type) {
			case APPEND:
				ioFlushAppend(io, (struct append *)r);
				break;
			case SEND:
				ioFlushSend(io, (struct send *)r);
				break;
			case TRANSMIT:
				ioDestroyTransmit((struct transmit *)r);
				break;
			case SNAPSHOT_PUT:
				ioFlushSnapshotPut(io,
						   (struct snapshot_put *)r);
				break;
			case SNAPSHOT_GET:
				ioFlushSnapshotGet(io,
						   (struct snapshot_get *)r);
				break;
			case ASYNC_WORK:
				ioFlushAsyncWork(io, (struct async_work *)r);
				break;
			default:
				assert(0);
		}
	}
}

static void ioMethodClose(struct raft_io *raft_io, raft_io_close_cb cb)
{
	if (cb != NULL) {
		cb(raft_io);
	}
}

static int ioMethodLoad(struct raft_io *io,
			raft_term *term,
			raft_id *voted_for,
			struct raft_snapshot **snapshot,
			raft_index *start_index,
			struct raft_entry **entries,
			size_t *n_entries)
{
	struct io *s;
	int rv;

	s = io->impl;

	*term = s->term;
	*voted_for = s->voted_for;
	*start_index = 1;

	*n_entries = s->n;

	/* Make a copy of the persisted entries, storing their data into a
	 * single batch. */
	rv = entryBatchCopy(s->entries, entries, s->n);
	assert(rv == 0);

	if (s->snapshot != NULL) {
		*snapshot = raft_malloc(sizeof **snapshot);
		assert(*snapshot != NULL);
		rv = snapshotCopy(s->snapshot, *snapshot);
		assert(rv == 0);
		*start_index = (*snapshot)->index + 1;
	} else {
		*snapshot = NULL;
	}

	return 0;
}

static int ioMethodBootstrap(struct raft_io *raft_io,
			     const struct raft_configuration *conf)
{
	struct io *io = raft_io->impl;
	struct raft_buffer buf;
	struct raft_entry *entries;
	int rv;

	if (io->term != 0) {
		return RAFT_CANTBOOTSTRAP;
	}

	assert(io->voted_for == 0);
	assert(io->snapshot == NULL);
	assert(io->entries == NULL);
	assert(io->n == 0);

	/* Encode the given configuration. */
	rv = configurationEncode(conf, &buf);
	if (rv != 0) {
		return rv;
	}

	entries = raft_calloc(1, sizeof *io->entries);
	if (entries == NULL) {
		return RAFT_NOMEM;
	}

	entries[0].term = 1;
	entries[0].type = RAFT_CHANGE;
	entries[0].buf = buf;

	io->term = 1;
	io->voted_for = 0;
	io->snapshot = NULL;
	io->entries = entries;
	io->n = 1;

	return 0;
}

static int ioMethodRecover(struct raft_io *io,
			   const struct raft_configuration *conf)
{
	/* TODO: implement this API */
	(void)io;
	(void)conf;
	return RAFT_IOERR;
}

static int ioMethodSetTerm(struct raft_io *raft_io, const raft_term term)
{
	struct io *io = raft_io->impl;

	if (faultTick(&io->term_fault_countdown)) {
		return RAFT_IOERR;
	}

	io->term = term;
	io->voted_for = 0;

	return 0;
}

static int ioMethodSetVote(struct raft_io *raft_io, const raft_id server_id)
{
	struct io *io = raft_io->impl;

	if (faultTick(&io->vote_fault_countdown)) {
		return RAFT_IOERR;
	}

	io->voted_for = server_id;

	return 0;
}

static int ioMethodAppend(struct raft_io *raft_io,
			  struct raft_io_append *req,
			  const struct raft_entry entries[],
			  unsigned n,
			  raft_io_append_cb cb)
{
	struct io *io = raft_io->impl;
	struct append *r;

	r = raft_malloc(sizeof *r);
	assert(r != NULL);

	r->type = APPEND;
	r->completion_time = *io->time + io->disk_latency;
	r->req = req;
	r->entries = entries;
	r->n = n;

	req->cb = cb;

	queue_insert_tail(&io->requests, &r->queue);

	return 0;
}

static int ioMethodTruncate(struct raft_io *raft_io, raft_index index)
{
	struct io *io = raft_io->impl;
	size_t n;

	n = (size_t)(index - 1); /* Number of entries left after truncation */

	if (n > 0) {
		struct raft_entry *entries;

		/* Create a new array of entries holding the non-truncated
		 * entries */
		entries = raft_malloc(n * sizeof *entries);
		if (entries == NULL) {
			return RAFT_NOMEM;
		}
		memcpy(entries, io->entries, n * sizeof *io->entries);

		/* Release any truncated entry */
		if (io->entries != NULL) {
			size_t i;
			for (i = n; i < io->n; i++) {
				raft_free(io->entries[i].buf.base);
			}
			raft_free(io->entries);
		}
		io->entries = entries;
	} else {
		/* Release everything we have */
		if (io->entries != NULL) {
			size_t i;
			for (i = 0; i < io->n; i++) {
				raft_free(io->entries[i].buf.base);
			}
			raft_free(io->entries);
			io->entries = NULL;
		}
	}

	io->n = n;

	return 0;
}

static int ioMethodSnapshotPut(struct raft_io *raft_io,
			       unsigned trailing,
			       struct raft_io_snapshot_put *req,
			       const struct raft_snapshot *snapshot,
			       raft_io_snapshot_put_cb cb)
{
	struct io *io = raft_io->impl;
	struct snapshot_put *r;

	r = raft_malloc(sizeof *r);
	assert(r != NULL);

	r->type = SNAPSHOT_PUT;
	r->req = req;
	r->req->cb = cb;
	r->snapshot = snapshot;
	r->completion_time = *io->time + io->disk_latency;
	r->trailing = trailing;

	queue_insert_tail(&io->requests, &r->queue);

	return 0;
}

static int ioMethodAsyncWork(struct raft_io *raft_io,
			     struct raft_io_async_work *req,
			     raft_io_async_work_cb cb)
{
	struct io *io = raft_io->impl;
	struct async_work *r;

	r = raft_malloc(sizeof *r);
	assert(r != NULL);

	r->type = ASYNC_WORK;
	r->req = req;
	r->req->cb = cb;
	r->completion_time = *io->time + io->work_duration;

	queue_insert_tail(&io->requests, &r->queue);
	return 0;
}

static int ioMethodSnapshotGet(struct raft_io *raft_io,
			       struct raft_io_snapshot_get *req,
			       raft_io_snapshot_get_cb cb)
{
	struct io *io = raft_io->impl;
	struct snapshot_get *r;

	r = raft_malloc(sizeof *r);
	assert(r != NULL);

	r->type = SNAPSHOT_GET;
	r->req = req;
	r->req->cb = cb;
	r->completion_time = *io->time + io->disk_latency;

	queue_insert_tail(&io->requests, &r->queue);

	return 0;
}

static raft_time ioMethodTime(struct raft_io *raft_io)
{
	struct io *io = raft_io->impl;
	return *io->time;
}

static int ioMethodRandom(struct raft_io *raft_io, int min, int max)
{
	struct io *io = raft_io->impl;
	int t = (int)io->randomized_election_timeout;
	if (t < min) {
		return min;
	} else if (t > max) {
		return max;
	} else {
		return t;
	}
}

/* Queue up a request which will be processed later, when io_stub_flush()
 * is invoked. */
static int ioMethodSend(struct raft_io *raft_io,
			struct raft_io_send *req,
			const struct raft_message *message,
			raft_io_send_cb cb)
{
	struct io *io = raft_io->impl;
	struct send *r;
	struct peer *peer;

	if (faultTick(&io->send_fault_countdown)) {
		return RAFT_IOERR;
	}

	r = raft_malloc(sizeof *r);
	assert(r != NULL);

	r->type = SEND;
	r->req = req;
	r->message = *message;
	r->req->cb = cb;

	peer = ioGetPeer(io, message->server_id);
	r->completion_time = *io->time + peer->send_latency;

	queue_insert_tail(&io->requests, &r->queue);

	return 0;
}

static void ioReceive(struct io *io, struct raft_message *message)
{
	io->recv_cb(io->io, message);
	io->n_recv[message->type]++;
}

static void ioDeliverTransmit(struct io *io, struct transmit *transmit)
{
	struct raft_message *message = &transmit->message;
	struct peer *peer; /* Destination peer */

	/* If this message type is in the drop list, let's discard it */
	if (io->drop[message->type - 1]) {
		ioDestroyTransmit(transmit);
		return;
	}

	peer = ioGetPeer(io, message->server_id);

	/* We don't have any peer with this ID or it's disconnected or if the
	 * connection is saturated, let's drop the message */
	if (peer == NULL || !peer->connected || peer->saturated) {
		ioDestroyTransmit(transmit);
		return;
	}

	/* Update the message object with our details. */
	message->server_id = io->id;
	message->server_address = io->address;

	ioReceive(peer->io, message);
	raft_free(transmit);
}

/* Connect @raft_io to @other, enabling delivery of messages sent from @io to
 * @other.
 */
static void ioConnect(struct raft_io *raft_io, struct raft_io *other)
{
	struct io *io = raft_io->impl;
	struct io *io_other = other->impl;
	assert(io->n_peers < MAX_PEERS);
	io->peers[io->n_peers].io = io_other;
	io->peers[io->n_peers].connected = true;
	io->peers[io->n_peers].saturated = false;
	io->peers[io->n_peers].send_latency = SEND_LATENCY;
	io->n_peers++;
}

/* Return whether the connection with the given peer is saturated. */
static bool ioSaturated(struct raft_io *raft_io, struct raft_io *other)
{
	struct io *io = raft_io->impl;
	struct io *io_other = other->impl;
	struct peer *peer;
	peer = ioGetPeer(io, io_other->id);
	return peer != NULL && peer->saturated;
}

/* Disconnect @raft_io and @other, causing calls to @io->send() to fail
 * asynchronously when sending messages to @other. */
static void ioDisconnect(struct raft_io *raft_io, struct raft_io *other)
{
	struct io *io = raft_io->impl;
	struct io *io_other = other->impl;
	struct peer *peer;
	peer = ioGetPeer(io, io_other->id);
	assert(peer != NULL);
	peer->connected = false;
}

/* Reconnect @raft_io and @other. */
static void ioReconnect(struct raft_io *raft_io, struct raft_io *other)
{
	struct io *io = raft_io->impl;
	struct io *io_other = other->impl;
	struct peer *peer;
	peer = ioGetPeer(io, io_other->id);
	assert(peer != NULL);
	peer->connected = true;
}

/* Saturate the connection from @io to @other, causing messages sent from @io to
 * @other to be dropped. */
static void ioSaturate(struct raft_io *io, struct raft_io *other)
{
	struct io *s;
	struct io *s_other;
	struct peer *peer;
	s = io->impl;
	s_other = other->impl;
	peer = ioGetPeer(s, s_other->id);
	assert(peer != NULL && peer->connected);
	peer->saturated = true;
}

/* Desaturate the connection from @raft_io to @other, re-enabling delivery of
 * messages sent from @raft_io to @other. */
static void ioDesaturate(struct raft_io *raft_io, struct raft_io *other)
{
	struct io *io = raft_io->impl;
	struct io *io_other = other->impl;
	struct peer *peer;
	peer = ioGetPeer(io, io_other->id);
	assert(peer != NULL && peer->connected);
	peer->saturated = false;
}

/* Enable or disable silently dropping all outgoing messages of type @type. */
void ioDrop(struct io *io, int type, bool flag)
{
	io->drop[type - 1] = flag;
}

static int ioInit(struct raft_io *raft_io, unsigned index, raft_time *time)
{
	struct io *io;
	io = raft_malloc(sizeof *io);
	assert(io != NULL);
	io->io = raft_io;
	io->index = index;
	io->time = time;
	io->term = 0;
	io->voted_for = 0;
	io->snapshot = NULL;
	io->entries = NULL;
	io->n = 0;
	queue_init(&io->requests);
	io->n_peers = 0;
	io->randomized_election_timeout = ELECTION_TIMEOUT + index * 100;
	io->network_latency = NETWORK_LATENCY;
	io->disk_latency = DISK_LATENCY;
	io->work_duration = WORK_DURATION;
	io->append_fault_countdown = -1;
	io->vote_fault_countdown = -1;
	io->term_fault_countdown = -1;
	io->send_fault_countdown = -1;
	memset(io->drop, 0, sizeof io->drop);
	memset(io->n_send, 0, sizeof io->n_send);
	memset(io->n_recv, 0, sizeof io->n_recv);
	io->n_append = 0;

	raft_io->impl = io;
	raft_io->version = 2;
	raft_io->init = ioMethodInit;
	raft_io->close = ioMethodClose;
	raft_io->start = ioMethodStart;
	raft_io->load = ioMethodLoad;
	raft_io->bootstrap = ioMethodBootstrap;
	raft_io->recover = ioMethodRecover;
	raft_io->set_term = ioMethodSetTerm;
	raft_io->set_vote = ioMethodSetVote;
	raft_io->append = ioMethodAppend;
	raft_io->truncate = ioMethodTruncate;
	raft_io->send = ioMethodSend;
	raft_io->snapshot_put = ioMethodSnapshotPut;
	raft_io->async_work = ioMethodAsyncWork;
	raft_io->snapshot_get = ioMethodSnapshotGet;
	raft_io->time = ioMethodTime;
	raft_io->random = ioMethodRandom;

	return 0;
}

/* Release all memory held by the given stub I/O implementation. */
void ioClose(struct raft_io *raft_io)
{
	struct io *io = raft_io->impl;
	size_t i;
	for (i = 0; i < io->n; i++) {
		struct raft_entry *entry = &io->entries[i];
		raft_free(entry->buf.base);
	}
	if (io->entries != NULL) {
		raft_free(io->entries);
	}
	if (io->snapshot != NULL) {
		snapshotClose(io->snapshot);
		raft_free(io->snapshot);
	}
	raft_free(io);
}

/* Custom emit tracer function which include the server ID. */
static void emit(struct raft_tracer *t,
		 const char *file,
		 unsigned int line,
		 const char *func,
		 unsigned int level,
		 const char *message)
{
	unsigned id = *(unsigned *)t->impl;
	(void)func;
	(void)level;
	fprintf(stderr, "%d: %30s:%*d - %s\n", id, file, 3, line, message);
}

static int serverInit(struct raft_fixture *f, unsigned i, struct raft_fsm *fsm)
{
	int rv;
	struct raft_fixture_server *s;
	s = raft_malloc(sizeof(*s));
	if (s == NULL) {
		return RAFT_NOMEM;
	}
	f->servers[i] = s;
	s->alive = true;
	s->id = i + 1;
	sprintf(s->address, "%llu", s->id);
	rv = ioInit(&s->io, i, &f->time);
	if (rv != 0) {
		return rv;
	}
	rv = raft_init(&s->raft, &s->io, fsm, s->id, s->address);
	if (rv != 0) {
		return rv;
	}
	raft_set_election_timeout(&s->raft, ELECTION_TIMEOUT);
	raft_set_heartbeat_timeout(&s->raft, HEARTBEAT_TIMEOUT);
	raft_set_install_snapshot_timeout(&s->raft, INSTALL_SNAPSHOT_TIMEOUT);
	s->tracer.impl = (void *)&s->id;
	s->tracer.emit = emit;
	s->raft.tracer = NULL;
	return 0;
}

static void serverClose(struct raft_fixture_server *s)
{
	raft_close(&s->raft, NULL);
	ioClose(&s->io);
	raft_fini(&s->raft);
	raft_free(s);
}

/* Connect the server with the given index to all others */
static void serverConnectToAll(struct raft_fixture *f, unsigned i)
{
	unsigned j;
	for (j = 0; j < f->n; j++) {
		struct raft_io *io1 = &f->servers[i]->io;
		struct raft_io *io2 = &f->servers[j]->io;
		if (i == j) {
			continue;
		}
		ioConnect(io1, io2);
	}
}

int raft_fixture_init(struct raft_fixture *f)
{
	f->time = 0;
	f->n = 0;
	f->log = logInit();
	if (f->log == NULL) {
		return RAFT_NOMEM;
	}
	f->commit_index = 0;
	f->hook = NULL;
	f->event = raft_malloc(sizeof(*f->event));
	if (f->event == NULL) {
		return RAFT_NOMEM;
	}
	return 0;
}

void raft_fixture_close(struct raft_fixture *f)
{
	unsigned i;
	for (i = 0; i < f->n; i++) {
		struct io *io = f->servers[i]->io.impl;
		ioFlushAll(io);
	}
	for (i = 0; i < f->n; i++) {
		serverClose(f->servers[i]);
	}
	raft_free(f->event);
	logClose(f->log);
}

int raft_fixture_configuration(struct raft_fixture *f,
			       unsigned n_voting,
			       struct raft_configuration *configuration)
{
	unsigned i;
	assert(f->n > 0);
	assert(n_voting > 0);
	assert(n_voting <= f->n);
	raft_configuration_init(configuration);
	for (i = 0; i < f->n; i++) {
		struct raft_fixture_server *s;
		int role = i < n_voting ? RAFT_VOTER : RAFT_STANDBY;
		int rv;
		s = f->servers[i];
		rv = raft_configuration_add(configuration, s->id, s->address,
					    role);
		if (rv != 0) {
			return rv;
		}
	}
	return 0;
}

int raft_fixture_bootstrap(struct raft_fixture *f,
			   struct raft_configuration *configuration)
{
	unsigned i;
	for (i = 0; i < f->n; i++) {
		struct raft *raft = raft_fixture_get(f, i);
		int rv;
		rv = raft_bootstrap(raft, configuration);
		if (rv != 0) {
			return rv;
		}
	}
	return 0;
}

int raft_fixture_start(struct raft_fixture *f)
{
	unsigned i;
	int rv;
	for (i = 0; i < f->n; i++) {
		struct raft_fixture_server *s = f->servers[i];
		rv = raft_start(&s->raft);
		if (rv != 0) {
			return rv;
		}
	}
	return 0;
}

unsigned raft_fixture_n(struct raft_fixture *f)
{
	return f->n;
}

raft_time raft_fixture_time(struct raft_fixture *f)
{
	return f->time;
}

struct raft *raft_fixture_get(struct raft_fixture *f, unsigned i)
{
	assert(i < f->n);
	return &f->servers[i]->raft;
}

bool raft_fixture_alive(struct raft_fixture *f, unsigned i)
{
	assert(i < f->n);
	return f->servers[i]->alive;
}

unsigned raft_fixture_leader_index(struct raft_fixture *f)
{
	if (f->leader_id != 0) {
		return (unsigned)(f->leader_id - 1);
	}
	return f->n;
}

raft_id raft_fixture_voted_for(struct raft_fixture *f, unsigned i)
{
	struct io *io = f->servers[i]->io.impl;
	return io->voted_for;
}

/* Update the leader and check for election safety.
 *
 * From figure 3.2:
 *
 *   Election Safety -> At most one leader can be elected in a given
 *   term.
 *
 * Return true if the current leader turns out to be different from the one at
 * the time this function was called.
 */
static bool updateLeaderAndCheckElectionSafety(struct raft_fixture *f)
{
	raft_id leader_id = 0;
	unsigned leader_i = 0;
	raft_term leader_term = 0;
	unsigned i;
	bool changed;

	for (i = 0; i < f->n; i++) {
		struct raft *raft = raft_fixture_get(f, i);
		unsigned j;

		/* If the server is not alive or is not the leader, skip to the
		 * next server. */
		if (!raft_fixture_alive(f, i) ||
		    raft_state(raft) != RAFT_LEADER) {
			continue;
		}

		/* Check that no other server is leader for this term. */
		for (j = 0; j < f->n; j++) {
			struct raft *other = raft_fixture_get(f, j);

			if (other->id == raft->id ||
			    other->state != RAFT_LEADER) {
				continue;
			}

			if (other->current_term == raft->current_term) {
				fprintf(stderr,
					"server %llu and %llu are both leaders "
					"in term %llu",
					raft->id, other->id,
					raft->current_term);
				abort();
			}
		}

		if (raft->current_term > leader_term) {
			leader_id = raft->id;
			leader_i = i;
			leader_term = raft->current_term;
		}
	}

	/* Check that the leader is stable, in the sense that it has been
	 * acknowledged by all alive servers connected to it, and those servers
	 * together with the leader form a majority. */
	if (leader_id != 0) {
		unsigned n_acks = 0;
		bool acked = true;
		unsigned n_quorum = 0;

		for (i = 0; i < f->n; i++) {
			struct raft *raft = raft_fixture_get(f, i);
			const struct raft_server *server =
			    configurationGet(&raft->configuration, raft->id);

			/* If the server is not in the configuration or is idle,
			 * then don't count it. */
			if (server == NULL || server->role == RAFT_SPARE) {
				continue;
			}

			n_quorum++;

			/* If this server is itself the leader, or it's not
			 * alive or it's not connected to the leader, then don't
			 * count it in for stability. */
			if (i == leader_i || !raft_fixture_alive(f, i) ||
			    raft_fixture_saturated(f, leader_i, i)) {
				continue;
			}

			if (raft->current_term != leader_term) {
				acked = false;
				break;
			}

			if (raft->state != RAFT_FOLLOWER) {
				acked = false;
				break;
			}

			if (raft->follower_state.current_leader.id == 0) {
				acked = false;
				break;
			}

			if (raft->follower_state.current_leader.id !=
			    leader_id) {
				acked = false;
				break;
			}

			n_acks++;
		}

		if (!acked || n_acks < (n_quorum / 2)) {
			leader_id = 0;
		}
	}

	changed = leader_id != f->leader_id;
	f->leader_id = leader_id;

	return changed;
}

/* Check for leader append-only.
 *
 * From figure 3.2:
 *
 *   Leader Append-Only -> A leader never overwrites or deletes entries in its
 *   own log; it only appends new entries.
 */
static void checkLeaderAppendOnly(struct raft_fixture *f)
{
	struct raft *raft;
	raft_index index;
	raft_index last = logLastIndex(f->log);

	/* If the cached log is empty it means there was no leader before. */
	if (last == 0) {
		return;
	}

	/* If there's no new leader, just return. */
	if (f->leader_id == 0) {
		return;
	}

	raft = raft_fixture_get(f, (unsigned)f->leader_id - 1);
	last = logLastIndex(f->log);

	for (index = 1; index <= last; index++) {
		const struct raft_entry *entry1;
		const struct raft_entry *entry2;
		size_t i;

		entry1 = logGet(f->log, index);
		entry2 = logGet(raft->log, index);

		assert(entry1 != NULL);

		/* Check if the entry was snapshotted. */
		if (entry2 == NULL) {
			assert(raft->log->snapshot.last_index >= index);
			continue;
		}

		/* Entry was not overwritten. */
		assert(entry1->type == entry2->type);
		assert(entry1->term == entry2->term);
		for (i = 0; i < entry1->buf.len; i++) {
			assert(((uint8_t *)entry1->buf.base)[i] ==
			       ((uint8_t *)entry2->buf.base)[i]);
		}
	}
}

/* Make a copy of the the current leader log, in order to perform the Leader
 * Append-Only check at the next iteration. */
static void copyLeaderLog(struct raft_fixture *f)
{
	struct raft *raft = raft_fixture_get(f, (unsigned)f->leader_id - 1);
	struct raft_entry *entries;
	unsigned n;
	size_t i;
	int rv;
	logClose(f->log);
	f->log = logInit();
	if (f->log == NULL) {
		assert(false);
		return;
	}

	rv = logAcquire(raft->log, 1, &entries, &n);
	assert(rv == 0);
	for (i = 0; i < n; i++) {
		struct raft_entry *entry = &entries[i];
		struct raft_buffer buf;
		buf.len = entry->buf.len;
		buf.base = raft_malloc(buf.len);
		assert(buf.base != NULL);
		memcpy(buf.base, entry->buf.base, buf.len);
		/* FIXME(cole) what to do here for is_local? */
		rv = logAppend(f->log, entry->term, entry->type, buf, (struct raft_entry_local_data){}, false, NULL);
		assert(rv == 0);
	}
	logRelease(raft->log, 1, entries, n);
}

/* Update the commit index to match the one from the current leader. */
static void updateCommitIndex(struct raft_fixture *f)
{
	struct raft *raft = raft_fixture_get(f, (unsigned)f->leader_id - 1);
	if (raft->commit_index > f->commit_index) {
		f->commit_index = raft->commit_index;
	}
}

/* Return the lowest tick time across all servers, along with the associated
 * server index */
static void getLowestTickTime(struct raft_fixture *f, raft_time *t, unsigned *i)
{
	unsigned j;
	*t = (raft_time)-1 /* Maximum value */;
	for (j = 0; j < f->n; j++) {
		struct io *io = f->servers[j]->io.impl;
		if (io->next_tick < *t) {
			*t = io->next_tick;
			*i = j;
		}
	}
}

/* Return the completion time of the request with the lowest completion time
 * across all servers, along with the associated server index. */
static void getLowestRequestCompletionTime(struct raft_fixture *f,
					   raft_time *t,
					   unsigned *i)
{
	unsigned j;
	*t = (raft_time)-1 /* Maximum value */;
	for (j = 0; j < f->n; j++) {
		struct io *io = f->servers[j]->io.impl;
		queue *head;
		QUEUE_FOREACH(head, &io->requests)
		{
			struct ioRequest *r =
			    QUEUE_DATA(head, struct ioRequest, queue);
			if (r->completion_time < *t) {
				*t = r->completion_time;
				*i = j;
			}
		}
	}
}

/* Fire the tick callback of the i'th server. */
static void fireTick(struct raft_fixture *f, unsigned i)
{
	struct io *io = f->servers[i]->io.impl;
	f->time = io->next_tick;
	f->event->server_index = i;
	f->event->type = RAFT_FIXTURE_TICK;
	io->next_tick += io->tick_interval;
	if (f->servers[i]->alive) {
		io->tick_cb(io->io);
	}
}

/* Complete the first request with completion time @t on the @i'th server. */
static void completeRequest(struct raft_fixture *f, unsigned i, raft_time t)
{
	struct io *io = f->servers[i]->io.impl;
	queue *head;
	struct ioRequest *r = NULL;
	bool found = false;
	f->time = t;
	f->event->server_index = i;
	QUEUE_FOREACH(head, &io->requests)
	{
		r = QUEUE_DATA(head, struct ioRequest, queue);
		if (r->completion_time == t) {
			found = true;
			break;
		}
	}
	assert(found);
	queue_remove(head);
	switch (r->type) {
		case APPEND:
			ioFlushAppend(io, (struct append *)r);
			f->event->type = RAFT_FIXTURE_DISK;
			break;
		case SEND:
			ioFlushSend(io, (struct send *)r);
			f->event->type = RAFT_FIXTURE_NETWORK;
			break;
		case TRANSMIT:
			ioDeliverTransmit(io, (struct transmit *)r);
			f->event->type = RAFT_FIXTURE_NETWORK;
			break;
		case SNAPSHOT_PUT:
			ioFlushSnapshotPut(io, (struct snapshot_put *)r);
			f->event->type = RAFT_FIXTURE_DISK;
			break;
		case SNAPSHOT_GET:
			ioFlushSnapshotGet(io, (struct snapshot_get *)r);
			f->event->type = RAFT_FIXTURE_DISK;
			break;
		case ASYNC_WORK:
			ioFlushAsyncWork(io, (struct async_work *)r);
			f->event->type = RAFT_FIXTURE_WORK;
			break;
		default:
			assert(0);
	}
}

struct raft_fixture_event *raft_fixture_step(struct raft_fixture *f)
{
	raft_time tick_time;
	raft_time completion_time;
	unsigned i = f->n;
	unsigned j = f->n;

	getLowestTickTime(f, &tick_time, &i);
	getLowestRequestCompletionTime(f, &completion_time, &j);

	assert(i < f->n || j < f->n);

	if (tick_time < completion_time ||
	    (tick_time == completion_time && i <= j)) {
		fireTick(f, i);
	} else {
		completeRequest(f, j, completion_time);
	}

	/* If the leader has not changed check the Leader Append-Only
	 * guarantee. */
	if (!updateLeaderAndCheckElectionSafety(f)) {
		checkLeaderAppendOnly(f);
	}

	/* If we have a leader, update leader-related state . */
	if (f->leader_id != 0) {
		copyLeaderLog(f);
		updateCommitIndex(f);
	}

	if (f->hook != NULL) {
		f->hook(f, f->event);
	}

	return f->event;
}

struct raft_fixture_event *raft_fixture_step_n(struct raft_fixture *f,
					       unsigned n)
{
	unsigned i;
	assert(n > 0);
	for (i = 0; i < n - 1; i++) {
		raft_fixture_step(f);
	}
	return raft_fixture_step(f);
}

bool raft_fixture_step_until(struct raft_fixture *f,
			     bool (*stop)(struct raft_fixture *f, void *arg),
			     void *arg,
			     unsigned max_msecs)
{
	raft_time start = f->time;
	while (!stop(f, arg) && (f->time - start) < max_msecs) {
		raft_fixture_step(f);
	}
	return f->time - start < max_msecs;
}

/* A step function which return always false, forcing raft_fixture_step_n to
 * advance time at each iteration. */
static bool spin(struct raft_fixture *f, void *arg)
{
	(void)f;
	(void)arg;
	return false;
}

void raft_fixture_step_until_elapsed(struct raft_fixture *f, unsigned msecs)
{
	raft_fixture_step_until(f, spin, NULL, msecs);
}

static bool hasLeader(struct raft_fixture *f, void *arg)
{
	(void)arg;
	return f->leader_id != 0;
}

bool raft_fixture_step_until_has_leader(struct raft_fixture *f,
					unsigned max_msecs)
{
	return raft_fixture_step_until(f, hasLeader, NULL, max_msecs);
}

static bool hasNoLeader(struct raft_fixture *f, void *arg)
{
	(void)arg;
	return f->leader_id == 0;
}

bool raft_fixture_step_until_has_no_leader(struct raft_fixture *f,
					   unsigned max_msecs)
{
	return raft_fixture_step_until(f, hasNoLeader, NULL, max_msecs);
}

/* Enable/disable dropping outgoing messages of a certain type from all servers
 * except one. */
static void dropAllExcept(struct raft_fixture *f,
			  int type,
			  bool flag,
			  unsigned i)
{
	unsigned j;
	for (j = 0; j < f->n; j++) {
		struct raft_fixture_server *s = f->servers[j];
		if (j == i) {
			continue;
		}
		ioDrop(s->io.impl, type, flag);
	}
}

/* Set the randomized election timeout of the given server to the minimum value
 * compatible with its current state and timers. */
static void minimizeRandomizedElectionTimeout(struct raft_fixture *f,
					      unsigned i)
{
	struct raft *raft = &f->servers[i]->raft;
	raft_time now = raft->io->time(raft->io);
	unsigned timeout = raft->election_timeout;
	assert(raft->state == RAFT_FOLLOWER);

	/* If the minimum election timeout value would make the timer expire in
	 * the past, cap it. */
	if (now - raft->election_timer_start > timeout) {
		timeout = (unsigned)(now - raft->election_timer_start);
	}

	raft->follower_state.randomized_election_timeout = timeout;
}

/* Set the randomized election timeout to the maximum value on all servers
 * except the given one. */
static void maximizeAllRandomizedElectionTimeoutsExcept(struct raft_fixture *f,
							unsigned i)
{
	unsigned j;
	for (j = 0; j < f->n; j++) {
		struct raft *raft = &f->servers[j]->raft;
		unsigned timeout = raft->election_timeout * 2;
		if (j == i) {
			continue;
		}
		assert(raft->state == RAFT_FOLLOWER);
		raft->follower_state.randomized_election_timeout = timeout;
	}
}

void raft_fixture_hook(struct raft_fixture *f, raft_fixture_event_cb hook)
{
	f->hook = hook;
}

void raft_fixture_start_elect(struct raft_fixture *f, unsigned i)
{
	struct raft *raft = raft_fixture_get(f, i);
	unsigned j;

	/* Make sure there's currently no leader. */
	assert(f->leader_id == 0);

	/* Make sure that the given server is voting. */
	assert(configurationGet(&raft->configuration, raft->id)->role ==
	       RAFT_VOTER);

	/* Make sure all servers are currently followers. */
	for (j = 0; j < f->n; j++) {
		assert(raft_state(&f->servers[j]->raft) == RAFT_FOLLOWER);
	}

	/* Pretend that the last randomized election timeout was set at the
	 * maximum value on all server expect the one to be elected, which is
	 * instead set to the minimum possible value compatible with its current
	 * state. */
	minimizeRandomizedElectionTimeout(f, i);
	maximizeAllRandomizedElectionTimeoutsExcept(f, i);
}

void raft_fixture_elect(struct raft_fixture *f, unsigned i)
{
	struct raft *raft = raft_fixture_get(f, i);
	raft_fixture_start_elect(f, i);
	raft_fixture_step_until_has_leader(f, ELECTION_TIMEOUT * 20);
	assert(f->leader_id == raft->id);
}

void raft_fixture_depose(struct raft_fixture *f)
{
	unsigned leader_i;

	/* Make sure there's a leader. */
	assert(f->leader_id != 0);
	leader_i = (unsigned)f->leader_id - 1;
	assert(raft_state(&f->servers[leader_i]->raft) == RAFT_LEADER);

	/* Set a very large election timeout on all followers, to prevent them
	 * from starting an election. */
	maximizeAllRandomizedElectionTimeoutsExcept(f, leader_i);

	/* Prevent all servers from sending append entries results, so the
	 * leader will eventually step down. */
	dropAllExcept(f, RAFT_IO_APPEND_ENTRIES_RESULT, true, leader_i);

	raft_fixture_step_until_has_no_leader(f, ELECTION_TIMEOUT * 3);
	assert(f->leader_id == 0);

	dropAllExcept(f, RAFT_IO_APPEND_ENTRIES_RESULT, false, leader_i);
}

struct step_apply
{
	unsigned i;
	raft_index index;
};

static bool hasAppliedIndex(struct raft_fixture *f, void *arg)
{
	struct step_apply *apply = (struct step_apply *)arg;
	struct raft *raft;
	unsigned n = 0;
	unsigned i;

	if (apply->i < f->n) {
		raft = raft_fixture_get(f, apply->i);
		return raft_last_applied(raft) >= apply->index;
	}

	for (i = 0; i < f->n; i++) {
		raft = raft_fixture_get(f, i);
		if (raft_last_applied(raft) >= apply->index) {
			n++;
		}
	}
	return n == f->n;
}

bool raft_fixture_step_until_applied(struct raft_fixture *f,
				     unsigned i,
				     raft_index index,
				     unsigned max_msecs)
{
	struct step_apply apply = {i, index};
	return raft_fixture_step_until(f, hasAppliedIndex, &apply, max_msecs);
}

struct step_state
{
	unsigned i;
	int state;
};

static bool hasState(struct raft_fixture *f, void *arg)
{
	struct step_state *target = (struct step_state *)arg;
	struct raft *raft;
	raft = raft_fixture_get(f, target->i);
	return raft_state(raft) == target->state;
}

bool raft_fixture_step_until_state_is(struct raft_fixture *f,
				      unsigned i,
				      int state,
				      unsigned max_msecs)
{
	struct step_state target = {i, state};
	return raft_fixture_step_until(f, hasState, &target, max_msecs);
}

struct step_term
{
	unsigned i;
	raft_term term;
};

static bool hasTerm(struct raft_fixture *f, void *arg)
{
	struct step_term *target = (struct step_term *)arg;
	struct raft *raft;
	raft = raft_fixture_get(f, target->i);
	return raft->current_term == target->term;
}

bool raft_fixture_step_until_term_is(struct raft_fixture *f,
				     unsigned i,
				     raft_term term,
				     unsigned max_msecs)
{
	struct step_term target = {i, term};
	return raft_fixture_step_until(f, hasTerm, &target, max_msecs);
}

struct step_vote
{
	unsigned i;
	unsigned j;
};

static bool hasVotedFor(struct raft_fixture *f, void *arg)
{
	struct step_vote *target = (struct step_vote *)arg;
	struct raft *raft;
	raft = raft_fixture_get(f, target->i);
	return raft->voted_for == target->j + 1;
}

bool raft_fixture_step_until_voted_for(struct raft_fixture *f,
				       unsigned i,
				       unsigned j,
				       unsigned max_msecs)
{
	struct step_vote target = {i, j};
	return raft_fixture_step_until(f, hasVotedFor, &target, max_msecs);
}

struct step_deliver
{
	unsigned i;
	unsigned j;
};

static bool hasDelivered(struct raft_fixture *f, void *arg)
{
	struct step_deliver *target = (struct step_deliver *)arg;
	struct raft *raft;
	struct io *io;
	struct raft_message *message;
	queue *head;
	raft = raft_fixture_get(f, target->i);
	io = raft->io->impl;
	QUEUE_FOREACH(head, &io->requests)
	{
		struct ioRequest *r;
		r = QUEUE_DATA(head, struct ioRequest, queue);
		message = NULL;
		switch (r->type) {
			case SEND:
				message = &((struct send *)r)->message;
				break;
			case TRANSMIT:
				message = &((struct transmit *)r)->message;
				break;
		}
		if (message != NULL && message->server_id == target->j + 1) {
			return false;
		}
	}
	return true;
}

bool raft_fixture_step_until_delivered(struct raft_fixture *f,
				       unsigned i,
				       unsigned j,
				       unsigned max_msecs)
{
	struct step_deliver target = {i, j};
	return raft_fixture_step_until(f, hasDelivered, &target, max_msecs);
}

void raft_fixture_disconnect(struct raft_fixture *f, unsigned i, unsigned j)
{
	struct raft_io *io1 = &f->servers[i]->io;
	struct raft_io *io2 = &f->servers[j]->io;
	ioDisconnect(io1, io2);
}

void raft_fixture_reconnect(struct raft_fixture *f, unsigned i, unsigned j)
{
	struct raft_io *io1 = &f->servers[i]->io;
	struct raft_io *io2 = &f->servers[j]->io;
	ioReconnect(io1, io2);
}

void raft_fixture_saturate(struct raft_fixture *f, unsigned i, unsigned j)
{
	struct raft_io *io1 = &f->servers[i]->io;
	struct raft_io *io2 = &f->servers[j]->io;
	ioSaturate(io1, io2);
}

static void disconnectFromAll(struct raft_fixture *f, unsigned i)
{
	unsigned j;
	for (j = 0; j < f->n; j++) {
		if (j == i) {
			continue;
		}
		raft_fixture_saturate(f, i, j);
		raft_fixture_saturate(f, j, i);
	}
}

static void reconnectToAll(struct raft_fixture *f, unsigned i)
{
	unsigned j;
	for (j = 0; j < f->n; j++) {
		if (j == i) {
			continue;
		}
		/* Don't reconnect to disconnected peers */
		if (!f->servers[j]->alive) {
			continue;
		}
		raft_fixture_desaturate(f, i, j);
		raft_fixture_desaturate(f, j, i);
	}
}

bool raft_fixture_saturated(struct raft_fixture *f, unsigned i, unsigned j)
{
	struct raft_io *io1 = &f->servers[i]->io;
	struct raft_io *io2 = &f->servers[j]->io;
	return ioSaturated(io1, io2);
}

void raft_fixture_desaturate(struct raft_fixture *f, unsigned i, unsigned j)
{
	struct raft_io *io1 = &f->servers[i]->io;
	struct raft_io *io2 = &f->servers[j]->io;
	ioDesaturate(io1, io2);
}

void raft_fixture_kill(struct raft_fixture *f, unsigned i)
{
	disconnectFromAll(f, i);
	f->servers[i]->alive = false;
}

void raft_fixture_revive(struct raft_fixture *f, unsigned i)
{
	reconnectToAll(f, i);
	f->servers[i]->alive = true;
}

int raft_fixture_grow(struct raft_fixture *f, struct raft_fsm *fsm)
{
	unsigned i;
	unsigned j;
	int rc;
	i = f->n;
	f->n++;

	rc = serverInit(f, i, fsm);
	if (rc != 0) {
		return rc;
	}

	serverConnectToAll(f, i);
	for (j = 0; j < f->n; j++) {
		struct raft_io *io1 = &f->servers[i]->io;
		struct raft_io *io2 = &f->servers[j]->io;
		ioConnect(io2, io1);
	}

	return 0;
}

void raft_fixture_set_randomized_election_timeout(struct raft_fixture *f,
						  unsigned i,
						  unsigned msecs)
{
	struct io *io = f->servers[i]->io.impl;
	io->randomized_election_timeout = msecs;
}

void raft_fixture_set_network_latency(struct raft_fixture *f,
				      unsigned i,
				      unsigned msecs)
{
	struct io *io = f->servers[i]->io.impl;
	io->network_latency = msecs;
}

void raft_fixture_set_disk_latency(struct raft_fixture *f,
				   unsigned i,
				   unsigned msecs)
{
	struct io *io = f->servers[i]->io.impl;
	io->disk_latency = msecs;
}

void raft_fixture_set_send_latency(struct raft_fixture *f,
				   unsigned i,
				   unsigned j,
				   unsigned msecs)
{
	struct io *io = f->servers[i]->io.impl;
	struct peer *peer = ioGetPeer(io, f->servers[j]->id);
	peer->send_latency = msecs;
}

void raft_fixture_set_term(struct raft_fixture *f, unsigned i, raft_term term)
{
	struct io *io = f->servers[i]->io.impl;
	io->term = term;
}

void raft_fixture_set_snapshot(struct raft_fixture *f,
			       unsigned i,
			       struct raft_snapshot *snapshot)
{
	struct io *io = f->servers[i]->io.impl;
	io->snapshot = snapshot;
}

void raft_fixture_add_entry(struct raft_fixture *f,
			    unsigned i,
			    struct raft_entry *entry)
{
	struct io *io = f->servers[i]->io.impl;
	struct raft_entry *entries;
	entries = raft_realloc(io->entries, (io->n + 1) * sizeof *entries);
	assert(entries != NULL);
	entries[io->n] = *entry;
	io->entries = entries;
	io->n++;
}

void raft_fixture_append_fault(struct raft_fixture *f, unsigned i, int delay)
{
	struct io *io = f->servers[i]->io.impl;
	io->append_fault_countdown = delay;
}

void raft_fixture_vote_fault(struct raft_fixture *f, unsigned i, int delay)
{
	struct io *io = f->servers[i]->io.impl;
	io->vote_fault_countdown = delay;
}

void raft_fixture_term_fault(struct raft_fixture *f, unsigned i, int delay)
{
	struct io *io = f->servers[i]->io.impl;
	io->term_fault_countdown = delay;
}

void raft_fixture_send_fault(struct raft_fixture *f, unsigned i, int delay)
{
	struct io *io = f->servers[i]->io.impl;
	io->send_fault_countdown = delay;
}

unsigned raft_fixture_n_send(struct raft_fixture *f, unsigned i, int type)
{
	struct io *io = f->servers[i]->io.impl;
	return io->n_send[type];
}

unsigned raft_fixture_n_recv(struct raft_fixture *f, unsigned i, int type)
{
	struct io *io = f->servers[i]->io.impl;
	return io->n_recv[type];
}

void raft_fixture_make_unavailable(struct raft_fixture *f, unsigned i)
{
	struct raft *r = &f->servers[i]->raft;
	convertToUnavailable(r);
}
