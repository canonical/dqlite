#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#include <sqlite3.h>
#include <uv.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "conn.h"
#include "error.h"
#include "log.h"
#include "metrics.h"
#include "options.h"
#include "queue.h"
#ifdef DQLITE_EXPERIMENTAL
#include "fsm.h"
#include "raft.h"
#include "registry.h"
#include "replication.h"
#endif /* DQLITE_EXPERIMENTAL */

int dqlite_init(const char **errmsg)
{
	int rc;

	assert(errmsg != NULL);

	/* Configure SQLite for single-thread mode. This is a global config.
	 *
	 * TODO: add an option to turn failures into warnings instead. This
	 * would degrade performance but allow clients to use this process'
	 * SQLite instance for other purposes that require multi-thread.
	 */
	rc = sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);
	if (rc != SQLITE_OK) {
		*errmsg = "failed to set SQLite to single-thread mode";
		return DQLITE_ERROR;
	}

	*errmsg = NULL;

	return 0;
}

/* Manage client TCP connections to a dqlite node */
struct dqlite__server
{
	/* read-only */
	dqlite__error error; /* Last error occurred, if any */

	/* private */
	dqlite_cluster *cluster;	 /* Cluster implementation */
	struct dqlite_logger *logger;    /* Optional logger implementation */
	struct dqlite__metrics *metrics; /* Operational metrics */
	struct options options;		 /* Configuration values */
#ifdef DQLITE_EXPERIMENTAL
	char name[256];
	sqlite3_vfs *vfs;
	struct registry registry;
	sqlite3_wal_replication replication;
	struct
	{
		struct dqlite_cluster cluster;
		struct raft_io io;
		struct raft_fsm fsm;
		struct raft raft;
		struct raft_io_uv_transport transport;
		raft_io_uv_accept_cb accept_cb;
		unsigned id;
		const char *address;
	} raft;
#endif				    /* DQLITE_EXPERIMENTAL */
	struct dqlite__queue queue; /* Queue of incoming connections */
	pthread_mutex_t mutex;      /* Serialize access to incoming queue */
	uv_loop_t loop;		    /* UV loop */
	uv_async_t stop;	    /* Event to stop the UV loop */
	uv_async_t incoming;	/* Event to process the incoming queue */
	int running;		    /* Indicate that the loop is running */
	sem_t ready;		    /* Notifiy that the loop is running */
	uv_timer_t startup;	 /* Used for unblocking the ready sem */
	sem_t stopped;		    /* Notifiy that the loop has been stopped */
};

/* Callback for the uv_walk() call in dqlite__server_stop_cb.
 *
 * This callback gets fired once for every active handle in the UV loop and is
 * in charge of closing each of them.
 */
static void dqlite__server_stop_walk_cb(uv_handle_t *handle, void *arg)
{
	struct dqlite__server *s;
	struct conn *conn;

	assert(handle != NULL);
	assert(arg != NULL);
	assert(handle->type == UV_ASYNC || handle->type == UV_TIMER ||
	       handle->type == UV_TCP || handle->type == UV_NAMED_PIPE);

	s = (struct dqlite__server *)arg;

	switch (handle->type) {
		case UV_ASYNC:
			assert(handle == (uv_handle_t *)&s->stop ||
			       handle == (uv_handle_t *)&s->incoming);

			uv_close(handle, NULL);

			break;

		case UV_TCP:
		case UV_NAMED_PIPE:
			assert(handle->data != NULL);

			conn = (struct conn *)handle->data;

			/* Abort the client connection and release any allocated
			 * resources. */
			conn__abort(conn);

			break;

		case UV_TIMER:
			/* If this is the startup timer, let's close it
			 * explicitely. */
			if (handle == (uv_handle_t *)&s->startup) {
				uv_close(handle, NULL);
			}

			/* In all other cases this must be a timer created by a
			 * conn object, which gets closed by the conn__abort
			 * call above, so there's nothing to do in that case. */

			break;

		default:
			/* Should not be reached because we assert all possible
			 * handle types above */
			assert(0);
			break;
	}
}

/* Callback invoked when the stop async handle gets fired.
 *
 * This callback will walk through all active handles and close them. After the
 * last handle (which must be the 'stop' async handle) is closed, the loop gets
 * stopped.
 */
#ifdef DQLITE_EXPERIMENTAL
static void raft_close_cb(struct raft *raft)
{
	struct dqlite__server *s = raft->data;
	uv_walk(&s->loop, dqlite__server_stop_walk_cb, (void *)s);
}
#endif /* DQLITE_EXPERIMENTAL */

static void dqlite__server_stop_cb(uv_async_t *stop)
{
	struct dqlite__server *s;

	assert(stop != NULL);
	assert(stop->data != NULL);

	s = (struct dqlite__server *)stop->data;

	/* We expect that we're being executed after dqlite_server_stop and so
	 * the running flag is off. */
	assert(!s->running);

	/* Give a final pass to the incoming queue, to unblock any call to
	 * dqlite_server_handle that might be blocked. There's no need to
	 * acquire the mutex since now the running flag is off and no new
	 * incoming connection can be enqueued. */
	dqlite__queue_process(&s->queue);

#ifdef DQLITE_EXPERIMENTAL
	raft_close(&s->raft.raft, raft_close_cb);
#else
	/* Loop through all connections and abort them, then stop the event
	 * loop. */
	uv_walk(&s->loop, dqlite__server_stop_walk_cb, (void *)s);
#endif /* DQLITE_EXPERIMENTAL */
}

/* Callback invoked when the incoming async handle gets fired.
 *
 * This callback will scan the incoming queue and create new connections.
 */
static void dqlite__server_incoming_cb(uv_async_t *incoming)
{
	struct dqlite__server *s;

	assert(incoming != NULL);
	assert(incoming->data != NULL);
	s = (struct dqlite__server *)incoming->data;

	/* Acquire the queue lock, so no new incoming connection can be
	 * pushed. */
	pthread_mutex_lock(&s->mutex);

	dqlite__queue_process(&s->queue);

	pthread_mutex_unlock(&s->mutex);
}

/* Callback invoked as soon as the loop as started.
 *
 * It unblocks the s->ready semaphore.
 */
static void dqlite__service_startup_cb(uv_timer_t *startup)
{
	int err;
	struct dqlite__server *s;

	assert(startup != NULL);
	assert(startup->data != NULL);

	s = (struct dqlite__server *)startup->data;

	s->running = 1;

	err = sem_post(&s->ready);
	assert(err == 0); /* No reason for which posting should fail */
}

#ifdef DQLITE_EXPERIMENTAL
static int transport__init(struct raft_io_uv_transport *transport,
			   unsigned id,
			   const char *address)
{
	struct dqlite__server *s = transport->impl;
	s->raft.id = id;
	s->raft.address = address;
	return 0;
}

static int transport__listen(struct raft_io_uv_transport *transport,
			     raft_io_uv_accept_cb cb)
{
	struct dqlite__server *s = transport->impl;
	s->raft.accept_cb = cb;
	return 0;
}

static int transport__connect(struct raft_io_uv_transport *transport,
			      struct raft_io_uv_connect *req,
			      unsigned id,
			      const char *address,
			      raft_io_uv_connect_cb cb)
{
	(void)transport;
	(void)req;
	(void)address;
	(void)cb;
	(void)id;
	return 0;
}

static void transport__close(struct raft_io_uv_transport *transport,
			     raft_io_uv_transport_close_cb cb)
{
	cb(transport);
}

static const char *cluster__leader(void *ctx)
{
	char *address;
	(void)ctx;
	/* Allocate a string, as regular implementations of the cluster
	 * interface are expected to do. */
	address = malloc(strlen("127.0.0.1:666") + 1);
	if (address == NULL) {
		return NULL;
	}
	strcpy(address, "127.0.0.1:666");
	return address;
}

static int cluster__barrier(void *ctx)
{
	(void)ctx;
	return 0;
}

int dqlite_server_bootstrap(dqlite_server *s)
{
	struct raft_configuration configuration;
	int rc;
	raft_configuration_init(&configuration);
	rc = raft_configuration_add(&configuration, s->raft.raft.id,
				    s->raft.raft.address, true);
	assert(rc == 0);
	rc = raft_bootstrap(&s->raft.raft, &configuration);
	assert(rc == 0);
	raft_configuration_close(&configuration);
	return 0;
}

#endif /* DQLITE_EXPERIMENTAL */

int server__create(dqlite_cluster *cluster,
		   const char *dir,
		   unsigned id,
		   const char *address,
		   dqlite_server **out)
{
	dqlite_server *s;
	int err;

	assert(out != NULL);

	s = sqlite3_malloc(sizeof *s);
	if (s == NULL) {
		err = DQLITE_NOMEM;
		goto err;
	}

	dqlite__error_init(&s->error);

	s->logger = NULL;
	s->metrics = NULL;

	s->cluster = cluster;

	options__init(&s->options);

	dqlite__queue_init(&s->queue);

	err = pthread_mutex_init(&s->mutex, NULL);
	assert(err == 0); /* Docs say that pthread_mutex_init can't fail */

	err = sem_init(&s->ready, 0, 0);
	if (err != 0) {
		dqlite__error_sys(&s->error, "failed to init ready semaphore");
		return DQLITE_ERROR;
	}

	err = sem_init(&s->stopped, 0, 0);
	if (err != 0) {
		dqlite__error_sys(&s->error,
				  "failed to init stopped semaphore");
		return DQLITE_ERROR;
	}

	s->running = 0;

	/* Initialize the event loop. */
	err = uv_loop_init(&s->loop);
	assert(err == 0);

	*out = s;

#ifdef DQLITE_EXPERIMENTAL
	sprintf(s->name, "dqlite-%u", id);
	s->vfs = dqlite_vfs_create(s->name, s->logger);
	assert(s->vfs != NULL);
	sqlite3_vfs_register(s->vfs, 0);
	registry__init(&s->registry, &s->options);
	s->cluster = &s->raft.cluster;
	s->raft.cluster.ctx = s;
	s->raft.cluster.xLeader = cluster__leader;
	s->raft.cluster.xBarrier = cluster__barrier;
	s->raft.transport.impl = s;
	s->raft.transport.init = transport__init;
	s->raft.transport.listen = transport__listen;
	s->raft.transport.connect = transport__connect;
	s->raft.transport.close = transport__close;
	err = raft_io_uv_init(&s->raft.io, &s->loop, dir, &s->raft.transport);
	assert(err == 0);
	err = fsm__init(&s->raft.fsm, s->logger, &s->registry);
	assert(err == 0);
	err = raft_init(&s->raft.raft, &s->raft.io, &s->raft.fsm, id, address);
	assert(err == 0);
	s->raft.raft.data = s;
	raft_set_election_timeout(&s->raft.raft, 250);
	err = replication__init(&s->replication, s->logger, &s->raft.raft);
	assert(err == 0);
	s->replication.zName = s->name;
	sqlite3_wal_replication_register(&s->replication, 0);
#endif /* DQLITE_EXPERIMENTAL */

	return 0;

err:
	assert(err != 0);

	*out = NULL;

	return err;
}

int dqlite_server_create(dqlite_cluster *cluster, dqlite_server **out)
{
	return server__create(cluster, NULL, 0, NULL, out);
}

#ifdef DQLITE_EXPERIMENTAL
int dqlite_server_create2(const char *dir,
			  unsigned id,
			  const char *address,
			  dqlite_server **out)
{
	return server__create(NULL, dir, id, address, out);
}
#endif /* DQLITE_EXPERIMENTAL */

void dqlite_server_destroy(dqlite_server *s)
{
	int err;

	assert(s != NULL);

	err = uv_loop_close(&s->loop);
	assert(err == 0);

	if (s->metrics != NULL) {
		sqlite3_free(s->metrics);
	}

	options__close(&s->options);

	/* The sem_destroy call should only fail if the given semaphore is
	 * invalid, which must not be our case. */
	err = sem_destroy(&s->stopped);
	assert(err == 0);

	err = sem_destroy(&s->ready);
	assert(err == 0);

	/* The pthread_mutex_destroy call is a no-op on Linux . */
	err = pthread_mutex_destroy(&s->mutex);
	assert(err == 0);

	dqlite__queue_close(&s->queue);
	dqlite__error_close(&s->error);

#ifdef DQLITE_EXPERIMENTAL
	raft_io_uv_close(&s->raft.io);
	sqlite3_wal_replication_unregister(&s->replication);
	replication__close(&s->replication);
	fsm__close(&s->raft.fsm);
	registry__close(&s->registry);
	sqlite3_vfs_unregister(s->vfs);
	dqlite_vfs_destroy(s->vfs);
#endif /* DQLITE_EXPERIMENTAL */

	sqlite3_free(s);
}

/* Set a config option */
int dqlite_server_config(dqlite_server *s, int op, void *arg)
{
	int err = 0;

	assert(s != NULL);
	(void)arg;

	switch (op) {
		case DQLITE_CONFIG_LOGGER:
			s->logger = arg;
			break;

		case DQLITE_CONFIG_VFS:
			err = options__set_vfs(&s->options, (const char *)arg);
			break;

		case DQLITE_CONFIG_WAL_REPLICATION:
			err = options__set_replication(&s->options,
						       (const char *)arg);
			break;

		case DQLITE_CONFIG_HEARTBEAT_TIMEOUT:
			s->options.heartbeat_timeout = *(uint16_t *)arg;
			break;

		case DQLITE_CONFIG_PAGE_SIZE:
			s->options.page_size = *(uint16_t *)arg;
			break;

		case DQLITE_CONFIG_CHECKPOINT_THRESHOLD:
			s->options.checkpoint_threshold = *(uint32_t *)arg;
			break;

		case DQLITE_CONFIG_METRICS:
			if (*(uint8_t *)arg == 1) {
				if (s->metrics == NULL) {
					s->metrics =
					    sqlite3_malloc(sizeof *s->metrics);
					dqlite__metrics_init(s->metrics);
				}
			} else {
				if (s->metrics == NULL) {
					sqlite3_free(s->metrics);
					s->metrics = NULL;
				}
			}
			break;

		default:
			dqlite__error_printf(&s->error, "unknown op code %d",
					     op);
			err = DQLITE_ERROR;
			break;
	}

	return err;
}

int dqlite_server_run(struct dqlite__server *s)
{
	int err;

	assert(s != NULL);

	dqlite__infof(s, "starting event loop");

	/* Initialize async handles. */
	err = uv_async_init(&s->loop, &s->stop, dqlite__server_stop_cb);
	if (err != 0) {
		dqlite__error_uv(&s->error, err,
				 "failed to init stop event handle");
		err = DQLITE_ERROR;
		goto out;
	}
	s->stop.data = (void *)s;

	err = uv_async_init(&s->loop, &s->incoming, dqlite__server_incoming_cb);
	if (err != 0) {
		dqlite__error_uv(&s->error, err,
				 "failed to init accept event handle");
		err = DQLITE_ERROR;
		goto out;
	}
	s->incoming.data = (void *)s;

	/* Schedule dqlite__service_startup_cb to be fired as soon as the loop
	 * starts. It will unblock clients of dqlite_service_ready. */
	err = uv_timer_init(&s->loop, &s->startup);
	if (err != 0) {
		dqlite__error_uv(&s->error, err, "failed to init timer");
		err = DQLITE_ERROR;
		goto out;
	}
	s->startup.data = (void *)s;

	err = uv_timer_start(&s->startup, dqlite__service_startup_cb, 0, 0);
	if (err != 0) {
		dqlite__error_uv(&s->error, err, "failed to startup timer");
		err = DQLITE_ERROR;
		goto out;
	}

#ifdef DQLITE_EXPERIMENTAL
	err = raft_start(&s->raft.raft);
	assert(err == 0);
#endif /* DQLITE_EXPERIMENTAL */

	err = uv_run(&s->loop, UV_RUN_DEFAULT);
	if (err != 0) {
		dqlite__error_uv(&s->error, err,
				 "event loop finished unclealy");
		goto out;
	}

out:
	/* Unblock any client of dqlite_server_ready (no reason for which
	 * posting should fail). */
	assert(sem_post(&s->ready) == 0);

	dqlite__infof(s, "event loop stopped");

	return err;
}

int dqlite_server_ready(dqlite_server *s)
{
	assert(s != NULL);

	/* Wait for the ready semaphore */
	sem_wait(&s->ready);

	return s->running;
}

int dqlite_server_stop(dqlite_server *s, char **errmsg)
{
	int err = 0;
	dqlite__error e;

	assert(s != NULL);
	assert(errmsg != NULL);

	dqlite__infof(s, "stopping event loop");

	/* Grab the queue mutex, so we can be sure no new incoming request will
	 * be enqueued from this point on. */
	pthread_mutex_lock(&s->mutex);

	/* Create an error instance since the one on d is not thread-safe */
	dqlite__error_init(&e);

	/* Turn off the running flag, so calls to dqlite_server_handle will fail
	 * with DQLITE_STOPPED. This needs to happen before we send the stop
	 * signal since the stop callback expects to see that the flag is
	 * off. */
	s->running = 0;

	err = uv_async_send(&s->stop);
	if (err != 0) {
		dqlite__error_uv(&e, err, "failed to fire stop event");
		err = dqlite__error_copy(&e, errmsg);
		if (err != 0) {
			*errmsg = "error message unavailable (out of memory)";
		}
		err = DQLITE_ERROR;
	}

	pthread_mutex_unlock(&s->mutex);

	dqlite__error_close(&e);

	if (err != 0) {
		/* Wait for the stopped semaphore, which signals that the loop
		 * has exited. */
		sem_wait(&s->stopped);
	}

	return err;
}

/* Start handling a new connection */
int dqlite_server_handle(dqlite_server *s, int fd, char **errmsg)
{
	int err;
	dqlite__error e;
	struct conn *conn;
	struct dqlite__queue_item item;

	assert(s != NULL);
	assert(s->cluster != NULL);

	dqlite__infof(s, "handling new connection (fd=%d)", fd);

	pthread_mutex_lock(&s->mutex);

	/* Create an error instance since the one on d is not thread-safe */
	dqlite__error_init(&e);

	if (!s->running) {
		err = DQLITE_STOPPED;
		dqlite__error_printf(&e, "server is not running");
		goto err_not_running_or_conn_malloc;
	}

	conn = sqlite3_malloc(sizeof(*conn));
	if (conn == NULL) {
		dqlite__error_oom(&e, "failed to allocate connection");
		err = DQLITE_NOMEM;
		goto err_not_running_or_conn_malloc;
	}
	conn__init(conn, fd, s->logger, s->cluster, &s->loop, &s->options,
		   s->metrics);
#ifdef DQLITE_EXPERIMENTAL
	conn->gateway.registry = &s->registry;
#endif /* DQLITE_EXPERIMENTAL */

	err = dqlite__queue_item_init(&item, conn);
	if (err != 0) {
		dqlite__error_printf(&e,
				     "failed to init incoming queue item: %s",
				     strerror(errno));
		err = DQLITE_ERROR;
		goto err_item_init_or_queue_push;
	}

	err = dqlite__queue_push(&s->queue, &item);
	if (err != 0) {
		dqlite__error_wrapf(&e, &s->queue.error,
				    "failed to push incoming queue item");
		goto err_item_init_or_queue_push;
	}

	err = uv_async_send(&s->incoming);
	if (err != 0) {
		dqlite__error_uv(&e, err,
				 "failed to fire incoming connection event");
		err = DQLITE_ERROR;
		goto err_incoming_send;
	}

	pthread_mutex_unlock(&s->mutex);

	dqlite__queue_item_wait(&item);

	if (!dqlite__error_is_null(&item.error)) {
		dqlite__error_wrapf(&e, &item.error,
				    "failed to process incoming queue item");
		err = DQLITE_ERROR;
		goto err_item_wait;
	}

	dqlite__error_close(&e);
	dqlite__queue_item_close(&item);

	return 0;

err_incoming_send:
	dqlite__queue_pop(&s->queue);

err_item_init_or_queue_push:
	conn__close(conn);
	sqlite3_free(conn);

err_not_running_or_conn_malloc:
	if (dqlite__error_copy(&e, errmsg) != 0) {
		*errmsg = "error message unavailable (out of memory)";
	}

	dqlite__error_close(&e);

	pthread_mutex_unlock(&s->mutex);

	return err;

err_item_wait:
	conn__close(conn);
	sqlite3_free(conn);
	dqlite__queue_item_close(&item);

	return err;
}

const char *dqlite_server_errmsg(dqlite_server *s)
{
	return s->error;
}

dqlite_cluster *dqlite_server_cluster(dqlite_server *s)
{
	assert(s != NULL);

	return s->cluster;
}

dqlite_logger *dqlite_server_logger(dqlite_server *s)
{
	assert(s != NULL);

	return s->logger;
}
