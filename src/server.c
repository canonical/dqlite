#include "../include/dqlite.h"

#include "lib/assert.h"

#include "conn.h"
#include "fsm.h"
#include "replication.h"
#include "server.h"
#include "transport.h"
#include "vfs.h"

/* Information about incoming connection */
struct incoming
{
	int fd;        /* Connection file descriptor */
	sem_t pending; /* Block until the connection gets processed */
	int status;    /* Result status code */
	queue queue;   /* Incoming queue */
};

void incoming__init(struct incoming *i, int fd)
{
	int rv;
	i->fd = fd;
	rv = sem_init(&i->pending, 0, 0);
	assert(rv == 0);
}

void incoming__done(struct incoming *i, int status)
{
	int rv;
	i->status = status;
	rv = sem_post(&i->pending);
	assert(rv == 0);
}

void incoming__wait(struct incoming *i)
{
	sem_wait(&i->pending);
}

void incoming__close(struct incoming *i)
{
	int rv;
	rv = sem_destroy(&i->pending);
	assert(rv == 0);
}

int dqlite_initialize()
{
	int rc;

	/* Configure SQLite for single-thread mode. This is a global config.
	 *
	 * TODO: add an option to turn failures into warnings instead. This
	 * would degrade performance but allow clients to use this process'
	 * SQLite instance for other purposes that require multi-thread.
	 */
	rc = sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);
	if (rc != SQLITE_OK) {
		assert(rc == SQLITE_MISUSE);
		return DQLITE_MISUSE;
	}
	return 0;
}

int dqlite__init(struct dqlite *d,
		 unsigned id,
		 const char *address,
		 const char *dir)
{
	int rv;
	rv = config__init(&d->config, id, address);
	if (rv != 0) {
		goto err;
	}
	rv = vfs__init(&d->vfs, &d->config);
	if (rv != 0) {
		goto err_after_vfs_init;
	}
	registry__init(&d->registry, &d->config);
	rv = raft_uv_proxy__init(&d->raft_transport);
	if (rv != 0) {
		goto err_after_registry_init;
	}
	rv = uv_loop_init(&d->loop);
	if (rv != 0) {
		/* TODO: better error reporting */
		rv = DQLITE_INTERNAL;
		goto err_after_raft_transport_init;
	}
	rv = raft_io_uv_init(&d->raft_io, &d->loop, dir, &d->raft_transport);
	if (rv != 0) {
		/* TODO: better error reporting */
		rv = DQLITE_INTERNAL;
		goto err_after_loop_init;
	}
	rv = fsm__init(&d->raft_fsm, &d->config, &d->registry);
	if (rv != 0) {
		goto err_after_raft_io_init;
	}
	/* TODO: properly handle closing the dqlite server without running it */
	rv = raft_init(&d->raft, &d->raft_io, &d->raft_fsm, d->config.id,
		       d->config.address);
	if (rv != 0) {
		return rv;
	}
	rv = replication__init(&d->replication, &d->config, &d->raft);
	if (rv != 0) {
		goto err_after_raft_fsm_init;
	}
	rv = sem_init(&d->ready, 0, 0);
	if (rv != 0) {
		/* TODO: better error reporting */
		rv = DQLITE_INTERNAL;
		goto err_after_raft_replication_init;
	}
	rv = sem_init(&d->stopped, 0, 0);
	if (rv != 0) {
		/* TODO: better error reporting */
		rv = DQLITE_INTERNAL;
		goto err_after_ready_init;
	}
	rv = pthread_mutex_init(&d->mutex, NULL);
	assert(rv == 0); /* Docs say that pthread_mutex_init can't fail */
	QUEUE__INIT(&d->queue);
	QUEUE__INIT(&d->conns);
	d->running = false;
	return 0;

err_after_ready_init:
	sem_destroy(&d->ready);
err_after_raft_replication_init:
	replication__close(&d->replication);
err_after_raft_fsm_init:
	fsm__close(&d->raft_fsm);
err_after_raft_io_init:
	raft_io_uv_close(&d->raft_io);
err_after_loop_init:
	uv_loop_close(&d->loop);
err_after_raft_transport_init:
	raft_uv_proxy__close(&d->raft_transport);
err_after_registry_init:
	registry__close(&d->registry);
err_after_vfs_init:
	vfs__close(&d->vfs);
err:
	return rv;
}

void dqlite__close(struct dqlite *d)
{
	int rv;
	rv = pthread_mutex_destroy(&d->mutex); /* This is a no-op on Linux . */
	assert(rv == 0);
	rv = sem_destroy(&d->stopped);
	assert(rv == 0); /* Fails only if sem object is not valid */
	rv = sem_destroy(&d->ready);
	assert(rv == 0); /* Fails only if sem object is not valid */
	replication__close(&d->replication);
	fsm__close(&d->raft_fsm);
	raft_io_uv_close(&d->raft_io);
	uv_loop_close(&d->loop);
	raft_uv_proxy__close(&d->raft_transport);
	registry__close(&d->registry);
	vfs__close(&d->vfs);
	config__close(&d->config);
}

int dqlite_bootstrap(dqlite *d)
{
	struct raft_configuration configuration;
	int rv;
	raft_configuration_init(&configuration);
	rv = raft_configuration_add(&configuration, d->config.id,
				    d->config.address, true);
	if (rv != 0) {
		assert(rv == RAFT_ENOMEM);
		return DQLITE_NOMEM;
	};
	rv = raft_bootstrap(&d->raft, &configuration);
	if (rv != 0) {
		return DQLITE_INTERNAL;
	}
	raft_configuration_close(&configuration);
	return 0;
}

/* Callback invoked when the stop async handle gets fired.
 *
 * This callback will walk through all active handles and close them. After the
 * last handle (which must be the 'stop' async handle) is closed, the loop gets
 * stopped.
 */
static void raft_close_cb(struct raft *raft)
{
	struct dqlite *s = raft->data;
	uv_close((struct uv_handle_s *)&s->stop, NULL);
	uv_close((struct uv_handle_s *)&s->incoming, NULL);
	uv_close((struct uv_handle_s *)&s->startup, NULL);
}

static void destroy_conn(struct conn *conn) {
	QUEUE__REMOVE(&conn->queue);
	sqlite3_free(conn);
}

static void process_incoming(struct dqlite *d)
{
	while (!QUEUE__IS_EMPTY(&d->queue)) {
		queue *head;
		struct incoming *incoming;
		struct conn *conn;
		int rv;
		int status;
		head = QUEUE__HEAD(&d->queue);
		QUEUE__REMOVE(head);
		incoming = QUEUE__DATA(head, struct incoming, queue);
		conn = sqlite3_malloc(sizeof *conn);
		if (conn == NULL) {
			status = DQLITE_NOMEM;
			goto done;
		}
		rv = conn__start(conn, &d->config, &d->loop, &d->registry,
				 &d->raft, incoming->fd, &d->raft_transport,
				 destroy_conn);
		if (rv != 0) {
			status = rv;
			goto done;
		}
		QUEUE__PUSH(&d->conns, &conn->queue);
		status = 0;
	done:
		incoming__done(incoming, status);
	}
}

static void stop_cb(uv_async_t *stop)
{
	struct dqlite *d = stop->data;
	queue *head;
	struct conn *conn;

	/* We expect that we're being executed after dqlite__stop and so the
	 * running flag is off. */
	assert(!d->running);

	/* Give a final pass to the incoming queue, to unblock any call to
	 * dqlite_handle that might be blocked. There's no need to
	 * acquire the mutex since now the running flag is off and no new
	 * incoming connection can be enqueued. */
	process_incoming(d);

	QUEUE__FOREACH(head, &d->conns)
	{
		conn = QUEUE__DATA(head, struct conn, queue);
		conn__stop(conn);
	}

	raft_close(&d->raft, raft_close_cb);
}

/* Callback invoked when the incoming async handle gets fired.
 *
 * This callback will scan the incoming queue and create new connections.
 */
static void incoming_cb(uv_async_t *incoming)
{
	struct dqlite *d = incoming->data;
	/* Acquire the queue lock, so no new incoming connection can be
	 * pushed. */
	pthread_mutex_lock(&d->mutex);
	process_incoming(d);
	pthread_mutex_unlock(&d->mutex);
}

/* Callback invoked as soon as the loop as started.
 *
 * It unblocks the s->ready semaphore.
 */
static void startup_cb(uv_timer_t *startup)
{
	struct dqlite *d = startup->data;
	int rv;
	d->running = true;
	rv = sem_post(&d->ready);
	assert(rv == 0); /* No reason for which posting should fail */
}

int dqlite_run(struct dqlite *d)
{
	int rv;

	/* TODO: implement proper cleanup upon error by spinning the loop a few
	 * times. */

	/* Initialize notification handles. */
	d->stop.data = d;
	rv = uv_async_init(&d->loop, &d->stop, stop_cb);
	assert(rv == 0);
	d->incoming.data = d;
	rv = uv_async_init(&d->loop, &d->incoming, incoming_cb);
	assert(rv == 0);

	/* Schedule startup_cb to be fired as soon as the loop starts. It will
	 * unblock clients of dqlite_ready. */
	d->startup.data = d;
	rv = uv_timer_init(&d->loop, &d->startup);
	assert(rv == 0);
	rv = uv_timer_start(&d->startup, startup_cb, 0, 0);
	assert(rv == 0);

	d->raft.data = d;
	rv = raft_start(&d->raft);
	if (rv != 0) {
		return rv;
	}

	rv = uv_run(&d->loop, UV_RUN_DEFAULT);
	assert(rv == 0);

	/* Unblock any client of dqlite_ready */
	rv = sem_post(&d->ready);
	assert(rv == 0); /* no reason for which posting should fail */

	return 0;
}

bool dqlite_ready(struct dqlite *d)
{
	/* Wait for the ready semaphore */
	sem_wait(&d->ready);
	return d->running;
}

int dqlite_handle(dqlite *d, int fd)
{
	struct incoming *incoming;
	int rv;

	pthread_mutex_lock(&d->mutex);

	if (!d->running) {
		pthread_mutex_unlock(&d->mutex);
		return DQLITE_STOPPED;
	}

	incoming = sqlite3_malloc(sizeof *incoming);
	if (incoming == NULL) {
		pthread_mutex_unlock(&d->mutex);
		return DQLITE_NOMEM;
	}

	incoming__init(incoming, fd);
	QUEUE__PUSH(&d->queue, &incoming->queue);

	rv = uv_async_send(&d->incoming);
	assert(rv == 0);

	pthread_mutex_unlock(&d->mutex);

	/* Wait for the pending mutex to be released by the main dqlite loop */
	incoming__wait(incoming);

	rv = incoming->status;
	incoming__close(incoming);
	sqlite3_free(incoming);
	return rv;

	return 0;
}

int dqlite_stop(dqlite *d)
{
	int rv;

	/* Grab the queue mutex, so we can be sure no new incoming request will
	 * be enqueued from this point on. */
	pthread_mutex_lock(&d->mutex);

	/* Turn off the running flag, so calls to dqlite_handle will fail
	 * with DQLITE_STOPPED. This needs to happen before we send the stop
	 * signal since the stop callback expects to see that the flag is
	 * off. */
	d->running = false;

	rv = uv_async_send(&d->stop);
	assert(rv == 0);

	pthread_mutex_unlock(&d->mutex);

	return 0;
}

#if 0

int dqlite_create(const char *dir,
			 unsigned id,
			 const char *address,
			 dqlite **out)
{
	return server__create(dir, id, address, out);
}
/* Set a config option */
int dqlite_config(dqlite *s, int op, void *arg)
{
	int err = 0;

	assert(s != NULL);
	(void)arg;

	switch (op) {
		case DQLITE_CONFIG_LOGGER:
			s->logger = arg;
			break;

		case DQLITE_CONFIG_VFS:
			err = config__set_vfs(&s->options, (const char *)arg);
			break;

		case DQLITE_CONFIG_WAL_REPLICATION:
			err = config__set_replication(&s->options,
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

#endif
