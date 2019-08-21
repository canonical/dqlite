#include "../include/dqlite.h"

#include "lib/assert.h"

#include "conn.h"
#include "fsm.h"
#include "logger.h"
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

/* Covert raft state code to dqlite one. */
static int convertRaftState(int state)
{
	int converted;

	switch (state) {
		case RAFT_UNAVAILABLE:
			converted = DQLITE_UNAVAILABLE;
			break;
		case RAFT_FOLLOWER:
			converted = DQLITE_FOLLOWER;
			break;
		case RAFT_CANDIDATE:
			converted = DQLITE_CANDIDATE;
			break;
		case RAFT_LEADER:
			converted = DQLITE_LEADER;
			break;
		default:
			assert(0); /* Can't happen */
	}

	return converted;
}

/* Invoke the configured state watch function, if any. */
static void raftWatch(struct raft *r, int old_state)
{
	struct dqlite_task *d = r->data;
	int new_state = raft_state(r);

	if (d->config.watcher.f != NULL) {
		d->config.watcher.f(d->config.watcher.data,
				    convertRaftState(old_state),
				    convertRaftState(new_state));
	}
}

void raftRingLoggerWalkCb(void *data,
			  raft_time time,
			  int level,
			  const char *message)
{
	char buf[2048];
	char *cursor = buf;
	struct tm tm;
	time_t secs = time / 1000;
	unsigned msecs = time % 1000;

	memset(buf, 0, sizeof buf);

	gmtime_r(&secs, &tm);

	strftime(cursor, 10, "%H:%M:%S", &tm);
	cursor = buf + strlen(buf);

	sprintf(cursor, ".%03d ", msecs);
	cursor = buf + strlen(buf);

	/* First, render the logging level. */
	switch (level) {
		case RAFT_DEBUG:
			sprintf(cursor, "[DEBUG]: ");
			break;
		case RAFT_INFO:
			sprintf(cursor, "[INFO ]: ");
			break;
		case RAFT_WARN:
			sprintf(cursor, "[WARN ]: ");
			break;
		case RAFT_ERROR:
			sprintf(cursor, "[ERROR]: ");
			break;
		default:
			sprintf(cursor, "[     ]: ");
			break;
	};

	cursor = buf + strlen(buf);

	fprintf(stdout, "%s%s\n", buf, message);
}

/* Bump raft's ring logger to stdout. */
static void dumpRaftRingLogger(struct dqlite_task *d)
{
	raft_ring_logger_walk(&d->raft_logger, raftRingLoggerWalkCb, NULL);
}

int dqlite__init(struct dqlite_task *d,
		 unsigned id,
		 const char *address,
		 const char *dir)
{
	int rv;
	rv = config__init(&d->config, id, address);
	if (rv != 0) {
		goto err;
	}
	rv = vfsInit(&d->vfs, &d->config);
	if (rv != 0) {
		goto err_after_config_init;
	}
	registry__init(&d->registry, &d->config);
	rv = uv_loop_init(&d->loop);
	if (rv != 0) {
		/* TODO: better error reporting */
		rv = DQLITE_INTERNAL;
		goto err_after_vfs_init;
	}
	rv = raftProxyInit(&d->raft_transport, &d->loop);
	if (rv != 0) {
		goto err_after_loop_init;
	}
	rv = raft_uv_init(&d->raft_io, &d->loop, dir, &d->raft_transport);
	if (rv != 0) {
		/* TODO: better error reporting */
		rv = DQLITE_INTERNAL;
		goto err_after_raft_transport_init;
	}
	rv = fsm__init(&d->raft_fsm, &d->config, &d->registry);
	if (rv != 0) {
		goto err_after_raft_io_init;
	}
	rv = raft_ring_logger_init(&d->raft_logger, 1024 * 1024);
	if (rv != 0) {
		goto err_after_raft_fsm_init;
	}

	/* TODO: properly handle closing the dqlite server without running it */
	rv = raft_init(&d->raft, &d->raft_io, &d->raft_fsm, &d->raft_logger,
		       d->config.id, d->config.address);
	if (rv != 0) {
		dumpRaftRingLogger(d);
		return rv;
	}
	/* TODO: expose these values through some API */
	raft_set_election_timeout(&d->raft, 3000);
	raft_set_heartbeat_timeout(&d->raft, 500);
	raft_set_snapshot_threshold(&d->raft, 1024);
	raft_set_snapshot_trailing(&d->raft, 8192);
	raft_watch(&d->raft, raftWatch);
	rv = replication__init(&d->replication, &d->config, &d->raft);
	if (rv != 0) {
		goto err_after_raft_logger_init;
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
err_after_raft_logger_init:
	raft_ring_logger_close(&d->raft_logger);
err_after_raft_fsm_init:
	fsm__close(&d->raft_fsm);
err_after_raft_io_init:
	raft_uv_close(&d->raft_io);
err_after_raft_transport_init:
	raftProxyClose(&d->raft_transport);
err_after_loop_init:
	uv_loop_close(&d->loop);
err_after_vfs_init:
	vfsClose(&d->vfs);
err_after_config_init:
	config__close(&d->config);
err:
	return rv;
}

void dqlite__close(struct dqlite_task *d)
{
	int rv;
	rv = pthread_mutex_destroy(&d->mutex); /* This is a no-op on Linux . */
	assert(rv == 0);
	rv = sem_destroy(&d->stopped);
	assert(rv == 0); /* Fails only if sem object is not valid */
	rv = sem_destroy(&d->ready);
	assert(rv == 0); /* Fails only if sem object is not valid */
	raft_ring_logger_close(&d->raft_logger);
	replication__close(&d->replication);
	fsm__close(&d->raft_fsm);
	raft_uv_close(&d->raft_io);
	uv_loop_close(&d->loop);
	raftProxyClose(&d->raft_transport);
	registry__close(&d->registry);
	vfsClose(&d->vfs);
	config__close(&d->config);
}

dqlite_task_attr *dqlite_task_attr_create()
{
	dqlite_task_attr *a;
	a = sqlite3_malloc(sizeof *a);
	if (a == NULL) {
		return NULL;
	}
	memset(a, 0, sizeof *a);
	return a;
}

void dqlite_task_attr_destroy(dqlite_task_attr *a)
{
	sqlite3_free(a);
}

void dqlite_task_attr_set_connect_func(
    dqlite_task_attr *a,
    int (*f)(void *arg, unsigned id, const char *address, int *fd),
    void *arg)
{
	a->connect.f = f;
	a->connect.arg = arg;
}

static int maybeBootstrap(dqlite_task *d,
			  const unsigned id,
			  const char *address)
{
	struct raft_configuration configuration;
	int rv;
	if (id != 1) {
		return 0;
	}
	raft_configuration_init(&configuration);
	rv = raft_configuration_add(&configuration, id, address, true);
	if (rv != 0) {
		assert(rv == RAFT_NOMEM);
		rv = DQLITE_NOMEM;
		goto out;
	};
	rv = raft_bootstrap(&d->raft, &configuration);
	if (rv != 0) {
		if (rv == RAFT_CANTBOOTSTRAP) {
			rv = 0;
		} else {
			rv = DQLITE_INTERNAL;
		}
		goto out;
	}
out:
	raft_configuration_close(&configuration);
	return rv;
}

/* Callback invoked when the stop async handle gets fired.
 *
 * This callback will walk through all active handles and close them. After the
 * last handle (which must be the 'stop' async handle) is closed, the loop gets
 * stopped.
 */
static void raftCloseCb(struct raft *raft)
{
	struct dqlite_task *s = raft->data;
	uv_close((struct uv_handle_s *)&s->stop, NULL);
	uv_close((struct uv_handle_s *)&s->incoming, NULL);
	uv_close((struct uv_handle_s *)&s->startup, NULL);
}

static void destroy_conn(struct conn *conn)
{
	QUEUE__REMOVE(&conn->queue);
	sqlite3_free(conn);
}

static void process_incoming(struct dqlite_task *d)
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
	struct dqlite_task *d = stop->data;
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
	raft_close(&d->raft, raftCloseCb);
}

/* Callback invoked when the incoming async handle gets fired.
 *
 * This callback will scan the incoming queue and create new connections.
 */
static void incoming_cb(uv_async_t *incoming)
{
	struct dqlite_task *d = incoming->data;
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
	struct dqlite_task *d = startup->data;
	int rv;
	d->running = true;
	rv = sem_post(&d->ready);
	assert(rv == 0); /* No reason for which posting should fail */
}

static int taskRun(struct dqlite_task *d)
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
	 * unblock clients of taskReady. */
	d->startup.data = d;
	rv = uv_timer_init(&d->loop, &d->startup);
	assert(rv == 0);
	rv = uv_timer_start(&d->startup, startup_cb, 0, 0);
	assert(rv == 0);

	d->raft.data = d;
	rv = raft_start(&d->raft);
	if (rv != 0) {
		dumpRaftRingLogger(d);
		return rv;
	}

	rv = uv_run(&d->loop, UV_RUN_DEFAULT);
	assert(rv == 0);

	/* Unblock any client of taskReady */
	rv = sem_post(&d->ready);
	assert(rv == 0); /* no reason for which posting should fail */

	return 0;
}

static void *taskStart(void *arg)
{
	struct dqlite_task *t = arg;
	int rv;
	rv = taskRun(t);
	if (rv != 0) {
		uintptr_t result = rv;
		return (void *)result;
	}
	return NULL;
}

static void taskDestroy(dqlite_task *d)
{
	dqlite__close(d);
	sqlite3_free(d);
}

/* Wait until a dqlite server is ready and can handle connections.
**
** Returns true if the server has been successfully started, false otherwise.
**
** This is a thread-safe API, but must be invoked before any call to
** dqlite_stop or dqlite_handle.
*/
static bool taskReady(struct dqlite_task *d)
{
	/* Wait for the ready semaphore */
	sem_wait(&d->ready);
	return d->running;
}

int dqlite_task_start(unsigned id,
		      const char *address,
		      const char *dir,
		      dqlite_task_attr *attr,
		      dqlite_task **d)
{
	int rv;

	*d = sqlite3_malloc(sizeof **d);

	rv = dqlite__init(*d, id, address, dir);
	if (rv != 0) {
		return rv;
	}

	if (attr != NULL) {
		if (attr->connect.f != NULL) {
			raftProxySetConnectFunc(&(*d)->raft_transport,
						attr->connect.f,
						attr->connect.arg);
		}
	}

	rv = maybeBootstrap(*d, id, address);
	if (rv != 0) {
		goto err;
	}

	rv = pthread_create(&(*d)->thread, 0, &taskStart, *d);
	if (rv != 0) {
		goto err;
	}

	if (!taskReady(*d)) {
		rv = DQLITE_ERROR;
		goto err;
	}

	return 0;

err:
	taskDestroy(*d);
	return rv;
}

int dqlite_handle(dqlite_task *d, int fd)
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

int dqlite_task_stop(dqlite_task *d)
{
	void *result;
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

	rv = pthread_join(d->thread, &result);
	assert(rv == 0);

	rv = (uintptr_t)result;

	taskDestroy(d);

	return rv;
}

/* Set a config option */
int dqlite_config(struct dqlite_task *d, int op, ...)
{
	va_list args;
	int rv = 0;
	va_start(args, op);
	switch (op) {
		case DQLITE_CONFIG_LOGGER:
			d->config.logger.emit = va_arg(args, dqlite_emit);
			d->config.logger.data = va_arg(args, void *);
			break;
		case DQLITE_CONFIG_HEARTBEAT_TIMEOUT:
			d->config.heartbeat_timeout = *va_arg(args, unsigned *);
			break;
		case DQLITE_CONFIG_PAGE_SIZE:
			d->config.page_size = *va_arg(args, unsigned *);
			break;
		case DQLITE_CONFIG_CHECKPOINT_THRESHOLD:
			d->config.checkpoint_threshold =
			    *va_arg(args, unsigned *);
			break;
		case DQLITE_CONFIG_WATCHER:
			d->config.watcher.f = va_arg(args, dqlite_watch);
			d->config.watcher.data = va_arg(args, void *);
			break;
		default:
			rv = DQLITE_MISUSE;
			break;
	}
	va_end(args);
	return rv;
}

int dqlite_cluster(dqlite_task *d, struct dqlite_server *servers[], unsigned *n)
{
	unsigned i;
	/* TODO: this is not thread-safe, we should use an async handle */
	*n = d->raft.configuration.n;
	*servers = sqlite3_malloc(*n * sizeof **servers);
	if (*servers == NULL) {
		return DQLITE_NOMEM;
	}
	for (i = 0; i < *n; i++) {
		struct dqlite_server *server = &(*servers)[i];
		server->id = d->raft.configuration.servers[i].id;
		/* TODO: make a copy of the address? */
		server->address = d->raft.configuration.servers[i].address;
	}
	return 0;
}

bool dqlite_leader(dqlite_task *d, struct dqlite_server *server)
{
	/* TODO: this is not thread-safe, we should use an async handle */
	raft_leader(&d->raft, &server->id, &server->address);
	return server->id != 0;
}

int dqlite_dump(dqlite_task *d, const char *filename, void **buf, size_t *len)
{
	return vfsFileRead(d->config.name, filename, buf, len);
}
