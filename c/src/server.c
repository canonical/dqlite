#define _GNU_SOURCE

#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#include <uv.h>
#include <sqlite3.h>

#include "log.h"
#include "error.h"
#include "queue.h"
#include "conn.h"
#include "dqlite.h"

int dqlite_init(const char **errmsg) {
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
struct dqlite__server {
	/* read-only */
	dqlite__error        error;    /* Last error occurred, if any */

	/* private */
	FILE                *log;      /* Log output stream */
	dqlite_cluster      *cluster;  /* Cluster implementation */
	struct dqlite__queue queue;    /* Queue of incoming connections */
	pthread_mutex_t      mutex;    /* Serialize access to incoming queue */
	uv_loop_t            loop;     /* UV loop */
	uv_async_t           stop;     /* Event to stop the UV loop */
	uv_async_t           incoming; /* Event to process the incoming queue */
	int                  running;  /* Indicate that the loop is running */
	sem_t                ready;    /* Notifiy that the loop is running */
	uv_timer_t           startup;  /* Used for unblocking the ready sem */
	sem_t                stopped;  /* Notifiy that the loop has been stopped */
};

/* Close callback for the 'stop' async event handle
 *
 * This callback must be fired when *all* other UV handles have been closed and
 * it's hence safe to stop the loop.
 */
static void dqlite__server_stop_close_cb(uv_handle_t *stop)
{
	struct dqlite__server* s;

	assert(stop != NULL);
	assert(stop->data != NULL);

	s = (struct dqlite__server*)stop->data;

	/* All handles must have been closed */
	assert(!uv_loop_alive(&s->loop));

	uv_stop(&s->loop);
}

/* Callback for the uv_walk() call in dqlite__server_stop_cb.
 *
 * This callback gets fired once for every active handle in the UV loop and is
 * in charge of closing each of them.
 */
static void dqlite__server_stop_walk_cb(uv_handle_t *handle, void *arg)
{
	struct dqlite__server* s;
	struct dqlite__conn *conn;
	uv_close_cb callback;

	assert(handle != NULL);
	assert(arg != NULL);
	assert(
		handle->type == UV_ASYNC ||
		handle->type == UV_TIMER ||
		handle->type == UV_TCP);

	s = (struct dqlite__server*)arg;

	switch(handle->type) {

	case UV_ASYNC:
		assert(
			handle == (uv_handle_t*)&s->stop ||
			handle == (uv_handle_t*)&s->incoming);

		callback = NULL;

		/* FIXME: here we rely on the fact that the stop handle is
                 *        the last one to be walked into. This behavior is not
                 *        advertised by the libuv docs and hence might change.
                 */
		if (handle == (uv_handle_t*)&s->stop)
			callback = dqlite__server_stop_close_cb;

		uv_close(handle, callback);

		break;

	case UV_TCP:
		assert(handle->data != NULL);

		conn = (struct dqlite__conn*)handle->data;

		/* Abort the client connection and release any allocated
		 * resources. */
		dqlite__conn_abort(conn);
		dqlite__conn_close(conn);

		sqlite3_free(conn);

		break;

	case UV_TIMER:
		/* Double check that this is not the startup timer which gets
		 * closed at startup time. */
		assert(handle != (uv_handle_t*)&s->startup);

		/* This must be a timer created by a conn object, which gets
		 * closed by the dqlite__conn_abort call above, so there's
		 * nothing to do in that case. */

		break;

	default:
		/* Should not be reached because we assert all possible handle
		 * types above */
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
static void dqlite__server_stop_cb(uv_async_t *stop)
{
	struct dqlite__server *s;

	assert(stop != NULL);
	assert(stop->data != NULL);

	s = (struct dqlite__server*)stop->data;

	/* We expect that we're being executed after dqlite_server_stop and so
	 * the running flag is off. */
	assert(!s->running);

	/* Give a final pass to the incoming queue, to unblock any call to
	 * dqlite_server_handle that might be blocked. There's no need to
	 * acquire the mutex since now the running flag is off and no new
	 * incoming connection can be enqueued. */
	dqlite__queue_process(&s->queue);

	/* Loop through all connections and abort them, then stop the event
	 * loop. */
	uv_walk(&s->loop, dqlite__server_stop_walk_cb, (void*)s);
}

/* Callback invoked when the incoming async handle gets fired.
 *
 * This callback will scan the incoming queue and create new connections.
 */
static void dqlite__server_incoming_cb(uv_async_t *incoming){
	struct dqlite__server *s;

	assert(incoming != NULL);
	assert(incoming->data != NULL);
	s = (struct dqlite__server*)incoming->data;

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

	s = (struct dqlite__server*)startup->data;

	/* Close the handle, since we're not going to need it anymore. */
	uv_close((uv_handle_t*)startup, NULL);

	s->running = 1;

	err = sem_post(&s->ready);
	assert(err == 0); /* No reason for which posting should fail */
}

/* Perform all memory allocations needed to create a dqlite_server object. */
dqlite_server *dqlite_server_alloc()
{
	struct dqlite__server *s;

	s = (struct dqlite__server*)(sqlite3_malloc(sizeof(*s)));
	if(s == NULL)
		goto err_alloc;

	return s;

 err_alloc:
	return NULL;
}

/* Release all memory allocated in dqlite_server_alloc() */
void dqlite_server_free(dqlite_server *s)
{
	assert(s != NULL);

	sqlite3_free(s);
}

/* Initialize internal state */
int dqlite_server_init(dqlite_server *s, FILE *log, dqlite_cluster *cluster)
{
	int err;

	assert(s != NULL);
	assert(log != NULL);
	assert(cluster != NULL);

	dqlite__error_init(&s->error);

	s->log = log;
	s->cluster = cluster;

	dqlite__queue_init(&s->queue);

	err = pthread_mutex_init(&s->mutex, NULL);
	assert(err == 0); /* Docs say that pthread_mutex_init can't fail */

	err = uv_loop_init(&s->loop);
	if (err != 0) {
		dqlite__error_uv(&s->error, err, "failed to init event loop");
		return DQLITE_ERROR;
	}

	err = uv_async_init(&s->loop, &s->stop, dqlite__server_stop_cb);
	if (err !=0) {
		dqlite__error_uv(&s->error, err, "failed to init stop event handle");
		return DQLITE_ERROR;
	}
	s->stop.data = (void*)s;

	err = uv_async_init(&s->loop, &s->incoming, dqlite__server_incoming_cb);
	if (err !=0) {
		dqlite__error_uv(&s->error, err, "failed to init accept event handle");
		return DQLITE_ERROR;
	}
	s->incoming.data = (void*)s;

	err = sem_init(&s->ready, 0, 0);
	if (err !=0) {
		dqlite__error_sys(&s->error, "failed to init ready semaphore");
		return DQLITE_ERROR;
	}

	err = uv_timer_init(&s->loop, &s->startup);
	if (err != 0) {
		dqlite__error_uv(&s->error, err, "failed to init timer");
		return DQLITE_ERROR;
	}
	s->startup.data = (void*)s;

	/* Schedule dqlite__service_startup_cb to be fired as soon as the loop
	 * starts. It will unblock clients of dqlite_service_ready. */
	err = uv_timer_start(&s->startup, dqlite__service_startup_cb, 0, 0);
	if (err != 0) {
		dqlite__error_uv(&s->error, err, "failed to startup timer");
		return DQLITE_ERROR;
	}

	err = sem_init(&s->stopped, 0, 0);
	if (err !=0) {
		dqlite__error_sys(&s->error, "failed to init stopped semaphore");
		return DQLITE_ERROR;
	}


	s->running = 0;

	return 0;
}

void dqlite_server_close(dqlite_server *s)
{
	int err;

	assert(s != NULL);

	/* The sem_destroy call should only fail if the given semaphore is
	 * invalid, which must not be our case. */
	err = sem_destroy(&s->ready);
	assert(err == 0);

	err = sem_destroy(&s->stopped);
	assert(err == 0);

	dqlite__queue_close(&s->queue);
	dqlite__error_close(&s->error);

}

/* Set a config option */
int dqlite_server_config(dqlite_server *s, int op, void *arg)
{
	int err = 0;

	assert(s != NULL);
	(void)arg;

	switch (op) {

	default:
		dqlite__error_printf(&s->error, "unknown op code %d", op);
		err = DQLITE_ERROR;
		break;
	}

	return err;
}

int dqlite_server_run(struct dqlite__server *s){
	int err;

	assert(s != NULL);

	dqlite__infof(s, "run event loop", "");

	err = uv_run(&s->loop, UV_RUN_DEFAULT);
	if (err != 0) {
		dqlite__error_uv(&s->error, err, "event loop finished unclealy");
		goto out;
	}

	err = uv_loop_close(&s->loop);
	if (err != 0) {
		dqlite__error_uv(&s->error, err, "failed to close event loop");
		goto out;
	}

 out:
	/* Unblock any client of dqlite_server_ready (no reason for which
	 * posting should fail). */
	assert(sem_post(&s->ready) == 0);

	/* Flush the log, but ignore errors */
	fflush(s->log);

	return err;
}

int dqlite_server_ready(dqlite_server *s)
{
	assert(s != NULL);

	/* Wait for the ready semaphore */
	sem_wait(&s->ready);

	return s->running;
}

int dqlite_server_stop(dqlite_server *s, char **errmsg){
	int err = 0;
	dqlite__error e;

	assert(s != NULL);
	assert(errmsg != NULL);

	dqlite__debugf(s, "stop event loop", "");

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
		if (err != 0)
			*errmsg = "error message unavailable (out of memory)";
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
int dqlite_server_handle(dqlite_server *s, int socket, char **errmsg){
	int err;
	dqlite__error e;
	struct dqlite__conn *conn;
	struct dqlite__queue_item item;

	assert(s != NULL);
	assert(s->log != NULL);
	assert(s->cluster != NULL);

	dqlite__debugf(s, "new connection", "socket=%d", socket);

	/* Create an error instance since the one on d is not thread-safe */
	dqlite__error_init(&e);

	conn = (struct dqlite__conn*)sqlite3_malloc(sizeof(*conn));
	if (conn == NULL) {
		dqlite__error_oom(&e, "failed to allocate connection");
		err = DQLITE_NOMEM;
		goto err_conn_malloc;
	}
	dqlite__conn_init(conn, s->log, socket, s->cluster, &s->loop);

	err = dqlite__queue_item_init(&item, conn);
	if (err != 0) {
		dqlite__error_printf(&e, "failed to init incoming queue item: %s", strerror(errno));
		err = DQLITE_ERROR;
		goto err_item_init;
	}

	pthread_mutex_lock(&s->mutex);

	if (!s->running) {
		err = DQLITE_STOPPED;
		dqlite__error_printf(&e, "server is not running");
		goto err_queue_push;
	}

	err = dqlite__queue_push(&s->queue, &item);
	if (err != 0) {
		dqlite__error_wrapf(&e, &s->queue.error, "failed to push incoming queue item");
		goto err_queue_push;
	}

	err = uv_async_send(&s->incoming);
	if (err != 0) {
		dqlite__error_uv(&e, err, "failed to fire incoming connection event");
		err = DQLITE_ERROR;
		goto err_incoming_send;
	}

	pthread_mutex_unlock(&s->mutex);

	dqlite__debugf(s, "wait connection ready", "socket=%d", socket);

	dqlite__queue_item_wait(&item);

	if(!dqlite__error_is_null(&item.error)) {
		dqlite__error_wrapf(&e, &item.error, "failed to process incoming queue item");
		err = DQLITE_ERROR;
		goto err_item_wait;
	}

	dqlite__error_close(&e);
	dqlite__queue_item_close(&item);

	return 0;

 err_incoming_send:
	dqlite__queue_pop(&s->queue);

 err_queue_push:
	pthread_mutex_unlock(&s->mutex);

 err_item_wait:
	dqlite__queue_item_close(&item);

 err_item_init:
	dqlite__conn_close(conn);
	sqlite3_free(conn);

 err_conn_malloc:
	err = dqlite__error_copy(&e, errmsg);
	if (err != 0)
		*errmsg = "error message unavailable (out of memory)";

	dqlite__error_close(&e);

	return err;
}

const char* dqlite_server_errmsg(dqlite_server *s){
	return s->error;
}

dqlite_cluster *dqlite_server_cluster(dqlite_server *s)
{
	assert(s != NULL);

	return s->cluster;
}

