#define _GNU_SOURCE

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include <uv.h>
#include <sqlite3.h>

#include "log.h"
#include "error.h"
#include "queue.h"
#include "conn.h"
#include "dqlite.h"

struct dqlite__service {
	/* read-only */
	dqlite__error   error;   /* Last error occurred, if any */
	FILE           *log;     /* Log output stream */
	dqlite_cluster *cluster; /* Cluster implementation */

	/* private */
	struct dqlite__queue queue; /* Queue of incoming connections */
	uv_loop_t       loop;       /* UV loop */
	uv_async_t      stop;       /* Event to stop the UV loop */
	uv_async_t      incoming;   /* Event to process incoming connections */
	sqlite3_mutex  *mutex;      /* Serialize access to the incoming queue */
};

/* Close callback for the stop event handle */
static void dqlite__service_stop_close_cb(uv_handle_t *stop)
{
	struct dqlite__service* s;

	assert(stop != NULL);

	s = (struct dqlite__service*)stop->data;

	uv_stop(&s->loop);
}

/* Callback for the uv_walk() call in dqlite__service_stop_cb */
static void dqlite__service_stop_walk_cb(uv_handle_t *handle, void *arg)
{
	struct dqlite__service* s;
	struct dqlite__conn *conn;
	uv_close_cb callback;

	assert(handle != NULL);
	assert(arg != NULL);
	assert(
		handle->type == UV_ASYNC ||
		handle->type == UV_TCP);

	s = (struct dqlite__service*)arg;

	switch(handle->type) {

	case UV_ASYNC:
		assert(
			handle == (uv_handle_t*)&s->stop ||
			handle == (uv_handle_t*)&s->incoming);

		callback = NULL;

		/* FIXME: here we rely on the fact that the stop handle is
                 *        the last one to walked into. This behavior is not
                 *        advertised by the libuv docs and hence might change.
                 */
		if (handle == (uv_handle_t*)&s->stop)
			callback = dqlite__service_stop_close_cb;

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

	default:
		break;
	}
}

/* Callback invoked when the stop async handle gets fired. */
static void dqlite__service_stop_cb(uv_async_t *stop)
{
	struct dqlite__service *s;

	assert(stop != NULL);
	assert(stop->data != NULL);

	s = (struct dqlite__service*)stop->data;

	assert(s != NULL);

	/* Loop through all connections and abort them, then stop the event
	 * loop. */
	uv_walk(&s->loop, dqlite__service_stop_walk_cb, (void*)s);
}

/* Callback invoked when the incoming async handle gets fired. */
static void dqlite__service_incoming_cb(uv_async_t *incoming){
	struct dqlite__service *s;

	assert(incoming != NULL);
	assert(incoming->data != NULL);

	s = (struct dqlite__service*)incoming->data;

	assert(s != NULL );
	assert(s->mutex != NULL);

	/* Acquire the queue lock, so no new incoming connection can be
	 * pushed. */
	sqlite3_mutex_enter(s->mutex);

	dqlite__queue_process(&s->queue, &s->loop);

	sqlite3_mutex_leave(s->mutex);

}

dqlite_service *dqlite_service_alloc()
{
	struct dqlite__service *s;

	s = (struct dqlite__service*)(sqlite3_malloc(sizeof(*s)));
	if(s == NULL)
		goto err_alloc;

	s->mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
	if (s->mutex == NULL)
		goto err_mutex_alloc;

	return s;

 err_mutex_alloc:
	sqlite3_free(s);

 err_alloc:
	return NULL;
}

void dqlite_service_free(dqlite_service *s)
{
	assert(s != NULL);

	sqlite3_mutex_free(s->mutex);
	sqlite3_free(s);
}

int dqlite_service_init(dqlite_service *s, FILE *log, dqlite_cluster *cluster)
{
	int err;

	assert(s != NULL);
	assert(log != NULL);
	assert(cluster != NULL);

	dqlite__error_init(&s->error);

	s->log = log;
	s->cluster = cluster;

	dqlite__queue_init(&s->queue);

	err = uv_loop_init(&s->loop);
	if (err != 0) {
		dqlite__error_uv(&s->error, err, "failed to init event loop");
		return DQLITE_ERROR;
	}

	err = uv_async_init(&s->loop, &s->stop, dqlite__service_stop_cb);
	if (err !=0) {
		dqlite__error_uv(&s->error, err, "failed to init stop event handle");
		return DQLITE_ERROR;
	}
	s->stop.data = (void*)s;

	err = uv_async_init(&s->loop, &s->incoming, dqlite__service_incoming_cb);
	if (err !=0) {
		dqlite__error_uv(&s->error, err, "failed to init accept event handle");
		return DQLITE_ERROR;
	}
	s->incoming.data = (void*)s;

	return 0;
}

void dqlite_service_close(dqlite_service *s)
{
	assert(s != NULL);

	dqlite__queue_close(&s->queue);
	dqlite__error_close(&s->error);
}

int dqlite_service_run(struct dqlite__service *s){
	int err;

	assert(s != NULL);

	dqlite__infof(s, "run event loop", "");

	err = uv_run(&s->loop, UV_RUN_DEFAULT);
	if (err != 0) {
		dqlite__error_uv(&s->error, err, "event loop finished unclealy");
		return DQLITE_ERROR;
	}

	dqlite__infof(s, "event loop done", "");

	err = uv_loop_close(&s->loop);
	if (err != 0) {
		dqlite__error_uv(&s->error, err, "failed to close event loop");
		return DQLITE_ERROR;
	}

	return 0;
}

int dqlite_service_stop(dqlite_service *s, char **errmsg){
	int err = 0;
	dqlite__error e;

	assert(s != NULL);
	assert(s->mutex != NULL);
	assert(errmsg != NULL);

	dqlite__debugf(s, "stop event loop", "");

	/* Create an error instance since the one on d is not thread-safe */
	dqlite__error_init(&e);

	err = uv_async_send(&s->stop);
	if (err != 0) {
		dqlite__error_uv(&e, err, "failed to fire stop event");
		err = dqlite__error_copy(&e, errmsg);
		if (err != 0)
			*errmsg = "error message unavailable (out of memory)";
		err = DQLITE_ERROR;
	}

	dqlite__error_close(&e);

	return err;
}

/* Start handling a new connection */
int dqlite_service_handle(dqlite_service *s, int socket, char **errmsg){
	int err;
	dqlite__error e;
	struct dqlite__conn *conn;
	struct dqlite__queue_item item;

	assert(s != NULL);
	assert(s->mutex != NULL);
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
	dqlite__conn_init(conn, s->log, socket, s->cluster);

	err = dqlite__queue_item_init(&item, conn);
	if (err != 0) {
		dqlite__error_printf(&e, "failed to init incoming queue item: %s", strerror(errno));
		err = DQLITE_ERROR;
		goto err_item_init;
	}

	sqlite3_mutex_enter(s->mutex);

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

	sqlite3_mutex_leave(s->mutex);

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
	sqlite3_mutex_leave(s->mutex);

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

const char* dqlite_service_errmsg(dqlite_service *s){
	return s->error;
}
