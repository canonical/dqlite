#ifndef DQLITE_SERVER_H
#define DQLITE_SERVER_H

#include <sqlite3.h>

#include <semaphore.h>

#include "client/protocol.h"
#include "config.h"
#include "id.h"
#include "lib/assert.h"
#include "logger.h"
#include "raft.h"
#include "registry.h"
#include "lib/threadpool.h"

#define DQLITE_ERRMSG_BUF_SIZE 300

/**
 * A single dqlite server instance.
 */
struct dqlite_node
{
	bool initialized; /* dqlite__init succeeded */

	pthread_t thread;                        /* Main run loop thread. */
	struct config config;                    /* Config values */
	struct sqlite3_vfs vfs;                  /* In-memory VFS */
	struct registry registry;                /* Databases */
	struct uv_loop_s loop;                   /* UV loop */
	struct pool_s pool;			 /* Thread pool */
	struct raft_uv_transport raft_transport; /* Raft libuv transport */
	struct raft_io raft_io;                  /* libuv I/O */
	struct raft_fsm raft_fsm;                /* dqlite FSM */
	sem_t ready;                             /* Server is ready */
	sem_t stopped;                           /* Notify loop stopped */
	sem_t handover_done;
	queue queue; /* Incoming connections */
	queue conns; /* Active connections */
	queue roles_changes;
	bool running;                 /* Loop is running */
	struct raft raft;             /* Raft instance */
	struct uv_stream_s *listener; /* Listening socket */
	struct uv_async_s handover;
	int handover_status;
	void (*handover_done_cb)(struct dqlite_node *, int);
	struct uv_async_s stop;    /* Trigger UV loop stop */
	struct uv_timer_s startup; /* Unblock ready sem */
	struct uv_timer_s timer;
	int raft_state;     /* Previous raft state */
	char *bind_address; /* Listen address */
	bool role_management;
	int (*connect_func)(
	    void *,
	    const char *,
	    int *);             /* Connection function for role management */
	void *connect_func_arg; /* User data for connection function */
	char errmsg[DQLITE_ERRMSG_BUF_SIZE]; /* Last error occurred */
	struct id_state random_state;        /* For seeding ID generation */
};

/* Dynamic array of node info objects. This is the in-memory representation of
 * the node store. */
struct node_store_cache
{
	struct client_node_info *nodes; /* owned */
	unsigned len;
	unsigned cap;
};

struct dqlite_server
{
	/* Threading stuff: */
	pthread_cond_t cond;
	pthread_mutex_t mutex;
	pthread_t refresh_thread;

	/* These fields are protected by the mutex: */
	bool shutdown;
	struct node_store_cache cache;
	/* We try to keep this pointing at the leader, but it might be out of
	 * date or not open. */
	struct client_proto proto;

	/* These fields are only accessed on the main thread: */
	bool started;
	bool is_new;
	bool bootstrap;
	char *dir_path; /* owned */
	dqlite_node *local;
	uint64_t local_id;
	char *local_addr; /* owned */
	char *bind_addr;  /* owned */
	dqlite_connect_func connect;
	void *connect_arg;
	unsigned long long refresh_period; /* in milliseconds */
	int dir_fd;
};

int dqlite__init(struct dqlite_node *d,
		 dqlite_node_id id,
		 const char *address,
		 const char *dir);

void dqlite__close(struct dqlite_node *d);

int dqlite__run(struct dqlite_node *d);

#endif
