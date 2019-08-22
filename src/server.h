#include <raft.h>
#include <raft/uv.h>
#include <sqlite3.h>

#include "config.h"
#include "lib/assert.h"
#include "logger.h"
#include "registry.h"

/**
 * A single dqlite server instance.
 */
struct dqlite_task
{
	pthread_t thread;                           /* Main run loop thread. */
	struct config config;                       /* Config values */
	struct sqlite3_vfs vfs;                     /* In-memory VFS */
	struct registry registry;                   /* Databases */
	struct uv_loop_s loop;                      /* UV loop */
	struct raft_uv_transport raft_transport;    /* Raft libuv transport */
	struct raft_io raft_io;                     /* libuv I/O */
	struct raft_fsm raft_fsm;                   /* dqlite FSM */
	struct raft_logger raft_logger;             /* Raft logger wrapper */
	struct sqlite3_wal_replication replication; /* Raft replication */
	sem_t ready;                                /* Server is ready */
	sem_t stopped;                              /* Notifiy loop stopped */
	pthread_mutex_t mutex;                      /* Access incoming queue */
	queue queue;                                /* Incoming connections */
	queue conns;                                /* Active connections */
	bool running;                               /* Loop is running */
	struct raft raft;                           /* Raft instance */
	struct uv_stream_s *listener;               /* Listening socket */
	struct uv_async_s stop;                     /* Trigger UV loop stop */
	struct uv_async_s incoming;                 /* Trigger process queue */
	struct uv_timer_s startup;                  /* Unblock ready sem */
};

int dqlite__init(struct dqlite_task *d,
		 unsigned id,
		 const char *address,
		 const char *dir,
		 int listen_fd);

void dqlite__close(struct dqlite_task *d);

int dqlite__run(struct dqlite_task *d);
