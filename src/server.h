#include <raft.h>
#include <raft/io_uv.h>
#include <sqlite3.h>

#include "config.h"
#include "logger.h"
#include "registry.h"

struct dqlite
{
	struct config config;                       /* Config values */
	struct sqlite3_vfs vfs;                     /* In-memory VFS */
	struct registry registry;                   /* Databases */
	struct sqlite3_wal_replication replication; /* Raft replication */
	pthread_mutex_t mutex;                      /* Access incoming queue */
	uv_loop_t loop;                             /* UV loop */
	uv_async_t stop;                            /* Trigger UV loop stop */
	uv_async_t incoming;                        /* Trigger process queue */
	int running;                                /* Loop is running */
	sem_t ready;                                /* Server is ready */
	uv_timer_t startup;                         /* Unblock ready sem */
	sem_t stopped;                              /* Notifiy loop stopped */
	struct raft_io_uv_transport raft_transport; /* Raft libuv transport */
	struct raft_io raft_io;                     /* libuv I/O */
	struct raft_fsm raft_fsm;                   /* dqlite FSM */
	struct raft raft;                           /* Raft instance */
};

int dqlite__init(struct dqlite *d,
		 unsigned id,
		 const char *address,
		 const char *dir);
