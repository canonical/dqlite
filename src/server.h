#include <raft.h>
#include <raft/uv.h>
#include <sqlite3.h>

#ifdef __APPLE__
#include <dispatch/dispatch.h>
#else
#include <semaphore.h>
#endif

#include "config.h"
#include "id.h"
#include "lib/assert.h"
#include "logger.h"
#include "registry.h"

#define DQLITE_ERRMSG_BUF_SIZE 300

/**
 * A single dqlite server instance.
 */
struct dqlite_node
{
	bool initialized;                        /* dqlite__init succeeded */

	pthread_t thread;                        /* Main run loop thread. */
	struct config config;                    /* Config values */
	struct sqlite3_vfs vfs;                  /* In-memory VFS */
	struct registry registry;                /* Databases */
	struct uv_loop_s loop;                   /* UV loop */
	struct raft_uv_transport raft_transport; /* Raft libuv transport */
	struct raft_io raft_io;                  /* libuv I/O */
	struct raft_fsm raft_fsm;                /* dqlite FSM */
#ifdef __APPLE__
	dispatch_semaphore_t ready;              /* Server is ready */
	dispatch_semaphore_t stopped;            /* Notifiy loop stopped */
#else
	sem_t ready;                             /* Server is ready */
	sem_t stopped;                           /* Notifiy loop stopped */
#endif
	queue queue;                             /* Incoming connections */
	queue conns;                             /* Active connections */
	bool running;                            /* Loop is running */
	struct raft raft;                        /* Raft instance */
	struct uv_stream_s *listener;            /* Listening socket */
	struct uv_async_s stop;                  /* Trigger UV loop stop */
	struct uv_timer_s startup;               /* Unblock ready sem */
	struct uv_prepare_s monitor;             /* Raft state change monitor */
	int raft_state;                          /* Previous raft state */
	char *bind_address;                      /* Listen address */
	char errmsg[DQLITE_ERRMSG_BUF_SIZE];     /* Last error occurred */
	struct id_state random_state;            /* For seeding ID generation */
};

int dqlite__init(struct dqlite_node *d,
		 dqlite_node_id id,
		 const char *address,
		 const char *dir);

void dqlite__close(struct dqlite_node *d);

int dqlite__run(struct dqlite_node *d);
