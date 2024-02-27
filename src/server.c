#include "server.h"

#include <errno.h>
#include <stdlib.h>
#include <sys/un.h>
#include <time.h>

#include "../include/dqlite.h"
#include "client/protocol.h"
#include "conn.h"
#include "fsm.h"
#include "id.h"
#include "lib/addr.h"
#include "lib/assert.h"
#include "lib/fs.h"
#include "logger.h"
#include "protocol.h"
#include "roles.h"
#include "src/lib/threadpool.h"
#include "tracing.h"
#include "translate.h"
#include "transport.h"
#include "utils.h"
#include "vfs.h"

/* Special ID for the bootstrap node. Equals to raft_digest("1", 0). */
#define BOOTSTRAP_ID 0x2dc171858c3155be

#define DATABASE_DIR_FMT "%s/database"

#define NODE_STORE_INFO_FORMAT_V1 "v1"

/* Called by raft every time the raft state changes. */
static void state_cb(struct raft *r,
		     unsigned short old_state,
		     unsigned short new_state)
{
	struct dqlite_node *d = r->data;
	queue *head;
	struct conn *conn;

	if (old_state == RAFT_LEADER && new_state != RAFT_LEADER) {
		tracef("node %llu@%s: leadership lost", r->id, r->address);
		QUEUE__FOREACH(head, &d->conns)
		{
			conn = QUEUE__DATA(head, struct conn, queue);
			gateway__leader_close(&conn->gateway,
					      RAFT_LEADERSHIPLOST);
		}
	}
}

int dqlite__init(struct dqlite_node *d,
		 dqlite_node_id id,
		 const char *address,
		 const char *dir)
{
	int rv;
	char db_dir_path[1024];
	int urandom;
	ssize_t count;

	d->initialized = false;
	memset(d->errmsg, 0, sizeof d->errmsg);

	rv = snprintf(db_dir_path, sizeof db_dir_path, DATABASE_DIR_FMT, dir);
	if (rv == -1 || rv >= (int)(sizeof db_dir_path)) {
		snprintf(d->errmsg, DQLITE_ERRMSG_BUF_SIZE,
			 "failed to init: snprintf(rv:%d)", rv);
		goto err;
	}

	rv = config__init(&d->config, id, address, db_dir_path);
	if (rv != 0) {
		snprintf(d->errmsg, DQLITE_ERRMSG_BUF_SIZE,
			 "config__init(rv:%d)", rv);
		goto err;
	}
	rv = VfsInit(&d->vfs, d->config.name);
	sqlite3_vfs_register(&d->vfs, 0);
	if (rv != 0) {
		goto err_after_config_init;
	}
	registry__init(&d->registry, &d->config);

	rv = uv_loop_init(&d->loop);
	if (rv != 0) {
		snprintf(d->errmsg, DQLITE_ERRMSG_BUF_SIZE,
			 "uv_loop_init(): %s", uv_strerror(rv));
		rv = DQLITE_ERROR;
		goto err_after_vfs_init;
	}
	rv = pool_init(&d->pool, &d->loop, 4, POOL_QOS_PRIO_FAIR);
	if (rv != 0) {
		snprintf(d->errmsg, DQLITE_ERRMSG_BUF_SIZE,
			 "pool_init(): %s", uv_strerror(rv));
		rv = DQLITE_ERROR;
		goto err_after_loop_init;
	}
	rv = raftProxyInit(&d->raft_transport, &d->loop);
	if (rv != 0) {
		goto err_after_pool_init;
	}
	rv = raft_uv_init(&d->raft_io, &d->loop, dir, &d->raft_transport);
	if (rv != 0) {
		snprintf(d->errmsg, DQLITE_ERRMSG_BUF_SIZE,
			 "raft_uv_init(): %s", d->raft_io.errmsg);
		rv = DQLITE_ERROR;
		goto err_after_raft_transport_init;
	}
	rv = fsm__init(&d->raft_fsm, &d->config, &d->registry);
	if (rv != 0) {
		goto err_after_raft_io_init;
	}

	/* TODO: properly handle closing the dqlite server without running it */
	rv = raft_init(&d->raft, &d->raft_io, &d->raft_fsm, d->config.id,
		       d->config.address);
	if (rv != 0) {
		snprintf(d->errmsg, DQLITE_ERRMSG_BUF_SIZE, "raft_init(): %s",
			 raft_errmsg(&d->raft));
		return DQLITE_ERROR;
	}
	/* TODO: expose these values through some API */
	raft_set_election_timeout(&d->raft, 3000);
	raft_set_heartbeat_timeout(&d->raft, 500);
	raft_set_snapshot_threshold(&d->raft, 1024);
	raft_set_snapshot_trailing(&d->raft, 8192);
	raft_set_pre_vote(&d->raft, true);
	raft_set_max_catch_up_rounds(&d->raft, 100);
	raft_set_max_catch_up_round_duration(&d->raft, 50 * 1000); /* 50 secs */
	raft_register_state_cb(&d->raft, state_cb);
	rv = sem_init(&d->ready, 0, 0);
	if (rv != 0) {
		snprintf(d->errmsg, DQLITE_ERRMSG_BUF_SIZE, "sem_init(): %s",
			 strerror(errno));
		rv = DQLITE_ERROR;
		goto err_after_raft_fsm_init;
	}
	rv = sem_init(&d->stopped, 0, 0);
	if (rv != 0) {
		snprintf(d->errmsg, DQLITE_ERRMSG_BUF_SIZE, "sem_init(): %s",
			 strerror(errno));
		rv = DQLITE_ERROR;
		goto err_after_ready_init;
	}
	rv = sem_init(&d->handover_done, 0, 0);
	if (rv != 0) {
		snprintf(d->errmsg, DQLITE_ERRMSG_BUF_SIZE, "sem_init(): %s",
			 strerror(errno));
		rv = DQLITE_ERROR;
		goto err_after_stopped_init;
	}

	QUEUE__INIT(&d->queue);
	QUEUE__INIT(&d->conns);
	QUEUE__INIT(&d->roles_changes);
	d->raft_state = RAFT_UNAVAILABLE;
	d->running = false;
	d->listener = NULL;
	d->bind_address = NULL;
	d->role_management = false;
	d->connect_func = transportDefaultConnect;
	d->connect_func_arg = NULL;

	urandom = open("/dev/urandom", O_RDONLY);
	assert(urandom != -1);
	count = read(urandom, d->random_state.data, sizeof(uint64_t[4]));
	(void)count;
	close(urandom);
	d->initialized = true;
	return 0;

err_after_stopped_init:
	sem_destroy(&d->stopped);
err_after_ready_init:
	sem_destroy(&d->ready);
err_after_raft_fsm_init:
	fsm__close(&d->raft_fsm);
err_after_raft_io_init:
	raft_uv_close(&d->raft_io);
err_after_raft_transport_init:
	raftProxyClose(&d->raft_transport);
err_after_pool_init:
	/* TODO: check if this is a proper place to close the pool */
	pool_close(&d->pool);
	pool_fini(&d->pool);
err_after_loop_init:
	uv_loop_close(&d->loop);
err_after_vfs_init:
	VfsClose(&d->vfs);
err_after_config_init:
	config__close(&d->config);
err:
	return rv;
}

void dqlite__close(struct dqlite_node *d)
{
	int rv;
	if (!d->initialized) {
		return;
	}
	raft_free(d->listener);
	rv = sem_destroy(&d->stopped);
	assert(rv == 0); /* Fails only if sem object is not valid */
	rv = sem_destroy(&d->ready);
	assert(rv == 0); /* Fails only if sem object is not valid */
	rv = sem_destroy(&d->handover_done);
	assert(rv == 0);
	fsm__close(&d->raft_fsm);
	// TODO assert rv of uv_loop_close after fixing cleanup logic related to
	// the TODO above referencing the cleanup logic without running the
	// node. See https://github.com/canonical/dqlite/issues/504.

	/* TODO: check if this is a proper place to close the pool */
	/* pool_close(&d->pool); */
	pool_fini(&d->pool);
	uv_loop_close(&d->loop);
	raftProxyClose(&d->raft_transport);
	registry__close(&d->registry);
	sqlite3_vfs_unregister(&d->vfs);
	VfsClose(&d->vfs);
	config__close(&d->config);
	if (d->bind_address != NULL) {
		sqlite3_free(d->bind_address);
	}
}

int dqlite_node_create(dqlite_node_id id,
		       const char *address,
		       const char *data_dir,
		       dqlite_node **t)
{
	*t = sqlite3_malloc(sizeof **t);
	if (*t == NULL) {
		return DQLITE_NOMEM;
	}

	return dqlite__init(*t, id, address, data_dir);
}

int dqlite_node_set_bind_address(dqlite_node *t, const char *address)
{
	/* sockaddr_un is large enough for our purposes */
	struct sockaddr_un addr_un;
	struct sockaddr *addr = (struct sockaddr *)&addr_un;
	socklen_t addr_len = sizeof(addr_un);
	sa_family_t domain;
	size_t path_len;
	int fd;
	int rv;
	if (t->running) {
		return DQLITE_MISUSE;
	}

	rv =
	    AddrParse(address, addr, &addr_len, "8080", DQLITE_ADDR_PARSE_UNIX);
	if (rv != 0) {
		return rv;
	}
	domain = addr->sa_family;

	fd = socket(domain, SOCK_STREAM, 0);
	if (fd == -1) {
		return DQLITE_ERROR;
	}
	rv = fcntl(fd, FD_CLOEXEC);
	if (rv != 0) {
		close(fd);
		return DQLITE_ERROR;
	}

	if (domain == AF_INET || domain == AF_INET6) {
		int reuse = 1;
		rv = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
				(const char *)&reuse, sizeof(reuse));
		if (rv != 0) {
			close(fd);
			return DQLITE_ERROR;
		}
	}

	rv = bind(fd, addr, addr_len);
	if (rv != 0) {
		close(fd);
		return DQLITE_ERROR;
	}

	rv = transport__stream(&t->loop, fd, &t->listener);
	if (rv != 0) {
		close(fd);
		return DQLITE_ERROR;
	}

	if (domain == AF_INET || domain == AF_INET6) {
		int sz = ((int)strlen(address)) + 1; /* Room for '\0' */
		t->bind_address = sqlite3_malloc(sz);
		if (t->bind_address == NULL) {
			close(fd);
			return DQLITE_NOMEM;
		}
		strcpy(t->bind_address, address);
	} else {
		path_len = sizeof addr_un.sun_path;
		t->bind_address = sqlite3_malloc((int)path_len);
		if (t->bind_address == NULL) {
			close(fd);
			return DQLITE_NOMEM;
		}
		memset(t->bind_address, 0, path_len);
		rv = uv_pipe_getsockname((struct uv_pipe_s *)t->listener,
					 t->bind_address, &path_len);
		if (rv != 0) {
			close(fd);
			sqlite3_free(t->bind_address);
			t->bind_address = NULL;
			return DQLITE_ERROR;
		}
		t->bind_address[0] = '@';
	}

	return 0;
}

const char *dqlite_node_get_bind_address(dqlite_node *t)
{
	return t->bind_address;
}

int dqlite_node_set_connect_func(dqlite_node *t,
				 int (*f)(void *arg,
					  const char *address,
					  int *fd),
				 void *arg)
{
	if (t->running) {
		return DQLITE_MISUSE;
	}
	raftProxySetConnectFunc(&t->raft_transport, f, arg);
	/* Also save this info for use in automatic role management. */
	t->connect_func = f;
	t->connect_func_arg = arg;
	return 0;
}

int dqlite_node_set_network_latency(dqlite_node *t,
				    unsigned long long nanoseconds)
{
	unsigned milliseconds;
	if (t->running) {
		return DQLITE_MISUSE;
	}

	/* 1 hour latency should be more than sufficient, also avoids overflow
	 * issues when converting to unsigned milliseconds later on */
	if (nanoseconds > 3600000000000ULL) {
		return DQLITE_MISUSE;
	}

	milliseconds = (unsigned)(nanoseconds / (1000000ULL));
	return dqlite_node_set_network_latency_ms(t, milliseconds);
}

int dqlite_node_set_network_latency_ms(dqlite_node *t, unsigned milliseconds)
{
	if (t->running) {
		return DQLITE_MISUSE;
	}

	/* Currently we accept at least 1 millisecond latency and maximum 3600 s
	 * of latency */
	if (milliseconds == 0 || milliseconds > 3600U * 1000U) {
		return DQLITE_MISUSE;
	}
	raft_set_heartbeat_timeout(&t->raft, (milliseconds * 15) / 10);
	raft_set_election_timeout(&t->raft, milliseconds * 15);
	return 0;
}

int dqlite_node_set_failure_domain(dqlite_node *n, unsigned long long code)
{
	n->config.failure_domain = code;
	return 0;
}

int dqlite_node_set_snapshot_params(dqlite_node *n,
				    unsigned snapshot_threshold,
				    unsigned snapshot_trailing)
{
	if (n->running) {
		return DQLITE_MISUSE;
	}

	if (snapshot_trailing < 4) {
		return DQLITE_MISUSE;
	}

	/* This is a safety precaution and allows to recover data from the
	 * second last raft snapshot and segment files in case the last raft
	 * snapshot is unusable. */
	if (snapshot_trailing < snapshot_threshold) {
		return DQLITE_MISUSE;
	}

	raft_set_snapshot_threshold(&n->raft, snapshot_threshold);
	raft_set_snapshot_trailing(&n->raft, snapshot_trailing);
	return 0;
}

#define KB(N) (1024 * N)
int dqlite_node_set_block_size(dqlite_node *n, size_t size)
{
	if (n->running) {
		return DQLITE_MISUSE;
	}

	switch (size) {
		case 512:      // fallthrough
		case KB(1):    // fallthrough
		case KB(2):    // fallthrough
		case KB(4):    // fallthrough
		case KB(8):    // fallthrough
		case KB(16):   // fallthrough
		case KB(32):   // fallthrough
		case KB(64):   // fallthrough
		case KB(128):  // fallthrough
		case KB(256):
			break;
		default:
			return DQLITE_ERROR;
	}

	raft_uv_set_block_size(&n->raft_io, size);
	return 0;
}
int dqlite_node_enable_disk_mode(dqlite_node *n)
{
	int rv;

	if (n->running) {
		return DQLITE_MISUSE;
	}

	rv = dqlite_vfs_enable_disk(&n->vfs);
	if (rv != 0) {
		return rv;
	}

	n->registry.config->disk = true;

	/* Close the default fsm and initialize the disk one. */
	fsm__close(&n->raft_fsm);
	rv = fsm__init_disk(&n->raft_fsm, &n->config, &n->registry);
	if (rv != 0) {
		return rv;
	}

	return 0;
}

static int maybeBootstrap(dqlite_node *d,
			  dqlite_node_id id,
			  const char *address)
{
	struct raft_configuration configuration;
	int rv;
	if (id != 1 && id != BOOTSTRAP_ID) {
		return 0;
	}
	raft_configuration_init(&configuration);
	rv = raft_configuration_add(&configuration, id, address, RAFT_VOTER);
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
			snprintf(d->errmsg, DQLITE_ERRMSG_BUF_SIZE,
				 "raft_bootstrap(): %s", raft_errmsg(&d->raft));
			rv = DQLITE_ERROR;
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
 * last handle is closed, the loop gets stopped.
 */
static void raftCloseCb(struct raft *raft)
{
	struct dqlite_node *s = raft->data;
	raft_uv_close(&s->raft_io);
	uv_close((struct uv_handle_s *)&s->stop, NULL);
	uv_close((struct uv_handle_s *)&s->handover, NULL);
	uv_close((struct uv_handle_s *)&s->startup, NULL);
	uv_close((struct uv_handle_s *)s->listener, NULL);
	uv_close((struct uv_handle_s *)&s->timer, NULL);
}

static void destroy_conn(struct conn *conn)
{
	QUEUE__REMOVE(&conn->queue);
	sqlite3_free(conn);
}

static void handoverDoneCb(struct dqlite_node *d, int status)
{
	d->handover_status = status;
	sem_post(&d->handover_done);
}

static void handoverCb(uv_async_t *handover)
{
	struct dqlite_node *d = handover->data;
	int rv;

	/* Nothing to do. */
	if (!d->running) {
		return;
	}

	if (d->role_management) {
		rv = uv_timer_stop(&d->timer);
		assert(rv == 0);
		RolesCancelPendingChanges(d);
	}

	RolesHandover(d, handoverDoneCb);
}

static void stopCb(uv_async_t *stop)
{
	struct dqlite_node *d = stop->data;
	queue *head;
	struct conn *conn;
	int rv;

	/* Nothing to do. */
	if (!d->running) {
		tracef("not running or already stopped");
		return;
	}
	pool_close(&d->pool);
	if (d->role_management) {
		rv = uv_timer_stop(&d->timer);
		assert(rv == 0);
		RolesCancelPendingChanges(d);
	}
	d->running = false;

	QUEUE__FOREACH(head, &d->conns)
	{
		conn = QUEUE__DATA(head, struct conn, queue);
		conn__stop(conn);
	}
	raft_close(&d->raft, raftCloseCb);
}

/* Callback invoked as soon as the loop as started.
 *
 * It unblocks the s->ready semaphore.
 */
static void startup_cb(uv_timer_t *startup)
{
	struct dqlite_node *d = startup->data;
	int rv;
	d->running = true;
	rv = sem_post(&d->ready);
	assert(rv == 0); /* No reason for which posting should fail */
}

static void listenCb(uv_stream_t *listener, int status)
{
	struct dqlite_node *t = listener->data;
	struct uv_stream_s *stream;
	struct conn *conn;
	struct id_state seed;
	int rv;

	if (!t->running) {
		tracef("not running");
		return;
	}

	if (status != 0) {
		/* TODO: log the error. */
		return;
	}

	switch (listener->type) {
		case UV_TCP:
			stream = raft_malloc(sizeof(struct uv_tcp_s));
			if (stream == NULL) {
				return;
			}
			rv = uv_tcp_init(&t->loop, (struct uv_tcp_s *)stream);
			assert(rv == 0);
			break;
		case UV_NAMED_PIPE:
			stream = raft_malloc(sizeof(struct uv_pipe_s));
			if (stream == NULL) {
				return;
			}
			rv = uv_pipe_init(&t->loop, (struct uv_pipe_s *)stream,
					  0);
			assert(rv == 0);
			break;
		default:
			assert(0);
	}

	rv = uv_accept(listener, stream);
	if (rv != 0) {
		goto err;
	}

	/* We accept unix socket connections only from the same process. */
	if (listener->type == UV_NAMED_PIPE) {
		int fd = stream->io_watcher.fd;
#if defined(SO_PEERCRED)  // Linux
		struct ucred cred;
		socklen_t len = sizeof(cred);
		rv = getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cred, &len);
		if (rv != 0) {
			goto err;
		}
		if (cred.pid != getpid()) {
			goto err;
		}
#elif defined(LOCAL_PEERPID)  // BSD
		pid_t pid = -1;
		socklen_t len = sizeof(pid);
		rv = getsockopt(fd, SOL_LOCAL, LOCAL_PEERPID, &pid, &len);
		if (rv != 0) {
			goto err;
		}
		if (pid != getpid()) {
			goto err;
		}
#else
		// The unix socket connection can't be verified and from
		// security perspective it's better to block it entirely
		goto err;
#endif
	}

	seed = t->random_state;
	idJump(&t->random_state);

	conn = sqlite3_malloc(sizeof *conn);
	if (conn == NULL) {
		goto err;
	}
	rv = conn__start(conn, &t->config, /* &t->loop TP_TODO! */ NULL, &t->registry, &t->raft,
			 stream, &t->raft_transport, seed, destroy_conn);
	if (rv != 0) {
		goto err_after_conn_alloc;
	}

	QUEUE__PUSH(&t->conns, &conn->queue);

	return;

err_after_conn_alloc:
	sqlite3_free(conn);
err:
	uv_close((struct uv_handle_s *)stream, (uv_close_cb)raft_free);
}

/* Runs every tick on the main thread to kick off roles adjustment. */
static void roleManagementTimerCb(uv_timer_t *handle)
{
	struct dqlite_node *d = handle->data;
	RolesAdjust(d);
}

static int taskRun(struct dqlite_node *d)
{
	int rv;

	/* TODO: implement proper cleanup upon error by spinning the loop a few
	 * times. */
	assert(d->listener != NULL);

	rv = uv_listen(d->listener, 128, listenCb);
	if (rv != 0) {
		return rv;
	}
	d->listener->data = d;

	d->handover.data = d;
	rv = uv_async_init(&d->loop, &d->handover, handoverCb);
	assert(rv == 0);
	/* Initialize notification handles. */
	d->stop.data = d;
	rv = uv_async_init(&d->loop, &d->stop, stopCb);
	assert(rv == 0);

	/* Schedule startup_cb to be fired as soon as the loop starts. It will
	 * unblock clients of taskReady. */
	d->startup.data = d;
	rv = uv_timer_init(&d->loop, &d->startup);
	assert(rv == 0);
	rv = uv_timer_start(&d->startup, startup_cb, 0, 0);
	assert(rv == 0);

	/* Schedule the role management callback. */
	d->timer.data = d;
	rv = uv_timer_init(&d->loop, &d->timer);
	assert(rv == 0);
	if (d->role_management) {
		/* TODO make the interval configurable */
		rv = uv_timer_start(&d->timer, roleManagementTimerCb, 1000,
				    1000);
		assert(rv == 0);
	}

	d->raft.data = d;
	rv = raft_start(&d->raft);
	if (rv != 0) {
		snprintf(d->errmsg, DQLITE_ERRMSG_BUF_SIZE, "raft_start(): %s",
			 raft_errmsg(&d->raft));
		/* Unblock any client of taskReady */
		sem_post(&d->ready);
		return rv;
	}

	rv = uv_run(&d->loop, UV_RUN_DEFAULT);
	assert(rv == 0);

	/* Unblock any client of taskReady */
	rv = sem_post(&d->ready);
	assert(rv == 0); /* no reason for which posting should fail */

	return 0;
}

int dqlite_node_set_target_voters(dqlite_node *n, int voters)
{
	n->config.voters = voters;
	return 0;
}

int dqlite_node_set_target_standbys(dqlite_node *n, int standbys)
{
	n->config.standbys = standbys;
	return 0;
}

int dqlite_node_enable_role_management(dqlite_node *n)
{
	n->role_management = true;
	return 0;
}

int dqlite_node_set_snapshot_compression(dqlite_node *n, bool enabled)
{
	return raft_uv_set_snapshot_compression(&n->raft_io, enabled);
}

int dqlite_node_set_auto_recovery(dqlite_node *n, bool enabled)
{
	raft_uv_set_auto_recovery(&n->raft_io, enabled);
	return 0;
}

const char *dqlite_node_errmsg(dqlite_node *n)
{
	if (n != NULL) {
		return n->errmsg;
	}
	return "node is NULL";
}

static void *taskStart(void *arg)
{
	struct dqlite_node *t = arg;
	int rv;
	rv = taskRun(t);
	if (rv != 0) {
		uintptr_t result = (uintptr_t)rv;
		return (void *)result;
	}
	return NULL;
}

void dqlite_node_destroy(dqlite_node *d)
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
static bool taskReady(struct dqlite_node *d)
{
	/* Wait for the ready semaphore */
	sem_wait(&d->ready);
	return d->running;
}

static int dqliteDatabaseDirSetup(dqlite_node *t)
{
	int rv;
	if (!t->config.disk) {
		// nothing to do
		return 0;
	}

	rv = FsEnsureDir(t->config.dir);
	if (rv != 0) {
		snprintf(t->errmsg, DQLITE_ERRMSG_BUF_SIZE,
			 "Error creating database dir: %d", rv);
		return rv;
	}

	rv = FsRemoveDirFiles(t->config.dir);
	if (rv != 0) {
		snprintf(t->errmsg, DQLITE_ERRMSG_BUF_SIZE,
			 "Error removing files in database dir: %d", rv);
		return rv;
	}

	return rv;
}

int dqlite_node_start(dqlite_node *t)
{
	int rv;
	tracef("dqlite node start");

	dqliteTracingMaybeEnable(true);

	rv = dqliteDatabaseDirSetup(t);
	if (rv != 0) {
		tracef("database dir setup failed %s", t->errmsg);
		goto err;
	}

	rv = maybeBootstrap(t, t->config.id, t->config.address);
	if (rv != 0) {
		tracef("bootstrap failed %d", rv);
		goto err;
	}

	rv = pthread_create(&t->thread, 0, &taskStart, t);
	if (rv != 0) {
		tracef("pthread create failed %d", rv);
		rv = DQLITE_ERROR;
		goto err;
	}

	if (!taskReady(t)) {
		tracef("!taskReady");
		rv = DQLITE_ERROR;
		goto err;
	}

	return 0;

err:
	return rv;
}

int dqlite_node_handover(dqlite_node *d)
{
	int rv;

	rv = uv_async_send(&d->handover);
	assert(rv == 0);

	sem_wait(&d->handover_done);

	return d->handover_status;
}

int dqlite_node_stop(dqlite_node *d)
{
	tracef("dqlite node stop");
	void *result;
	int rv;

	rv = uv_async_send(&d->stop);
	assert(rv == 0);

	rv = pthread_join(d->thread, &result);
	assert(rv == 0);

	return (int)((uintptr_t)result);
}

int dqlite_node_recover(dqlite_node *n,
			struct dqlite_node_info infos[],
			int n_info)
{
	tracef("dqlite node recover");
	int i;
	int ret;

	struct dqlite_node_info_ext *infos_ext =
	    calloc((size_t)n_info, sizeof(*infos_ext));
	if (infos_ext == NULL) {
		return DQLITE_NOMEM;
	}
	for (i = 0; i < n_info; i++) {
		infos_ext[i].size = sizeof(*infos_ext);
		infos_ext[i].id = infos[i].id;
		infos_ext[i].address = PTR_TO_UINT64(infos[i].address);
		infos_ext[i].dqlite_role = DQLITE_VOTER;
	}

	ret = dqlite_node_recover_ext(n, infos_ext, n_info);
	free(infos_ext);
	return ret;
}

static bool node_info_valid(struct dqlite_node_info_ext *info)
{
	/* Reject any size smaller than the original definition of the
	 * extensible struct. */
	if (info->size < DQLITE_NODE_INFO_EXT_SZ_ORIG) {
		return false;
	}

	/* Require 8 byte allignment */
	if (info->size % sizeof(uint64_t)) {
		return false;
	}

	/* If the user uses a newer, and larger version of the struct, make sure
	 * the unknown fields are zeroed out. */
	uint64_t known_size = sizeof(struct dqlite_node_info_ext);
	if (info->size > known_size) {
		const uint64_t num_known_fields = known_size / sizeof(uint64_t);
		const uint64_t num_extra_fields =
		    (info->size - known_size) / sizeof(uint64_t);
		const uint64_t *extra_fields =
		    ((const uint64_t *)info) + num_known_fields;
		for (uint64_t i = 0; i < num_extra_fields; i++) {
			if (extra_fields[i] != (uint64_t)0) {
				return false;
			}
		}
	}

	return true;
}

int dqlite_node_recover_ext(dqlite_node *n,
			    struct dqlite_node_info_ext infos[],
			    int n_info)
{
	tracef("dqlite node recover ext");
	struct raft_configuration configuration;
	int i;
	int rv;

	raft_configuration_init(&configuration);
	for (i = 0; i < n_info; i++) {
		struct dqlite_node_info_ext *info = &infos[i];
		if (!node_info_valid(info)) {
			rv = DQLITE_MISUSE;
			goto out;
		}
		int raft_role = translateDqliteRole((int)info->dqlite_role);
		const char *address =
		    UINT64_TO_PTR(info->address, const char *);
		rv = raft_configuration_add(&configuration, info->id, address,
					    raft_role);
		if (rv != 0) {
			assert(rv == RAFT_NOMEM);
			rv = DQLITE_NOMEM;
			goto out;
		};
	}

	rv = raft_recover(&n->raft, &configuration);
	if (rv != 0) {
		rv = DQLITE_ERROR;
		goto out;
	}

out:
	raft_configuration_close(&configuration);
	return rv;
}

dqlite_node_id dqlite_generate_node_id(const char *address)
{
	tracef("generate node id");
	struct timespec ts;
	int rv;
	unsigned long long n;

	rv = clock_gettime(CLOCK_REALTIME, &ts);
	assert(rv == 0);

	n = (unsigned long long)(ts.tv_sec * 1000 * 1000 * 1000 + ts.tv_nsec);

	return raft_digest(address, n);
}

static void pushNodeInfo(struct node_store_cache *cache,
			 struct client_node_info info)
{
	unsigned cap = cache->cap;
	struct client_node_info *new;

	if (cache->len == cap) {
		if (cap == 0) {
			cap = 5;
		}
		cap *= 2;
		new = callocChecked(cap, sizeof *new);
		memcpy(new, cache->nodes, cache->len * sizeof *new);
		free(cache->nodes);
		cache->nodes = new;
		cache->cap = cap;
	}
	cache->nodes[cache->len] = info;
	cache->len += 1;
}

static void emptyCache(struct node_store_cache *cache)
{
	unsigned i;

	for (i = 0; i < cache->len; i += 1) {
		free(cache->nodes[i].addr);
	}
	free(cache->nodes);
	cache->nodes = NULL;
	cache->len = 0;
	cache->cap = 0;
}

static const struct client_node_info *findNodeInCache(
    const struct node_store_cache *cache,
    uint64_t id)
{
	unsigned i;

	for (i = 0; i < cache->len; i += 1) {
		if (cache->nodes[i].id == id) {
			return &cache->nodes[i];
		}
	}
	return NULL;
}

/* Called at startup to parse the node store read from disk into an in-memory
 * representation. */
static int parseNodeStore(char *buf, size_t len, struct node_store_cache *cache)
{
	const char *p = buf;
	const char *end = buf + len;
	char *nl;
	const char *version_str;
	const char *addr;
	const char *id_str;
	const char *dig;
	unsigned long long id;
	const char *role_str;
	int role;
	struct client_node_info info;

	version_str = p;
	nl = memchr(p, '\n', (size_t)(end - version_str));
	if (nl == NULL) {
		return 1;
	}
	*nl = '\0';
	p = nl + 1;
	if (strcmp(version_str, NODE_STORE_INFO_FORMAT_V1) != 0) {
		return 1;
	}

	while (p != end) {
		addr = p;
		nl = memchr(p, '\n', (size_t)(end - addr));
		if (nl == NULL) {
			return 1;
		}
		*nl = '\0';
		p = nl + 1;

		id_str = p;
		nl = memchr(p, '\n', (size_t)(end - id_str));
		if (nl == NULL) {
			return 1;
		}
		*nl = '\0';
		p = nl + 1;
		/* Be stricter than strtoull: digits only! */
		for (dig = id_str; dig != nl; dig += 1) {
			if (*dig < '0' || *dig > '9') {
				return 1;
			}
		}
		errno = 0;
		id = strtoull(id_str, NULL, 10);
		if (errno != 0) {
			return 1;
		}

		role_str = p;
		nl = memchr(p, '\n', (size_t)(end - role_str));
		if (nl == NULL) {
			return 1;
		}
		*nl = '\0';
		p = nl + 1;
		if (strcmp(role_str, "spare") == 0) {
			role = DQLITE_SPARE;
		} else if (strcmp(role_str, "standby") == 0) {
			role = DQLITE_STANDBY;
		} else if (strcmp(role_str, "voter") == 0) {
			role = DQLITE_VOTER;
		} else {
			return 1;
		}

		info.addr = strdupChecked(addr);
		info.id = (uint64_t)id;
		info.role = role;
		pushNodeInfo(cache, info);
	}
	return 0;
}

/* Write the in-memory node store to disk. This discards errors, because:
 *
 * - we can't do much to handle any of these error cases
 * - we don't want to stop everything when this encounters an error, since the
 *   persisted node store is an optimization, so it's not disastrous for it to
 *   be missing or out of date
 * - there is already a "retry" mechanism in the form of the refreshTask thread,
 *   which periodically tries to write the node store file
 */
static void writeNodeStore(struct dqlite_server *server)
{
	int store_fd;
	FILE *f;
	unsigned i;
	ssize_t k;
	const char *role_name;
	int rv;

	store_fd = openat(server->dir_fd, "node-store-tmp",
			  O_RDWR | O_CREAT | O_TRUNC, 0644);
	if (store_fd < 0) {
		return;
	}
	f = fdopen(store_fd, "w+");
	if (f == NULL) {
		close(store_fd);
		return;
	}

	k = fprintf(f, "%s\n", NODE_STORE_INFO_FORMAT_V1);
	if (k < 0) {
		fclose(f);
		return;
	}
	for (i = 0; i < server->cache.len; i += 1) {
		role_name =
		    (server->cache.nodes[i].role == DQLITE_SPARE)
			? "spare"
			: ((server->cache.nodes[i].role == DQLITE_STANDBY)
			       ? "standby"
			       : "voter");
		k = fprintf(f, "%s\n%" PRIu64 "\n%s\n",
			    server->cache.nodes[i].addr,
			    server->cache.nodes[i].id, role_name);
		if (k < 0) {
			fclose(f);
			return;
		}
	}

	fclose(f);
	rv = renameat(server->dir_fd, "node-store-tmp", server->dir_fd,
		      "node-store");
	(void)rv;
}

/* Called at startup to parse the node store read from disk into an in-memory
 * representation. */
static int parseLocalInfo(char *buf,
			  size_t len,
			  char **local_addr,
			  uint64_t *local_id)
{
	const char *p = buf;
	const char *end = buf + len;
	char *nl;
	const char *version_str;
	const char *addr;
	const char *id_str;
	const char *dig;
	unsigned long long id;

	version_str = p;
	nl = memchr(version_str, '\n', (size_t)(end - version_str));
	if (nl == NULL) {
		return 1;
	}
	*nl = '\0';
	p = nl + 1;
	if (strcmp(version_str, NODE_STORE_INFO_FORMAT_V1) != 0) {
		return 1;
	}

	addr = p;
	nl = memchr(addr, '\n', (size_t)(end - addr));
	if (nl == NULL) {
		return 1;
	}
	*nl = '\0';
	p = nl + 1;

	id_str = p;
	nl = memchr(id_str, '\n', (size_t)(end - id_str));
	if (nl == NULL) {
		return 1;
	}
	*nl = '\0';
	p = nl + 1;
	for (dig = id_str; dig != nl; dig += 1) {
		if (*dig < '0' || *dig > '9') {
			return 1;
		}
	}
	errno = 0;
	id = strtoull(id_str, NULL, 10);
	if (errno != 0) {
		return 1;
	}

	if (p != end) {
		return 1;
	}

	*local_addr = strdupChecked(addr);
	*local_id = (uint64_t)id;
	return 0;
}

/* Write the local node's info to disk. */
static int writeLocalInfo(struct dqlite_server *server)
{
	int info_fd;
	FILE *f;
	ssize_t k;
	int rv;

	info_fd = openat(server->dir_fd, "server-info-tmp",
			 O_RDWR | O_CREAT | O_TRUNC, 0664);
	if (info_fd < 0) {
		return 1;
	}
	f = fdopen(info_fd, "w+");
	if (f == NULL) {
		close(info_fd);
		return 1;
	}
	k = fprintf(f, "%s\n%s\n%" PRIu64 "\n", NODE_STORE_INFO_FORMAT_V1,
		    server->local_addr, server->local_id);
	if (k < 0) {
		fclose(f);
		return 1;
	}
	rv = renameat(server->dir_fd, "server-info-tmp", server->dir_fd,
		      "server-info");
	if (rv != 0) {
		fclose(f);
		return 1;
	}
	fclose(f);
	return 0;
}

int dqlite_server_create(const char *path, dqlite_server **server)
{
	int rv;

	*server = callocChecked(1, sizeof **server);
	rv = pthread_cond_init(&(*server)->cond, NULL);
	assert(rv == 0);
	rv = pthread_mutex_init(&(*server)->mutex, NULL);
	assert(rv == 0);
	(*server)->dir_path = strdupChecked(path);
	(*server)->connect = transportDefaultConnect;
	(*server)->proto.connect = transportDefaultConnect;
	(*server)->dir_fd = -1;
	(*server)->refresh_period = 30 * 1000;
	return 0;
}

int dqlite_server_set_address(dqlite_server *server, const char *address)
{
	free(server->local_addr);
	server->local_addr = strdupChecked(address);
	return 0;
}

int dqlite_server_set_auto_bootstrap(dqlite_server *server, bool on)
{
	server->bootstrap = on;
	return 0;
}

int dqlite_server_set_auto_join(dqlite_server *server,
				const char *const *addrs,
				unsigned n)
{
	/* We don't know the ID or role of this server, so leave those fields
	 * zeroed. In dqlite_server_start, we must take care not to use this
	 * initial node store cache to do anything except find a server to
	 * connect to. Once we've done that, we immediately fetch a fresh list
	 * of cluster members that includes ID and role information, and clear
	 * away the temporary node store cache. */
	struct client_node_info info = {0};
	unsigned i;

	for (i = 0; i < n; i += 1) {
		info.addr = strdupChecked(addrs[i]);
		pushNodeInfo(&server->cache, info);
	}
	return 0;
}

int dqlite_server_set_bind_address(dqlite_server *server, const char *addr)
{
	free(server->bind_addr);
	server->bind_addr = strdupChecked(addr);
	return 0;
}

int dqlite_server_set_connect_func(dqlite_server *server,
				   dqlite_connect_func f,
				   void *arg)
{
	server->connect = f;
	server->connect_arg = arg;
	server->proto.connect = f;
	server->proto.connect_arg = arg;
	return 0;
}

static int openAndHandshake(struct client_proto *proto,
			    const char *addr,
			    uint64_t id,
			    struct client_context *context)
{
	int rv;

	rv = clientOpen(proto, addr, id);
	if (rv != 0) {
		return 1;
	}
	rv = clientSendHandshake(proto, context);
	if (rv != 0) {
		clientClose(proto);
		return 1;
	}
	/* TODO client identification? */
	return 0;
}

/* TODO prioritize voters > standbys > spares */
static int connectToSomeServer(struct dqlite_server *server,
			       struct client_context *context)
{
	unsigned i;
	int rv;

	for (i = 0; i < server->cache.len; i += 1) {
		rv = openAndHandshake(&server->proto,
				      server->cache.nodes[i].addr,
				      server->cache.nodes[i].id, context);
		if (rv == 0) {
			return 0;
		}
	}
	return 1;
}

/* Given an open connection, make an honest effort to reopen it as a connection
 * to the current cluster leader. This bails out rather than retrying on
 * client/server/network errors, leaving the retry policy up to the caller. On
 * failure (rv != 0) the given client object may be closed or not: the caller
 * must check this by comparing proto->fd to -1. */
static int tryReconnectToLeader(struct client_proto *proto,
				struct client_context *context)
{
	char *addr;
	uint64_t id;
	int rv;

	rv = clientSendLeader(proto, context);
	if (rv != 0) {
		clientClose(proto);
		return 1;
	}

	rv = clientRecvServer(proto, &id, &addr, context);
	if (rv == DQLITE_CLIENT_PROTO_RECEIVED_FAILURE) {
		return 1;
	} else if (rv != 0) {
		clientClose(proto);
		return 1;
	}
	if (id == 0) {
		free(addr);
		return 1;
	} else if (id == proto->server_id) {
		free(addr);
		return 0;
	}

	clientClose(proto);
	rv = openAndHandshake(proto, addr, id, context);
	free(addr);
	if (rv != 0) {
		return 1;
	}
	return 0;
}

static int refreshNodeStoreCache(struct dqlite_server *server,
				 struct client_context *context)
{
	struct client_node_info *servers;
	uint64_t n_servers;
	int rv;

	rv = clientSendCluster(&server->proto, context);
	if (rv != 0) {
		clientClose(&server->proto);
		return 1;
	}
	rv = clientRecvServers(&server->proto, &servers, &n_servers, context);
	if (rv != 0) {
		clientClose(&server->proto);
		return 1;
	}
	emptyCache(&server->cache);
	server->cache.nodes = servers;
	server->cache.len = (unsigned)n_servers;
	assert((uint64_t)server->cache.len == n_servers);
	server->cache.cap = (unsigned)n_servers;
	return 0;
}

static int maybeJoinCluster(struct dqlite_server *server,
			    struct client_context *context)
{
	int rv;

	if (findNodeInCache(&server->cache, server->local_id) != NULL) {
		return 0;
	}

	rv = clientSendAdd(&server->proto, server->local_id, server->local_addr,
			   context);
	if (rv != 0) {
		clientClose(&server->proto);
		return 1;
	}
	rv = clientRecvEmpty(&server->proto, context);
	if (rv != 0) {
		clientClose(&server->proto);
		return 1;
	}
	rv = refreshNodeStoreCache(server, context);
	if (rv != 0) {
		return 1;
	}
	return 0;
}

static int bootstrapOrJoinCluster(struct dqlite_server *server,
				  struct client_context *context)
{
	struct client_node_info info;
	int rv;

	if (server->is_new && server->bootstrap) {
		rv = openAndHandshake(&server->proto, server->local_addr,
				      server->local_id, context);
		if (rv != 0) {
			return 1;
		}

		info.addr = strdupChecked(server->local_addr);
		info.id = server->local_id;
		info.role = DQLITE_VOTER;
		pushNodeInfo(&server->cache, info);
	} else {
		rv = connectToSomeServer(server, context);
		if (rv != 0) {
			return 1;
		}

		rv = tryReconnectToLeader(&server->proto, context);
		if (rv != 0) {
			return 1;
		}

		rv = refreshNodeStoreCache(server, context);
		if (rv != 0) {
			return 1;
		}

		rv = maybeJoinCluster(server, context);
		if (rv != 0) {
			return 1;
		}
	}

	writeNodeStore(server);
	return 0;
}

static void *refreshTask(void *arg)
{
	struct dqlite_server *server = arg;
	struct client_context context;
	struct timespec ts;
	unsigned long long nsec;
	int rv;

	rv = pthread_mutex_lock(&server->mutex);
	assert(rv == 0);
	for (;;) {
		rv = clock_gettime(CLOCK_REALTIME, &ts);
		assert(rv == 0);
		nsec = (unsigned long long)ts.tv_nsec;
		nsec += server->refresh_period * 1000 * 1000;
		while (nsec > 1000 * 1000 * 1000) {
			nsec -= 1000 * 1000 * 1000;
			ts.tv_sec += 1;
		}
		/* The type of tv_nsec is "an implementation-defined signed type
		 * capable of holding [the range 0..=999,999,999]". int is the
		 * narrowest such type (on all the targets we care about), so
		 * cast to that before doing the assignment to avoid warnings.
		 */
		ts.tv_nsec = (int)nsec;

		rv = pthread_cond_timedwait(&server->cond, &server->mutex, &ts);
		if (server->shutdown) {
			rv = pthread_mutex_unlock(&server->mutex);
			assert(rv == 0);
			break;
		}
		assert(rv == 0 || rv == ETIMEDOUT);

		clientContextMillis(&context, 5000);
		if (server->proto.fd == -1) {
			rv = connectToSomeServer(server, &context);
			if (rv != 0) {
				continue;
			}
			(void)tryReconnectToLeader(&server->proto, &context);
			if (server->proto.fd == -1) {
				continue;
			}
		}
		rv = refreshNodeStoreCache(server, &context);
		if (rv != 0) {
			continue;
		}
		writeNodeStore(server);
	}
	return NULL;
}

int dqlite_server_start(dqlite_server *server)
{
	int info_fd;
	int store_fd;
	off_t full_size;
	ssize_t size;
	char *buf;
	ssize_t n_read;
	struct client_context context;
	int rv;

	if (server->started) {
		goto err;
	}

	if (server->bootstrap && server->cache.len > 0) {
		goto err;
	}

	server->is_new = true;
	server->dir_fd = open(server->dir_path, O_RDONLY | O_DIRECTORY);
	if (server->dir_fd < 0) {
		goto err;
	}
	info_fd = openat(server->dir_fd, "server-info", O_RDWR | O_CREAT, 0664);
	if (info_fd < 0) {
		goto err_after_open_dir;
	}
	store_fd = openat(server->dir_fd, "node-store", O_RDWR | O_CREAT, 0664);
	if (store_fd < 0) {
		goto err_after_open_info;
	}

	full_size = lseek(info_fd, 0, SEEK_END);
	assert(full_size >= 0);
	if (full_size > (off_t)SSIZE_MAX) {
		goto err_after_open_store;
	}
	size = (ssize_t)full_size;
	if (size > 0) {
		server->is_new = false;
		/* TODO mmap it? */
		buf = mallocChecked((size_t)size);
		n_read = pread(info_fd, buf, (size_t)size, 0);
		if (n_read < size) {
			free(buf);
			goto err_after_open_store;
		}
		free(server->local_addr);
		server->local_addr = NULL;
		rv = parseLocalInfo(buf, (size_t)size, &server->local_addr,
				    &server->local_id);
		free(buf);
		if (rv != 0) {
			goto err_after_open_store;
		}
	}

	full_size = lseek(store_fd, 0, SEEK_END);
	assert(full_size >= 0);
	if (full_size > (off_t)SSIZE_MAX) {
		goto err_after_open_store;
	}
	size = (ssize_t)full_size;
	if (size > 0) {
		if (server->is_new) {
			goto err_after_open_store;
		}

		/* TODO mmap it? */
		buf = mallocChecked((size_t)size);
		n_read = pread(store_fd, buf, (size_t)size, 0);
		if (n_read < size) {
			free(buf);
			goto err_after_open_store;
		}
		emptyCache(&server->cache);
		rv = parseNodeStore(buf, (size_t)size, &server->cache);
		free(buf);
		if (rv != 0) {
			goto err_after_open_store;
		}
	}

	if (server->is_new) {
		server->local_id =
		    server->bootstrap
			? BOOTSTRAP_ID
			: dqlite_generate_node_id(server->local_addr);
	}

	rv = dqlite_node_create(server->local_id, server->local_addr,
				server->dir_path, &server->local);
	if (rv != 0) {
		goto err_after_create_node;
	}
	rv = dqlite_node_set_bind_address(
	    server->local, (server->bind_addr != NULL) ? server->bind_addr
						       : server->local_addr);
	if (rv != 0) {
		goto err_after_create_node;
	}
	rv = dqlite_node_set_connect_func(server->local, server->connect,
					  server->connect_arg);
	if (rv != 0) {
		goto err_after_create_node;
	}

	rv = dqlite_node_start(server->local);
	if (rv != 0) {
		goto err_after_create_node;
	}
	/* TODO set weight and failure domain here */

	rv = writeLocalInfo(server);
	if (rv != 0) {
		goto err_after_start_node;
	}

	clientContextMillis(&context, 5000);

	rv = bootstrapOrJoinCluster(server, &context);
	if (rv != 0) {
		goto err_after_start_node;
	}

	rv = pthread_create(&server->refresh_thread, NULL, refreshTask, server);
	assert(rv == 0);

	close(store_fd);
	close(info_fd);
	server->started = true;
	return 0;

err_after_start_node:
	dqlite_node_stop(server->local);
err_after_create_node:
	dqlite_node_destroy(server->local);
	server->local = NULL;
err_after_open_store:
	close(store_fd);
err_after_open_info:
	close(info_fd);
err_after_open_dir:
	close(server->dir_fd);
	server->dir_fd = -1;
err:
	return 1;
}

dqlite_node_id dqlite_server_get_id(dqlite_server *server)
{
	return server->local_id;
}

int dqlite_server_handover(dqlite_server *server)
{
	int rv = dqlite_node_handover(server->local);
	if (rv != 0) {
		return 1;
	}
	return 0;
}

int dqlite_server_stop(dqlite_server *server)
{
	void *ret;
	int rv;

	if (!server->started) {
		return 1;
	}

	rv = pthread_mutex_lock(&server->mutex);
	assert(rv == 0);
	server->shutdown = true;
	rv = pthread_mutex_unlock(&server->mutex);
	assert(rv == 0);
	rv = pthread_cond_signal(&server->cond);
	assert(rv == 0);
	rv = pthread_join(server->refresh_thread, &ret);
	assert(rv == 0);

	emptyCache(&server->cache);

	clientClose(&server->proto);

	server->started = false;
	rv = dqlite_node_stop(server->local);
	if (rv != 0) {
		return 1;
	}
	return 0;
}

void dqlite_server_destroy(dqlite_server *server)
{
	pthread_cond_destroy(&server->cond);
	pthread_mutex_destroy(&server->mutex);

	emptyCache(&server->cache);

	free(server->dir_path);
	if (server->local != NULL) {
		dqlite_node_destroy(server->local);
	}
	free(server->local_addr);
	free(server->bind_addr);
	close(server->dir_fd);
	free(server);
}
