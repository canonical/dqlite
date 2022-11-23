#include "server.h"

#include <errno.h>
#include <stdlib.h>
#include <sys/un.h>
#include <time.h>

#include "../include/dqlite.h"
#include "conn.h"
#include "fsm.h"
#include "lib/addr.h"
#include "lib/assert.h"
#include "lib/fs.h"
#include "logger.h"
#include "protocol.h"
#include "tracing.h"
#include "translate.h"
#include "transport.h"
#include "utils.h"
#include "vfs.h"

/* Special ID for the bootstrap node. Equals to raft_digest("1", 0). */
#define BOOTSTRAP_ID 0x2dc171858c3155be

#define DATABASE_DIR_FMT "%s/database"

int dqlite__init(struct dqlite_node *d,
		 dqlite_node_id id,
		 const char *address,
		 const char *dir)
{
	int rv;
	char db_dir_path[1024];

	d->initialized = false;
	memset(d->errmsg, 0, sizeof d->errmsg);

	rv = snprintf(db_dir_path, sizeof db_dir_path, DATABASE_DIR_FMT, dir);
	if (rv == -1 || rv >= (int)(sizeof db_dir_path)) {
		snprintf(d->errmsg, DQLITE_ERRMSG_BUF_SIZE, "failed to init: snprintf(rv:%d)", rv);
		goto err;
	}

	rv = config__init(&d->config, id, address, db_dir_path);
	if (rv != 0) {
		snprintf(d->errmsg, DQLITE_ERRMSG_BUF_SIZE, "config__init(rv:%d)", rv);
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
		snprintf(d->errmsg, DQLITE_ERRMSG_BUF_SIZE, "uv_loop_init(): %s",
			 uv_strerror(rv));
		rv = DQLITE_ERROR;
		goto err_after_vfs_init;
	}
	rv = raftProxyInit(&d->raft_transport, &d->loop);
	if (rv != 0) {
		goto err_after_loop_init;
	}
	rv = raft_uv_init(&d->raft_io, &d->loop, dir, &d->raft_transport);
	if (rv != 0) {
		snprintf(d->errmsg, DQLITE_ERRMSG_BUF_SIZE, "raft_uv_init(): %s",
			 d->raft_io.errmsg);
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
#ifdef __APPLE__
	d->ready = dispatch_semaphore_create(0);
	d->stopped = dispatch_semaphore_create(0);
#else
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
#endif

	QUEUE__INIT(&d->queue);
	QUEUE__INIT(&d->conns);
	d->raft_state = RAFT_UNAVAILABLE;
	d->running = false;
	d->listener = NULL;
	d->bind_address = NULL;
	d->initialized = true;
	return 0;

err_after_ready_init:
#ifdef __APPLE__
	dispatch_release(d->ready);
#else
	sem_destroy(&d->ready);
#endif
err_after_raft_fsm_init:
	fsm__close(&d->raft_fsm);
err_after_raft_io_init:
	raft_uv_close(&d->raft_io);
err_after_raft_transport_init:
	raftProxyClose(&d->raft_transport);
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
#ifdef __APPLE__
	dispatch_release(d->stopped);
	dispatch_release(d->ready);
#else
	rv = sem_destroy(&d->stopped);
	assert(rv == 0); /* Fails only if sem object is not valid */
	rv = sem_destroy(&d->ready);
	assert(rv == 0); /* Fails only if sem object is not valid */
#endif
	fsm__close(&d->raft_fsm);
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

	rv = AddrParse(address, addr, &addr_len, "8080", DQLITE_ADDR_PARSE_UNIX);
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

int dqlite_node_set_network_latency_ms(dqlite_node *t,
				       unsigned milliseconds)
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

int dqlite_node_set_snapshot_params(dqlite_node *n, unsigned snapshot_threshold,
                                    unsigned snapshot_trailing)
{
	if (n->running) {
		return DQLITE_MISUSE;
	}

	if (snapshot_trailing < 4) {
		return DQLITE_MISUSE;
	}

	/* This is a safety precaution and allows to recover data from the second
	 * last raft snapshot and segment files in case the last raft snapshot is
	 * unusable. */
	if (snapshot_trailing < snapshot_threshold) {
		return DQLITE_MISUSE;
	}

	raft_set_snapshot_threshold(&n->raft, snapshot_threshold);
	raft_set_snapshot_trailing(&n->raft, snapshot_trailing);
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
			snprintf(d->errmsg, DQLITE_ERRMSG_BUF_SIZE, "raft_bootstrap(): %s",
				 raft_errmsg(&d->raft));
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
 * last handle (which must be the 'stop' async handle) is closed, the loop gets
 * stopped.
 */
static void raftCloseCb(struct raft *raft)
{
	struct dqlite_node *s = raft->data;
	raft_uv_close(&s->raft_io);
	uv_close((struct uv_handle_s *)&s->stop, NULL);
	uv_close((struct uv_handle_s *)&s->startup, NULL);
	uv_close((struct uv_handle_s *)&s->monitor, NULL);
	uv_close((struct uv_handle_s *)s->listener, NULL);
}

static void destroy_conn(struct conn *conn)
{
	QUEUE__REMOVE(&conn->queue);
	sqlite3_free(conn);
}

static void stop_cb(uv_async_t *stop)
{
	struct dqlite_node *d = stop->data;
	queue *head;
	struct conn *conn;

	/* Nothing to do. */
	if (!d->running) {
	    tracef("not running or already stopped");
	    return;
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
#ifdef __APPLE__
	dispatch_semaphore_signal(d->ready);
#else
	rv = sem_post(&d->ready);
	assert(rv == 0); /* No reason for which posting should fail */
#endif
}

static void listenCb(uv_stream_t *listener, int status)
{
	struct dqlite_node *t = listener->data;
	struct uv_stream_s *stream;
	struct conn *conn;
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
#if defined(SO_PEERCRED) // Linux
		struct ucred cred;
		socklen_t len = sizeof(cred);
		rv = getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cred, &len);
		if (rv != 0) {
			goto err;
		}
		if (cred.pid != getpid()) {
			goto err;
		}
#elif defined(LOCAL_PEERPID) // BSD
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

	conn = sqlite3_malloc(sizeof *conn);
	if (conn == NULL) {
		goto err;
	}
	rv = conn__start(conn, &t->config, &t->loop, &t->registry, &t->raft,
			 stream, &t->raft_transport, destroy_conn);
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

static void monitor_cb(uv_prepare_t *monitor)
{
	struct dqlite_node *d = monitor->data;
	int state = raft_state(&d->raft);
	queue *head;
	struct conn *conn;

	if (state == RAFT_UNAVAILABLE) {
		return;
	}

	if (d->raft_state == RAFT_LEADER && state != RAFT_LEADER) {
		tracef("node %llu@%s: leadership lost", d->raft.id, d->raft.address);
		QUEUE__FOREACH(head, &d->conns)
		{
			conn = QUEUE__DATA(head, struct conn, queue);
			gateway__leader_close(&conn->gateway, RAFT_LEADERSHIPLOST);
		}
	}

	d->raft_state = state;
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

	/* Initialize notification handles. */
	d->stop.data = d;
	rv = uv_async_init(&d->loop, &d->stop, stop_cb);
	assert(rv == 0);

	/* Schedule startup_cb to be fired as soon as the loop starts. It will
	 * unblock clients of taskReady. */
	d->startup.data = d;
	rv = uv_timer_init(&d->loop, &d->startup);
	assert(rv == 0);
	rv = uv_timer_start(&d->startup, startup_cb, 0, 0);
	assert(rv == 0);

	/* Schedule raft state change monitor. */
	d->monitor.data = d;
	rv = uv_prepare_init(&d->loop, &d->monitor);
	assert(rv == 0);
	rv = uv_prepare_start(&d->monitor, monitor_cb);
	assert(rv == 0);

	d->raft.data = d;
	rv = raft_start(&d->raft);
	if (rv != 0) {
		snprintf(d->errmsg, DQLITE_ERRMSG_BUF_SIZE, "raft_start(): %s",
			 raft_errmsg(&d->raft));
		/* Unblock any client of taskReady */
#ifdef __APPLE__
		dispatch_semaphore_signal(d->ready);
#else
		sem_post(&d->ready);
#endif
		return rv;
	}

	rv = uv_run(&d->loop, UV_RUN_DEFAULT);
	assert(rv == 0);

	/* Unblock any client of taskReady */
#ifdef __APPLE__
	dispatch_semaphore_signal(d->ready);
#else
	rv = sem_post(&d->ready);
	assert(rv == 0); /* no reason for which posting should fail */
#endif

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
#ifdef __APPLE__
	dispatch_semaphore_wait(d->ready, DISPATCH_TIME_FOREVER);
#else
	sem_wait(&d->ready);
#endif
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
		snprintf(t->errmsg, DQLITE_ERRMSG_BUF_SIZE, "Error creating database dir: %d", rv);
		return rv;
	}

	rv = FsRemoveDirFiles(t->config.dir);
	if (rv != 0) {
		snprintf(t->errmsg, DQLITE_ERRMSG_BUF_SIZE, "Error removing files in database dir: %d", rv);
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

    struct dqlite_node_info_ext *infos_ext = calloc((size_t)n_info, sizeof(*infos_ext));
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
    /* Reject any size smaller than the original definition of the extensible
     * struct. */
    if (info->size < DQLITE_NODE_INFO_EXT_SZ_ORIG) {
        return false;
    }

    /* Require 8 byte allignment */
    if (info->size % sizeof(uint64_t)) {
        return false;
    }

    /* If the user uses a newer, and larger version of the struct, make sure the unknown
     * fields are zeroed out. */
    uint64_t known_size = sizeof(struct dqlite_node_info_ext);
    if (info->size > known_size) {
        const uint64_t num_known_fields = known_size / sizeof(uint64_t);
        const uint64_t num_extra_fields = (info->size - known_size) / sizeof(uint64_t);
        const uint64_t *extra_fields = ((const uint64_t *)info) + num_known_fields;
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
		const char *address = UINT64_TO_PTR(info->address, const char *);
		rv = raft_configuration_add(&configuration, info->id,
					    address, raft_role);
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
