#include "../../include/dqlite.h"
#include "../lib/addr.h"
#include "../transport.h"
#include "protocol.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#define MAGIC_BOOTSTRAP_ID 1

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
	/* we try to keep this pointing at the leader,
	 * but it might be out of date */
	struct client_proto proto;

	/* These fields are immutable after construction: */
	bool is_new;
	bool bootstrap;
	char *dir_path; /* owned */
	dqlite_node *local;
	uint64_t local_id;
	char *local_addr; /* owned */
	char *bind_addr;  /* owned */
	dqlite_connect_func connect;
	void *connect_arg;
	int store_fd;
	int info_fd;
	int voters;
	int standbys;
};

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

static int parseNodeStore(const char *buf,
			  size_t buf_len,
			  struct node_store_cache *cache)
{
	const char *p;
	size_t n;
	size_t k;
	size_t i;
	const char *addr;
	const char *id_str;
	long long id;
	const char *role_str;
	int role;
	struct client_node_info info;

	p = buf;
	n = buf_len;
	while (n > 0) {
		addr = p;
		k = strnlen(p, n);
		if (k == n) {
			return 1;
		}
		p += k + 1;
		n -= k + 1;

		id_str = p;
		k = strnlen(p, n);
		if (k == n) {
			return 1;
		}
		p += k + 1;
		n -= k + 1;
		for (i = 0; i < k; i += 1) {
			if (id_str[i] < '0' || id_str[i] > '9') {
				return 1;
			}
		}
		errno = 0;
		id = strtoll(id_str, NULL, 10);
		if (errno != 0) {
			return 1;
		}

		role_str = p;
		k = strnlen(p, n);
		if (k == n) {
			return 1;
		}
		p += k + 1;
		n -= k + 1;
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

/* TODO atomicity */
static int writeNodeStore(struct dqlite_server *server)
{
	unsigned i;
	ssize_t limit = 0;
	char *buf;
	char *p;
	ssize_t k;
	ssize_t written;
	off_t off;

	/* Upper-bound the amount of memory required to serialize the list
	 * of nodes. */
	for (i = 0; i < server->cache.len; i += 1) {
		/* NUL-terminated address string */
		limit += (ssize_t)strlen(server->cache.nodes[i].addr) + 1;
		/* ID string: at most 20 decimal digits to represent a uint64_t,
		 * plus NUL terminator */
		limit += 20 + 1;
		/* role string: longest of {"voter", "standby", "spare"},
		 * with NUL terminator */
		limit += 7 + 1;
	}

	buf = mallocChecked((size_t)limit);
	p = buf;
	for (i = 0; i < server->cache.len; i += 1) {
		k = snprintf(p, (size_t)limit, "%s",
			     server->cache.nodes[i].addr);
		limit -= k + 1;
		p += k + 1;

		k = snprintf(p, (size_t)limit, "%" PRIu64,
			     server->cache.nodes[i].id);
		limit -= k + 1;
		p += k + 1;

		k = snprintf(
		    p, (size_t)limit, "%s",
		    (server->cache.nodes[i].role == DQLITE_SPARE)
			? "spare"
			: ((server->cache.nodes[i].role == DQLITE_STANDBY)
			       ? "standby"
			       : "voter"));
		limit -= k + 1;
		p += k + 1;
	}

	off = ftruncate(server->store_fd, 0);
	if (off != 0) {
		goto err_after_alloc_buf;
	}
	written = pwrite(server->store_fd, buf, (size_t)(p - buf), 0);
	if (written < p - buf) {
		goto err_after_alloc_buf;
	}

	free(buf);
	return 0;

err_after_alloc_buf:
	free(buf);
	return 1;
}

static int parseLocalInfo(const char *buf,
			  size_t len,
			  char **local_addr,
			  uint64_t *local_id)
{
	const char *p = buf;
	size_t n = len;
	size_t k;
	size_t i;
	const char *addr;
	const char *id_str;
	long long id;

	addr = p;
	k = strnlen(addr, n);
	if (k == n) {
		return 1;
	}
	p += k + 1;
	n -= k + 1;

	id_str = p;
	k = strnlen(id_str, n);
	if (k == n) {
		return 1;
	}
	p += k + 1;
	n -= k + 1;
	for (i = 0; i < k; i += 1) {
		if (id_str[i] < '0' || id_str[i] > '9') {
			return 1;
		}
	}
	errno = 0;
	id = strtoll(id_str, NULL, 10);
	if (errno != 0) {
		return 1;
	}

	*local_addr = strdupChecked(addr);
	*local_id = (uint64_t)id;
	return 0;
}

/* TODO atomicity */
static int writeLocalInfo(struct dqlite_server *server)
{
	ssize_t limit = (ssize_t)strlen(server->local_addr) + 1 + 20 + 1;
	char *buf = mallocChecked((size_t)limit);
	char *p = buf;
	ssize_t k;
	ssize_t written;
	off_t off;

	k = snprintf(p, (size_t)limit, "%s", server->local_addr);
	p += k + 1;
	limit -= k + 1;

	k = snprintf(p, (size_t)limit, "%" PRIu64, server->local_id);
	p += k + 1;
	limit -= k + 1;

	off = ftruncate(server->store_fd, 0);
	if (off != 0) {
		goto err_after_alloc_buf;
	}
	written = pwrite(server->info_fd, buf, (size_t)(p - buf), 0);
	if (written < (ssize_t)(p - buf)) {
		goto err_after_alloc_buf;
	}

	free(buf);
	return 0;

err_after_alloc_buf:
	free(buf);
	return 1;
}

int dqlite_server_create(const char *path, dqlite_server **server)
{
	int dir_fd;
	int info_fd;
	int store_fd;
	ssize_t size;
	bool is_new;
	char *buf;
	ssize_t n_read;
	char *local_addr;
	uint64_t local_id;
	struct node_store_cache cache = {0};
	struct dqlite_server *result;
	int rv;

	/* Open the data directory, which will be used by both the client and
	 * the server. */
	dir_fd = open(path, O_RDWR | O_DIRECTORY);
	if (dir_fd < 0) {
		goto err;
	}
	info_fd = openat(dir_fd, "info", O_RDWR | O_CREAT,
			 S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
	if (info_fd < 0) {
		close(dir_fd);
		goto err;
	}
	store_fd = openat(dir_fd, "node-store", O_RDWR | O_CREAT,
			  S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
	if (store_fd < 0) {
		close(dir_fd);
		goto err_after_open_info;
	}
	close(dir_fd);

	/* Load this node's information from disk, if it exists. */
	size = lseek(info_fd, 0, SEEK_END);
	if (size < 0) {
		goto err_after_open_store;
	}
	if (size > 0) {
		is_new = false;
		buf = mallocChecked((size_t)size);
		n_read = pread(info_fd, buf, (size_t)size, 0);
		if (n_read < (ssize_t)size) {
			free(buf);
			goto err_after_open_store;
		}
		rv = parseLocalInfo(buf, (size_t)size, &local_addr, &local_id);
		if (rv != 0) {
			free(buf);
			goto err_after_open_store;
		}
		free(buf);
	}

	/* Initialize the in-memory node store from the persisted store, if
	 * necessary. */
	size = lseek(store_fd, 0, SEEK_END);
	if (size < 0) {
		goto err_after_open_store;
	}
	if (size > 0) {
		if (is_new) {
			/* This shouldn't happen, since we don't initialize the
			 * node store until local node info has been persisted.
			 */
			goto err_after_open_store;
		}

		buf = mallocChecked((size_t)size);
		n_read = pread(info_fd, buf, (size_t)size, 0);
		if (n_read < (ssize_t)size) {
			free(buf);
			goto err_after_open_store;
		}
		rv = parseNodeStore(buf, (size_t)size, &cache);
		if (rv != 0) {
			free(buf);
			goto err_after_open_store;
		}
		free(buf);
	}

	result = callocChecked(1, sizeof *result);
	rv = pthread_cond_init(&result->cond, NULL);
	assert(rv == 0);
	rv = pthread_mutex_init(&result->mutex, NULL);
	assert(rv == 0);
	result->cache = cache;
	result->is_new = is_new;
	result->local_id = local_id;
	result->local_addr = local_addr;
	result->bind_addr = strdupChecked(local_addr);
	result->store_fd = store_fd;
	result->info_fd = info_fd;
	result->connect = transportDefaultConnect;
	result->voters = 3;
	result->standbys = 1;

	*server = result;
	return 0;

err_after_open_store:
	close(store_fd);
err_after_open_info:
	close(info_fd);
err:
	return 1;
}

int dqlite_node_set_address(dqlite_server *server, const char *address)
{
	free(server->local_addr);
	server->local_addr = strdupChecked(address);
	return 0;
}

int dqlite_node_set_bootstrap(dqlite_server *server)
{
	server->bootstrap = true;
	return 0;
}

int dqlite_server_set_peer_address(dqlite_server *server, const char *addr)
{
	struct client_node_info info = {0};
	info.addr = strdupChecked(addr);
	pushNodeInfo(&server->cache, info);
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

int dqlite_server_set_target_voters(dqlite_server *server, int n)
{
	/* TODO checks */
	server->voters = n;
	return 0;
}

int dqlite_server_set_target_standbys(dqlite_server *server, int n)
{
	/* TODO checks */
	server->standbys = n;
	return 0;
}

static int connectToSomeServer(struct dqlite_server *server)
{
	unsigned i;
	int rv;

	if (server->cache.len == 0) {
		/* nothing to connect to! */
		return 1;
	}
	for (i = 0; i < server->cache.len; i += 1) {
		rv = clientOpen(&server->proto, server->cache.nodes[i].addr,
				server->cache.nodes[i].id);
		if (rv == 0) {
			return 0;
		}
	}
	return 1;
}

static int reconnectToLeader(struct client_proto *proto,
			     struct client_context *context)
{
	char *addr;
	uint64_t id;
	int rv;

	rv = clientSendLeader(proto, context);
	if (rv != 0) {
		return 1;
	}
	rv = clientRecvServer(proto, &id, &addr, context);
	if (rv != 0) {
		return 1;
	}
	clientClose(proto);
	rv = clientOpen(proto, addr, id);
	free(addr);
	if (rv != 0) {
		return 1;
	}
	return 0;
}

/* TODO should this make sure that we are asking the leader? */
static int refreshNodeStoreCache(struct dqlite_server *server,
				 struct client_context *context)
{
	struct client_node_info *servers;
	uint64_t n_servers;
	int rv;

	rv = clientSendCluster(&server->proto, context);
	if (rv != 0) {
		return 1;
	}
	rv = clientRecvServers(&server->proto, &servers, &n_servers, context);
	if (rv != 0) {
		return 1;
	}
	emptyCache(&server->cache);
	server->cache.nodes = servers;
	server->cache.len = (unsigned)n_servers;
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
		return 1;
	}
	rv = clientRecvEmpty(&server->proto, context);
	if (rv != 0) {
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
		rv = clientOpen(&server->proto, server->local_addr,
				server->local_id);
		if (rv != 0) {
			return 1;
		}

		rv = writeLocalInfo(server);
		if (rv != 0) {
			return 1;
		}
		/* TODO assert that no peers have been declared */
		info.addr = strdupChecked(server->local_addr);
		info.id = server->local_id;
		info.role = DQLITE_VOTER;
		pushNodeInfo(&server->cache, info);
		rv = writeNodeStore(server);
		if (rv != 0) {
			return 1;
		}
		return 0;
	}

	rv = connectToSomeServer(server);
	if (rv != 0) {
		return 1;
	}

	rv = reconnectToLeader(&server->proto, context);
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

	rv = writeLocalInfo(server);
	if (rv != 0) {
		return 1;
	}

	rv = writeNodeStore(server);
	if (rv != 0) {
		return 1;
	}

	return 0;
}

static void *refreshTask(void *arg)
{
	struct dqlite_server *server = arg;
	struct client_context context;
	struct timespec ts;
	int rv;

	rv = pthread_mutex_lock(&server->mutex);
	assert(rv == 0);
	for (;;) {
		rv = clock_gettime(CLOCK_REALTIME, &ts);
		assert(rv == 0);
		/* TODO make this configurable */
		ts.tv_sec += 30;
		rv = pthread_cond_timedwait(&server->cond, &server->mutex, &ts);
		assert(rv == 0);
		if (server->shutdown) {
			break;
		}
		clientContextMillis(&context, 5000);
		rv = refreshNodeStoreCache(server, &context);
		if (rv != 0) {
			continue;
		}
		(void)writeNodeStore(server);
	}
	return NULL;
}

int dqlite_server_start(dqlite_server *server)
{
	struct client_context context;
	int rv;

	clientContextMillis(&context, 5000);

	if (server->local_addr == NULL) {
		goto err;
	}
	server->local_id = server->bootstrap
			       ? MAGIC_BOOTSTRAP_ID
			       : dqlite_generate_node_id(server->local_addr);
	/* Configure and start the local server. */
	rv = dqlite_node_create(server->local_id, server->local_addr,
				server->dir_path, &server->local);
	if (rv != 0) {
		goto err_after_create_node;
	}
	if (server->bind_addr != NULL) {
		rv = dqlite_node_set_bind_address(server->local,
						  server->bind_addr);
		free(server->bind_addr);
		server->bind_addr = NULL;
		if (rv != 0) {
			goto err_after_create_node;
		}
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

	rv = bootstrapOrJoinCluster(server, &context);
	if (rv != 0) {
		goto err_after_start_node;
	}

	/* Launch a background thread to refresh the node store. */
	rv = pthread_create(&server->refresh_thread, NULL, refreshTask, server);
	if (rv != 0) {
		goto err_after_start_node;
	}

	return 0;

err_after_start_node:
	dqlite_node_stop(server->local);
err_after_create_node:
	dqlite_node_destroy(server->local);
err:
	return 1;
}
