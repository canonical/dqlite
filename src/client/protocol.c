#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

#include "../lib/assert.h"

#include "protocol.h"
#include "../message.h"
#include "../protocol.h"
#include "../request.h"
#include "../response.h"
#include "../tracing.h"
#include "../tuple.h"

static ssize_t readExact(int fd, void *buf, size_t n)
{
	size_t got = 0;
	ssize_t rv;
	while (got < n) {
		rv = read(fd, (char *)buf + got, n - got);
		if (rv < 0) {
			if (errno == EINTR) {
				continue;
			} else {
				return rv;
			}
		} else if (rv == 0) {
			break;
		}
		got += (size_t)rv;
	}
	return (ssize_t)got;
}

int clientInit(struct client_proto *c, int fd)
{
	tracef("init client");
	int rv;
	c->fd = fd;
	c->db_name = NULL;
	c->db_is_init = false;

	rv = buffer__init(&c->read);
	if (rv != 0) {
		tracef("init client read buffer init failed");
		goto err;
	}
	rv = buffer__init(&c->write);
	if (rv != 0) {
		tracef("init client write buffer init failed");
		goto err_after_read_buffer_init;
	}

	c->errcode = 0;
	c->errmsg = NULL;

	return 0;

err_after_read_buffer_init:
	buffer__close(&c->read);
err:
	return rv;
}

void clientClose(struct client_proto *c)
{
	tracef("client close");
	buffer__close(&c->write);
	buffer__close(&c->read);
	if (c->db_name != NULL) {
		free(c->db_name);
	}
	if (c->errmsg != NULL) {
		free(c->errmsg);
	}
}

int clientSendHandshake(struct client_proto *c)
{
	uint64_t protocol;
	ssize_t rv;

	tracef("client send handshake");
	protocol = ByteFlipLe64(DQLITE_PROTOCOL_VERSION);

	rv = write(c->fd, &protocol, sizeof protocol);
	if (rv < 0) {
		tracef("client send handshake failed %zd", rv);
		return DQLITE_ERROR;
	}

	return 0;
}

static int writeMessage(struct client_proto *c, uint8_t type, uint8_t schema)
{
	struct message message = {0};
	size_t n;
	size_t words;
	void *cursor;
	ssize_t rv;
	n = buffer__offset(&c->write);
	words = (n - message__sizeof(&message)) / 8;
	message.words = (uint32_t)words;
	message.type = type;
	message.schema = schema;
	cursor = buffer__cursor(&c->write, 0);
	message__encode(&message, &cursor);
	rv = write(c->fd, buffer__cursor(&c->write, 0), n);
	if (rv != (ssize_t)n) {
		tracef("request write failed rv:%zd", rv);
		return DQLITE_ERROR;
	}
	return 0;
}

#define BUFFER_REQUEST(LOWER, UPPER)                             \
	{                                                        \
		struct message _message = {0};                   \
		size_t _n1;                                      \
		size_t _n2;                                      \
		void *_cursor;                                   \
		_n1 = message__sizeof(&_message);                \
		_n2 = request_##LOWER##__sizeof(&request);       \
		buffer__reset(&c->write);                        \
		_cursor = buffer__advance(&c->write, _n1 + _n2); \
		if (_cursor == NULL) {                           \
			return DQLITE_NOMEM;                     \
		}                                                \
		assert(_n2 % 8 == 0);                            \
		message__encode(&_message, &_cursor);            \
		request_##LOWER##__encode(&request, &_cursor);   \
	}

/* Write out a request. */
#define REQUEST(LOWER, UPPER, SCHEMA)                                  \
	{                                                              \
		int _rv;                                               \
		BUFFER_REQUEST(LOWER, UPPER);                          \
		_rv = writeMessage(c, DQLITE_REQUEST_##UPPER, SCHEMA); \
		if (_rv != 0) {                                        \
			return _rv;                                    \
		}                                                      \
	}

static int readMessage(struct client_proto *c, uint8_t *type)
{
	struct message message = {0};
	struct cursor cursor;
	void *p;
	size_t n;
	ssize_t rv;

	buffer__reset(&c->read);
	n = message__sizeof(&message);
	p = buffer__advance(&c->read, n);
	if (p == NULL) {
		tracef("buffer advance failed");
		return DQLITE_ERROR;
	}
	rv = readExact(c->fd, p, n);
	if (rv != (ssize_t)n) {
		tracef("read head failed rv:%zd", rv);
		return DQLITE_ERROR;
	}

	cursor.p = p;
	cursor.cap = n;
	rv = message__decode(&cursor, &message);
	if (rv != 0) {
		tracef("message decode failed rv:%zd", rv);
		return DQLITE_ERROR;
	}

	buffer__reset(&c->read);
	n = message.words * 8;
	p = buffer__advance(&c->read, n);
	if (p == NULL) {
		tracef("buffer advance failed");
		return DQLITE_ERROR;
	}
	rv = readExact(c->fd, p, n);
	if (rv != (ssize_t)n) {
		tracef("read body failed rv:%zd", rv);
	}

	*type = message.type;
	return 0;
}

/* Read and decode a response. */
#define RESPONSE(LOWER, UPPER)                                                 \
	{                                                                      \
		struct response_failure _failure;                              \
		uint8_t _type;                                                 \
		int _rv;                                                       \
		_rv = readMessage(c, &_type);                                  \
		if (_rv != 0) {                                                \
			return _rv;                                            \
		}                                                              \
		cursor.p = buffer__cursor(&c->read, 0);                        \
		cursor.cap = buffer__offset(&c->read);                         \
		if (_type != DQLITE_RESPONSE_##UPPER &&                        \
		    _type == DQLITE_RESPONSE_FAILURE) {                        \
			_rv = response_failure__decode(&cursor, &_failure);    \
			if (_rv != 0) {                                        \
				tracef("decode as failure failed rv:%d", _rv); \
				return DQLITE_ERROR;                           \
			}                                                      \
			c->errcode = _failure.code;                            \
			if (c->errmsg != NULL) {                               \
				free(c->errmsg);                               \
			}                                                      \
			c->errmsg = strdup(_failure.message);                  \
			if (c->errmsg == NULL) {                               \
				return DQLITE_NOMEM;                           \
			}                                                      \
			return DQLITE_ERROR_RESPONSE;                          \
		} else if (_type != DQLITE_RESPONSE_##UPPER) {                 \
			return DQLITE_ERROR;                                   \
		}			                                       \
		_rv = response_##LOWER##__decode(&cursor, &response);          \
		if (_rv != 0) {                                                \
			tracef("decode failed rv:%d", _rv);                    \
			return  DQLITE_ERROR;                                  \
		}                                                              \
	}

int clientSendLeader(struct client_proto *c)
{
	tracef("client send leader");
	struct request_leader request = {0};
	REQUEST(leader, LEADER, 0);
	return 0;
}

int clientSendClient(struct client_proto *c, uint64_t id)
{
	tracef("client send client");
	struct request_client request;
	request.id = id;
	REQUEST(client, CLIENT, 0);
	return 0;
}

int clientSendOpen(struct client_proto *c, const char *name)
{
	tracef("client send open name %s", name);
	struct request_open request;
	c->db_name = strdup(name);
	if (c->db_name == NULL) {
		return DQLITE_NOMEM;
	}
	request.filename = name;
	request.flags = 0;    /* TODO: this is unused, should we drop it? */
	request.vfs = "test"; /* TODO: this is unused, should we drop it? */
	REQUEST(open, OPEN, 0);
	return 0;
}

int clientRecvDb(struct client_proto *c)
{
	tracef("client recvdb");
	struct cursor cursor;
	struct response_db response;
	RESPONSE(db, DB);
	c->db_id = response.id;
	c->db_is_init = true;
	return 0;
}

int clientSendPrepare(struct client_proto *c, const char *sql)
{
	tracef("client send prepare");
	struct request_prepare request;
	request.db_id = c->db_id;
	request.sql = sql;
	REQUEST(prepare, PREPARE, 0);
	return 0;
}

int clientRecvStmt(struct client_proto *c, unsigned *stmt_id)
{
	struct cursor cursor;
	struct response_stmt response;
	RESPONSE(stmt, STMT);
	*stmt_id = response.id;
	tracef("client recv stmt stmt_id %u", *stmt_id);
	return 0;
}

static int bufferParams(struct client_proto *c,
			struct value *params,
			size_t n_params)
{
	struct tuple_encoder tup;
	size_t i;
	int rv;

	if (n_params == 0) {
		return 0;
	}
	rv = tuple_encoder__init(&tup, n_params, TUPLE__PARAMS32, &c->write);
	if (rv != 0) {
		return rv;
	}
	for (i = 0; i < n_params; ++i) {
		rv = tuple_encoder__next(&tup, &params[i]);
		if (rv != 0) {
			return rv;
		}
	}
	return 0;
}

int clientSendExec(struct client_proto *c,
		   unsigned stmt_id,
		   struct value *params,
		   size_t n_params)
{
	tracef("client send exec id %u", stmt_id);
	struct request_exec request;
	int rv;

	request.db_id = c->db_id;
	request.stmt_id = stmt_id;
	BUFFER_REQUEST(exec, EXEC);

	rv = bufferParams(c, params, n_params);
	if (rv != 0) {
		return rv;
	}
	rv = writeMessage(c, DQLITE_REQUEST_EXEC, 1);
	return rv;
}

int clientSendExecSQL(struct client_proto *c,
		      const char *sql,
		      struct value *params,
		      size_t n_params)
{
	tracef("client send exec sql");
	struct request_exec_sql request;
	int rv;

	request.db_id = c->db_id;
	request.sql = sql;
	BUFFER_REQUEST(exec_sql, EXEC_SQL);

	rv = bufferParams(c, params, n_params);
	if (rv != 0) {
		return rv;
	}
	rv = writeMessage(c, DQLITE_REQUEST_EXEC_SQL, 1);
	return rv;
}

int clientRecvResult(struct client_proto *c,
		     unsigned *last_insert_id,
		     unsigned *rows_affected)
{
	struct cursor cursor;
	struct response_result response;
	RESPONSE(result, RESULT);
	*last_insert_id = (unsigned)response.last_insert_id;
	*rows_affected = (unsigned)response.rows_affected;
	tracef("client recv result last_insert_id %u rows_affected %u", *last_insert_id, *rows_affected);
	return 0;
}

int clientSendQuery(struct client_proto *c,
		    unsigned stmt_id,
		    struct value *params,
		    size_t n_params)
{
	tracef("client send query stmt_id %u", stmt_id);
	struct request_query request;
	int rv;

	request.db_id = c->db_id;
	request.stmt_id = stmt_id;
	BUFFER_REQUEST(query, QUERY);

	rv = bufferParams(c, params, n_params);
	if (rv != 0) {
		return rv;
	}
	rv = writeMessage(c, DQLITE_REQUEST_QUERY, 1);
	return rv;
}

int clientSendQuerySQL(struct client_proto *c,
		       const char *sql,
		       struct value *params,
		       size_t n_params)
{
	tracef("client send query sql sql %s", sql);
	struct request_query_sql request;
	int rv;

	request.db_id = c->db_id;
	request.sql = sql;
	BUFFER_REQUEST(query_sql, QUERY_SQL);

	rv = bufferParams(c, params, n_params);
	if (rv != 0) {
		return rv;
	}
	rv = writeMessage(c, DQLITE_REQUEST_QUERY_SQL, 1);
	return rv;
}

int clientRecvRows(struct client_proto *c, struct rows *rows)
{
	tracef("client recv rows");
	struct cursor cursor;
	struct response_failure failure;
	struct tuple_decoder decoder;
	uint64_t column_count;
	struct row *last;
	unsigned i;
	uint8_t type = 0;
	int rv;

	rv = readMessage(c, &type);
	if (rv != 0) {
		return rv;
	}
	if (type == DQLITE_RESPONSE_FAILURE) {
		cursor.p = buffer__cursor(&c->read, 0);
		cursor.cap = buffer__offset(&c->read);
		rv = response_failure__decode(&cursor, &failure);
		if (rv != 0) {
			tracef("decode as failure failed rv:%d", rv);
			return DQLITE_ERROR;
		}
		c->errcode = failure.code;
		if (c->errmsg != NULL) {
			free(c->errmsg);
		}
		c->errmsg = strdup(failure.message);
		return DQLITE_ERROR_RESPONSE;
	} else if (type != DQLITE_RESPONSE_ROWS) {
		tracef("bad message type:%" PRIu8, type);
		return DQLITE_ERROR;
	}

	cursor.p = buffer__cursor(&c->read, 0);
	cursor.cap = buffer__offset(&c->read);
	rv = uint64__decode(&cursor, &column_count);
	if (rv != 0) {
		tracef("client recv rows decode failed %d", rv);
		return DQLITE_ERROR;
	}
	rows->column_count = (unsigned)column_count;
	for (i = 0; i < rows->column_count; i++) {
		rows->column_names = malloc(column_count * sizeof *rows->column_names);
		if (rows->column_names == NULL) {
			return DQLITE_ERROR;
		}
		rv = text__decode(&cursor, &rows->column_names[i]);
		if (rv != 0) {
			return DQLITE_ERROR;
		}
	}
	last = NULL;
	while (1) {
		uint64_t eof;
		struct row *row;
		if (cursor.cap < 8) {
			/* No EOF marker fond */
			return DQLITE_ERROR;
		}
		eof = ByteFlipLe64(*(uint64_t *)cursor.p);
		if (eof == DQLITE_RESPONSE_ROWS_DONE ||
		    eof == DQLITE_RESPONSE_ROWS_PART) {
			break;
		}
		row = malloc(sizeof *row);
		if (row == NULL) {
			tracef("malloc");
			return DQLITE_NOMEM;
		}
		row->values = malloc(column_count * sizeof *row->values);
		if (row->values == NULL) {
			tracef("malloc");
			free(row);
			return DQLITE_NOMEM;
		}
		row->next = NULL;
		rv = tuple_decoder__init(&decoder, (unsigned)column_count,
					 TUPLE__ROW, &cursor);
		if (rv != 0) {
			tracef("decode init error %d", rv);
			free(row->values);
			free(row);
			return DQLITE_ERROR;
		}
		for (i = 0; i < rows->column_count; i++) {
			rv = tuple_decoder__next(&decoder, &row->values[i]);
			if (rv != 0) {
				tracef("decode error %d", rv);
				free(row->values);
				free(row);
				return DQLITE_ERROR;
			}
		}
		if (last == NULL) {
			rows->next = row;
		} else {
			last->next = row;
		}
		last = row;
	}
	return 0;
}

void clientCloseRows(struct rows *rows)
{
	struct row *row = rows->next;
	while (row != NULL) {
		struct row *next;
		next = row->next;
		free(row->values);
		free(row);
		row = next;
	}
	free(rows->column_names);
}

int clientSendInterrupt(struct client_proto *c)
{
	tracef("client send interrupt");
	struct request_interrupt request;
	request.db_id = c->db_id;
	REQUEST(interrupt, INTERRUPT, 0);
	return 0;
}

int clientSendFinalize(struct client_proto *c, unsigned stmt_id)
{
	tracef("client send finalize %u", stmt_id);
	struct request_finalize request;
	request.db_id = c->db_id;
	request.stmt_id = stmt_id;
	REQUEST(finalize, FINALIZE, 0);
	return 0;
}

int clientSendAdd(struct client_proto *c, unsigned id, const char *address)
{
	tracef("client send add id %u address %s", id, address);
	struct request_add request;
	request.id = id;
	request.address = address;
	REQUEST(add, ADD, 0);
	return 0;
}

int clientSendAssign(struct client_proto *c, unsigned id, int role)
{
	tracef("client send assign id %u role %d", id, role);
	assert(role == DQLITE_VOTER || role == DQLITE_STANDBY || role == DQLITE_SPARE);
	struct request_assign request;
	request.id = id;
	request.role = (uint64_t)role;
	REQUEST(assign, ASSIGN, 0);
	return 0;
}

int clientSendRemove(struct client_proto *c, unsigned id)
{
	tracef("client send remove id %u", id);
	struct request_remove request;
	request.id = id;
	REQUEST(remove, REMOVE, 0);
	return 0;
}

int clientSendDump(struct client_proto *c)
{
	tracef("client send dump");
	struct request_dump request;
	assert(c->db_is_init);
	assert(c->db_name != NULL);
	request.filename = c->db_name;
	REQUEST(dump, DUMP, 0);
	return 0;
}

int clientSendCluster(struct client_proto *c)
{
	tracef("client send cluster");
	struct request_cluster request;
	request.format = DQLITE_REQUEST_CLUSTER_FORMAT_V1;
	REQUEST(cluster, CLUSTER, 0);
	return 0;
}

int clientSendTransfer(struct client_proto *c, unsigned id)
{
	tracef("client send transfer id %u", id);
	struct request_transfer request;
	request.id = id;
	REQUEST(transfer, TRANSFER, 0);
	return 0;
}

int clientSendDescribe(struct client_proto *c)
{
	tracef("client send describe");
	struct request_describe request;
	request.format = DQLITE_REQUEST_DESCRIBE_FORMAT_V0;
	REQUEST(describe, DESCRIBE, 0);
	return 0;
}

int clientSendWeight(struct client_proto *c, unsigned weight)
{
	tracef("client send weight %u", weight);
	struct request_weight request;
	request.weight = (unsigned)weight;
	REQUEST(weight, WEIGHT, 0);
	return 0;
}

int clientRecvServer(struct client_proto *c, uint64_t *id, char **address)
{
	tracef("client recv server");
	struct cursor cursor;
	struct response_server response;
	*id = 0;
	*address = NULL;
	RESPONSE(server, SERVER);
	*address = strdup(response.address);
	if (*address == NULL) {
		return DQLITE_NOMEM;
	}
	*id = response.id;
	return 0;
}

int clientRecvWelcome(struct client_proto *c)
{
	tracef("client recv welcome");
	struct cursor cursor;
	struct response_welcome response;
	RESPONSE(welcome, WELCOME);
	return 0;
}

int clientRecvEmpty(struct client_proto *c)
{
	tracef("client recv empty");
	struct cursor cursor;
	struct response_empty response;
	RESPONSE(empty, EMPTY);
	return 0;
}

int clientRecvFailure(struct client_proto *c, uint64_t *code, const char **msg)
{
	tracef("client recv failure");
	struct cursor cursor;
	struct response_failure response;
	RESPONSE(failure, FAILURE);
	*code = response.code;
	*msg = response.message;
	return 0;
}

int clientRecvServers(struct client_proto *c, struct client_node_info **servers, size_t *n_servers)
{
	tracef("client recv servers");
	struct cursor cursor;
	uint64_t i = 0;
	uint64_t j;
	uint64_t raw_role;
	const char *raw_addr;
	struct response_servers response;
	int rv;

	*servers = NULL;
	*n_servers = 0;

	RESPONSE(servers, SERVERS);

	struct client_node_info *srvs = calloc(response.n, sizeof(*srvs));
	if (!srvs) {
		rv = DQLITE_NOMEM;
		goto err_after_alloc_srvs;
	}
	for (; i < response.n; ++i) {
		rv = uint64__decode(&cursor, &srvs[i].id);
		if (rv != 0) {
			goto err_after_alloc_srvs;
		}
		rv = text__decode(&cursor, &raw_addr);
		if (rv != 0) {
			goto err_after_alloc_srvs;
		}
		srvs[i].addr = strdup(raw_addr);
		if (srvs[i].addr == NULL) {
			rv = DQLITE_NOMEM;
			goto err_after_alloc_srvs;
		}
		rv = uint64__decode(&cursor, &raw_role);
		if (rv != 0) {
			free(srvs[i].addr);
			goto err_after_alloc_srvs;
		}
		srvs[i].role = (int)raw_role;
	}

	*n_servers = response.n;
	*servers = srvs;
	return 0;

err_after_alloc_srvs:
	for (j = 0; j < i; ++j) {
		free(srvs[i].addr);
	}
	free(srvs);
	return rv;
}

int clientRecvFiles(struct client_proto *c, struct client_file **files, size_t *n_files)
{
	tracef("client recv files");
	struct cursor cursor;
	struct response_files response;
	size_t i = 0;
	size_t j;
	const char *raw_name;
	int rv;
	*files = NULL;
	*n_files = 0;
	RESPONSE(files, FILES);

	struct client_file *fs = calloc(response.n, sizeof(*fs));
	if (fs == NULL) {
		return DQLITE_NOMEM;
	}
	for (; i < response.n; ++i) {
		rv = text__decode(&cursor, &raw_name);
		if (rv != 0) {
			goto err_after_alloc_fs;
		}
		fs[i].name = strdup(raw_name);
		if (fs[i].name == NULL) {
			rv = DQLITE_NOMEM;
			goto err_after_alloc_fs;
		}
		rv = uint64__decode(&cursor, &fs[i].size);
		if (rv != 0) {
			free(fs[i].name);
			goto err_after_alloc_fs;
		}
		if (cursor.cap != fs[i].size) {
			free(fs[i].name);
			rv = DQLITE_PARSE;
			goto err_after_alloc_fs;
		}
		fs[i].blob = malloc(fs[i].size);
		if (fs[i].blob == NULL) {
			free(fs[i].name);
			rv = DQLITE_NOMEM;
			goto err_after_alloc_fs;
		}
		memcpy(fs[i].blob, cursor.p, fs[i].size);
	}

	*files = fs;
	*n_files = response.n;
	return 0;

err_after_alloc_fs:
	for (j = 0; j < i; ++j) {
		free(fs[i].name);
		free(fs[i].blob);
	}
	free(fs);
	return rv;
}

int clientRecvMetadata(struct client_proto *c, unsigned *failure_domain, unsigned *weight)
{
	tracef("client recv metadata");
	struct cursor cursor;
	struct response_metadata response;
	*failure_domain = 0;
	*weight = 0;
	RESPONSE(metadata, METADATA);
	*failure_domain = (unsigned)response.failure_domain;
	*weight = (unsigned)response.weight;
	return 0;
}
