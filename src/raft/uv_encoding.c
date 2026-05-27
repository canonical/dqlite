#include "uv_encoding.h"

#include <limits.h>
#include <string.h>

#include "../lib/assert.h"
#include "../raft.h"
#include "byte.h"
#include "configuration.h"

/**
 * Size of the request preamble.
 */
#define RAFT_IO_UV__PREAMBLE_SIZE               \
	(sizeof(uint64_t) /* Message type. */ + \
	 sizeof(uint64_t) /* Message size. */)



#define REQUEST_VOTE_V1_SIZE (sizeof(uint64_t) + /* Term. */ \
			    sizeof(uint64_t) + /* Candidate ID. */ \
			    sizeof(uint64_t) + /* Last log index. */ \
			    sizeof(uint64_t) /* Last log term. */)

#define REQUEST_VOTE_V2_SIZE (REQUEST_VOTE_V1_SIZE + sizeof(uint64_t) /* Flags. */)
#define REQUEST_VOTE_RESULT_V1_SIZE (sizeof(uint64_t) + /* Term. */ \
				      sizeof(uint64_t) /* Vote granted. */)
#define REQUEST_VOTE_RESULT_V2_SIZE \
	(REQUEST_VOTE_RESULT_V1_SIZE + sizeof(uint64_t) /* Flags. */)
#define APPEND_ENTRIES_RESULT_V0_SIZE (sizeof(uint64_t) + /* Term. */ \
				       sizeof(uint64_t) + /* Success. */ \
				       sizeof(uint64_t) /* Last log index. */)
#define APPEND_ENTRIES_RESULT_V1_SIZE \
	(APPEND_ENTRIES_RESULT_V0_SIZE + sizeof(uint64_t) /* 64 bit Flags. */)
#define APPEND_ENTRIES_REQUEST_PREFIX_SIZE \
	(4 * sizeof(uint64_t) /* term, prev_log_index, prev_log_term, leader_commit */)
#define TIMEOUT_NOW_SIZE (sizeof(uint64_t) + /* Term. */ \
			  sizeof(uint64_t) + /* Last log index. */ \
			  sizeof(uint64_t) /* Last log term. */)

static size_t sizeofAppendEntries(const struct raft_append_entries *p)
{
	return sizeof(uint64_t) + /* Leader's term. */
	       sizeof(uint64_t) + /* Leader ID */
	       sizeof(uint64_t) + /* Previous log entry index */
	       sizeof(uint64_t) + /* Previous log entry term */
	       sizeof(uint64_t) + /* Leader's commit index */
	       sizeof(uint64_t) + /* Number of entries in the batch */
	       16 * p->n_entries /* One header per entry */;
}


static size_t sizeofInstallSnapshot(const struct raft_install_snapshot *p)
{
	size_t conf_size = configurationEncodedSize(&p->conf);
	return sizeof(uint64_t) + /* Leader's term. */
	       sizeof(uint64_t) + /* Leader ID */
	       sizeof(uint64_t) + /* Snapshot's last index */
	       sizeof(uint64_t) + /* Term of last index */
	       sizeof(uint64_t) + /* Configuration's index */
	       sizeof(uint64_t) + /* Length of configuration */
	       conf_size +        /* Configuration data */
	       sizeof(uint64_t);  /* Length of snapshot data */
}

size_t uvSizeofBatchHeader(size_t n)
{
	size_t res = 8 + /* Number of entries in the batch, little endian */
		16 * n; /* One header per entry */;
	return res;
}

static void encodeRequestVote(const struct raft_request_vote *p, void *buf)
{
	void *cursor = buf;
	uint64_t flags = 0;

	if (p->disrupt_leader) {
		flags |= 1 << 0;
	}
	if (p->pre_vote) {
		flags |= 1 << 1;
	}

	bytePut64(&cursor, p->term);
	bytePut64(&cursor, p->candidate_id);
	bytePut64(&cursor, p->last_log_index);
	bytePut64(&cursor, p->last_log_term);
	bytePut64(&cursor, flags);
}

static void encodeRequestVoteResult(const struct raft_request_vote_result *p,
				    void *buf)
{
	void *cursor = buf;
	uint64_t flags = 0;

	if (p->pre_vote) {
		flags |= (1 << 0);
	}

	bytePut64(&cursor, p->term);
	bytePut64(&cursor, p->vote_granted);
	bytePut64(&cursor, flags);
}

static void encodeAppendEntries(const struct raft_append_entries *p, void *buf)
{
	void *cursor;

	cursor = buf;

	bytePut64(&cursor, p->term);           /* Leader's term. */
	bytePut64(&cursor, p->prev_log_index); /* Previous index. */
	bytePut64(&cursor, p->prev_log_term);  /* Previous term. */
	bytePut64(&cursor, p->leader_commit);  /* Commit index. */

	uvEncodeBatchHeader(p->entries, p->n_entries, cursor);
}

static void encodeAppendEntriesResult(
    const struct raft_append_entries_result *p,
    void *buf)
{
	void *cursor = buf;

	bytePut64(&cursor, p->term);
	bytePut64(&cursor, p->rejected);
	bytePut64(&cursor, p->last_log_index);
	bytePut64(&cursor, p->features);
}

static void encodeInstallSnapshot(const struct raft_install_snapshot *p,
				  void *buf)
{
	void *cursor;
	size_t conf_size = configurationEncodedSize(&p->conf);

	cursor = buf;

	bytePut64(&cursor, p->term);       /* Leader's term. */
	bytePut64(&cursor, p->last_index); /* Snapshot last index. */
	bytePut64(&cursor, p->last_term);  /* Term of last index. */
	bytePut64(&cursor, p->conf_index); /* Configuration index. */
	bytePut64(&cursor, conf_size);     /* Configuration length. */
	configurationEncodeToBuf(&p->conf, cursor);
	cursor = (uint8_t *)cursor + conf_size;
	bytePut64(&cursor, p->data.len); /* Snapshot data size. */
}

static void encodeTimeoutNow(const struct raft_timeout_now *p, void *buf)
{
	void *cursor = buf;

	bytePut64(&cursor, p->term);
	bytePut64(&cursor, p->last_log_index);
	bytePut64(&cursor, p->last_log_term);
}

int uvEncodeMessage(const struct raft_message *message,
		    uv_buf_t **bufs,
		    unsigned *n_bufs)
{
	uv_buf_t header;
	void *cursor;

	/* Figure out the length of the header for this request and allocate a
	 * buffer for it. */
	header.len = RAFT_IO_UV__PREAMBLE_SIZE;
	switch (message->type) {
		case RAFT_IO_REQUEST_VOTE:
			header.len += REQUEST_VOTE_V2_SIZE;
			break;
		case RAFT_IO_REQUEST_VOTE_RESULT:
			header.len += REQUEST_VOTE_RESULT_V2_SIZE;
			break;
		case RAFT_IO_APPEND_ENTRIES:
			header.len +=
			    sizeofAppendEntries(&message->append_entries);
			break;
		case RAFT_IO_APPEND_ENTRIES_RESULT:
			header.len += APPEND_ENTRIES_RESULT_V1_SIZE;
			break;
		case RAFT_IO_INSTALL_SNAPSHOT:
			header.len +=
			    sizeofInstallSnapshot(&message->install_snapshot);
			break;
		case RAFT_IO_TIMEOUT_NOW:
			header.len += TIMEOUT_NOW_SIZE;
			break;
		default:
			return RAFT_MALFORMED;
	};

	header.base = raft_malloc(header.len);
	if (header.base == NULL) {
		goto oom;
	}

	cursor = header.base;

	/* Encode the request preamble, with message type and message size. */
	bytePut64(&cursor, message->type);
	bytePut64(&cursor, header.len - RAFT_IO_UV__PREAMBLE_SIZE);

	/* Encode the request header. */
	switch (message->type) {
		case RAFT_IO_REQUEST_VOTE:
			encodeRequestVote(&message->request_vote, cursor);
			break;
		case RAFT_IO_REQUEST_VOTE_RESULT:
			encodeRequestVoteResult(&message->request_vote_result,
						cursor);
			break;
		case RAFT_IO_APPEND_ENTRIES:
			encodeAppendEntries(&message->append_entries, cursor);
			break;
		case RAFT_IO_APPEND_ENTRIES_RESULT:
			encodeAppendEntriesResult(
			    &message->append_entries_result, cursor);
			break;
		case RAFT_IO_INSTALL_SNAPSHOT:
			encodeInstallSnapshot(&message->install_snapshot,
					      cursor);
			break;
		case RAFT_IO_TIMEOUT_NOW:
			encodeTimeoutNow(&message->timeout_now, cursor);
			break;
	};

	*n_bufs = 1;

	/* For AppendEntries request we also send the entries payload. */
	if (message->type == RAFT_IO_APPEND_ENTRIES) {
		*n_bufs += message->append_entries.n_entries;
	}

	/* For InstallSnapshot request we also send the snapshot payload. */
	if (message->type == RAFT_IO_INSTALL_SNAPSHOT) {
		*n_bufs += 1;
	}

	*bufs = raft_calloc(*n_bufs, sizeof **bufs);
	if (*bufs == NULL) {
		goto oom_after_header_alloc;
	}

	(*bufs)[0] = header;

	if (message->type == RAFT_IO_APPEND_ENTRIES) {
		unsigned i;
		for (i = 0; i < message->append_entries.n_entries; i++) {
			const struct raft_entry *entry =
			    &message->append_entries.entries[i];
			(*bufs)[i + 1].base = entry->buf.base;
			(*bufs)[i + 1].len = entry->buf.len;
		}
	}

	if (message->type == RAFT_IO_INSTALL_SNAPSHOT) {
		(*bufs)[1].base = message->install_snapshot.data.base;
		(*bufs)[1].len = message->install_snapshot.data.len;
	}

	return 0;

oom_after_header_alloc:
	raft_free(header.base);

oom:
	return RAFT_NOMEM;
}

void uvEncodeBatchHeader(const struct raft_entry *entries,
			 unsigned n,
			 void *buf)
{
	unsigned i;
	void *cursor = buf;

	/* Number of entries in the batch, little endian */
	bytePut64(&cursor, n);

	for (i = 0; i < n; i++) {
		const struct raft_entry *entry = &entries[i];

		/* Term in which the entry was created, little endian. */
		bytePut64(&cursor, entry->term);

		/* Message type (Either RAFT_COMMAND or RAFT_CHANGE) */
		bytePut8(&cursor, (uint8_t)entry->type);

		cursor = (uint8_t *)cursor + 3; /* Unused */

		/* Size of the log entry data, little endian. */
		bytePut32(&cursor, (uint32_t)entry->buf.len);
	}
}

static int decodeRequestVote(const uv_buf_t *buf, struct raft_request_vote *p)
{
	const void *cursor;

	if (buf->len != REQUEST_VOTE_V2_SIZE && buf->len != REQUEST_VOTE_V1_SIZE) {
		return RAFT_MALFORMED;
	}

	cursor = buf->base;

	p->term = byteGet64(&cursor);
	p->candidate_id = byteGet64(&cursor);
	p->last_log_index = byteGet64(&cursor);
	p->last_log_term = byteGet64(&cursor);

	/* Support for legacy request vote that doesn't have disrupt_leader. */
	if (buf->len == REQUEST_VOTE_V1_SIZE) {
		p->version = 1;
		p->disrupt_leader = false;
		p->pre_vote = false;
	} else {
		p->version = 2;
		uint64_t flags = byteGet64(&cursor);
		p->disrupt_leader = (bool)(flags & 1 << 0);
		p->pre_vote = (bool)(flags & 1 << 1);
	}

	return 0;
}

static int decodeRequestVoteResult(const uv_buf_t *buf,
				   struct raft_request_vote_result *p)
{
	const void *cursor;

	if (buf->len != REQUEST_VOTE_RESULT_V1_SIZE &&
	    buf->len != REQUEST_VOTE_RESULT_V2_SIZE) {
		return RAFT_MALFORMED;
	}

	cursor = buf->base;

	p->version = 1;
	p->term = byteGet64(&cursor);
	p->vote_granted = byteGet64(&cursor);

	if (buf->len > REQUEST_VOTE_RESULT_V1_SIZE) {
		p->version = 2;
		uint64_t flags = byteGet64(&cursor);
		p->pre_vote = (flags & (1 << 0));
	}

	return RAFT_OK;
}

int uvDecodeBatchHeader(const void *batch,
			size_t batch_len,
			struct raft_entry **entries,
			unsigned *n)
{
	const void *cursor = batch;
	size_t remaining = batch_len;
	size_t i;
	int rv;

	if (remaining < sizeof(uint64_t)) {
		return RAFT_MALFORMED;
	}

	*n = (unsigned)byteGet64(&cursor);
	remaining -= sizeof(uint64_t);

	if (*n == 0) {
		*entries = NULL;
		return RAFT_OK;
	}

	*entries = raft_malloc(*n * sizeof **entries);

	if (*entries == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}

	for (i = 0; i < *n; i++) {
		struct raft_entry *entry = &(*entries)[i];

		if (remaining < (sizeof(uint64_t) * 2)) {
			rv = RAFT_MALFORMED;
			goto err_after_alloc;
		}

		entry->term = byteGet64(&cursor);
		entry->type = byteGet8(&cursor);

		if (entry->type != RAFT_COMMAND &&
		    entry->type != RAFT_BARRIER && entry->type != RAFT_CHANGE) {
			rv = RAFT_MALFORMED;
			goto err_after_alloc;
		}

		cursor = (uint8_t *)cursor + 3; /* Unused */

		/* Size of the log entry data, little endian. */
		entry->buf.len = byteGet32(&cursor);
		remaining -= 16;
	}

	return RAFT_OK;

err_after_alloc:
	raft_free(*entries);
	*entries = NULL;

err:
	dqlite_assert(rv != 0);

	return rv;
}

static int decodeAppendEntries(const uv_buf_t *buf,
			       struct raft_append_entries *args)
{
	const void *cursor;

	dqlite_assert(buf != NULL);
	dqlite_assert(args != NULL);

	if (buf->len < APPEND_ENTRIES_REQUEST_PREFIX_SIZE) {
		return RAFT_MALFORMED;
	}

	cursor = buf->base;

	args->version = 0;
	args->term = byteGet64(&cursor);
	args->prev_log_index = byteGet64(&cursor);
	args->prev_log_term = byteGet64(&cursor);
	args->leader_commit = byteGet64(&cursor);

	return uvDecodeBatchHeader(cursor,
				 buf->len - APPEND_ENTRIES_REQUEST_PREFIX_SIZE,
				 &args->entries, &args->n_entries);
}

static int decodeAppendEntriesResult(const uv_buf_t *buf,
				     struct raft_append_entries_result *p)
{
	const void *cursor;

	if (buf->len != APPEND_ENTRIES_RESULT_V0_SIZE &&
	    buf->len != APPEND_ENTRIES_RESULT_V1_SIZE) {
		return RAFT_MALFORMED;
	}

	cursor = buf->base;

	p->version = 0;
	p->term = byteGet64(&cursor);
	p->rejected = byteGet64(&cursor);
	p->last_log_index = byteGet64(&cursor);
	p->features = 0;
	if (buf->len > APPEND_ENTRIES_RESULT_V0_SIZE) {
		p->version = 1;
		p->features = byteGet64(&cursor);
	}

	return RAFT_OK;
}

static int decodeInstallSnapshot(const uv_buf_t *buf,
				 struct raft_install_snapshot *args)
{
	const void *cursor;
	struct raft_buffer conf;
	size_t remaining;
	int rv;

	dqlite_assert(buf != NULL);
	dqlite_assert(args != NULL);

	cursor = buf->base;
	remaining = buf->len;

	if (remaining < (6 * sizeof(uint64_t))) {
		return RAFT_MALFORMED;
	}

	args->version = 0;
	args->term = byteGet64(&cursor);
	args->last_index = byteGet64(&cursor);
	args->last_term = byteGet64(&cursor);
	args->conf_index = byteGet64(&cursor);
	remaining -= (4 * sizeof(uint64_t));
	conf.len = (size_t)byteGet64(&cursor);
	remaining -= sizeof(uint64_t);

	if (conf.len > remaining - sizeof(uint64_t)) {
		return RAFT_MALFORMED;
	}

	conf.base = (void *)cursor;

	rv = configurationDecode(&conf, &args->conf);
	if (rv != RAFT_OK) {
		return rv;
	}
	cursor = (uint8_t *)cursor + conf.len;
	remaining -= conf.len;
	args->data.len = (size_t)byteGet64(&cursor);
	remaining -= sizeof(uint64_t);

	if (remaining != 0) {
		return RAFT_MALFORMED;
	}

	return RAFT_OK;
}

static int decodeTimeoutNow(const uv_buf_t *buf, struct raft_timeout_now *p)
{
	const void *cursor;

	if (buf->len != TIMEOUT_NOW_SIZE) {
		return RAFT_MALFORMED;
	}

	cursor = buf->base;

	p->version = 0;
	p->term = byteGet64(&cursor);
	p->last_log_index = byteGet64(&cursor);
	p->last_log_term = byteGet64(&cursor);

	return RAFT_OK;
}

int uvDecodeMessage(uint16_t type,
		    const uv_buf_t *header,
		    struct raft_message *message,
		    size_t *payload_len)
{
	unsigned i;
	int rv = 0;

	memset(message, 0, sizeof(*message));
	message->type = (unsigned short)type;

	/* Decode the header. */
	switch (type) {
		case RAFT_IO_REQUEST_VOTE:
			rv = decodeRequestVote(header, &message->request_vote);
			*payload_len = 0;
			break;
		case RAFT_IO_REQUEST_VOTE_RESULT:
			rv = decodeRequestVoteResult(
			    header, &message->request_vote_result);
			*payload_len = 0;
			break;
		case RAFT_IO_APPEND_ENTRIES:
			rv = decodeAppendEntries(header,
						 &message->append_entries);
			*payload_len = 0;
			for (i = 0; i < message->append_entries.n_entries;
			     i++) {
				*payload_len +=
				    message->append_entries.entries[i].buf.len;
			}
			break;
		case RAFT_IO_APPEND_ENTRIES_RESULT:
			rv = decodeAppendEntriesResult(
			    header, &message->append_entries_result);
			*payload_len = 0;
			break;
		case RAFT_IO_INSTALL_SNAPSHOT:
			rv = decodeInstallSnapshot(header,
						   &message->install_snapshot);
			*payload_len = message->install_snapshot.data.len;
			break;
		case RAFT_IO_TIMEOUT_NOW:
			rv = decodeTimeoutNow(header, &message->timeout_now);
			*payload_len = 0;
			break;
		default:
			rv = RAFT_IOERR;
			*payload_len = 0;
			break;
	};

	return rv;
}

int uvDecodeEntriesBatch(uint8_t *batch,
			 size_t offset,
			 struct raft_entry *entries,
			 unsigned n)
{
	uint8_t *cursor;

	dqlite_assert(batch != NULL);

	cursor = batch + offset;

	for (size_t i = 0; i < n; i++) {
		struct raft_entry *entry = &entries[i];
		entry->batch = batch;
		entry->buf.base = (entry->buf.len > 0) ? cursor : NULL;

		cursor += entry->buf.len;
		if (entry->buf.len % 8 != 0) {
			/* Add padding */
			cursor = cursor + 8 - (entry->buf.len % 8);
		}

		entry->is_local = false;
	}
	return 0;
}
