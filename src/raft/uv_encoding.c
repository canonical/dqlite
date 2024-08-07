#include "uv_encoding.h"

#include <limits.h>
#include <stdio.h>
#include <string.h>

#include "../raft.h"
#include "assert.h"
#include "byte.h"
#include "configuration.h"

/**
 * Size of the request preamble.
 */
#define RAFT_IO_UV__PREAMBLE_SIZE               \
	(sizeof(uint64_t) /* Message type. */ + \
	 sizeof(uint64_t) /* Message size. */)

static size_t sizeofRequestVoteV1(void)
{
	return sizeof(uint64_t) + /* Term. */
	       sizeof(uint64_t) + /* Candidate ID. */
	       sizeof(uint64_t) + /* Last log index. */
	       sizeof(uint64_t) /* Last log term. */;
}

static size_t sizeofRequestVote(void)
{
	return sizeofRequestVoteV1() +
	       sizeof(uint64_t) /* Leadership transfer. */;
}

static size_t sizeofRequestVoteResultV1(void)
{
	return sizeof(uint64_t) + /* Term. */
	       sizeof(uint64_t) /* Vote granted. */;
}

static size_t sizeofRequestVoteResult(void)
{
	return sizeofRequestVoteResultV1() + /* Size of older version 1 message
					      */
	       sizeof(uint64_t) /* Flags. */;
}

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

static size_t sizeofAppendEntriesResultV0(void)
{
	return sizeof(uint64_t) + /* Term. */
	       sizeof(uint64_t) + /* Success. */
	       sizeof(uint64_t) /* Last log index. */;
}

static size_t sizeofAppendEntriesResult(void)
{
	return sizeofAppendEntriesResultV0() +
	       sizeof(uint64_t) /* 64 bit Flags. */;
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

static size_t sizeofTimeoutNow(void)
{
	return sizeof(uint64_t) + /* Term. */
	       sizeof(uint64_t) + /* Last log index. */
	       sizeof(uint64_t) /* Last log term. */;
}

static size_t size_install_snapshot_result(void)
{
	return sizeof(uint32_t); /* Result. */
}

static size_t size_signature(const struct raft_signature *s)
{
	return strlen(s->db) + 1 + /* DB name. */
		sizeof(pageno_t) * 2 + /* Page range (from, to). */
		sizeof(uint8_t) + /* Ask calculated. */
		sizeof(uint32_t); /* Result. */
}

static size_t size_signature_result(const struct raft_signature_result *s)
{
	size_t cs_size = (sizeof(pageno_t) + sizeof(checksum_t)) * s->cs_nr;

	return strlen(s->db) + 1 + /* DB name. */
		cs_size + /* Checksums. */
		sizeof(uint64_t) + /* Checksum number. */
		sizeof(uint8_t) + /* Calculated. */
		sizeof(uint32_t); /* Result. */
}

static size_t size_install_snapshot_cp(const struct raft_install_snapshot_cp *m)
{
	size_t page_data_size = sizeof(uint64_t) + /* Length itself. */
		m->page_data.len /* Data. */;

	return strlen(m->db) + 1 + /* DB name. */
		sizeof(pageno_t) + /* Page number. */
		page_data_size + /* Page_data. */
		sizeof(uint32_t); /* Result. */
}

static size_t size_install_snapshot_cp_result(void)
{
	return sizeof(pageno_t) + /* Last known page number. */
		sizeof(uint32_t); /* Result. */
}

static size_t size_install_snapshot_mv(const struct raft_install_snapshot_mv *m)
{
	size_t page_ranges_size = (sizeof(pageno_t) * 2) * m->mv_nr /* from, to */;

	return strlen(m->db) + 1 + /* DB name. */
		sizeof(uint64_t) + /* Number of page ranges. */
		page_ranges_size + /* Page ranges (from, to). */
		sizeof(uint32_t); /* Result. */
}

static size_t size_install_snapshot_mv_result(const struct raft_install_snapshot_mv_result *m)
{
	return strlen(m->db) + 1 + /* DB name. */
	    sizeof(pageno_t) + /* Last known page number. */
		sizeof(uint32_t); /* Result. */
}

size_t uvSizeofBatchHeader(size_t n, bool with_local_data)
{
	size_t res = 8 + /* Number of entries in the batch, little endian */
		16 * n; /* One header per entry */;
	if (with_local_data) {
#ifdef DQLITE_NEXT
		res += 8; /* Local data length, applies to all entries */
#endif
	}
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

	uvEncodeBatchHeader(p->entries, p->n_entries, cursor, false /* no local data */);
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

static void encode_install_snapshot_result(const struct raft_install_snapshot_result *m,
		void *buf)
{
	void *cursor = buf;

	bytePut32(&cursor, m->result);
}

static void encode_signature(const struct raft_signature *m,
		void *buf)
{
	void *cursor = buf;

	bytePutString(&cursor, m->db);
	bytePut32(&cursor, m->page_from_to.from);
	bytePut32(&cursor, m->page_from_to.to);
	bytePut8(&cursor, m->ask_calculated);
	bytePut32(&cursor, m->result);
}

static void encode_signature_result(const struct raft_signature_result *m,
		void *buf)
{
	void *cursor = buf;

	bytePutString(&cursor, m->db);
	bytePut64(&cursor, m->cs_nr);
	for (uint64_t i = 0; i < m->cs_nr; i++) {
		bytePut32(&cursor, m->cs[i].page_no);
		bytePut32(&cursor, m->cs[i].checksum);
	}
	bytePut8(&cursor, m->calculated);
	bytePut32(&cursor, m->result);
}

static void encode_install_snapshot_cp(const struct raft_install_snapshot_cp *m,
		void *buf)
{
	void *cursor = buf;

	bytePutString(&cursor, m->db);
	bytePut32(&cursor, m->page_no);
	bytePut64(&cursor, m->page_data.len);
	memcpy(cursor, m->page_data.base, m->page_data.len);
	cursor += m->page_data.len;
	bytePut32(&cursor, m->result);
}

static void encode_install_snapshot_cp_result(const struct raft_install_snapshot_cp_result *m,
		void *buf)
{
	void *cursor = buf;

	bytePut32(&cursor, m->last_known_page_no);
	bytePut32(&cursor, m->result);
}

static void encode_install_snapshot_mv(const struct raft_install_snapshot_mv *m,
		void *buf)
{
	void *cursor = buf;

	bytePutString(&cursor, m->db);
	bytePut64(&cursor, m->mv_nr);
	for (uint64_t i = 0; i < m->mv_nr; i++) {
		bytePut32(&cursor, m->mv[i].from);
		bytePut32(&cursor, m->mv[i].to);
	}
	bytePut32(&cursor, m->result);
}

static void encode_install_snapshot_mv_result(const struct raft_install_snapshot_mv_result *m,
		void *buf)
{
	void *cursor = buf;

	bytePutString(&cursor, m->db);
	bytePut32(&cursor, m->last_known_page_no);
	bytePut32(&cursor, m->result);
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
			header.len += sizeofRequestVote();
			break;
		case RAFT_IO_REQUEST_VOTE_RESULT:
			header.len += sizeofRequestVoteResult();
			break;
		case RAFT_IO_APPEND_ENTRIES:
			header.len +=
			    sizeofAppendEntries(&message->append_entries);
			break;
		case RAFT_IO_APPEND_ENTRIES_RESULT:
			header.len += sizeofAppendEntriesResult();
			break;
		case RAFT_IO_INSTALL_SNAPSHOT:
			header.len +=
			    sizeofInstallSnapshot(&message->install_snapshot);
			break;
		case RAFT_IO_TIMEOUT_NOW:
			header.len += sizeofTimeoutNow();
			break;
		case RAFT_IO_INSTALL_SNAPSHOT_RESULT:
			header.len += size_install_snapshot_result();
			break;
		case RAFT_IO_SIGNATURE:
			header.len += size_signature(&message->signature);
			break;
		case RAFT_IO_SIGNATURE_RESULT:
			header.len += size_signature_result(&message->signature_result);
			break;
		case RAFT_IO_INSTALL_SNAPSHOT_CP:
			header.len += size_install_snapshot_cp(&message->install_snapshot_cp);
			break;
		case RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT:
			header.len += size_install_snapshot_cp_result();
			break;
		case RAFT_IO_INSTALL_SNAPSHOT_MV:
			header.len += size_install_snapshot_mv(&message->install_snapshot_mv);
			break;
		case RAFT_IO_INSTALL_SNAPSHOT_MV_RESULT:
			header.len += size_install_snapshot_mv_result(&message->install_snapshot_mv_result);
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
		case RAFT_IO_INSTALL_SNAPSHOT_RESULT:
			encode_install_snapshot_result(&message->install_snapshot_result, cursor);
			break;
		case RAFT_IO_SIGNATURE:
			encode_signature(&message->signature, cursor);
			break;
		case RAFT_IO_SIGNATURE_RESULT:
			encode_signature_result(&message->signature_result, cursor);
			break;
		case RAFT_IO_INSTALL_SNAPSHOT_CP:
			encode_install_snapshot_cp(&message->install_snapshot_cp, cursor);
			break;
		case RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT:
			encode_install_snapshot_cp_result(&message->install_snapshot_cp_result, cursor);
			break;
		case RAFT_IO_INSTALL_SNAPSHOT_MV:
			encode_install_snapshot_mv(&message->install_snapshot_mv, cursor);
			break;
		case RAFT_IO_INSTALL_SNAPSHOT_MV_RESULT:
			encode_install_snapshot_mv_result(&message->install_snapshot_mv_result, cursor);
			break;
		default:
			return RAFT_MALFORMED;
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
			 void *buf,
			 bool with_local_data)
{
	unsigned i;
	void *cursor = buf;

	/* Number of entries in the batch, little endian */
	bytePut64(&cursor, n);

	if (with_local_data) {
#ifdef DQLITE_NEXT
		/* Local data size per entry, little endian */
		bytePut64(&cursor, (uint64_t)sizeof(struct raft_entry_local_data));
#endif
	}

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

static void decodeRequestVote(const uv_buf_t *buf, struct raft_request_vote *p)
{
	const void *cursor;

	cursor = buf->base;

	p->version = 1;
	p->term = byteGet64(&cursor);
	p->candidate_id = byteGet64(&cursor);
	p->last_log_index = byteGet64(&cursor);
	p->last_log_term = byteGet64(&cursor);

	/* Support for legacy request vote that doesn't have disrupt_leader. */
	if (buf->len == sizeofRequestVoteV1()) {
		p->disrupt_leader = false;
		p->pre_vote = false;
	} else {
		p->version = 2;
		uint64_t flags = byteGet64(&cursor);
		p->disrupt_leader = (bool)(flags & 1 << 0);
		p->pre_vote = (bool)(flags & 1 << 1);
	}
}

static void decodeRequestVoteResult(const uv_buf_t *buf,
				    struct raft_request_vote_result *p)
{
	const void *cursor;

	cursor = buf->base;

	p->version = 1;
	p->term = byteGet64(&cursor);
	p->vote_granted = byteGet64(&cursor);

	if (buf->len > sizeofRequestVoteResultV1()) {
		p->version = 2;
		uint64_t flags = byteGet64(&cursor);
		p->pre_vote = (flags & (1 << 0));
	}
}

int uvDecodeBatchHeader(const void *batch,
			struct raft_entry **entries,
			unsigned *n,
			uint64_t *local_data_size)
{
	const void *cursor = batch;
	size_t i;
	int rv;

	*n = (unsigned)byteGet64(&cursor);

	if (*n == 0) {
		*entries = NULL;
		return 0;
	}

	if (local_data_size != NULL) {
#ifdef DQLITE_NEXT
		uint64_t z = byteGet64(&cursor);
		if (z == 0 || z > sizeof(struct raft_entry_local_data) || z % sizeof(uint64_t) != 0) {
			rv = RAFT_MALFORMED;
			goto err;
		}
		*local_data_size = z;
#endif
	}

	*entries = raft_malloc(*n * sizeof **entries);

	if (*entries == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}

	for (i = 0; i < *n; i++) {
		struct raft_entry *entry = &(*entries)[i];

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
	}

	return 0;

err_after_alloc:
	raft_free(*entries);
	*entries = NULL;

err:
	assert(rv != 0);

	return rv;
}

static int decodeAppendEntries(const uv_buf_t *buf,
			       struct raft_append_entries *args)
{
	const void *cursor;
	int rv;

	assert(buf != NULL);
	assert(args != NULL);

	cursor = buf->base;

	args->version = 0;
	args->term = byteGet64(&cursor);
	args->prev_log_index = byteGet64(&cursor);
	args->prev_log_term = byteGet64(&cursor);
	args->leader_commit = byteGet64(&cursor);

	rv = uvDecodeBatchHeader(cursor, &args->entries, &args->n_entries, false);
	if (rv != 0) {
		return rv;
	}

	return 0;
}

static void decodeAppendEntriesResult(const uv_buf_t *buf,
				      struct raft_append_entries_result *p)
{
	const void *cursor;

	cursor = buf->base;

	p->version = 0;
	p->term = byteGet64(&cursor);
	p->rejected = byteGet64(&cursor);
	p->last_log_index = byteGet64(&cursor);
	p->features = 0;
	if (buf->len > sizeofAppendEntriesResultV0()) {
		p->version = 1;
		p->features = byteGet64(&cursor);
	}
}

static int decodeInstallSnapshot(const uv_buf_t *buf,
				 struct raft_install_snapshot *args)
{
	const void *cursor;
	struct raft_buffer conf;
	int rv;

	assert(buf != NULL);
	assert(args != NULL);

	cursor = buf->base;

	args->version = 0;
	args->term = byteGet64(&cursor);
	args->last_index = byteGet64(&cursor);
	args->last_term = byteGet64(&cursor);
	args->conf_index = byteGet64(&cursor);
	conf.len = (size_t)byteGet64(&cursor);
	conf.base = (void *)cursor;

	rv = configurationDecode(&conf, &args->conf);
	if (rv != 0) {
		return rv;
	}
	cursor = (uint8_t *)cursor + conf.len;
	args->data.len = (size_t)byteGet64(&cursor);

	return 0;
}

static void decodeTimeoutNow(const uv_buf_t *buf, struct raft_timeout_now *p)
{
	const void *cursor;

	cursor = buf->base;

	p->version = 0;
	p->term = byteGet64(&cursor);
	p->last_log_index = byteGet64(&cursor);
	p->last_log_term = byteGet64(&cursor);
}

static void decode_install_snapshot_result(const uv_buf_t *buf,
		struct raft_install_snapshot_result *p)
{
	const void *cursor = buf->base;

	p->version = 0;
	p->result = byteGet32(&cursor);
}

static void decode_signature(const uv_buf_t *buf,
		struct raft_signature *p)
{
	const void *cursor = buf->base;

	p->version = 0;
	p->db = byteGetString(&cursor, 100 /* TODO */);
	p->page_from_to.from = byteGet32(&cursor);
	p->page_from_to.to = byteGet32(&cursor);
	p->ask_calculated = byteGet8(&cursor);
	p->result = byteGet32(&cursor);
}

static void decode_signature_result(const uv_buf_t *buf,
		struct raft_signature_result *p)
{
	const void *cursor = buf->base;

	p->version = 0;
	p->db = byteGetString(&cursor, 100 /* TODO max string length. */);
	p->cs_nr = byteGet64(&cursor);
	for (uint64_t i = 0; i < p->cs_nr; i++) {
		p->cs[i].page_no = byteGet32(&cursor);
		p->cs[i].checksum = byteGet32(&cursor);
	}
	p->calculated = byteGet8(&cursor);
	p->result = byteGet32(&cursor);
}

static void decode_install_snapshot_cp(const uv_buf_t *buf,
		struct raft_install_snapshot_cp *p)
{
	const void *cursor = buf->base;

	p->version = 0;
	p->db = byteGetString(&cursor, 100 /* TODO max string length. */);
	p->page_no = byteGet32(&cursor);
	p->page_data.len = byteGet64(&cursor);
	p->page_data.base = (void *)cursor;
	cursor += p->page_data.len;
	p->result = byteGet32(&cursor);
}

static void decode_install_snapshot_cp_result(const uv_buf_t *buf,
		struct raft_install_snapshot_cp_result *p)
{
	const void *cursor = buf->base;

	p->version = 0;
	p->last_known_page_no = byteGet32(&cursor);
	/* TODO: Probably needs db. */
	p->result = byteGet32(&cursor);
}

static void decode_install_snapshot_mv(const uv_buf_t *buf,
		struct raft_install_snapshot_mv *p)
{
	const void *cursor = buf->base;

	p->version = 0;
	p->db = byteGetString(&cursor, 100 /* TODO */);
	p->mv_nr = byteGet64(&cursor);
	for (uint64_t i = 0; i < p->mv_nr; i++) {
		p->mv[i].from = byteGet32(&cursor);
		p->mv[i].to = byteGet32(&cursor);
	}
	p->result = byteGet32(&cursor);
}

static void decode_install_snapshot_mv_result(const uv_buf_t *buf,
		struct raft_install_snapshot_mv_result *p)
{
	const void *cursor = buf->base;

	p->version = 0;
	p->db = byteGetString(&cursor, 100 /* TODO */);
	p->last_known_page_no = byteGet32(&cursor);
	p->result = byteGet32(&cursor);
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

	*payload_len = 0;

	/* Decode the header. */
	switch (type) {
		case RAFT_IO_REQUEST_VOTE:
			decodeRequestVote(header, &message->request_vote);
			break;
		case RAFT_IO_REQUEST_VOTE_RESULT:
			decodeRequestVoteResult(header,
						&message->request_vote_result);
			break;
		case RAFT_IO_APPEND_ENTRIES:
			rv = decodeAppendEntries(header,
						 &message->append_entries);
			for (i = 0; i < message->append_entries.n_entries;
			     i++) {
				*payload_len +=
				    message->append_entries.entries[i].buf.len;
			}
			break;
		case RAFT_IO_APPEND_ENTRIES_RESULT:
			decodeAppendEntriesResult(
			    header, &message->append_entries_result);
			break;
		case RAFT_IO_INSTALL_SNAPSHOT:
			rv = decodeInstallSnapshot(header,
						   &message->install_snapshot);
			*payload_len += message->install_snapshot.data.len;
			break;
		case RAFT_IO_TIMEOUT_NOW:
			decodeTimeoutNow(header, &message->timeout_now);
			break;
		case RAFT_IO_INSTALL_SNAPSHOT_RESULT:
			decode_install_snapshot_result(header, &message->install_snapshot_result);
			break;
		case RAFT_IO_SIGNATURE:
			decode_signature(header, &message->signature);
			break;
		case RAFT_IO_SIGNATURE_RESULT:
			decode_signature_result(header, &message->signature_result);
			break;
		case RAFT_IO_INSTALL_SNAPSHOT_CP:
			decode_install_snapshot_cp(header, &message->install_snapshot_cp);
			break;
		case RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT:
			decode_install_snapshot_cp_result(header, &message->install_snapshot_cp_result);
			break;
		case RAFT_IO_INSTALL_SNAPSHOT_MV:
			decode_install_snapshot_mv(header, &message->install_snapshot_mv);
			break;
		case RAFT_IO_INSTALL_SNAPSHOT_MV_RESULT:
			decode_install_snapshot_mv_result(header, &message->install_snapshot_mv_result);
			break;
		default:
			rv = RAFT_IOERR;
			break;
	};

	return rv;
}

int uvDecodeEntriesBatch(uint8_t *batch,
			 size_t offset,
			 struct raft_entry *entries,
			 unsigned n,
			 uint64_t local_data_size)
{
	uint8_t *cursor;

	assert(batch != NULL);

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

		entry->local_data = (struct raft_entry_local_data){};
		assert(local_data_size <= sizeof(entry->local_data.buf));
		assert(local_data_size % 8 == 0);
#ifdef DQLITE_NEXT
		memcpy(entry->local_data.buf, cursor, local_data_size);
		cursor += local_data_size;
#endif
	}
	return 0;
}
