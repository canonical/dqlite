#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include <capnp_c.h>
#include <sqlite3.h>

#include "dqlite.h"
#include "error.h"
#include "lifecycle.h"
#include "protocol.capnp.h"
#include "response.h"

/* Render the response header and return its size.
 *
 * This is a generic function able to handle more than one segment, although in
 * practice we only have single-segment responses at the moment.
 */
static size_t dqlite__response_header_render(struct dqlite__response *r,
					struct capn *session)
{
	uint32_t *entry;
	uint32_t num_entries;
	size_t size;

	assert(r != NULL);
	assert(session != NULL);
	assert(session->segnum > 0);

	/* segnum == 1:
	 *   [segnum][segsiz]
	 * segnum == 2:
	 *   [segnum][segsiz][segsiz][zeroes]
	 * segnum == 3:
	 *   [segnum][segsiz][segsiz][segsiz]
	 * segnum == 4:
	 *   [segnum][segsiz][segsiz][segsiz][segsiz][zeroes]
	 */
	num_entries = ((2 + session->segnum) / 2) * 2;
	size = 4 * num_entries;

	entry = (uint32_t*)r->buf1;
	*entry = capn_flip32(session->segnum - 1);

	/* Zero out the spare position in the header sizes */
	r->buf1[num_entries-1] = 0;

	return size;
}

/* Render the response filling our internal buffer.
 *
 * This is a generic function able to handle more than one segment, although in
 * practice we only have single-segment responses at the moment.
 */
static int dqlite__response_render(struct dqlite__response *r, struct capn *session)
{
	uint32_t *entry;
	size_t headerSize;
	size_t dataSize = 0;
	size_t i;
	struct capn_ptr root;
	struct capn_segment *segment;
	uint8_t *buf;

	/* Render the header */
	headerSize = dqlite__response_header_render(r, session);

	buf = r->buf1; /* Use the static buffer by default, if it's big enough */

	root = capn_root(session);
	segment = root.seg;
	for (i = 0; i < session->segnum; i++, segment = segment->next) {
		assert(segment != NULL);
		dataSize += segment->len;
		entry = ((uint32_t*)buf) + 1 + i;
		*entry = capn_flip32(segment->len / 8);
	}
	assert( !segment );

	r->size = headerSize + dataSize;

	/* Allocate a larger buffer if the default one is not big enough */
	if (r->size>DQLITE__RESPONSE_BUF_SIZE) {
		assert( !r->buf2 );
		r->buf2 = (uint8_t*)sqlite3_malloc(r->size);
		if( !r->buf2 ){
			dqlite__error_oom(&r->error, "failed to create response buffer");
			return DQLITE_NOMEM;
		}
		memcpy(r->buf2, buf, headerSize);
		buf = r->buf2;
	}

	buf += headerSize;

	for (segment = root.seg; segment; segment = segment->next) {
		memcpy(buf, segment->data, segment->len);
		buf += segment->len;
	}

	return 0;
}

/* Convert a string to a capn_text object */
static capn_text dqlite__response_text(const char *chars)
{
	return (capn_text) {
		.len = (int)strlen(chars),
			.str = chars,
			.seg = 0,
			};
}

void dqlite__response_init(struct dqlite__response *r)
{
	assert(r != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_RESPONSE);

	dqlite__error_init(&r->error);

	r->buf2 = NULL;
	r->size = 0;
}

void dqlite__response_close(struct dqlite__response* r)
{
	assert(r != NULL);

	dqlite__error_close(&r->error);

	if( r->buf2 != NULL)
		sqlite3_free(r->buf2);

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_RESPONSE);
}

/* Helper to initialize command response variables */
#define DQLITE__RESPONSE_INIT \
	int err = 0; \
	struct capn    session; \
	capn_ptr       root; \
	struct capn_segment *segment; \
				      \
	capn_init_malloc(&session); \
	root = capn_root(&session); \
	segment = root.seg

/* Helper to encode a given object */
#define DQLITE__RESPONSE_ENCODE(OBJ, NEW_PTR, WRITE) \
	ptr = NEW_PTR(segment); \
	WRITE(&OBJ, ptr); \
	err = capn_setp(root, 0, ptr.p); \
	if( err != 0) { \
		dqlite__error_printf(&r->error, "failed to set object pointer: %d", err); \
		goto out; \
	} \
	err = dqlite__response_render(r, &session); \
	if( err != 0 ) { \
		dqlite__error_printf(&r->error, "failed to serialize object: %d", err); \
		goto out; \
	}

int dqlite__response_cluster(
	struct dqlite__response *r,
	const char* leader,
	uint16_t heartbeat_timeout)
{
	DQLITE__RESPONSE_INIT;

	struct Cluster cluster;
	Cluster_ptr ptr;

	assert(r != NULL);
	assert(leader != NULL);
	assert(heartbeat_timeout > 0);

	cluster.leader = dqlite__response_text(leader);
	cluster.heartbeatTimeout = heartbeat_timeout;

	DQLITE__RESPONSE_ENCODE(cluster, new_Cluster, write_Cluster);

 out:
	return err;
}

int dqlite__response_servers(struct dqlite__response *r, const char **addresses)
{
	DQLITE__RESPONSE_INIT;

	struct Address *address_list;
	struct Servers servers;
	Servers_ptr ptr;
	int i;
	int n = 0;

	assert(r != NULL);
	assert(addresses != NULL);

	for(i = 0; *(addresses + i) != NULL; i++) {
		n++;
	}

	assert(n > 0);

	address_list = (struct Address*)sqlite3_malloc(sizeof(*address_list) * n);
	if (address_list == NULL) {
		dqlite__error_oom(&r->error, "failed to create servers list");
		return DQLITE_NOMEM;
	}

	servers.addresses = new_Address_list(segment, n);

	for(i = 0; *(addresses + i) != NULL; i++) {
		struct Address *address = address_list + i;
		address->value = dqlite__response_text(*(addresses + i));
		set_Address(address, servers.addresses, i);
	}

	DQLITE__RESPONSE_ENCODE(servers, new_Servers, write_Servers);

 out:
	sqlite3_free(address_list);

	return err;
}

uint8_t *dqlite__response_data(struct dqlite__response* r)
{
	assert(r != NULL);

	if( r->buf2 ){
		return r->buf2;
	}

	return r->buf1;
}

size_t dqlite__response_size(struct dqlite__response* r)
{
	assert(r != NULL);
	assert(r->size > 0);

	return r->size;
}
