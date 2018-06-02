#include <assert.h>
#include <stdint.h>
#include <string.h>

#include <capnp_c.h>

#include "dqlite.h"
#include "error.h"
#include "lifecycle.h"
#include "protocol.capnp.h"
#include "request.h"

/* The preable is a 32-bit unsigned integer, containing the number of
 * segments. */
#define DQLITE_REQUEST_PREAMBLE_SIZE sizeof(uint32_t)

/* Set the maximum segment size to 1M, to prevent eccessive memmory allocation
 * in case of client bugs */
#define DQLITE_REQUEST_MAX_SEGMENT_SIZE 1048576

static const char *dqlite__request_type_names[] = {
	"Helo",      /* DQLITE__REQUEST_HELO */
	"Heartbeat", /* DQLITE__REQUEST_HEARTBEAT */
	0
};

void dqlite__request_init(struct dqlite__request *r)
{
	assert(r != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_REQUEST);

	dqlite__error_init(&r->error);

	r->segnum = 0;
	r->segsize = 0;
}

void dqlite__request_close(struct dqlite__request* r)
{
	assert(r != NULL);

	dqlite__error_close(&r->error);

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_REQUEST);
}

size_t dqlite__request_preamble_size(struct dqlite__request *r)
{
	assert(r != NULL);

	return DQLITE_REQUEST_PREAMBLE_SIZE;
}

int dqlite__request_preamble(struct dqlite__request *r, const uint8_t *buf)
{
	uint32_t segnum;

	assert(r != NULL);
	assert(buf != NULL);

	assert(r->segnum == 0);

	memcpy(&segnum, buf, DQLITE_REQUEST_PREAMBLE_SIZE);
	segnum = capn_flip32(segnum);

	segnum++; /* The wire encoding is zero-based */

	/* At the moment we require requests to have exactly one segment */
	if (segnum != 1) {
		dqlite__error_printf(&r->error, "too many segments: %d", segnum);
		return DQLITE_REQUEST_ERR_PARSE;
	}

	r->segnum = segnum;

	return 0;
}

size_t dqlite__request_header_size(struct dqlite__request *r)
{
	assert(r != NULL);
	assert(r->segnum == 1);

	return 8 * (r->segnum/2) + 4;
}

int dqlite__request_header(struct dqlite__request *r, uint8_t *buf)
{
	uint32_t segsize;

	assert(r != NULL);
	assert(buf != NULL);

	assert(r->segnum == 1);
	assert(r->segsize == 0);

	memcpy(&segsize, buf, sizeof(uint32_t));
	if (segsize == 0 || segsize > DQLITE_REQUEST_MAX_SEGMENT_SIZE) {
		dqlite__error_printf(&r->error, "invalid segment size: %d", segsize);
		return DQLITE_REQUEST_ERR_PARSE;
	}

	r->segsize = segsize;

	return 0;
}

size_t dqlite__request_data_size(struct dqlite__request *r)
{
	assert(r != NULL);
	assert(r->segsize != 0 && r->segsize <= DQLITE_REQUEST_MAX_SEGMENT_SIZE);

	return r->segsize * sizeof(uint64_t);
}

int dqlite__request_data(struct dqlite__request *r, uint8_t *buf)
{
	struct capn session;
	struct capn_segment segment;
	Request_ptr request_ptr;

	assert(r != NULL);
	assert(buf != NULL);

	/* Requests are supposed to be contained in one segment */
	assert( r->segnum==1 );

	segment.data = (char*)buf;
	segment.len = dqlite__request_data_size(r);
	segment.cap = segment.len;

	capn_init_malloc(&session);
	capn_append_segment(&session, &segment);

	request_ptr.p = capn_getp(capn_root(&session), 0 /* off */, 1 /* resolve */);

	read_Request(&r->request, request_ptr);

	return 0;
}

int dqlite__request_type(struct dqlite__request *r){
	assert(r != NULL);

	return r->request.which;
}

const char *dqlite__request_type_name(struct dqlite__request *r)
{
	int type;
	int i;

	assert(r != NULL);

	type = dqlite__request_type(r);

	for (i = 0; dqlite__request_type_names[i] != NULL; i++) {
		if (i == type) {
			return dqlite__request_type_names[type];
		}
	}

	return "Unknown";
}
