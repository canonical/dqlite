#ifndef DQLITE_REQUEST_H
#define DQLITE_REQUEST_H

#include <stdint.h>

#include "dqlite.h"
#include "error.h"
#include "message.h"

/*
 * Request structures.
 */

struct dqlite__request_helo {
	uint64_t client_id;
};

struct dqlite__request_heartbeat {
	uint64_t timestamp;
};

struct dqlite__request_open {
	const char* name;
};

/*
 * Decoder for incoming requests.
 */
struct dqlite__request {
	/*public */
	uint64_t               timestamp; /* Time at which the request was received */

	/* read-only */
	struct dqlite__message message;  /* Request message info */
	dqlite__error          error;    /* Last error occurred, if any. */
	union {                          /* Request data */
		struct dqlite__request_helo      helo;
		struct dqlite__request_heartbeat heartbeat;
		struct dqlite__request_open      open;
	};
};

void dqlite__request_init(struct dqlite__request *r);
void dqlite__request_close(struct dqlite__request* r);

DQLITE_INLINE void dqlite__request_header_buf(
	struct dqlite__request *r,
	uint8_t **buf,
	size_t *len)
{
	assert(r != NULL);
	dqlite__message_header_buf(&r->message, buf, len);
}

int dqlite__request_header_received(struct dqlite__request *r);

DQLITE_INLINE void dqlite__request_body_buf(
	struct dqlite__request *r,
	uint8_t **buf,
	size_t *len)
{
	assert(r != NULL);
	dqlite__message_body_buf(&r->message, buf, len);
}

int dqlite__request_body_received(struct dqlite__request *r);

void dqlite__request_processed(struct dqlite__request *r);

/* Return the request's type code or name */
uint16_t dqlite__request_type(struct dqlite__request *r);
const char *dqlite__request_type_name(struct dqlite__request *r);

#endif /* DQLITE_REQUEST_H */

