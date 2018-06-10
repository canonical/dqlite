#ifndef DQLITE_RESPONSE_H
#define DQLITE_RESPONSE_H

#include <unistd.h>
#include <stdint.h>

#include "error.h"
#include "message.h"

/* The size of pre-allocated response buffer. This should generally fit in
 * a single IP packet, given typical MTU sizes */
#define DQLITE__RESPONSE_BUF_SIZE 1024

/* Response types */

/*
 * Encoder for outgoing responses.
 */
struct dqlite__response {
	/* read-only */
	dqlite__error  error;

	/* private */
	struct dqlite__message message;
};

void dqlite__response_init(struct dqlite__response* r);
void dqlite__response_close(struct dqlite__response* r);

/* APIs for encoding responses */
int dqlite__response_welcome(
	struct dqlite__response *r,
	const char* leader,
	uint16_t heartbeat_timeout /* In milliseconds */);
int dqlite__response_servers(struct dqlite__response *r, const char** servers);
int dqlite__response_db(struct dqlite__response *r, uint64_t id);

DQLITE_INLINE void dqlite__response_header_buf(
	struct dqlite__response *r,
	uint8_t **buf,
	size_t *len)
{
	assert(r != NULL);
	dqlite__message_header_buf(&r->message, buf, len);
}

DQLITE_INLINE void dqlite__response_body_buf(
	struct dqlite__response *r,
	uint8_t **buf,
	size_t *len)
{
	assert(r != NULL);
	dqlite__message_body_buf(&r->message, buf, len);
}

#endif /* DQLITE_RESPONSE_H */
