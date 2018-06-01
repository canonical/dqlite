#ifndef DQLITE_REQUEST_H
#define DQLITE_REQUEST_H

#include <stdint.h>

#include "protocol.capnp.h"
#include "error.h"

/* Errors */
#define DQLITE_REQUEST_ERR_PARSE -1

/* Request types */
#define DQLITE_REQUEST_LEADER    Request_leader
#define DQLITE_REQUEST_HEARTBEAT Request_heartbeat

/*
 * Decoder for incoming requests.
 */
struct dqlite__request {
	/* read-only */
	dqlite__error  error;

	/* private */
	struct Request request; /* Container for the request being decoded */
	uint32_t       segnum;  /* Number of segments the request (currently always 1) */
	uint32_t       segsize; /* Segment size */
};

void dqlite__request_init(struct dqlite__request *r);
void dqlite__request_close(struct dqlite__request* r);

/* Request preamble size and parsing */
size_t dqlite__request_preamble_size(struct dqlite__request *r);
int dqlite__request_preamble(struct dqlite__request *r, const uint8_t *buf);

/* Request header size and parsing */
size_t dqlite__request_header_size(struct dqlite__request*);
int dqlite__request_header(struct dqlite__request*, uint8_t *buf);

/* Request data size and parsing */
size_t dqlite__request_data_size(struct dqlite__request*);
int dqlite__request_data(struct dqlite__request*, uint8_t *buf);

/* Return the request's type code or name */
int dqlite__request_type(struct dqlite__request *r);
const char *dqlite__request_type_name(struct dqlite__request *r);

#endif /* DQLITE_REQUEST_H */

