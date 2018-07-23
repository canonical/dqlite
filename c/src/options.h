#ifndef DQLITE_OPTIONS_H
#define DQLITE_OPTIONS_H

#include <stdint.h>

/* Value object holding configuration options. */
struct dqlite__options {
	uint16_t heartbeat_timeout;    /* In milliseconds */
	uint16_t page_size;            /* Database page size */
	uint32_t checkpoint_threshold; /* In outstanding WAL frames */
};

/* Apply default values to the given options object. */
void dqlite__options_defaults(struct dqlite__options *o);

#endif /* DQLITE_OPTIONS_H */
