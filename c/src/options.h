#ifndef DQLITE_OPTIONS_H
#define DQLITE_OPTIONS_H

#include <stdint.h>

/* Value object holding configuration options. */
struct dqlite__options {
	const char *vfs;                  /* Registered VFS to use. */
	const char *wal_replication;      /* Registered replication to use */
	uint16_t    heartbeat_timeout;    /* In milliseconds */
	uint16_t    page_size;            /* Database page size */
	uint32_t    checkpoint_threshold; /* In outstanding WAL frames */
};

/* Apply default values to the given options object. */
void dqlite__options_defaults(struct dqlite__options *o);

/* Release any memory hold be the options object. */
void dqlite__options_close(struct dqlite__options *o);

/* Set the vfs field, making a copy of the given string. */
int dqlite__options_set_vfs(struct dqlite__options *o, const char *vfs);

/* Set the wal_replication field, making a copy of the given string. */
int dqlite__options_set_wal_replication(struct dqlite__options *o,
                                        const char *            wal_replication);

#endif /* DQLITE_OPTIONS_H */
