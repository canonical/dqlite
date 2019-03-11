#ifndef DQLITE_OPTIONS_H
#define DQLITE_OPTIONS_H

/**
 * Value object holding configuration options.
 */
struct options
{
	const char *vfs;	       /* VFS to use when opening dbs */
	const char *replication;       /* Replication to use when opening dbs */
	unsigned heartbeat_timeout;    /* In milliseconds */
	unsigned page_size;	    /* Database page size */
	unsigned checkpoint_threshold; /* In outstanding WAL frames */
};

/**
 * Apply default values to the given options object.
 */
void options__init(struct options *o);

/**
 * Release any memory held by the options object.
 */
void options__close(struct options *o);

/**
 * Set the vfs field, making a copy of the given string.
 */
int options__set_vfs(struct options *o, const char *vfs);

/**
 * Set the wal_replication field, making a copy of the given string.
 */
int options__set_replication(struct options *o, const char *replication);

#endif /* DQLITE_OPTIONS_H */
