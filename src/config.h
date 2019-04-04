#ifndef CONFIG_H_
#define CONFIG_H_

/**
 * Value object holding dqlite configuration.
 */
struct config
{
	unsigned id;		       /* Unique instance ID */
	char *address;		       /* Instance address */
	const char *vfs;	       /* VFS to use when opening dbs */
	const char *replication;       /* Replication to use when opening dbs */
	unsigned heartbeat_timeout;    /* In milliseconds */
	unsigned page_size;	    /* Database page size */
	unsigned checkpoint_threshold; /* In outstanding WAL frames */
};

/**
 * Initialize the config object with required values and set the rest to sane
 * defaults. A copy will be made of the given @address.
 */
int config__init(struct config *c, unsigned id, const char *address);

/**
 * Release any memory held by the config object.
 */
void config__close(struct config *c);

/**
 * Set the vfs field, making a copy of the given string.
 */
int config__set_vfs(struct config *c, const char *vfs);

/**
 * Set the wal_replication field, making a copy of the given string.
 */
int config__set_replication(struct config *c, const char *replication);

#endif /* DQLITE_OPTIONS_H */
