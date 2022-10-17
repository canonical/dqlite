#ifndef CONFIG_H_
#define CONFIG_H_

#include "logger.h"

/**
 * Value object holding dqlite configuration.
 */
struct config
{
	dqlite_node_id id;             /* Unique instance ID */
	char *address;                 /* Instance address */
	unsigned heartbeat_timeout;    /* In milliseconds */
	unsigned page_size;            /* Database page size */
	unsigned checkpoint_threshold; /* In outstanding WAL frames */
	struct logger logger;          /* Custom logger */
	char name[256];                /* VFS/replication registriatio name */
	unsigned long long failure_domain; /* User-provided failure domain */
	unsigned long long int weight;     /* User-provided node weight */
	char dir[1024];                /* Data dir for on-disk database */
	bool disk;                     /* Disk-mode or not */
};

/**
 * Initialize the config object with required values and set the rest to sane
 * defaults. A copy will be made of the given @address.
 */
int config__init(struct config *c, dqlite_node_id id,
		 const char *address, const char *dir);

/**
 * Release any memory held by the config object.
 */
void config__close(struct config *c);

#endif /* DQLITE_OPTIONS_H */
