/******************************************************************************
 *
 * Collect various performance metrics.
 *
 *****************************************************************************/

#ifndef DQLITE_METRICS_H
#define DQLITE_METRICS_H

#include <stdint.h>

struct dqlite__metrics {
	uint64_t requests; /* Total number of requests served. */
	uint64_t duration; /* Total time spent to server requests. */
};

void dqlite__metrics_init(struct dqlite__metrics *m);

#endif /* DQLITE_METRICS_H */
