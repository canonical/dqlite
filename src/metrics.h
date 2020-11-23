/******************************************************************************
 *
 * Collect various performance metrics.
 *
 *****************************************************************************/

#ifndef DQLITE_METRICS_H
#define DQLITE_METRICS_H

#include <stdint.h>

struct dqlite_metrics
{
	uint64_t requests; /* Total number of requests served. */
	uint64_t duration; /* Total time spent to server requests. */
};

void dqlite_metrics_init(struct dqlite_metrics *m);

#endif /* DQLITE_METRICS_H */
