#include <stdlib.h>

#include "./lib/assert.h"

#include "metrics.h"

void dqlite_metrics_init(struct dqlite_metrics *m)
{
	assert(m != NULL);

	m->requests = 0;
	m->duration = 0;
}
