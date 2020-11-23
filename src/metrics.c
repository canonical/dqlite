#include <stdlib.h>

#include "./lib/assert.h"

#include "metrics.h"

void dqliteMetrics_init(struct dqliteMetrics *m)
{
	assert(m != NULL);

	m->requests = 0;
	m->duration = 0;
}
