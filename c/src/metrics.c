#include <assert.h>
#include <stdlib.h>

#include "metrics.h"

void dqlite__metrics_init(struct dqlite__metrics *m) {
	assert(m != NULL);

	m->requests = 0;
	m->duration = 0;
}
