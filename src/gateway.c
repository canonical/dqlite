#include "gateway.h"

void gateway__init(struct gateway *g,
		   struct dqlite_logger *logger,
		   struct options *options,
		   struct registry *registry)
{
	g->logger = logger;
	g->options = options;
	g->registry = registry;
	g->leader = NULL;
}

void gateway__close(struct gateway *g)
{
	(void)g;
}
