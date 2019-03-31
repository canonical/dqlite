#include "gateway.h"

void gateway__init(struct gateway *g,
		   struct dqlite_logger *logger,
		   struct options *options,
		   struct registry *registry,
		   struct raft *raft)
{
	g->logger = logger;
	g->options = options;
	g->registry = registry;
	g->raft = raft;
	g->leader = NULL;
}

void gateway__close(struct gateway *g)
{
	(void)g;
}
