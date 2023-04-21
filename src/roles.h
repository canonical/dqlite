#ifndef DQLITE_ROLE_MANAGEMENT_H
#define DQLITE_ROLE_MANAGEMENT_H

#include "server.h"

struct all_node_info
{
	uint64_t id;
	char *address;
	int role;
	bool online;
	uint64_t failure_domain;
	uint64_t weight;
};

/* Determine what roles changes should be made to the cluster, without
 * side-effects. The given callback will be invoked for each computed change,
 * with first argument the node whose role should be adjusted, second argument
 * the node's new role, and third argument taken from the last argument of this
 * function.
 *
 * The memory pointed to by @cluster is "borrowed" and not freed by this
 * function, but it may be modified as part of this function's bookkeeping. */
void RolesComputeChanges(int voters,
			 int standbys,
			 struct all_node_info *cluster,
			 unsigned n_cluster,
			 dqlite_node_id my_id,
			 void (*cb)(uint64_t, int, void *),
			 void *arg);

/* If necessary, try to assign new roles to nodes in the cluster to achieve
 * the configured number of voters and standbys. Polling the cluster and
 * assigning roles happens asynchronously. This can safely be called on any
 * server, but does nothing if called on a server that is not the leader. */
void RolesAdjust(struct dqlite_node *d);

/* Begin a graceful shutdown of this node. Leadership and the voter role will
 * be transferred to other nodes if necessary, and then the callback will be
 * invoked on the loop thread. The callback's second argument will be 0 if the
 * handover succeeded and nonzero otherwise. */
void RolesHandover(struct dqlite_node *d,
		   void (*cb)(struct dqlite_node *, int));

/* Drain the queue of changes computed by RoleManagementAdjust. This should be
 * done when the node is shutting down, to avoid a memory leak. */
void RolesCancelPendingChanges(struct dqlite_node *d);

#endif
