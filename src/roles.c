#include <stdlib.h>

#include <raft.h>

#include "client/protocol.h"
#include "lib/queue.h"
#include "roles.h"
#include "server.h"
#include "translate.h"

/* Overview
 * --------
 *
 * This file implements automatic role management for dqlite servers. When
 * automatic role management is enabled, servers in a dqlite cluster will
 * autonomously (without client intervention) promote and demote each other
 * to maintain a specified number of voters and standbys, taking into account
 * the health, failure domain, and weight of each server.
 *
 * We implement two ingredients of role management: adjustments and handovers.
 * Adjustment runs on the cluster leader every tick (the frequency is defined
 * in server.c). The first step is to "poll" every server in the cluster to find
 * out whether it's online, and if so, its failure domain and weight. It demotes
 * to spare any servers that appear to have gone offline, then, if the numbers
 * of (online) voters and standbys don't match the target values, chooses
 * servers that should be promoted or demoted. The preference ordering for
 * promotion is based on the failure domains and weights previously gathered,
 * and is defined in compareNodesForPromotion, below.
 *
 * The actual roles changes are computed in a batch each time adjustment
 * occurs, and are stored in a queue. Individual "change records" are taken
 * off this queue and applied asynchronously. Since we only have a blocking
 * client implementation available, the exchanges of requests and responses
 * that implements polling a single server happens on the libuv blocking
 * thread pool (see pollClusterAfterWorkCb). We don't start a new round of
 * adjustment if a "tick" occurs while the queue of changes from the last
 * round is still nonempty.
 *
 * A handover is triggered when we call dqlite_node_handover on a node that's
 * the current cluster leader, or is a voter. Before shutting down for real,
 * the node in question tries to cause another node to become leader (using
 * raft_transfer), if applicable, and then promotes another node to voter
 * (if possible) before demoting itself. This is intended to smooth over
 * availability problems that can result if a privileged node (leader or
 * non-leader voter) crashes out of the cluster unceremoniously. The handover
 * task also needs to poll the cluster to figure out which nodes are good
 * candidates for promotion to voter.
 *
 * Unresolved
 * ----------
 *
 * - Should the failure-domains accounting for standbys use information about
 *   voters' failure domains? Vice versa?
 * - Should we try multiple candidates when doing an adjustment, if the
 *   preferred candidate can't be promoted?
 * - Should we retry when some step in the handover process fails? How, and
 *   how many times?
 * - Should we have dedicated code somewhere to (possibly) promote newly-
 *   joined nodes? go-dqlite does this, but I'm not convinced it's important,
 *   or that it should run on the server if we do decide we want it.
 */

/* XXX */
#define NUM_TRACKED_DOMAINS 5

struct change_record
{
	raft_id id;
	int role; /* dqlite role codes */
	queue queue;
};

struct counted_failure_domain
{
	unsigned long long domain;
	int count;
};

struct compare_data
{
	unsigned n;
	struct counted_failure_domain domains[NUM_TRACKED_DOMAINS];
};

struct polling
{
	void (*cb)(struct polling *);
	struct dqlite_node *node;
	struct all_node_info *cluster;
	unsigned *count;
	unsigned n_cluster;
	unsigned i;
};

struct handover_voter_data
{
	struct dqlite_node *node;
	dqlite_node_id target_id;
	char *leader_addr;
	dqlite_node_id leader_id;
};

static int domainCount(uint64_t needle, const struct compare_data *data)
{
	unsigned i;
	for (i = 0; i < data->n; i += 1) {
		if (data->domains[i].domain == needle) {
			return data->domains[i].count;
		}
	}
	return 0;
}

static void addDomain(uint64_t domain, struct compare_data *data)
{
	unsigned i;
	for (i = 0; i < data->n; i += 1) {
		if (data->domains[i].domain == domain) {
			data->domains[i].count += 1;
			return;
		}
	}
	if (i < NUM_TRACKED_DOMAINS) {
		data->domains[i].domain = domain;
		data->domains[i].count = 1;
		data->n += 1;
	}
}

static void removeDomain(uint64_t domain, struct compare_data *data)
{
	unsigned i;
	for (i = 0; i < data->n; i += 1) {
		if (data->domains[i].domain == domain) {
			if (data->domains[i].count > 0) {
				data->domains[i].count -= 1;
			}
			return;
		}
	}
}

static int compareNodesForPromotion(const void *l, const void *r, void *p)
{
	struct compare_data *data = p;
	const struct all_node_info *left = l;
	const struct all_node_info *right = r;
	int result;

	/* Nodes whose failure domains appear fewer times are preferred. */
	result = domainCount(left->failure_domain, data) -
		 domainCount(right->failure_domain, data);
	if (result != 0) {
		return result;
	}

	/* Nodes with lower weights are preferred. */
	result = (int)(left->weight - right->weight);
	if (result != 0) {
		return result;
	}

	/* We prefer to promote a standby rather than a spare. If
	 * left->role > right->role, then right is more "senior" than left,
	 * so we want right to come first, so return 1.*/
	return (left->role > right->role) - (left->role < right->role);
}

static int compareNodesForDemotion(const void *l, const void *r, void *p)
{
	/* XXX */
	return -compareNodesForPromotion(l, r, p);
}

static void changeCb(struct raft_change *change, int status);

/* Take one role change record off the queue and apply it. */
static void startChange(struct dqlite_node *d)
{
	queue *head;
	struct change_record *rec;
	struct raft_change *change;
	uint64_t id;
	int role;
	int rv;

	if (QUEUE__IS_EMPTY(&d->roles_changes)) {
		return;
	}

	head = QUEUE__HEAD(&d->roles_changes);
	QUEUE__REMOVE(head);
	rec = QUEUE__DATA(head, struct change_record, queue);
	id = rec->id;
	role = rec->role;
	raft_free(rec);

	change = raft_malloc(sizeof *change);
	if (change == NULL) {
		return;
	}
	change->data = d;
	/* TODO request ID */
	rv = raft_assign(&d->raft, change, id, translateDqliteRole(role),
			 changeCb);
	if (rv != 0) {
		/* TODO */
		raft_free(change);
	}
}

/* When a role change has completed, start the next one. */
static void changeCb(struct raft_change *change, int status)
{
	struct dqlite_node *d = change->data;

	raft_free(change);
	if (status != 0) {
		/* TODO */
	}
	startChange(d);
}

static void queueChange(uint64_t id, int role, void *arg)
{
	struct dqlite_node *d = arg;
	queue *head;
	struct change_record *rec;

	/* If we already queued a role change for this node, just update
	 * that record instead of queueing a new one. */
	QUEUE__FOREACH(head, &d->roles_changes)
	{
		rec = QUEUE__DATA(head, struct change_record, queue);
		if (rec->id == id) {
			rec->role = role;
			return;
		}
	}

	rec = raft_malloc(sizeof *rec);
	if (rec == NULL) {
		return;
	}
	rec->id = id;
	rec->role = role;
	QUEUE__PUSH(&d->roles_changes, &rec->queue);
}

void RolesComputeChanges(int voters,
			 int standbys,
			 struct all_node_info *cluster,
			 unsigned n_cluster,
			 dqlite_node_id my_id,
			 void (*cb)(uint64_t, int, void *),
			 void *arg)
{
	int voter_count = 0;
	int standby_count = 0;
	struct compare_data voter_compare = {0};
	struct compare_data standby_compare = {0};
	unsigned i;

	/* Count (online) voters and standbys in the cluster, and demote any
	 * offline nodes to spare. */
	for (i = 0; i < n_cluster; i += 1) {
		if (!cluster[i].online && cluster[i].role != DQLITE_SPARE) {
			cb(cluster[i].id, DQLITE_SPARE, arg);
			cluster[i].role = DQLITE_SPARE;
		} else if (cluster[i].online &&
			   cluster[i].role == DQLITE_VOTER) {
			voter_count += 1;
			addDomain(cluster[i].failure_domain, &voter_compare);
		} else if (cluster[i].online &&
			   cluster[i].role == DQLITE_STANDBY) {
			standby_count += 1;
			addDomain(cluster[i].failure_domain, &standby_compare);
		}
	}

	/* If we don't have enough voters, promote some standbys and spares. */
	if (voter_count < voters) {
		qsort_r(cluster, n_cluster, sizeof *cluster,
			compareNodesForPromotion, &voter_compare);
	}
	for (i = 0; i < n_cluster && voter_count < voters; i += 1) {
		if (!cluster[i].online || cluster[i].role == DQLITE_VOTER) {
			continue;
		}
		cb(cluster[i].id, DQLITE_VOTER, arg);
		if (cluster[i].role == DQLITE_STANDBY) {
			standby_count -= 1;
			removeDomain(cluster[i].failure_domain,
				     &standby_compare);
		}
		cluster[i].role = DQLITE_VOTER;
		voter_count += 1;
		addDomain(cluster[i].failure_domain, &voter_compare);
	}

	/* If we have too many voters, demote some of them. We always demote
	 * to spare in this step -- if it turns out that it would be better
	 * for some of these nodes to end up as standbys, that change will
	 * be picked up in the next step, and the two role changes will be
	 * consolidated by queueChangeCb. */
	if (voter_count > voters) {
		qsort_r(cluster, n_cluster, sizeof *cluster,
			compareNodesForDemotion, &voter_compare);
	}
	for (i = 0; i < n_cluster && voter_count > voters; i += 1) {
		if (cluster[i].role != DQLITE_VOTER || cluster[i].id == my_id) {
			continue;
		}
		cb(cluster[i].id, DQLITE_SPARE, arg);
		cluster[i].role = DQLITE_SPARE;
		voter_count -= 1;
		removeDomain(cluster[i].failure_domain, &voter_compare);
	}

	/* If we don't have enough standbys, promote some spares. */
	if (standby_count < standbys) {
		qsort_r(cluster, n_cluster, sizeof *cluster,
			compareNodesForPromotion, &standby_compare);
	}
	for (i = 0; i < n_cluster && standby_count < standbys; i += 1) {
		if (!cluster[i].online || cluster[i].role != DQLITE_SPARE) {
			continue;
		}
		cb(cluster[i].id, DQLITE_STANDBY, arg);
		cluster[i].role = DQLITE_STANDBY;
		standby_count += 1;
		addDomain(cluster[i].failure_domain, &standby_compare);
	}

	/* If we have too many standbys, demote some of them. */
	if (standby_count > standbys) {
		qsort_r(cluster, n_cluster, sizeof *cluster,
			compareNodesForDemotion, &standby_compare);
	}
	for (i = 0; i < n_cluster && standby_count > standbys; i += 1) {
		if (cluster[i].role != DQLITE_STANDBY) {
			continue;
		}
		cb(cluster[i].id, DQLITE_SPARE, arg);
		cluster[i].role = DQLITE_SPARE;
		standby_count -= 1;
		removeDomain(cluster[i].failure_domain, &standby_compare);
	}
}

/* Process information about the state of the cluster and queue up any
 * necessary role adjustments. This runs on the main thread. */
static void adjustClusterCb(struct polling *polling)
{
	struct dqlite_node *d;
	if (polling == NULL) {
		return;
	}
	d = polling->node;
	RolesComputeChanges(d->config.voters, d->config.standbys,
			    polling->cluster, polling->n_cluster, d->config.id,
			    queueChange, d);
	/* Start pulling role changes off the queue. */
	startChange(d);
}

/* Runs on the blocking thread pool to retrieve information about a single
 * server for use in roles adjustment. */
static void pollClusterWorkCb(uv_work_t *work)
{
	struct polling *polling = work->data;
	struct dqlite_node *d = polling->node;
	struct client_proto proto = {0};
	struct client_context context;
	int rv;

	proto.connect = d->connect_func;
	proto.connect_arg = d->connect_func_arg;
	rv = clientOpen(&proto, polling->cluster[polling->i].address,
			polling->cluster[polling->i].id);
	if (rv != 0) {
		return;
	}
	clientContextMillis(&context, 5000);
	rv = clientSendHandshake(&proto, &context);
	if (rv != 0) {
		goto close;
	}
	rv = clientSendDescribe(&proto, &context);
	rv = clientRecvMetadata(&proto,
				&polling->cluster[polling->i].failure_domain,
				&polling->cluster[polling->i].weight, &context);
	if (rv != 0) {
		goto close;
	}
	polling->cluster[polling->i].online = true;

close:
	clientClose(&proto);
}

/* Runs on the main thread after polling each server for roles adjustment. */
static void pollClusterAfterWorkCb(uv_work_t *work, int status)
{
	struct polling *polling = work->data;
	uv_work_t *work_objs;
	struct polling *polling_objs;
	unsigned i;

	/* The only path to status != 0 involves calling uv_cancel on this task,
	 * which we don't do. */
	assert(status == 0);

	*polling->count += 1;
	/* If all nodes have been polled, invoke the callback. */
	if (*polling->count == polling->n_cluster) {
		polling->cb(polling);
		/* Free the shared data, now that all tasks have finished. */
		raft_free(polling->count);
		for (i = 0; i < polling->n_cluster; i += 1) {
			raft_free(polling->cluster[i].address);
		}
		raft_free(polling->cluster);
		work_objs = work - polling->i;
		raft_free(work_objs);
		polling_objs = polling - polling->i;
		raft_free(polling_objs);
	}
}

/* Poll every node in the cluster to learn whether it's online, and if so, its
 * weight and failure domain. */
static void pollCluster(struct dqlite_node *d, void (*cb)(struct polling *))
{
	struct all_node_info *cluster;
	const struct raft_server *server;
	struct polling *polling_objs;
	struct polling *polling;
	struct uv_work_s *work_objs;
	struct uv_work_s *work;
	unsigned *count;
	unsigned n;
	unsigned i;
	unsigned j;
	unsigned ii;
	int rv;

	n = d->raft.configuration.n;
	cluster = raft_calloc(n, sizeof *cluster);
	if (cluster == NULL) {
		goto err;
	}
	count = raft_malloc(sizeof *count);
	if (count == NULL) {
		goto err_after_alloc_cluster;
	}
	*count = 0;
	for (i = 0; i < n; i += 1) {
		server = &d->raft.configuration.servers[i];
		cluster[i].id = server->id;
		cluster[i].address = raft_malloc(strlen(server->address) + 1);
		if (cluster[i].address == NULL) {
			goto err_after_alloc_addrs;
		}
		memcpy(cluster[i].address, server->address,
		       strlen(server->address) + 1);
		cluster[i].role = translateRaftRole(server->role);
	}
	polling_objs = raft_calloc(n, sizeof *polling_objs);
	if (polling_objs == NULL) {
		goto err_after_alloc_addrs;
	}
	work_objs = raft_calloc(n, sizeof *work_objs);
	if (work_objs == NULL) {
		goto err_after_alloc_polling;
	}
	for (j = 0; j < n; j += 1) {
		polling = &polling_objs[j];
		polling->cb = cb;
		polling->node = d;
		polling->cluster = cluster;
		polling->n_cluster = n;
		polling->count = count;
		polling->i = j;
		work = &work_objs[j];
		work->data = polling;
		rv = uv_queue_work(&d->loop, work, pollClusterWorkCb,
				   pollClusterAfterWorkCb);
		/* uv_queue_work can't fail unless a NULL callback is passed. */
		assert(rv == 0);
	}
	return;

err_after_alloc_polling:
	raft_free(polling_objs);
err_after_alloc_addrs:
	for (ii = 0; ii < i; ii += 1) {
		raft_free(cluster[ii].address);
	}
	raft_free(count);
err_after_alloc_cluster:
	raft_free(cluster);
err:
	cb(NULL);
}

/* Runs on the thread pool to open a connection to the leader, promote another
 * node to voter, and demote the calling node to spare. */
static void handoverVoterWorkCb(uv_work_t *work)
{
	struct handover_voter_data *data = work->data;
	struct client_proto proto = {0};
	struct client_context context;
	int rv;

	proto.connect = data->node->connect_func;
	proto.connect_arg = data->node->connect_func_arg;
	rv = clientOpen(&proto, data->leader_addr, data->leader_id);
	if (rv != 0) {
		return;
	}
	clientContextMillis(&context, 5000);
	rv = clientSendHandshake(&proto, &context);
	if (rv != 0) {
		goto close;
	}
	rv = clientSendAssign(&proto, data->target_id, DQLITE_VOTER, &context);
	if (rv != 0) {
		goto close;
	}
	rv = clientRecvEmpty(&proto, &context);
	if (rv != 0) {
		goto close;
	}
	rv = clientSendAssign(&proto, data->node->config.id, DQLITE_SPARE,
			      &context);
	if (rv != 0) {
		goto close;
	}
	rv = clientRecvEmpty(&proto, &context);

close:
	clientClose(&proto);
}

static void handoverVoterAfterWorkCb(uv_work_t *work, int status)
{
	struct handover_voter_data *data = work->data;
	struct dqlite_node *node = data->node;
	int handover_status = 0;
	void (*cb)(struct dqlite_node *, int);

	if (status != 0) {
		handover_status = DQLITE_ERROR;
	}
	raft_free(data->leader_addr);
	raft_free(data);
	raft_free(work);
	cb = node->handover_done_cb;
	cb(node, handover_status);
	node->handover_done_cb = NULL;
}

/* Having gathered information about the cluster, pick a non-voter node
 * to promote in our place. */
static void handoverVoterCb(struct polling *polling)
{
	struct dqlite_node *node;
	raft_id leader_id;
	const char *borrowed_addr;
	char *leader_addr;
	struct compare_data voter_compare = {0};
	unsigned i;
	struct all_node_info *cluster;
	unsigned n_cluster;
	dqlite_node_id target_id;
	struct handover_voter_data *data;
	uv_work_t *work;
	void (*cb)(struct dqlite_node *, int);
	int rv;

	if (polling == NULL) {
		return;
	}
	node = polling->node;
	cluster = polling->cluster;
	n_cluster = polling->n_cluster;
	cb = node->handover_done_cb;

	raft_leader(&node->raft, &leader_id, &borrowed_addr);
	if (leader_id == node->raft.id || leader_id == 0) {
		goto finish;
	}
	leader_addr = raft_malloc(strlen(borrowed_addr) + 1);
	if (leader_addr == NULL) {
		goto finish;
	}
	memcpy(leader_addr, borrowed_addr, strlen(borrowed_addr) + 1);

	/* Select a non-voter to transfer to -- the logic is similar to
	 * adjustClusterCb. */
	for (i = 0; i < n_cluster; i += 1) {
		if (cluster[i].online && cluster[i].role == DQLITE_VOTER &&
		    cluster[i].id != node->raft.id) {
			addDomain(cluster[i].failure_domain, &voter_compare);
		}
	}
	qsort_r(cluster, n_cluster, sizeof *cluster, compareNodesForPromotion,
		&voter_compare);
	target_id = 0;
	for (i = 0; i < n_cluster; i += 1) {
		if (cluster[i].online && cluster[i].role != DQLITE_VOTER &&
		    cluster[i].id != node->raft.id) {
			target_id = cluster[i].id;
			break;
		}
	}
	/* If no transfer candidates found, give up. */
	if (target_id == 0) {
		goto err_after_alloc_leader_addr;
	}

	/* Submit the handover work. */
	data = raft_malloc(sizeof *data);
	if (data == NULL) {
		goto err_after_alloc_leader_addr;
	}
	data->node = node;
	data->target_id = target_id;
	data->leader_addr = leader_addr;
	data->leader_id = leader_id;
	work = raft_malloc(sizeof *work);
	if (work == NULL) {
		goto err_after_alloc_data;
	}
	work->data = data;
	rv = uv_queue_work(&node->loop, work, handoverVoterWorkCb,
			   handoverVoterAfterWorkCb);
	if (rv != 0) {
		goto err_after_alloc_work;
	}
	return;

err_after_alloc_work:
	raft_free(work);
err_after_alloc_data:
	raft_free(data);
err_after_alloc_leader_addr:
	raft_free(leader_addr);
finish:
	node->handover_done_cb = NULL;
	cb(node, DQLITE_ERROR);
}

static void handoverTransferCb(struct raft_transfer *req)
{
	struct dqlite_node *d = req->data;
	raft_free(req);
	pollCluster(d, handoverVoterCb);
}

void RolesAdjust(struct dqlite_node *d)
{
	/* Only the leader can assign roles. */
	if (raft_state(&d->raft) != RAFT_LEADER) {
		return;
	}
	/* If a series of role adjustments is already in progress, don't kick
	 * off another one. */
	if (!QUEUE__IS_EMPTY(&d->roles_changes)) {
		return;
	}
	assert(d->running);
	pollCluster(d, adjustClusterCb);
}

void RolesHandover(struct dqlite_node *d, void (*cb)(struct dqlite_node *, int))
{
	struct raft_transfer *req;
	int rv;

	req = raft_malloc(sizeof *req);
	if (req == NULL) {
		goto err;
	}
	d->handover_done_cb = cb;
	req->data = d;
	/* We try the leadership transfer unconditionally -- Raft will tell us
	 * if we're not the leader. */
	rv = raft_transfer(&d->raft, req, 0, handoverTransferCb);
	if (rv == RAFT_NOTLEADER) {
		raft_free(req);
		pollCluster(d, handoverVoterCb);
		return;
	} else if (rv != 0) {
		raft_free(req);
		goto err;
	}
	return;

err:
	d->handover_done_cb = NULL;
	cb(d, DQLITE_ERROR);
}

void RolesCancelPendingChanges(struct dqlite_node *d)
{
	queue *head;
	struct change_record *rec;

	while (!QUEUE__IS_EMPTY(&d->roles_changes)) {
		head = QUEUE__HEAD(&d->roles_changes);
		rec = QUEUE__DATA(head, struct change_record, queue);
		QUEUE__REMOVE(head);
		raft_free(rec);
	}
}
