/* Membership-related APIs. */

#ifndef MEMBERSHIP_H_
#define MEMBERSHIP_H_

#include "../raft.h"

/* Helper returning an error if the configuration can't be changed, either
 * because this node is not the leader or because a configuration change is
 * already in progress. */
int membershipCanChangeConfiguration(struct raft *r);

/* Populate the given configuration object with the most recent committed
 * configuration, the one contained in the entry at
 * r->configuration_committed_index. */
int membershipFetchLastCommittedConfiguration(struct raft *r,
					      struct raft_configuration *conf);

/* Update the information about the progress that the non-voting server
 * currently being promoted is making in catching with logs.
 *
 * Return false if the server being promoted did not yet catch-up with logs, and
 * true if it did.
 *
 * This function must be called only by leaders after a @raft_assign request
 * has been submitted. */
bool membershipUpdateCatchUpRound(struct raft *r);

/* Update the local configuration replacing it with the content of the given
 * RAFT_CHANGE entry, which has just been received in as part of an
 * AppendEntries RPC request. The uncommitted configuration index will be
 * updated accordingly.
 *
 * It must be called only by followers. */
int membershipUncommittedChange(struct raft *r,
				const raft_index index,
				const struct raft_entry *entry);

/* Rollback any promotion configuration change that was applied locally, but
 * failed to be committed. It must be called by followers after they receive an
 * AppendEntries RPC request that instructs them to evict the uncommitted entry
 * from their log. */
int membershipRollback(struct raft *r);

/* Initialize the state of a leadership transfer request. */
void membershipLeadershipTransferInit(struct raft *r,
				      struct raft_transfer *req,
				      raft_id id,
				      raft_transfer_cb cb);

/* Start the leadership transfer by sending a TimeoutNow message to the target
 * server. */
int membershipLeadershipTransferStart(struct raft *r);

/* Finish a leadership transfer (whether successful or not), resetting the
 * leadership transfer state and firing the user callback. */
void membershipLeadershipTransferClose(struct raft *r);

#endif /* MEMBERSHIP_H_ */
