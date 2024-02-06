/* Convert from one state to another. */

#ifndef CONVERT_H_
#define CONVERT_H_

#include "../raft.h"

/* Convert from unavailable, or candidate or leader to follower.
 *
 * From Figure 3.1:
 *
 *   If election timeout elapses without receiving AppendEntries RPC from
 *   current leader or granting vote to candidate: convert to candidate.
 *
 * The above implies that we need to reset the election timer when converting to
 * follower. */
void convertToFollower(struct raft *r);

/* Convert from follower to candidate, starting a new election.
 *
 * From Figure 3.1:
 *
 *   On conversion to candidate, start election
 *
 * If the disrupt_leader flag is true, the server will set the disrupt leader
 * flag of the RequestVote messages it sends.  */
int convertToCandidate(struct raft *r, bool disrupt_leader);

/* Convert from candidate to leader.
 *
 * From Figure 3.1:
 *
 *   Upon election: send initial empty AppendEntries RPC (heartbeat) to each
 *   server.
 *
 * From Section 3.4:
 *
 *   Once a candidate wins an election, it becomes leader. It then sends
 *   heartbeat messages to all of the other servers to establish its authority
 *   and prevent new elections.
 *
 * From Section 3.3:
 *
 *   The leader maintains a nextIndex for each follower, which is the index
 *   of the next log entry the leader will send to that follower. When a
 *   leader first comes to power, it initializes all nextIndex values to the
 *   index just after the last one in its log. */
int convertToLeader(struct raft *r);

void convertToUnavailable(struct raft *r);

#endif /* CONVERT_H_ */
