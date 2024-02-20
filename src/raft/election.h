/* Election-related logic and helpers. */

#ifndef ELECTION_H_
#define ELECTION_H_

#include "../raft.h"

/* Reset the election_timer clock and set randomized_election_timeout to a
 * random value between election_timeout and 2 * election_timeout.
 *
 * From Section 3.4:
 *
 *   Raft uses randomized election timeouts to ensure that split votes are rare
 *   and that they are resolved quickly. To prevent split votes in the first
 *   place, election timeouts are chosen randomly from a fixed interval (e.g.,
 *   150-300 ms). This spreads out the servers so that in most cases only a
 *   single server will time out.
 *
 * From Section 9.4:
 *
 *   We used AvailSim to approximate a WAN spanning the continental US. Each
 *   message was assigned a latency chosen randomly from the uniform range of
 *   30-40 ms, and the servers' election timeout range was set accordingly to
 *   300-600 ms (about 10-20 times the one-way network latency). When only one
 *   of the five servers has failed, the average election completes within about
 *   475 ms, and 99.9% of elections complete within 1.5 s. Even when two of the
 *   five servers have failed, the average election takes about 650 ms (about 20
 *   times the one-way network latency), and 99.9% of elections complete in 3
 *   s. We believe these election times are more than adequate for most WAN
 *   deployments.
 *
 * Must be called in follower or candidate state. */
void electionResetTimer(struct raft *r);

/* Return true if the election timer has expired.
 *
 * Must be called in follower or candidate state. */
bool electionTimerExpired(struct raft *r);

/* Start a new election round.
 *
 * From Figure 3.1:
 *
 *   [Rules for Servers] Candidates: On conversion to candidates, start
 *   election:
 *
 *   - Increment current term
 *   - Vote for self
 *   - Reset election timer
 *   - Send RequestVote RPCs to all other servers
 *
 * From Section 3.4:
 *
 *   To begin an election, a follower increments its current term and
 *   transitions to candidate state.  It then votes for itself and issues
 *   RequestVote RPCs in parallel to each of the other servers in the
 *   cluster.
 */
int electionStart(struct raft *r);

/* Decide whether our vote should be granted to the requesting server and update
 * our state accordingly.
 *
 * From Figure 3.1:
 *
 *   RequestVote RPC: Receiver Implementation:
 *
 *   - If votedFor is null or candidateId, and candidate's log is at least as
 *     up-to-date as receiver's log, grant vote.
 *
 * The outcome of the decision is stored through the @granted pointer. */
int electionVote(struct raft *r,
		 const struct raft_request_vote *args,
		 bool *granted);

/* Update the votes array by adding the vote from the server at the given
 * index. Return true if with this vote the server has reached the majority of
 * votes and won elections. */
bool electionTally(struct raft *r, size_t voter_index);

#endif /* ELECTION_H_ */
