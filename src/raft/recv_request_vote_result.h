/* Receive a RequestVote result. */

#ifndef RECV_REQUEST_VOTE_RESULT_H_
#define RECV_REQUEST_VOTE_RESULT_H_

#include "../raft.h"

/* Process a RequestVote RPC result from the given server. */
int recvRequestVoteResult(struct raft *r,
			  raft_id id,
			  const char *address,
			  const struct raft_request_vote_result *result);

#endif /* RAFT_RECV_REQUEST_VOTE_RESULT_H_ */
