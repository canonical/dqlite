/* RequestVote RPC handler. */

#ifndef RECV_REQUEST_VOTE_H_
#define RECV_REQUEST_VOTE_H_

#include "../raft.h"

/* Process a RequestVote RPC from the given server. */
int recvRequestVote(struct raft *r,
		    raft_id id,
		    const char *address,
		    const struct raft_request_vote *args);

#endif /* RECV_REQUEST_VOTE_H_ */
