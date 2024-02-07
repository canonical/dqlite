/* Receive a TimeoutNow message. */

#ifndef RECV_TIMEOUT_NOW_H_
#define RECV_TIMEOUT_NOW_H_

#include "../raft.h"

/* Process a TimeoutNow RPC from the given server. */
int recvTimeoutNow(struct raft *r,
		   raft_id id,
		   const char *address,
		   const struct raft_timeout_now *args);

#endif /* RECV_TIMEOUT_NOW_H_ */
