/* Receive an AppendEntries result message. */

#ifndef RECV_APPEND_ENTRIES_RESULT_H_
#define RECV_APPEND_ENTRIES_RESULT_H_

#include "../raft.h"

/* Process an AppendEntries RPC result from the given server. */
int recvAppendEntriesResult(struct raft *r,
			    raft_id id,
			    const char *address,
			    const struct raft_append_entries_result *result);

#endif /* RECV_APPEND_ENTRIES_RESULT_H_ */
