/* Receive an AppendEntries message. */

#ifndef RECV_APPEND_ENTRIES_H_
#define RECV_APPEND_ENTRIES_H_

#include "../raft.h"

/* Process an AppendEntries RPC from the given server. */
int recvAppendEntries(struct raft *r,
		      raft_id id,
		      const char *address,
		      struct raft_append_entries *args);

#endif /* RECV_APPEND_ENTRIES_H_ */
