#ifndef REQUEST_H_
#define REQUEST_H_

#include "../raft.h"

/* Abstract request type */
struct request
{
	/* Must be kept in sync with RAFT__REQUEST in raft.h */
	void *data;
	int type;
	raft_index index;
	void *queue[2];
	uint8_t req_id[16];
	uint8_t client_id[16];
	uint8_t unique_id[16];
	uint64_t reserved[4];
};

#endif /* REQUEST_H_ */
