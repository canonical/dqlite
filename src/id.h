/**
 * Generate, set, and extract dqlite-generated request IDs.
 *
 * A fresh ID is generated for each config or exec client request that
 * arrives at a gateway. These IDs are passed down into raft via the
 * req_id field of RAFT__REQUEST, and are suitable for diagnostic use
 * only.
 */

#ifndef DQLITE_ID_H_
#define DQLITE_ID_H_

#include <stdint.h>

/**
 * State used to generate a request ID.
 */
struct id_state
{
	uint64_t data[4];
};

/**
 * Generate a request ID, mutating the input state in the process.
 */
uint64_t idNext(struct id_state *state);

/**
 * Cause the given state to yield a different sequence of IDs.
 *
 * This is used to ensure that the sequences of IDs generated for
 * distinct clients are (in practice) disjoint.
 */
void idJump(struct id_state *state);

/**
 * Read a request ID from the req_id field of RAFT__REQUEST.
 */
uint64_t idExtract(const uint8_t buf[16]);

/**
 * Write a request ID to the req_id field of RAFT__REQUEST.
 */
void idSet(uint8_t buf[16], uint64_t id);

#endif /* DQLITE_ID_H_ */
