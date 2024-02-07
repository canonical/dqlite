/* Modify and inspect @raft_configuration objects. */

#ifndef CONFIGURATION_H_
#define CONFIGURATION_H_

#include "../raft.h"

/* Initialize an empty configuration. */
void configurationInit(struct raft_configuration *c);

/* Release all memory used by the given configuration. */
void configurationClose(struct raft_configuration *c);

/* Add a server to the given configuration.
 *
 * The given @address is copied and no reference to it is kept. In case of
 * error, @c is left unchanged.
 *
 * Errors:
 *
 * RAFT_DUPLICATEID
 *     @c already has a server with the given id.
 *
 * RAFT_DUPLICATEADDRESS
 *     @c already has a server with the given @address.
 *
 * RAFT_BADROLE
 *     @role is not one of ROLE_STANDBY, ROLE_VOTER or ROLE_SPARE.
 *
 * RAFT_NOMEM
 *     A copy of @address could not me made or the @c->servers could not
 *     be extended
 */
int configurationAdd(struct raft_configuration *c,
		     raft_id id,
		     const char *address,
		     int role);

/* Return the number of servers with the RAFT_VOTER role. */
unsigned configurationVoterCount(const struct raft_configuration *c);

/* Return the index of the server with the given ID (relative to the c->servers
 * array). If there's no server with the given ID, return the number of
 * servers. */
unsigned configurationIndexOf(const struct raft_configuration *c, raft_id id);

/* Return the index of the RAFT_VOTER server with the given ID (relative to the
 * sub array of c->servers that has only voting servers). If there's no server
 * with the given ID, or if it's not flagged as voting, return the number of
 * servers. */
unsigned configurationIndexOfVoter(const struct raft_configuration *c,
				   raft_id id);

/* Get the server with the given ID, or #NULL if no matching server is found. */
const struct raft_server *configurationGet(const struct raft_configuration *c,
					   raft_id id);

/* Remove a server from a raft configuration. The given ID must match the one of
 * an existing server in the configuration.
 *
 * In case of error @c is left unchanged.
 *
 * Errors:
 *
 * RAFT_BADID
 *     @c does not contain any server with the given @id
 *
 * RAFT_NOMEM
 *     Memory to hold the new set of servers could not be allocated.
 */
int configurationRemove(struct raft_configuration *c, raft_id id);

/* Deep copy @src to @dst.
 *
 * The configuration @src is assumed to be valid (i.e. each of its servers has a
 * valid ID, address and role).
 *
 * The @dst configuration object must be uninitialized or empty.
 *
 * In case of error, both @src and @dst are left unchanged.
 *
 * Errors:
 *
 * RAFT_NOMEM
 *     Memory to copy all the servers could not be allocated.
 */
int configurationCopy(const struct raft_configuration *src,
		      struct raft_configuration *dst);

/* Number of bytes needed to encode the given configuration object. */
size_t configurationEncodedSize(const struct raft_configuration *c);

/* Encode the given configuration object to the given pre-allocated buffer,
 * which is assumed to be at least configurationEncodedSize(c) bytes. */
void configurationEncodeToBuf(const struct raft_configuration *c, void *buf);

/* Encode the given configuration object. The memory of the returned buffer is
 * allocated using raft_malloc(), and client code is responsible for releasing
 * it when no longer needed.
 *
 * Errors:
 *
 * RAFT_NOMEM
 *     Memory for the encoded buffer could not be allocated.
 */
int configurationEncode(const struct raft_configuration *c,
			struct raft_buffer *buf);

/* Populate a configuration object by decoding the given serialized payload.
 *
 * The @c configuration object must be uninitialized or empty.
 *
 * In case of error, @c will be left empty.
 *
 * Errors:
 *
 * RAFT_MALFORMED
 *     The given buffer does not contain a valid encoded configuration.
 *
 * RAFT_NOMEM
 *     Memory to populate the given configuration could not be allocated.
 */
int configurationDecode(const struct raft_buffer *buf,
			struct raft_configuration *c);

/* Output the configuration to the raft tracer */
void configurationTrace(const struct raft *r,
			struct raft_configuration *c,
			const char *msg);

#endif /* CONFIGURATION_H_ */
