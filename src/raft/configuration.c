#include "configuration.h"

#include "../tracing.h"
#include "assert.h"
#include "byte.h"

/* Current encoding format version. */
#define ENCODING_FORMAT 1

void configurationInit(struct raft_configuration *c)
{
	c->servers = NULL;
	c->n = 0;
}

void configurationClose(struct raft_configuration *c)
{
	size_t i;
	assert(c != NULL);
	assert(c->n == 0 || c->servers != NULL);
	for (i = 0; i < c->n; i++) {
		raft_free(c->servers[i].address);
	}
	if (c->servers != NULL) {
		raft_free(c->servers);
	}
}

unsigned configurationIndexOf(const struct raft_configuration *c,
			      const raft_id id)
{
	unsigned i;
	assert(c != NULL);
	for (i = 0; i < c->n; i++) {
		if (c->servers[i].id == id) {
			return i;
		}
	}
	return c->n;
}

unsigned configurationIndexOfVoter(const struct raft_configuration *c,
				   const raft_id id)
{
	unsigned i;
	unsigned j = 0;
	assert(c != NULL);
	assert(id > 0);

	for (i = 0; i < c->n; i++) {
		if (c->servers[i].id == id) {
			if (c->servers[i].role == RAFT_VOTER) {
				return j;
			}
			return c->n;
		}
		if (c->servers[i].role == RAFT_VOTER) {
			j++;
		}
	}

	return c->n;
}

const struct raft_server *configurationGet(const struct raft_configuration *c,
					   const raft_id id)
{
	size_t i;
	assert(c != NULL);
	assert(id > 0);

	/* Grab the index of the server with the given ID */
	i = configurationIndexOf(c, id);

	if (i == c->n) {
		/* No server with matching ID. */
		return NULL;
	}
	assert(i < c->n);

	return &c->servers[i];
}

unsigned configurationVoterCount(const struct raft_configuration *c)
{
	unsigned i;
	unsigned n = 0;
	assert(c != NULL);
	for (i = 0; i < c->n; i++) {
		if (c->servers[i].role == RAFT_VOTER) {
			n++;
		}
	}
	return n;
}

int configurationCopy(const struct raft_configuration *src,
		      struct raft_configuration *dst)
{
	size_t i;
	int rv;

	configurationInit(dst);
	for (i = 0; i < src->n; i++) {
		struct raft_server *server = &src->servers[i];
		rv = configurationAdd(dst, server->id, server->address,
				      server->role);
		if (rv != 0) {
			goto err;
		}
	}

	return 0;

err:
	configurationClose(dst);
	assert(rv == RAFT_NOMEM);
	return rv;
}

int configurationAdd(struct raft_configuration *c,
		     raft_id id,
		     const char *address,
		     int role)
{
	struct raft_server *servers;
	struct raft_server *server;
	char *address_copy;
	size_t i;
	int rv;
	assert(c != NULL);
	assert(id != 0);

	if (role != RAFT_STANDBY && role != RAFT_VOTER && role != RAFT_SPARE) {
		rv = RAFT_BADROLE;
		goto err;
	}

	/* Check that neither the given id or address is already in use */
	for (i = 0; i < c->n; i++) {
		server = &c->servers[i];
		if (server->id == id) {
			rv = RAFT_DUPLICATEID;
			goto err;
		}
		if (strcmp(server->address, address) == 0) {
			rv = RAFT_DUPLICATEADDRESS;
			goto err;
		}
	}

	/* Make a copy of the given address */
	address_copy = raft_malloc(strlen(address) + 1);
	if (address_copy == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}
	strcpy(address_copy, address);

	/* Grow the servers array.. */
	servers = raft_realloc(c->servers, (c->n + 1) * sizeof *server);
	if (servers == NULL) {
		rv = RAFT_NOMEM;
		goto err_after_address_copy;
	}
	c->servers = servers;

	/* Fill the newly allocated slot (the last one) with the given details.
	 */
	server = &servers[c->n];
	server->id = id;
	server->address = address_copy;
	server->role = role;

	c->n++;

	return 0;

err_after_address_copy:
	raft_free(address_copy);
err:
	assert(rv == RAFT_BADROLE || rv == RAFT_DUPLICATEID ||
	       rv == RAFT_DUPLICATEADDRESS || rv == RAFT_NOMEM);
	return rv;
}

int configurationRemove(struct raft_configuration *c, const raft_id id)
{
	unsigned i;
	unsigned j;
	struct raft_server *servers;
	int rv;

	assert(c != NULL);

	i = configurationIndexOf(c, id);
	if (i == c->n) {
		rv = RAFT_BADID;
		goto err;
	}

	assert(i < c->n);

	/* If this is the last server in the configuration, reset everything. */
	if (c->n - 1 == 0) {
		assert(i == 0);
		servers = NULL;
		goto out;
	}

	/* Create a new servers array. */
	servers = raft_calloc(c->n - 1, sizeof *servers);
	if (servers == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}

	/* Copy the first part of the servers array into a new array, excluding
	 * the i'th server. */
	for (j = 0; j < i; j++) {
		servers[j] = c->servers[j];
	}

	/* Copy the second part of the servers array into a new array. */
	for (j = i + 1; j < c->n; j++) {
		servers[j - 1] = c->servers[j];
	}

out:
	/* Release the address of the server that was deleted. */
	raft_free(c->servers[i].address);

	/* Release the old servers array */
	raft_free(c->servers);

	c->servers = servers;
	c->n--;

	return 0;

err:
	assert(rv == RAFT_BADID || rv == RAFT_NOMEM);
	return rv;
}

size_t configurationEncodedSize(const struct raft_configuration *c)
{
	size_t n = 0;
	unsigned i;

	/* We need one byte for the encoding format version */
	n++;

	/* Then 8 bytes for number of servers. */
	n += sizeof(uint64_t);

	/* Then some space for each server. */
	for (i = 0; i < c->n; i++) {
		struct raft_server *server = &c->servers[i];
		assert(server->address != NULL);
		n += sizeof(uint64_t);            /* Server ID */
		n += strlen(server->address) + 1; /* Address */
		n++;                              /* Voting flag */
	};

	return bytePad64(n);
}

void configurationEncodeToBuf(const struct raft_configuration *c, void *buf)
{
	void *cursor = buf;
	unsigned i;

	/* Encoding format version */
	bytePut8(&cursor, ENCODING_FORMAT);

	/* Number of servers. */
	bytePut64(&cursor, c->n);

	for (i = 0; i < c->n; i++) {
		struct raft_server *server = &c->servers[i];
		assert(server->address != NULL);
		bytePut64(&cursor, server->id);
		bytePutString(&cursor, server->address);
		assert(server->role < 255);
		bytePut8(&cursor, (uint8_t)server->role);
	};
}

int configurationEncode(const struct raft_configuration *c,
			struct raft_buffer *buf)
{
	int rv;

	assert(c != NULL);
	assert(buf != NULL);

	/* The configuration can't be empty. */
	assert(c->n > 0);

	buf->len = configurationEncodedSize(c);
	buf->base = raft_malloc(buf->len);
	if (buf->base == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}

	configurationEncodeToBuf(c, buf->base);

	return 0;

err:
	assert(rv == RAFT_NOMEM);
	return rv;
}

int configurationDecode(const struct raft_buffer *buf,
			struct raft_configuration *c)
{
	const void *cursor;
	size_t i;
	size_t n;
	int rv;

	assert(c != NULL);
	assert(buf != NULL);

	/* TODO: use 'if' instead of assert for checking buffer boundaries */
	assert(buf->len > 0);

	configurationInit(c);

	cursor = buf->base;

	/* Check the encoding format version */
	if (byteGet8(&cursor) != ENCODING_FORMAT) {
		rv = RAFT_MALFORMED;
		goto err;
	}

	/* Read the number of servers. */
	n = (size_t)byteGet64(&cursor);

	/* Decode the individual servers. */
	for (i = 0; i < n; i++) {
		raft_id id;
		const char *address;
		int role;

		/* Server ID. */
		id = byteGet64(&cursor);

		/* Server Address. */
		address = byteGetString(
		    &cursor, buf->len - (size_t)((uint8_t *)cursor -
						 (uint8_t *)buf->base));
		if (address == NULL) {
			rv = RAFT_MALFORMED;
			goto err;
		}

		/* Role code. */
		role = byteGet8(&cursor);

		rv = configurationAdd(c, id, address, role);
		if (rv != 0) {
			/* Only valid configurations should be ever be encoded,
			 * so in case configurationAdd() fails because of
			 * invalid data we return RAFT_MALFORMED. */
			if (rv != RAFT_NOMEM) {
				rv = RAFT_MALFORMED;
			}
			goto err;
		}
	}

	return 0;

err:
	assert(rv == RAFT_MALFORMED || rv == RAFT_NOMEM);
	configurationClose(c);
	return rv;
}

void configurationTrace(const struct raft *r,
			struct raft_configuration *c,
			const char *msg)
{
	(void)r;
	tracef("%s", msg);
	tracef("=== CONFIG START ===");
	unsigned i;
	struct raft_server *s;
	for (i = 0; i < c->n; i++) {
		s = &c->servers[i];
		tracef("id:%llu address:%s role:%d", s->id, s->address,
		       s->role);
	}
	tracef("=== CONFIG END ===");
}
