#ifndef DQLITE_TEST_RESPONSE_H
#define DQLITE_TEST_RESPONSE_H

#include <stdint.h>
#include <unistd.h>

#include "../src/response.h"

struct test_response_welcome {
	const char *leader;
	uint64_t heartbeat_timeout;
};

struct test_response_servers {
	const char *addresses[3];
};

struct test_response_db {
	uint64_t id;
};

struct test_response_welcome test_response_welcome_parse(struct dqlite__response *r);
struct test_response_servers test_response_servers_parse(struct dqlite__response *r);
struct test_response_db test_response_db_parse(struct dqlite__response *r);

#endif /* DQLITE_TEST_RESPONSE_H */
