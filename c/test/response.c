#include <assert.h>
#include <string.h>

#include <CUnit/CUnit.h>

#include "../src/message.h"
#include "../src/response.h"

#include "response.h"

struct test_response_welcome test_response_welcome_parse(struct dqlite__response *r)
{
	int err;
	struct dqlite__message *message;
	struct test_response_welcome welcome;

	assert(r != NULL);

	message = &r->message;

	CU_ASSERT_EQUAL(message->type, DQLITE_WELCOME);

	err = dqlite__message_read_text(message, &welcome.leader);
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_read_uint64(message, &welcome.heartbeat_timeout);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	return welcome;
}

struct test_response_servers test_response_servers_parse(struct dqlite__response *r)
{
	int err = 0;
	struct dqlite__message *message;
	struct test_response_servers servers;
	int i;
	const char *address;

	assert(r != NULL);

	message = &r->message;

	CU_ASSERT_EQUAL(message->type, DQLITE_SERVERS);

	for (i = 0; err == 0; i++) {
		err = dqlite__message_read_text(message, &address);
		if (err != 0 && err != DQLITE_EOM) {
			CU_FAIL("parse error");
		}
		servers.addresses[i] = address;
		if (err == DQLITE_EOM) {
			break;
		}
	}

	return servers;
}

struct test_response_db test_response_db_parse(struct dqlite__response *r)
{
	int err;
	struct dqlite__message *message;
	struct test_response_db db;

	assert(r != NULL);

	message = &r->message;

	CU_ASSERT_EQUAL(message->type, DQLITE_DB);

	err = dqlite__message_read_uint64(message, &db.id);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	return db;
}
