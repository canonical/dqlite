#ifndef DQLITE_TEST_MESSAGE_HELPER_H
#define DQLITE_TEST_MESSAGE_HELPER_H

#include "../src/message.h"
#include "../src/request.h"
#include "../src/response.h"

/* Helper to initialize an 'incoming' message object with the data from an
 * 'outgoing' message object */
void test_message_send(struct dqlite__message *outgoing,
                       struct dqlite__message *incoming);

#define __TEST_MESSAGE_SEND_ARG(KIND, MEMBER, _) KIND##_t MEMBER,

#define TEST_MESSAGE_SEND_DEFINE(NAME, SCHEMA)                                      \
	void test_message_send_##NAME(                                              \
	    SCHEMA(__TEST_MESSAGE_SEND_ARG) struct dqlite__message *incoming)

#define __TEST_MESSAGE_SEND_ASSIGN(KIND, MEMBER, NAME) object.NAME.MEMBER = MEMBER;

#define TEST_MESSAGE_SEND_IMPLEMENT(NAME, CODE, OBJECT, SCHEMA)                     \
	void test_message_send_##NAME(                                              \
	    SCHEMA(__TEST_MESSAGE_SEND_ARG) struct dqlite__message *incoming) {     \
		int err;                                                            \
                                                                                    \
		struct OBJECT object;                                               \
                                                                                    \
		OBJECT##_init(&object);                                             \
                                                                                    \
		SCHEMA(__TEST_MESSAGE_SEND_ASSIGN, NAME);                           \
                                                                                    \
		object.type = CODE;                                                 \
                                                                                    \
		err = OBJECT##_encode(&object);                                     \
		munit_assert_int(err, ==, 0);                                       \
                                                                                    \
		test_message_send(&object.message, incoming);                       \
                                                                                    \
		OBJECT##_close(&object);                                            \
	}

TEST_MESSAGE_SEND_DEFINE(leader, DQLITE__REQUEST_SCHEMA_LEADER);
TEST_MESSAGE_SEND_DEFINE(client, DQLITE__REQUEST_SCHEMA_CLIENT);
TEST_MESSAGE_SEND_DEFINE(heartbeat, DQLITE__REQUEST_SCHEMA_HEARTBEAT);
TEST_MESSAGE_SEND_DEFINE(open, DQLITE__REQUEST_SCHEMA_OPEN);

TEST_MESSAGE_SEND_DEFINE(server, DQLITE__RESPONSE_SCHEMA_SERVER);
TEST_MESSAGE_SEND_DEFINE(welcome, DQLITE__RESPONSE_SCHEMA_WELCOME);
TEST_MESSAGE_SEND_DEFINE(servers, DQLITE__RESPONSE_SCHEMA_SERVERS);
TEST_MESSAGE_SEND_DEFINE(db, DQLITE__RESPONSE_SCHEMA_DB);

#endif /* DQLITE_TEST_MESSAGE_HELPER_H */
