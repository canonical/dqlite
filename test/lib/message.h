#ifndef DQLITE_TEST_MESSAGE_HELPER_H
#define DQLITE_TEST_MESSAGE_HELPER_H

#include "../../src/message.h"
#include "../../src/request.h"
#include "../../src/response.h"

/* Helper to initialize an 'incoming' message object with the data from an
 * 'outgoing' message object */
void test_message_send(struct message *outgoing, struct message *incoming);

#define TEST_MESSAGE_SEND_ARG(KIND, MEMBER, _) KIND##_t MEMBER,

#define TEST_MESSAGE_SEND_DEFINE(NAME, SCHEMA) \
	void test_message_send_##NAME(         \
	    SCHEMA(TEST_MESSAGE_SEND_ARG) struct message *incoming)

#define TEST_MESSAGE_SEND_ASSIGN(KIND, MEMBER, NAME) \
	object.NAME.MEMBER = MEMBER;

#define TEST_MESSAGE_SEND_IMPLEMENT(NAME, CODE, OBJECT, SCHEMA)     \
	void test_message_send_##NAME(                              \
	    SCHEMA(TEST_MESSAGE_SEND_ARG) struct message *incoming) \
	{                                                           \
		int err;                                            \
                                                                    \
		struct OBJECT object;                               \
                                                                    \
		OBJECT##_init(&object);                             \
                                                                    \
		SCHEMA(TEST_MESSAGE_SEND_ASSIGN, NAME);             \
                                                                    \
		object.type = CODE;                                 \
                                                                    \
		err = OBJECT##_encode(&object);                     \
		munit_assert_int(err, ==, 0);                       \
                                                                    \
		test_message_send(&object.message, incoming);       \
                                                                    \
		OBJECT##_close(&object);                            \
	}

TEST_MESSAGE_SEND_DEFINE(leader, REQUEST__SCHEMA_LEADER);
TEST_MESSAGE_SEND_DEFINE(client, REQUEST__SCHEMA_CLIENT);
TEST_MESSAGE_SEND_DEFINE(heartbeat, REQUEST__SCHEMA_HEARTBEAT);
TEST_MESSAGE_SEND_DEFINE(open, REQUEST__SCHEMA_OPEN);

TEST_MESSAGE_SEND_DEFINE(server, RESPONSE__SCHEMA_SERVER);
TEST_MESSAGE_SEND_DEFINE(welcome, RESPONSE__SCHEMA_WELCOME);
TEST_MESSAGE_SEND_DEFINE(servers, RESPONSE__SCHEMA_SERVERS);
TEST_MESSAGE_SEND_DEFINE(db, RESPONSE__SCHEMA_DB);

#endif /* DQLITE_TEST_MESSAGE_HELPER_H */
