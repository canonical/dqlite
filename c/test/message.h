#ifndef DQLITE_TEST_MESSAGE_HELPER_H
#define DQLITE_TEST_MESSAGE_HELPER_H

#include "../src/message.h"

/* Helper to initialize an 'incoming' message object with the data from an
 * 'outgoing' message object */
void test_message_send(
	struct dqlite__message *outgoing,
	struct dqlite__message *incoming);

#define __TEST_MESSAGE_SEND_ARG(KIND, MEMBER, _)	\
	KIND ## _t MEMBER,

#define TEST_MESSAGE_SEND_DEFINE(NAME, SCHEMA)		\
	void test_message_send_ ## NAME(		\
		SCHEMA(__TEST_MESSAGE_SEND_ARG)		\
		struct dqlite__message *incoming	\
		)

#define __TEST_MESSAGE_SEND_ASSIGN(KIND, MEMBER, NAME)	\
	object.NAME.MEMBER = MEMBER;

#define TEST_MESSAGE_SEND_IMPLEMENT(NAME, CODE, OBJECT, SCHEMA)		\
	void test_message_send_ ## NAME(				\
		SCHEMA(__TEST_MESSAGE_SEND_ARG)				\
		struct dqlite__message *incoming			\
		)							\
	{								\
		int err;						\
									\
		struct OBJECT object;					\
									\
		OBJECT ## _init(&object);				\
									\
		SCHEMA(__TEST_MESSAGE_SEND_ASSIGN, NAME);		\
									\
		object.type = CODE;					\
									\
		err = OBJECT ## _encode(&object);			\
		CU_ASSERT_EQUAL(err, 0);				\
									\
		test_message_send(&object.message, incoming);		\
									\
		OBJECT ## _close(&object);				\
	}


#endif /* DQLITE_TEST_MESSAGE_HELPER_H */
