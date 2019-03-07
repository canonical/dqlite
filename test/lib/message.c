#include "../src/message.h"
#include "../include/dqlite.h"
#include "../src/request.h"
#include "../src/response.h"

#include "./lib/message.h"
#include "munit.h"

void test_message_send(struct message *outgoing,
                       struct message *incoming) {
	int      err;
	uv_buf_t bufs[3];
	uv_buf_t buf;

	/* Get the send buffers of the outgoing message */
	message__send_start(outgoing, bufs);
	munit_assert_int(bufs[0].len, ==, MESSAGE__HEADER_LEN);

	/* Get the header buffer of the incoming message */
	message__header_recv_start(incoming, &buf);
	munit_assert_int(buf.len, ==, bufs[0].len);

	/* Copy the header data */
	memcpy(buf.base, bufs[0].base, bufs[0].len);

	/* Notify that the header is complete */
	err = message__header_recv_done(incoming);
	munit_assert_int(err, ==, 0);

	/* Get the body buffer of the incoming message */
	err = message__body_recv_start(incoming, &buf);
	munit_assert_int(err, ==, 0);

	munit_assert_int(buf.len, ==, bufs[1].len + bufs[2].len);

	/* Copy the body data */
	memcpy(buf.base, bufs[1].base, bufs[1].len);
}

TEST_MESSAGE_SEND_IMPLEMENT(leader,
                            DQLITE_REQUEST_LEADER,
                            request,
                            REQUEST__SCHEMA_LEADER);
TEST_MESSAGE_SEND_IMPLEMENT(client,
                            DQLITE_REQUEST_CLIENT,
                            request,
                            REQUEST__SCHEMA_CLIENT);
TEST_MESSAGE_SEND_IMPLEMENT(heartbeat,
                            DQLITE_REQUEST_HEARTBEAT,
                            request,
                            REQUEST__SCHEMA_HEARTBEAT);
TEST_MESSAGE_SEND_IMPLEMENT(open,
                            DQLITE_REQUEST_OPEN,
                            request,
                            REQUEST__SCHEMA_OPEN);

TEST_MESSAGE_SEND_IMPLEMENT(server,
                            DQLITE_RESPONSE_SERVER,
                            response,
                            RESPONSE__SCHEMA_SERVER);
TEST_MESSAGE_SEND_IMPLEMENT(welcome,
                            DQLITE_RESPONSE_WELCOME,
                            response,
                            RESPONSE__SCHEMA_WELCOME);
TEST_MESSAGE_SEND_IMPLEMENT(servers,
                            DQLITE_RESPONSE_SERVERS,
                            response,
                            RESPONSE__SCHEMA_SERVERS);
TEST_MESSAGE_SEND_IMPLEMENT(db,
                            DQLITE_RESPONSE_DB,
                            response,
                            RESPONSE__SCHEMA_DB);
