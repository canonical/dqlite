#include "../src/message.h"
#include "../include/dqlite.h"
#include "../src/request.h"
#include "../src/response.h"

#include "message.h"
#include "munit.h"

void test_message_send(struct dqlite__message *outgoing,
                       struct dqlite__message *incoming) {
	int      err;
	uv_buf_t bufs[3];
	uv_buf_t buf;

	/* Get the send buffers of the outgoing message */
	dqlite__message_send_start(outgoing, bufs);
	munit_assert_int(bufs[0].len, ==, DQLITE__MESSAGE_HEADER_LEN);

	/* Get the header buffer of the incoming message */
	dqlite__message_header_recv_start(incoming, &buf);
	munit_assert_int(buf.len, ==, bufs[0].len);

	/* Copy the header data */
	memcpy(buf.base, bufs[0].base, bufs[0].len);

	/* Notify that the header is complete */
	err = dqlite__message_header_recv_done(incoming);
	munit_assert_int(err, ==, 0);

	/* Get the body buffer of the incoming message */
	err = dqlite__message_body_recv_start(incoming, &buf);
	munit_assert_int(err, ==, 0);

	munit_assert_int(buf.len, ==, bufs[1].len + bufs[2].len);

	/* Copy the body data */
	memcpy(buf.base, bufs[1].base, bufs[1].len);
}

TEST_MESSAGE_SEND_IMPLEMENT(leader,
                            DQLITE_REQUEST_LEADER,
                            dqlite__request,
                            DQLITE__REQUEST_SCHEMA_LEADER);
TEST_MESSAGE_SEND_IMPLEMENT(client,
                            DQLITE_REQUEST_CLIENT,
                            dqlite__request,
                            DQLITE__REQUEST_SCHEMA_CLIENT);
TEST_MESSAGE_SEND_IMPLEMENT(heartbeat,
                            DQLITE_REQUEST_HEARTBEAT,
                            dqlite__request,
                            DQLITE__REQUEST_SCHEMA_HEARTBEAT);
TEST_MESSAGE_SEND_IMPLEMENT(open,
                            DQLITE_REQUEST_OPEN,
                            dqlite__request,
                            DQLITE__REQUEST_SCHEMA_OPEN);

TEST_MESSAGE_SEND_IMPLEMENT(server,
                            DQLITE_RESPONSE_SERVER,
                            dqlite__response,
                            DQLITE__RESPONSE_SCHEMA_SERVER);
TEST_MESSAGE_SEND_IMPLEMENT(welcome,
                            DQLITE_RESPONSE_WELCOME,
                            dqlite__response,
                            DQLITE__RESPONSE_SCHEMA_WELCOME);
TEST_MESSAGE_SEND_IMPLEMENT(servers,
                            DQLITE_RESPONSE_SERVERS,
                            dqlite__response,
                            DQLITE__RESPONSE_SCHEMA_SERVERS);
TEST_MESSAGE_SEND_IMPLEMENT(db,
                            DQLITE_RESPONSE_DB,
                            dqlite__response,
                            DQLITE__RESPONSE_SCHEMA_DB);
