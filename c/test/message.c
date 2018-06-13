#include <CUnit/CUnit.h>

#include "../src/message.h"

void test_message_send(
	struct dqlite__message *outgoing,
	struct dqlite__message *incoming)
{
	int err;
	uv_buf_t bufs[3];
	uv_buf_t buf;

	/* Get the send buffers of the outgoing message */
	dqlite__message_send_start(outgoing, bufs);
	CU_ASSERT_EQUAL(bufs[0].len, DQLITE__MESSAGE_HEADER_LEN);

	/* Get the header buffer of the incoming message */
	dqlite__message_header_recv_start(incoming, &buf);
	CU_ASSERT_EQUAL(buf.len, bufs[0].len);

	/* Copy the header data */
	memcpy(buf.base, bufs[0].base, bufs[0].len);

	/* Notify that the header is complete */
	err = dqlite__message_header_recv_done(incoming);
	CU_ASSERT_EQUAL(err, 0);

	/* Get the body buffer of the incoming message */
	err = dqlite__message_body_recv_start(incoming, &buf);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(buf.len, bufs[1].len + bufs[2].len);

	/* Copy the body data */
	memcpy(buf.base, bufs[1].base, bufs[1].len);
}
