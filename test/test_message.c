#include <stdint.h>
#include <string.h>
#include <time.h>

#include <sqlite3.h>
#include <uv.h>

#include "../include/dqlite.h"

#include "../src/lib/byte.h"
#include "../src/message.h"

#include "./lib/runner.h"

TEST_MODULE(message);

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data)
{
	struct message *message;

	(void)params;
	(void)user_data;

	message = munit_malloc(sizeof *message);
	message__init(message);

	return message;
}

static void tear_down(void *data)
{
	struct message *message = data;

	message__close(message);
	free(message);
}

TEST_SUITE(recv);
TEST_SETUP(recv, setup);
TEST_TEAR_DOWN(recv, tear_down);

TEST_GROUP(recv, header);
TEST_GROUP(recv, body);

TEST_SUITE(send);
TEST_SETUP(send, setup);
TEST_TEAR_DOWN(send, tear_down);

TEST_GROUP(send, header);
TEST_GROUP(send, body);
TEST_GROUP(send, start);

/******************************************************************************
 *
 * message__header_recv_start
 *
 ******************************************************************************/

/* The header buffer is the message itself. */
TEST_CASE(recv, header, start_base, NULL)
{
	struct message *message = data;
	uv_buf_t buf;

	(void)params;

	message__header_recv_start(message, &buf);
	munit_assert_ptr_equal(buf.base, message);

	return MUNIT_OK;
}

/* The header buffer lenght is 8 bytes. */
TEST_CASE(recv, header, start_len, NULL)
{
	struct message *message = data;
	uv_buf_t buf;

	(void)params;

	message__header_recv_start(message, &buf);
	munit_assert_int(buf.len, ==, MESSAGE__HEADER_LEN);
	munit_assert_int(buf.len, ==,
			 (sizeof message->words + sizeof message->type +
			  sizeof message->flags + sizeof message->extra));

	return MUNIT_OK;
}

/******************************************************************************
 *
 * message__header_recv_done
 *
 ******************************************************************************/

/* If the number of words of the message body is zero, an error is returned. */
TEST_CASE(recv, header, done_empty_body, NULL)
{
	struct message *message = data;
	int err;
	err = message__header_recv_done(message);

	(void)params;

	munit_assert_int(err, ==, DQLITE_PROTO);
	munit_assert_string_equal(message->error, "empty message body");

	return MUNIT_OK;
}

/* If the number of words of the message body exceeds the hard-coded limit, an
 * error is returned. */
TEST_CASE(recv, header, done_body_too_big, NULL)
{
	struct message *message = data;
	int err;
	uv_buf_t buf;

	(void)params;

	message__header_recv_start(message, &buf);

	/* Set a very high word count */
	buf.base[0] = 0;
	buf.base[1] = 0;
	buf.base[2] = 0;
	buf.base[3] = 127;

	err = message__header_recv_done(message);

	munit_assert_int(err, ==, DQLITE_PROTO);
	munit_assert_string_equal(message->error, "message body too large");

	return MUNIT_OK;
}

/******************************************************************************
 *
 * message__body_recv_start
 *
 ******************************************************************************/

/* The message body is 1 word long, the static buffer gets used. */
TEST_CASE(recv, body, start_1, NULL)
{
	struct message *message = data;
	int err;
	uv_buf_t buf;

	(void)params;

	message->words = 1;

	err = message__body_recv_start(message, &buf);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_equal(buf.base, message->body1);
	munit_assert_int(buf.len, ==, 8);

	return MUNIT_OK;
}

/* The message body is 513 words long, and the dynamic buffer gets allocated. */
TEST_CASE(recv, body, start_513, NULL)
{
	struct message *message = data;
	int err;
	uv_buf_t buf;

	(void)params;

	message->words = 513;

	err = message__body_recv_start(message, &buf);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_equal(buf.base, message->body2.base);
	munit_assert_int(buf.len, ==, message->body2.len);
	munit_assert_int(buf.len, ==, 4104);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * message__body_get
 *
 ******************************************************************************/

/* Attempting to read a string when the read cursor is not at word boundary
 * results in an error. */
TEST_CASE(recv, body, get_text_misaligned, NULL)
{
	struct message *message = data;

	char buf[8] = {0, 0, 'h', 'i', 0, 0, 0, 0};
	uint8_t n;
	text_t text;
	int err;

	(void)params;

	message->words = 1;
	memcpy(message->body1, buf, 8);

	err = message__body_get_uint8(message, &n);
	munit_assert_int(err, ==, 0);

	err = message__body_get_text(message, &text);
	munit_assert_int(err, ==, DQLITE_PARSE);

	munit_assert_string_equal(message->error, "misaligned read");

	return MUNIT_OK;
}

/* If no terminating null byte is found within the message body, an error is
 * returned. */
TEST_CASE(recv, body, get_text_not_found, NULL)
{
	struct message *message = data;
	int err;
	text_t text;
	char buf[8] = {255, 255, 255, 255, 255, 255, 255, 255};

	(void)params;

	message->words = 1;
	memcpy(message->body1, buf, 8);

	err = message__body_get_text(message, &text);

	munit_assert_int(err, ==, DQLITE_PARSE);

	munit_assert_string_equal(message->error, "no string found");

	return MUNIT_OK;
}

/* Read one string. */
TEST_CASE(recv, body, get_text_one_string, NULL)
{
	struct message *message = data;
	int err;
	text_t text;
	char buf[8] = {'h', 'e', 'l', 'l', 'o', '!', '!', 0};

	(void)params;

	message->words = 1;
	memcpy(message->body1, buf, 8);

	err = message__body_get_text(message, &text);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_string_equal(text, "hello!!");

	return MUNIT_OK;
}

/* Read two strings. */
TEST_CASE(recv, body, get_text_two_strings, NULL)
{
	struct message *message = data;
	int err;
	text_t text;
	char buf[16] = {'h', 'e', 'l', 'l', 'o', 0, 0, 0,
			'w', 'o', 'r', 'l', 'd', 0, 0, 0};

	(void)params;

	message->words = 2;
	memcpy(message->body1, buf, 16);

	err = message__body_get_text(message, &text);
	munit_assert_int(err, ==, 0);
	munit_assert_string_equal(text, "hello");

	err = message__body_get_text(message, &text);
	munit_assert_int(err, ==, DQLITE_EOM);
	munit_assert_string_equal(text, "world");

	return MUNIT_OK;
}

/* Read a string from a message that uses the dynamic message body buffer. */
TEST_CASE(recv, body, get_text_from_dyn_buf, NULL)
{
	struct message *message = data;
	int err;
	text_t text;
	uv_buf_t buf;

	(void)params;

	message->words = 513;

	err = message__body_recv_start(message, &buf);
	munit_assert_int(err, ==, 0);

	strcpy(buf.base, "hello");

	err = message__body_get_text(message, &text);
	munit_assert_int(err, ==, 0);

	munit_assert_string_equal(text, "hello");

	return MUNIT_OK;
}

/* Read four uint8 values. */
TEST_CASE(recv, body, get_uint8_four_values, NULL)
{
	struct message *message = data;
	int err;
	uint8_t buf;
	uint8_t value;

	(void)params;

	message->words = 1;

	buf = 12;
	memcpy(message->body1, &buf, 1);

	buf = 77;
	memcpy(message->body1 + 1, &buf, 1);

	buf = 128;
	memcpy(message->body1 + 2, &buf, 1);

	buf = 255;
	memcpy(message->body1 + 3, &buf, 1);

	err = message__body_get_uint8(message, &value);
	munit_assert_int(err, ==, 0);

	munit_assert_int(value, ==, 12);

	err = message__body_get_uint8(message, &value);
	munit_assert_int(err, ==, 0);

	munit_assert_int(value, ==, 77);

	err = message__body_get_uint8(message, &value);
	munit_assert_int(err, ==, 0);

	munit_assert_int(value, ==, 128);

	err = message__body_get_uint8(message, &value);
	munit_assert_int(err, ==, 0);

	munit_assert_int(value, ==, 255);

	return MUNIT_OK;
}

/* Trying to read a uint8 value past the end of the message body results in an
 * error. */
TEST_CASE(recv, body, get_uint8_overflow, NULL)
{
	struct message *message = data;

	int i;
	uint8_t value;
	int err;

	(void)params;

	message->words = 1;

	for (i = 0; i < 7; i++) {
		err = message__body_get_uint8(message, &value);
		munit_assert_int(err, ==, 0);
	}

	err = message__body_get_uint8(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	err = message__body_get_uint8(message, &value);
	munit_assert_int(err, ==, DQLITE_OVERFLOW);

	return MUNIT_OK;
}

/* Read two uint32 values. */
TEST_CASE(recv, body, get_uint32_two_values, NULL)
{
	struct message *message = data;
	int err;
	uint32_t buf;
	uint32_t value;

	(void)params;

	message->words = 1;

	buf = byte__flip32(12);
	memcpy(message->body1, &buf, 4);

	buf = byte__flip32(77);
	memcpy(message->body1 + 4, &buf, 4);

	err = message__body_get_uint32(message, &value);
	munit_assert_int(err, ==, 0);

	munit_assert_int(value, ==, 12);

	err = message__body_get_uint32(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_int(value, ==, 77);

	return MUNIT_OK;
}

/* Trying to read a uint32 when the read cursor is not 4-byte aligned results in
 * an error. */
TEST_CASE(recv, body, get_uint32_misaligned, NULL)
{
	struct message *message = data;

	uint8_t value1;
	uint32_t value2;
	int err;

	(void)params;

	message->words = 1;

	err = message__body_get_uint8(message, &value1);
	munit_assert_int(err, ==, 0);

	err = message__body_get_uint32(message, &value2);
	munit_assert_int(err, ==, DQLITE_PARSE);

	munit_assert_string_equal(message->error, "misaligned read");

	return MUNIT_OK;
}

/* Trying to read a uint32 value past the end of the message body results in an
 * error. */
TEST_CASE(recv, body, get_uint32_overflow, NULL)
{
	struct message *message = data;

	uint32_t value;
	int err;

	(void)params;

	message->words = 1;

	err = message__body_get_uint32(message, &value);
	munit_assert_int(err, ==, 0);

	err = message__body_get_uint32(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	err = message__body_get_uint32(message, &value);
	munit_assert_int(err, ==, DQLITE_OVERFLOW);

	return MUNIT_OK;
}

/* Read one uint64 value. */
TEST_CASE(recv, body, get_uint64_one_value, NULL)
{
	struct message *message = data;
	int err;
	uint64_t buf;
	uint64_t value;

	(void)params;

	message->words = 1;

	buf = byte__flip64(123456789);
	memcpy(message->body1, &buf, 8);

	err = message__body_get_uint64(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_int(value, ==, 123456789);

	return MUNIT_OK;
}

/* Read two uint64 values. */
TEST_CASE(recv, body, get_uint64_two_values, NULL)
{
	struct message *message = data;
	int err;
	uint64_t buf;
	uint64_t value;

	(void)params;

	message->words = 2;

	buf = byte__flip64(12);
	memcpy(message->body1, &buf, 8);

	buf = byte__flip64(77);
	memcpy(message->body1 + 8, &buf, 8);

	err = message__body_get_uint64(message, &value);
	munit_assert_int(err, ==, 0);

	munit_assert_int(value, ==, 12);

	err = message__body_get_uint64(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_int(value, ==, 77);

	return MUNIT_OK;
}

/* Trying to read a uint64 when the read cursor is not word aligned results in
 * an error. */
TEST_CASE(recv, body, get_uint64_misaligned, NULL)
{
	struct message *message = data;

	uint8_t value1;
	uint64_t value2;
	int err;

	(void)params;

	message->words = 2;

	err = message__body_get_uint8(message, &value1);
	munit_assert_int(err, ==, 0);

	err = message__body_get_uint64(message, &value2);
	munit_assert_int(err, ==, DQLITE_PARSE);

	munit_assert_string_equal(message->error, "misaligned read");

	return MUNIT_OK;
}

/* Trying to read a uint64 value past the end of the message body results in an
 * error. */
TEST_CASE(recv, body, get_uint64_overflow, NULL)
{
	struct message *message = data;

	uint64_t value;
	int err;

	(void)params;

	message->words = 1;

	err = message__body_get_uint64(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	err = message__body_get_uint64(message, &value);
	munit_assert_int(err, ==, DQLITE_OVERFLOW);

	return MUNIT_OK;
}

/* Read one int64 value. */
TEST_CASE(recv, body, get_int64_one_value, NULL)
{
	struct message *message = data;
	int err;
	uint64_t buf;
	int64_t value;

	(void)params;

	message->words = 1;

	buf = byte__flip64(123456789);
	memcpy(message->body1, &buf, 8);

	err = message__body_get_int64(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_int(value, ==, 123456789);

	return MUNIT_OK;
}

/* Read two int64 values. */
TEST_CASE(recv, body, get_int64_two_values, NULL)
{
	struct message *message = data;
	int err;
	uint64_t buf;
	int64_t value;

	(void)params;

	message->words = 2;

	buf = byte__flip64((uint64_t)(-12));
	memcpy(message->body1, &buf, 8);

	buf = byte__flip64(23);
	memcpy(message->body1 + 8, &buf, 8);

	err = message__body_get_int64(message, &value);
	munit_assert_int(err, ==, 0);

	munit_assert_int(value, ==, -12);

	err = message__body_get_int64(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_int(value, ==, 23);

	return MUNIT_OK;
}

/* Read a double value. */
TEST_CASE(recv, body, get_double_one_value, NULL)
{
	struct message *message = data;
	int err;
	uint64_t *buf;
	double pi = 3.1415926535;
	double value;

	(void)params;

	message->words = 1;

	buf = (uint64_t *)(&pi);
	*buf = byte__flip64(*buf);
	memcpy(message->body1, buf, 8);

	err = message__body_get_double(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_double(value, ==, 3.1415926535);

	return MUNIT_OK;
}

TEST_CASE(recv, body, get_servers_one, NULL)
{
	struct message *message = data;
	int err;
	servers_t servers;
	uv_buf_t buf;

	(void)params;

	message->words = 3;

	err = message__body_recv_start(message, &buf);
	munit_assert_int(err, ==, 0);

	*(uint64_t *)(buf.base) = byte__flip64(1);
	strcpy(buf.base + 8, "1.2.3.4:666");

	err = message__body_get_servers(message, &servers);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_int(servers[0].id, ==, 1);
	munit_assert_string_equal(servers[0].address, "1.2.3.4:666");

	munit_assert_int(servers[1].id, ==, 0);
	munit_assert_ptr_equal(servers[1].address, NULL);

	sqlite3_free(servers);

	return MUNIT_OK;
}

TEST_CASE(recv, body, get_servers_two, NULL)
{
	struct message *message = data;
	int err;
	servers_t servers;
	uv_buf_t buf;

	(void)params;

	message->words = 6;

	err = message__body_recv_start(message, &buf);
	munit_assert_int(err, ==, 0);

	*(uint64_t *)(buf.base) = byte__flip64(1);
	strcpy(buf.base + 8, "1.2.3.4:666");

	*(uint64_t *)(buf.base + 24) = byte__flip64(2);
	strcpy(buf.base + 32, "5.6.7.8:666");

	err = message__body_get_servers(message, &servers);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_int(servers[0].id, ==, 1);
	munit_assert_string_equal(servers[0].address, "1.2.3.4:666");

	munit_assert_int(servers[1].id, ==, 2);
	munit_assert_string_equal(servers[1].address, "5.6.7.8:666");

	munit_assert_int(servers[2].id, ==, 0);
	munit_assert_ptr_equal(servers[2].address, NULL);

	sqlite3_free(servers);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * message__header_put
 *
 ******************************************************************************/

/* Set the type of a message. */
TEST_CASE(send, header, put_type, NULL)
{
	struct message *message = data;

	(void)params;

	message__header_put(message, 123, 0);
	munit_assert_int(message->type, ==, 123);

	return MUNIT_OK;
}

/* Set the message flags. */
TEST_CASE(send, header, put_flags, NULL)
{
	struct message *message = data;

	(void)params;

	message__header_put(message, 0, 255);
	munit_assert_int(message->flags, ==, 255);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * message__body_put
 *
 ******************************************************************************/

/* Trying to write a string when the write cursor is not at word boundary
 * results in an error. */
TEST_CASE(send, body, put_text_misaligned, NULL)
{
	struct message *message = data;
	int err;

	(void)params;

	err = message__body_put_uint8(message, 123);
	munit_assert_int(err, ==, 0);

	err = message__body_put_text(message, "hello");
	munit_assert_int(err, ==, DQLITE_PROTO);

	munit_assert_string_equal(message->error, "misaligned write");

	return MUNIT_OK;
}

TEST_CASE(send, body, put_text_one, NULL)
{
	struct message *message = data;
	int err;

	(void)params;

	err = message__body_put_text(message, "hello");

	munit_assert_int(err, ==, 0);
	munit_assert_int(message->offset1, ==, 8);

	munit_assert_string_equal(message->body1, "hello");

	/* Padding */
	munit_assert_int(message->body1[6], ==, 0);
	munit_assert_int(message->body1[7], ==, 0);

	return MUNIT_OK;
}

TEST_CASE(send, body, put_text_one_no_pad, NULL)
{
	struct message *message = data;
	int err;

	(void)params;

	err = message__body_put_text(message, "hello!!");

	munit_assert_int(err, ==, 0);
	munit_assert_int(message->offset1, ==, 8);

	munit_assert_string_equal(message->body1, "hello!!");

	return MUNIT_OK;
}

TEST_CASE(send, body, put_text_two, NULL)
{
	struct message *message = data;
	int err;

	(void)params;

	err = message__body_put_text(message, "hello");
	munit_assert_int(err, ==, 0);

	err = message__body_put_text(message, "world");
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 16);

	munit_assert_string_equal(message->body1, "hello");

	/* Padding */
	munit_assert_int(message->body1[6], ==, 0);
	munit_assert_int(message->body1[7], ==, 0);

	munit_assert_string_equal(message->body1 + 8, "world");

	/* Padding */
	munit_assert_int(message->body1[8 + 6], ==, 0);
	munit_assert_int(message->body1[8 + 7], ==, 0);

	return MUNIT_OK;
}

/* The static body is not large enough to hold the given text, so the dynamic
 * buffer is allocated in order to hold the rest of it. */
TEST_CASE(send, body, put_text_body2, NULL)
{
	struct message *message = data;
	int err;

	(void)params;

	message->offset1 = 4088;

	err = message__body_put_text(message, "hello world");
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 4088);
	munit_assert_int(message->offset2, ==, 16);

	munit_assert_string_equal(message->body2.base, "hello world");

	return MUNIT_OK;
}

TEST_CASE(send, body, put_uint8_four, NULL)
{
	struct message *message = data;
	int err;

	(void)params;

	err = message__body_put_uint8(message, 25);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 1);

	err = message__body_put_uint8(message, 50);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 2);

	err = message__body_put_uint8(message, 100);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 3);

	err = message__body_put_uint8(message, 200);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 4);

	munit_assert_int(*(uint8_t *)(message->body1), ==, 25);
	munit_assert_int(*(uint8_t *)(message->body1 + 1), ==, 50);
	munit_assert_int(*(uint8_t *)(message->body1 + 2), ==, 100);
	munit_assert_int(*(uint8_t *)(message->body1 + 3), ==, 200);

	return MUNIT_OK;
}

TEST_CASE(send, body, put_uint32_two, NULL)
{
	struct message *message = data;
	int err;

	(void)params;

	err = message__body_put_uint32(message, 99);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 4);

	err = message__body_put_uint32(message, 66);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 8);

	munit_assert_int(byte__flip32(*(uint32_t *)(message->body1)), ==, 99);
	munit_assert_int(byte__flip32(*(uint32_t *)(message->body1 + 4)), ==,
			 66);

	return MUNIT_OK;
}

TEST_CASE(send, body, put_int64_one, NULL)
{
	struct message *message = data;
	int err;

	(void)params;

	err = message__body_put_int64(message, -12);

	munit_assert_int(err, ==, 0);
	munit_assert_int(message->offset1, ==, 8);

	munit_assert_int((int64_t)byte__flip64(*(uint64_t *)(message->body1)),
			 ==, -12);

	return MUNIT_OK;
}

TEST_CASE(send, body, put_uint64_one, NULL)
{
	struct message *message = data;
	int err;

	(void)params;

	err = message__body_put_uint64(message, 99);

	munit_assert_int(err, ==, 0);
	munit_assert_int(message->offset1, ==, 8);

	munit_assert_int(byte__flip64(*(uint64_t *)(message->body1)), ==, 99);

	return MUNIT_OK;
}

TEST_CASE(send, body, put_double_one, NULL)
{
	struct message *message = data;
	int err;
	uint64_t buf;
	double value;

	(void)params;

	err = message__body_put_double(message, 3.1415926535);

	munit_assert_int(err, ==, 0);
	munit_assert_int(message->offset1, ==, 8);

	buf = byte__flip64(*(uint64_t *)(message->body1));

	memcpy(&value, &buf, sizeof(buf));

	munit_assert_double(value, ==, 3.1415926535);

	return MUNIT_OK;
}

TEST_CASE(send, body, put_dyn_buf, NULL)
{
	struct message *message = data;
	int err;
	uint64_t i;

	(void)params;

	for (i = 0; i < 4096 / 8; i++) {
		err = message__body_put_uint64(message, i);
		munit_assert_int(err, ==, 0);
	}

	munit_assert_int(message->offset1, ==, 4096);
	munit_assert_int(message->offset2, ==, 0);

	err = message__body_put_uint64(message, 666);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset2, ==, 8);

	return MUNIT_OK;
}

TEST_CASE(send, body, put_servers_one, NULL)
{
	struct message *message = data;
	dqlite_server_info servers[] = {
	    {1, "1.2.3.4:666"},
	    {0, NULL},
	};
	int err;
	uint64_t id;
	const char *address;

	(void)params;

	err = message__body_put_servers(message, servers);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 24);

	id = byte__flip64(*(uint64_t *)(message->body1));
	munit_assert_int(id, ==, 1);

	address = (const char *)(message->body1 + 8);
	munit_assert_string_equal(address, "1.2.3.4:666");

	return MUNIT_OK;
}

/******************************************************************************
 *
 * message__send_start
 *
 ******************************************************************************/

TEST_CASE(send, start, no_dyn_buf, NULL)
{
	struct message *message = data;
	int err;
	uv_buf_t bufs[3];
	struct message message2;
	uv_buf_t buf;
	uint64_t value;
	text_t text;

	(void)params;

	message__header_put(message, 9, 123);

	err = message__body_put_uint64(message, 78);
	munit_assert_int(err, ==, 0);

	err = message__body_put_text(message, "hello");
	munit_assert_int(err, ==, 0);

	message__send_start(message, bufs);

	munit_assert_ptr_equal(bufs[0].base, message);
	munit_assert_int(bufs[0].len, ==, 8);

	munit_assert_ptr_equal(bufs[1].base, message->body1);
	munit_assert_int(bufs[1].len, ==, 16);

	munit_assert_ptr_equal(bufs[2].base, NULL);
	munit_assert_int(bufs[2].len, ==, 0);

	message__init(&message2);

	message__header_recv_start(&message2, &buf);
	memcpy(buf.base, bufs[0].base, bufs[0].len);

	err = message__header_recv_done(&message2);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message2.type, ==, 9);
	munit_assert_int(message2.flags, ==, 123);

	err = message__body_recv_start(&message2, &buf);
	munit_assert_int(err, ==, 0);

	memcpy(buf.base, bufs[1].base, bufs[1].len);

	err = message__body_get_uint64(&message2, &value);
	munit_assert_int(err, ==, 0);

	munit_assert_int(value, ==, 78);

	err = message__body_get_text(&message2, &text);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_string_equal(text, "hello");

	message__recv_reset(&message2);
	message__send_reset(message);

	message__close(&message2);

	return MUNIT_OK;
}

TEST_CASE(send, start, dyn_buf, NULL)
{
	struct message *message = data;
	int err;
	uint64_t i;
	uv_buf_t bufs[3];
	struct message message2;
	uv_buf_t buf;
	uint64_t value;
	text_t text;

	(void)params;

	message__header_put(message, 9, 123);

	for (i = 0; i < 4088 / 8; i++) {
		err = message__body_put_uint64(message, i);
		munit_assert_int(err, ==, 0);
	}
	munit_assert_int(message->offset1, ==, 4088);

	err = message__body_put_text(message, "hello world");
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 4088);
	munit_assert_int(message->offset2, ==, 16);

	message__send_start(message, bufs);

	munit_assert_ptr_equal(bufs[0].base, message);
	munit_assert_int(bufs[0].len, ==, 8);

	munit_assert_ptr_equal(bufs[1].base, message->body1);
	munit_assert_int(bufs[1].len, ==, 4088);

	munit_assert_ptr_not_equal(bufs[2].base, NULL);
	munit_assert_int(bufs[2].len, ==, 16);

	message__init(&message2);

	message__header_recv_start(&message2, &buf);
	memcpy(buf.base, bufs[0].base, bufs[0].len);

	err = message__header_recv_done(&message2);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message2.type, ==, 9);
	munit_assert_int(message2.flags, ==, 123);

	err = message__body_recv_start(&message2, &buf);
	munit_assert_int(err, ==, 0);

	memcpy(buf.base, bufs[1].base, bufs[1].len);
	memcpy(buf.base + bufs[1].len, bufs[2].base, bufs[2].len);

	for (i = 0; i < 4088 / 8; i++) {
		err = message__body_get_uint64(&message2, &value);
		munit_assert_int(err, ==, 0);
		munit_assert_int(value, ==, i);
	}

	err = message__body_get_text(&message2, &text);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_string_equal(text, "hello world");

	message__recv_reset(&message2);
	message__send_reset(message);

	message__close(&message2);

	return MUNIT_OK;
}
