#include <string.h>
#include <stdint.h>

#include <CUnit/CUnit.h>

#include "../src/message.h"

#include "suite.h"

static struct dqlite__message message;

void test_dqlite__message_setup()
{
	dqlite__message_init(&message);
}

void test_dqlite__message_teardown()
{
	dqlite__message_close(&message);
}

/*
 * dqlite__message_header_buf_suite
 */

void test_dqlite__message_header_buf_buf() {
	uint8_t *buf;
	size_t len;

	dqlite__message_header_buf(&message, &buf, &len);

	CU_ASSERT_PTR_EQUAL(buf, &message);
}

void test_dqlite__message_header_buf_len()
{
	uint8_t *buf;
	size_t len;

	dqlite__message_header_buf(&message, &buf, &len);
	CU_ASSERT_EQUAL(len, DQLITE__MESSAGE_HEADER_LEN);
	CU_ASSERT_EQUAL(
		len, (
			sizeof(message.words) +
			sizeof(message.type) +
			sizeof(message.flags) +
			sizeof(message.extra)
			)
		);

}


/*
 * dqlite__message_body_len_suite
 */

void test_dqlite__message_body_len_0()
{
	size_t size;

	message.words = 0;

	size = dqlite__message_body_len(&message);

	CU_ASSERT_EQUAL(size, 0);
}

void test_dqlite__message_body_len_8()
{
	size_t size;

	message.words = 1;

	size = dqlite__message_body_len(&message);

	CU_ASSERT_EQUAL(size, 8);
}

void test_dqlite__message_body_len_64()
{
	size_t size;

	message.words = 8;

	size = dqlite__message_body_len(&message);

	CU_ASSERT_EQUAL(size, 64);
}

void test_dqlite__message_body_len_1K()
{
	size_t size;

	message.words = 128;

	size = dqlite__message_body_len(&message);

	CU_ASSERT_EQUAL(size, 1024);
}

void test_dqlite__message_body_len_1M()
{
	size_t size;

	message.words = 131072;

	size = dqlite__message_body_len(&message);

	CU_ASSERT_EQUAL(size, 1048576);
}

/*
 * dqlite__message_write_suite
 */

void test_dqlite__message_write_text_one_string()
{
	int err;

	err = dqlite__message_write_text(&message, "hello");

	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_EQUAL(message.offset, 6);
}

void test_dqlite__message_write_text_two_strings()
{
	int err;

	err = dqlite__message_write_text(&message, "hello");
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_write_text(&message, "world");
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(message.offset, 12);
}

void test_dqlite__message_write_text_overflow()
{
	int err;

	err = dqlite__message_alloc(&message, 1);
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_write_text(&message, "hello world");
	CU_ASSERT_EQUAL(err, DQLITE_OVERFLOW);

	CU_ASSERT_STRING_EQUAL(message.error, "write overflow");

	/* The offset pointer was not advanced */
	CU_ASSERT_EQUAL(message.offset, 0);
}


/*
 * dqlite__message_read_suite
 */

void test_dqlite__message_read_text_one_string()
{
	int err;
	const char *text;

	err = dqlite__message_write_text(&message, "hello");
	CU_ASSERT_EQUAL(err, 0);

	dqlite__message_flush(&message, 0, 0);

	err = dqlite__message_read_text(&message, &text);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_STRING_EQUAL(text, "hello");
}

void test_dqlite__message_read_text_two_strings()
{
	int err;
	const char *text;

	err = dqlite__message_write_text(&message, "hello");
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_write_text(&message, "world");
	CU_ASSERT_EQUAL(err, 0);

	dqlite__message_flush(&message, 0, 0);

	err = dqlite__message_read_text(&message, &text);
	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_STRING_EQUAL(text, "hello");

	err = dqlite__message_read_text(&message, &text);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);
	CU_ASSERT_STRING_EQUAL(text, "world");
}

void test_dqlite__message_read_text_parse_error()
{
	int err;
	sqlite3_int64 value = 0xffffffffffffffff;
	const char *text;

	/* Write a value with all 1's */
	err = dqlite__message_write_int64(&message, value);
	CU_ASSERT_EQUAL(err, 0);

	dqlite__message_flush(&message, 0, 0);

	err = dqlite__message_read_text(&message, &text);

	CU_ASSERT_EQUAL(err, DQLITE_PARSE);

	CU_ASSERT_STRING_EQUAL(message.error, "no string found");
}

void test_dqlite__message_read_int64_one_value()
{
	int err;
	int64_t value;

	err = dqlite__message_alloc(&message, 1);
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_write_int64(&message, 123456789);
	CU_ASSERT_EQUAL(err, 0);

	dqlite__message_flush(&message, 0, 0);

	err = dqlite__message_read_int64(&message, &value);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_EQUAL(value, 123456789);
}

void test_dqlite__message_read_int64_two_values()
{
	int err;
	int64_t value;

	err = dqlite__message_write_int64(&message, -12);
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_write_int64(&message, 23);
	CU_ASSERT_EQUAL(err, 0);

	dqlite__message_flush(&message, 0, 0);

	err = dqlite__message_read_int64(&message, &value);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(value, -12);

	err = dqlite__message_read_int64(&message, &value);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_EQUAL(value, 23);
}

void test_dqlite__message_read_uint64_one_value()
{
	int err;
	uint64_t value;

	err = dqlite__message_alloc(&message, 1);
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_write_uint64(&message, 123456789);
	CU_ASSERT_EQUAL(err, 0);

	dqlite__message_flush(&message, 0, 0);

	err = dqlite__message_read_uint64(&message, &value);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_EQUAL(value, 123456789);
}

void test_dqlite__message_read_uint64_two_values()
{
	int err;
	uint64_t value;

	err = dqlite__message_write_uint64(&message, 12);
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_write_uint64(&message, 77);
	CU_ASSERT_EQUAL(err, 0);

	dqlite__message_flush(&message, 0, 0);

	err = dqlite__message_read_uint64(&message, &value);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(value, 12);

	err = dqlite__message_read_uint64(&message, &value);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_EQUAL(value, 77);
}
