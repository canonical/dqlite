#include <string.h>
#include <stdint.h>

#include <CUnit/CUnit.h>
#include <uv.h>

#include "../src/binary.h"
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
 * dqlite__message_header_recv_start_suite
 */

void test_dqlite__message_header_recv_start_base() {
	uv_buf_t buf;

	dqlite__message_header_recv_start(&message, &buf);

	CU_ASSERT_PTR_EQUAL(buf.base, &message);
}

void test_dqlite__message_header_recv_start_len()
{
	uv_buf_t buf;

	dqlite__message_header_recv_start(&message, &buf);
	CU_ASSERT_EQUAL(buf.len, DQLITE__MESSAGE_HEADER_LEN);
	CU_ASSERT_EQUAL(
		buf.len, (
			sizeof(message.words) +
			sizeof(message.type) +
			sizeof(message.flags) +
			sizeof(message.extra)
			)
		);

}

/*
 * dqlite__message_header_recv_done_suite
 */

void test_dqlite__message_header_recv_done_empty_body()
{
	int err;
	err = dqlite__message_header_recv_done(&message);

	CU_ASSERT_EQUAL(err, DQLITE_PROTO);
	CU_ASSERT_STRING_EQUAL(message.error, "empty message body");
}

void test_dqlite__message_header_recv_done_body_too_large()
{
	int err;

	message.words = 1 << 30;

	err = dqlite__message_header_recv_done(&message);

	CU_ASSERT_EQUAL(err, DQLITE_PROTO);
	CU_ASSERT_STRING_EQUAL(message.error, "message body too large");
}

/*
 * dqlite__message_body_recv_start_suite
 */

void test_dqlite__message_body_recv_start_1()
{
	int err;
	uv_buf_t buf;

	message.words = 1;

	err = dqlite__message_body_recv_start(&message, &buf);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_EQUAL(buf.base, message.body1);
	CU_ASSERT_EQUAL(buf.len, 8);
}

void test_dqlite__message_body_recv_start_513()
{
	int err;
	uv_buf_t buf;

	message.words = 513;

	err = dqlite__message_body_recv_start(&message, &buf);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_EQUAL(buf.base, message.body2.base);
	CU_ASSERT_EQUAL(buf.len, message.body2.len);
	CU_ASSERT_EQUAL(buf.len, 4104);
}
/*
 * dqlite__message_body_get_suite
 */

void test_dqlite__message_body_get_text_one_string()
{
	int err;
	text_t text;
	char buf[8] = {
		'h', 'e', 'l', 'l', 'o', '!', '!', 0,
	};

	message.words = 1;
	memcpy(message.body1, buf, 8);

	err = dqlite__message_body_get_text(&message, &text);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_STRING_EQUAL(text, "hello!!");
}

void test_dqlite__message_body_get_text_two_strings()
{
	int err;
	text_t text;
	char buf[16] = {
		'h', 'e', 'l', 'l', 'o', 0, 0, 0,
		'w', 'o', 'r', 'l', 'd', 0, 0, 0,
	};

	message.words = 2;
	memcpy(message.body1, buf, 16);

	err = dqlite__message_body_get_text(&message, &text);
	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_STRING_EQUAL(text, "hello");

	err = dqlite__message_body_get_text(&message, &text);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);
	CU_ASSERT_STRING_EQUAL(text, "world");
}

void test_dqlite__message_body_get_text_parse_error()
{
	int err;
	text_t text;
	char buf[8] = {
		255, 255, 255, 255, 255, 255, 255, 255,
	};

	message.words = 1;
	memcpy(message.body1, buf, 8);

	err = dqlite__message_body_get_text(&message, &text);

	CU_ASSERT_EQUAL(err, DQLITE_PARSE);

	CU_ASSERT_STRING_EQUAL(message.error, "no string found");
}

void test_dqlite__message_body_get_text_from_dyn_buf()
{
	int err;
	text_t text;
	uv_buf_t buf;

	message.words = 513;

	err = dqlite__message_body_recv_start(&message, &buf);
	CU_ASSERT_EQUAL(err, 0);

	memcpy(buf.base, "hello", strlen("hello"));
	buf.base[5] = 0;

	err = dqlite__message_body_get_text(&message, &text);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_STRING_EQUAL(text, "hello");
}

void test_dqlite__message_body_get_text_list_one_item()
{
	int err;
	text_list_t list;
	uv_buf_t buf;

	message.words = 1;

	err = dqlite__message_body_recv_start(&message, &buf);
	CU_ASSERT_EQUAL(err, 0);

	memcpy(buf.base, "hello", strlen("hello"));
	buf.base[5] = 0;
	buf.base[6] = 0;
	buf.base[7] = 0;

	err = dqlite__message_body_get_text_list(&message, &list);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_STRING_EQUAL(list[0], "hello");

	sqlite3_free(list);
}

void test_dqlite__message_body_get_text_list_two_items()
{
	int err;
	text_list_t list;
	uv_buf_t buf;

	message.words = 2;

	err = dqlite__message_body_recv_start(&message, &buf);
	CU_ASSERT_EQUAL(err, 0);

	memcpy(buf.base, "hello", strlen("hello"));
	buf.base[5] = 0;
	buf.base[6] = 0;
	buf.base[7] = 0;

	memcpy(buf.base + 8, "world", strlen("world"));
	buf.base[8 + 5] = 0;
	buf.base[8 + 6] = 0;
	buf.base[8 + 7] = 0;

	err = dqlite__message_body_get_text_list(&message, &list);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_STRING_EQUAL(list[0], "hello");
	CU_ASSERT_STRING_EQUAL(list[1], "world");

	sqlite3_free(list);
}

void test_dqlite__message_body_get_uint8_four_values()
{
	int err;
	uint8_t buf;
	uint8_t value;

	message.words = 1;

	buf = 12;
	memcpy(message.body1, &buf, 1);

	buf = 77;
	memcpy(message.body1 + 1, &buf, 1);

	buf = 128;
	memcpy(message.body1 + 2, &buf, 1);

	buf = 255;
	memcpy(message.body1 + 3, &buf, 1);

	err = dqlite__message_body_get_uint8(&message, &value);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(value, 12);

	err = dqlite__message_body_get_uint8(&message, &value);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(value, 77);

	err = dqlite__message_body_get_uint8(&message, &value);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(value, 128);

	err = dqlite__message_body_get_uint8(&message, &value);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(value, 255);
}

void test_dqlite__message_body_get_uint32_two_values()
{
	int err;
	uint32_t buf;
	uint32_t value;

	message.words = 1;

	buf = dqlite__flip32(12);
	memcpy(message.body1, &buf, 4);

	buf = dqlite__flip32(77);
	memcpy(message.body1 + 4, &buf, 4);

	err = dqlite__message_body_get_uint32(&message, &value);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(value, 12);

	err = dqlite__message_body_get_uint32(&message, &value);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_EQUAL(value, 77);
}

void test_dqlite__message_body_get_int64_one_value()
{
	int err;
	uint64_t buf;
	int64_t value;

	message.words = 1;

	buf = dqlite__flip64(123456789);
	memcpy(message.body1, &buf, 8);

	err = dqlite__message_body_get_int64(&message, &value);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_EQUAL(value, 123456789);
}

void test_dqlite__message_body_get_int64_two_values()
{
	int err;
	uint64_t buf;
	int64_t value;

	message.words = 2;

	buf = dqlite__flip64((uint64_t)(-12));
	memcpy(message.body1, &buf, 8);

	buf = dqlite__flip64(23);
	memcpy(message.body1 + 8, &buf, 8);

	err = dqlite__message_body_get_int64(&message, &value);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(value, -12);

	err = dqlite__message_body_get_int64(&message, &value);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_EQUAL(value, 23);
}

void test_dqlite__message_body_get_uint64_one_value()
{
	int err;
	uint64_t buf;
	uint64_t value;

	message.words = 1;

	buf = dqlite__flip64(123456789);
	memcpy(message.body1, &buf, 8);

	err = dqlite__message_body_get_uint64(&message, &value);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_EQUAL(value, 123456789);
}

void test_dqlite__message_body_get_uint64_two_values()
{
	int err;
	uint64_t buf;
	uint64_t value;

	message.words = 2;

	buf = dqlite__flip64(12);
	memcpy(message.body1, &buf, 8);

	buf = dqlite__flip64(77);
	memcpy(message.body1 + 8, &buf, 8);

	err = dqlite__message_body_get_uint64(&message, &value);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(value, 12);

	err = dqlite__message_body_get_uint64(&message, &value);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_EQUAL(value, 77);
}

void test_dqlite__message_body_get_double_one_value()
{
	int err;
	uint64_t *buf;
	double pi = 3.1415926535;
	double value;

	message.words = 1;

	buf = (uint64_t*)(&pi);
	*buf = dqlite__flip64(*buf);
	memcpy(message.body1, buf, 8);

	err = dqlite__message_body_get_double(&message, &value);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_EQUAL(value, 3.1415926535);
}

/*
 * dqlite__message_header_put_suite
 */

void test_dqlite__message_header_put_type()
{
	dqlite__message_header_put(&message, 123, 0);
	CU_ASSERT_EQUAL(message.type, 123);
}

void test_dqlite__message_header_put_flags()
{
	dqlite__message_header_put(&message, 0, 255);
	CU_ASSERT_EQUAL(message.flags, 255);
}

/*
 * dqlite__message_body_put_suite
 */

void test_dqlite__message_body_put_text_one()
{
	int err;

	err = dqlite__message_body_put_text(&message, "hello");

	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_EQUAL(message.offset1, 8);

	CU_ASSERT_STRING_EQUAL(message.body1, "hello");

	/* Padding */
	CU_ASSERT_EQUAL(message.body1[6], 0);
	CU_ASSERT_EQUAL(message.body1[7], 0);
}

void test_dqlite__message_body_put_text_one_no_pad()
{
	int err;

	err = dqlite__message_body_put_text(&message, "hello!!");

	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_EQUAL(message.offset1, 8);

	CU_ASSERT_STRING_EQUAL(message.body1, "hello!!");
}

void test_dqlite__message_body_put_text_two()
{
	int err;

	err = dqlite__message_body_put_text(&message, "hello");
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_body_put_text(&message, "world");
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(message.offset1, 16);

	CU_ASSERT_STRING_EQUAL(message.body1, "hello");

	/* Padding */
	CU_ASSERT_EQUAL(message.body1[6], 0);
	CU_ASSERT_EQUAL(message.body1[7], 0);

	CU_ASSERT_STRING_EQUAL(message.body1 + 8, "world");

	/* Padding */
	CU_ASSERT_EQUAL(message.body1[8 + 6], 0);
	CU_ASSERT_EQUAL(message.body1[8 + 7], 0);

}

void test_dqlite__message_body_put_uint8_four()
{
	int err;

	err = dqlite__message_body_put_uint8(&message, 25);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(message.offset1, 1);

	err = dqlite__message_body_put_uint8(&message, 50);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(message.offset1, 2);

	err = dqlite__message_body_put_uint8(&message, 100);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(message.offset1, 3);

	err = dqlite__message_body_put_uint8(&message, 200);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(message.offset1, 4);

	CU_ASSERT_EQUAL(*(uint8_t*)(message.body1), 25);
	CU_ASSERT_EQUAL(*(uint8_t*)(message.body1 + 1), 50);
	CU_ASSERT_EQUAL(*(uint8_t*)(message.body1 + 2), 100);
	CU_ASSERT_EQUAL(*(uint8_t*)(message.body1 + 3), 200);
}

void test_dqlite__message_body_put_uint32_two()
{
	int err;

	err = dqlite__message_body_put_uint32(&message, 99);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(message.offset1, 4);

	err = dqlite__message_body_put_uint32(&message, 66);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(message.offset1, 8);

	CU_ASSERT_EQUAL(dqlite__flip32(*(uint32_t*)(message.body1)), 99);
	CU_ASSERT_EQUAL(dqlite__flip32(*(uint32_t*)(message.body1 + 4)), 66);
}

void test_dqlite__message_body_put_int64_one()
{
	int err;

	err = dqlite__message_body_put_int64(&message, -12);

	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_EQUAL(message.offset1, 8);

	CU_ASSERT_EQUAL((int64_t)dqlite__flip64(*(uint64_t*)(message.body1)), -12);
}

void test_dqlite__message_body_put_uint64_one()
{
	int err;

	err = dqlite__message_body_put_uint64(&message, 99);

	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_EQUAL(message.offset1, 8);

	CU_ASSERT_EQUAL(dqlite__flip64(*(uint64_t*)(message.body1)), 99);
}

void test_dqlite__message_body_put_double_one()
{
	int err;
	uint64_t buf;

	err = dqlite__message_body_put_double(&message, 3.1415926535);

	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_EQUAL(message.offset1, 8);

	buf = dqlite__flip64(*(uint64_t*)(message.body1));
	CU_ASSERT_EQUAL(*(double*)(&buf), 3.1415926535);
}

void test_dqlite__message_body_put_dyn_buf()
{
	int err;
	uint64_t i;

	for (i = 0; i < 4096 / 8; i++) {
		err = dqlite__message_body_put_uint64(&message, i);
		CU_ASSERT_EQUAL(err, 0);
	}

	CU_ASSERT_EQUAL(message.offset1, 4096);
	CU_ASSERT_EQUAL(message.offset2, 0);

	err = dqlite__message_body_put_uint64(&message, 666);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(message.offset2, 8);
}

/*
 * dqlite__message_send_start_suite
 */

void test_dqlite__message_send_start_no_dyn_buf()
{
	int err;
	uv_buf_t bufs[3];
	struct dqlite__message message2;
	uv_buf_t buf;
	uint64_t value;
	text_t text;

	dqlite__message_header_put(&message, 9, 123);

	err = dqlite__message_body_put_uint64(&message, 78);
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_body_put_text(&message, "hello");
	CU_ASSERT_EQUAL(err, 0);

	dqlite__message_send_start(&message, bufs);

	CU_ASSERT_PTR_EQUAL(bufs[0].base, &message);
	CU_ASSERT_EQUAL(bufs[0].len, 8);

	CU_ASSERT_PTR_EQUAL(bufs[1].base, message.body1);
	CU_ASSERT_EQUAL(bufs[1].len, 16);

	CU_ASSERT_PTR_NULL(bufs[2].base);
	CU_ASSERT_EQUAL(bufs[2].len, 0);

	dqlite__message_init(&message2);

	dqlite__message_header_recv_start(&message2, &buf);
	memcpy(buf.base, bufs[0].base, bufs[0].len);

	err = dqlite__message_header_recv_done(&message2);
	CU_ASSERT_EQUAL(0, err);

	CU_ASSERT_EQUAL(message2.type, 9);
	CU_ASSERT_EQUAL(message2.flags, 123);

	err = dqlite__message_body_recv_start(&message2, &buf);
	CU_ASSERT_EQUAL(0, err);

	memcpy(buf.base, bufs[1].base, bufs[1].len);

	err = dqlite__message_body_get_uint64(&message2, &value);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(value, 78);

	err = dqlite__message_body_get_text(&message2, &text);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_STRING_EQUAL(text, "hello");

	dqlite__message_recv_reset(&message2);
	dqlite__message_send_reset(&message);

	dqlite__message_close(&message2);
}

void test_dqlite__message_send_start_dyn_buf()
{
	int err;
	uint64_t i;
	uv_buf_t bufs[3];
	struct dqlite__message message2;
	uv_buf_t buf;
	uint64_t value;
	text_t text;

	dqlite__message_header_put(&message, 9, 123);

	for (i = 0; i < 4088 / 8; i++) {
		err = dqlite__message_body_put_uint64(&message, i);
		CU_ASSERT_EQUAL(err, 0);
	}
	CU_ASSERT_EQUAL(message.offset1, 4088);

	err = dqlite__message_body_put_text(&message, "hello world");
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(message.offset1, 4088);
	CU_ASSERT_EQUAL(message.offset2, 16);

	dqlite__message_send_start(&message, bufs);

	CU_ASSERT_PTR_EQUAL(bufs[0].base, &message);
	CU_ASSERT_EQUAL(bufs[0].len, 8);

	CU_ASSERT_PTR_EQUAL(bufs[1].base, message.body1);
	CU_ASSERT_EQUAL(bufs[1].len, 4088);

	CU_ASSERT_PTR_NOT_NULL(bufs[2].base);
	CU_ASSERT_EQUAL(bufs[2].len, 16);

	dqlite__message_init(&message2);

	dqlite__message_header_recv_start(&message2, &buf);
	memcpy(buf.base, bufs[0].base, bufs[0].len);

	err = dqlite__message_header_recv_done(&message2);
	CU_ASSERT_EQUAL(0, err);

	CU_ASSERT_EQUAL(message2.type, 9);
	CU_ASSERT_EQUAL(message2.flags, 123);

	err = dqlite__message_body_recv_start(&message2, &buf);
	CU_ASSERT_EQUAL(0, err);

	memcpy(buf.base, bufs[1].base, bufs[1].len);
	memcpy(buf.base + bufs[1].len, bufs[2].base, bufs[2].len);

	for (i = 0; i < 4088 / 8; i++) {
		err = dqlite__message_body_get_uint64(&message2, &value);
		CU_ASSERT_EQUAL(err, 0);
		CU_ASSERT_EQUAL(value, i);
	}

	err = dqlite__message_body_get_text(&message2, &text);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_STRING_EQUAL(text, "hello world");

	dqlite__message_recv_reset(&message2);
	dqlite__message_send_reset(&message);

	dqlite__message_close(&message2);
}
