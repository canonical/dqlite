#include <stdint.h>
#include <string.h>
#include <time.h>

#include <sqlite3.h>
#include <uv.h>

#include "../include/dqlite.h"
#include "../src/binary.h"
#include "../src/message.h"

#include "leak.h"
#include "munit.h"

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data) {
	struct dqlite__message *message;
	(void)params;
	(void)user_data;

	message = munit_malloc(sizeof *message);
	dqlite__message_init(message);

	return message;
}

static void tear_down(void *data) {
	struct dqlite__message *message = data;

	dqlite__message_close(message);

	test_assert_no_leaks();
}

/******************************************************************************
 *
 * dqlite__message_header_recv_start
 *
 ******************************************************************************/

/* The header buffer is the message itself. */
static MunitResult test_header_recv_start_base(const MunitParameter params[],
                                               void *               data) {
	struct dqlite__message *message = data;
	uv_buf_t                buf;

	(void)params;

	dqlite__message_header_recv_start(message, &buf);

	munit_assert_ptr_equal(buf.base, message);

	return MUNIT_OK;
}

/* The header buffer lenght is 8 bytes. */
static MunitResult test_header_recv_start_len(const MunitParameter params[],
                                              void *               data) {
	struct dqlite__message *message = data;
	uv_buf_t                buf;

	(void)params;

	dqlite__message_header_recv_start(message, &buf);
	munit_assert_int(buf.len, ==, DQLITE__MESSAGE_HEADER_LEN);
	munit_assert_int(buf.len,
	                 ==,
	                 (sizeof message->words + sizeof message->type +
	                  sizeof message->flags + sizeof message->extra));

	return MUNIT_OK;
}

static MunitTest header_recv_start_tests[] = {
    {"/buf/base", test_header_recv_start_base, setup, tear_down, 0, NULL},
    {"/buf/len", test_header_recv_start_len, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL}};

/******************************************************************************
 *
 * dqlite__message_header_recv_done
 *
 ******************************************************************************/

/* If the number of words of the message body is zero, an error is returned. */
static MunitResult test_header_recv_done_empty_body(
    const MunitParameter params[],
    void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	err = dqlite__message_header_recv_done(message);

	(void)params;

	munit_assert_int(err, ==, DQLITE_PROTO);
	munit_assert_string_equal(message->error, "empty message body");

	return MUNIT_OK;
}

/* If the number of words of the message body exceeds the hard-coded limit, an
 * error is returned. */
static MunitResult test_header_recv_done_body_too_big(
    const MunitParameter params[],
    void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	uv_buf_t                buf;

	(void)params;

	dqlite__message_header_recv_start(message, &buf);

	/* Set a very high word count */
	buf.base[0] = 0;
	buf.base[1] = 0;
	buf.base[2] = 0;
	buf.base[3] = 127;

	err = dqlite__message_header_recv_done(message);

	munit_assert_int(err, ==, DQLITE_PROTO);
	munit_assert_string_equal(message->error, "message body too large");

	return MUNIT_OK;
}

static MunitTest header_recv_done_tests[] = {
    {"/body/empty",
     test_header_recv_done_empty_body,
     setup,
     tear_down,
     0,
     NULL},
    {"/body/too-large",
     test_header_recv_done_body_too_big,
     setup,
     tear_down,
     0,
     NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__message_body_recv_start
 *
 ******************************************************************************/

/* The message body is 1 word long, the static buffer gets used. */
static MunitResult test_body_recv_start_1(const MunitParameter params[],
                                          void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	uv_buf_t                buf;

	(void)params;

	message->words = 1;

	err = dqlite__message_body_recv_start(message, &buf);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_equal(buf.base, message->body1);
	munit_assert_int(buf.len, ==, 8);

	return MUNIT_OK;
}

/* The message body is 513 words long, and the dynamic buffer gets allocated. */
static MunitResult test_body_recv_start_513(const MunitParameter params[],
                                            void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	uv_buf_t                buf;

	(void)params;

	message->words = 513;

	err = dqlite__message_body_recv_start(message, &buf);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_equal(buf.base, message->body2.base);
	munit_assert_int(buf.len, ==, message->body2.len);
	munit_assert_int(buf.len, ==, 4104);

	return MUNIT_OK;
}

static MunitTest body_recv_start_tests[] = {
    {"/1-word", test_body_recv_start_1, setup, tear_down, 0, NULL},
    {"/513-words", test_body_recv_start_513, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL}};

/******************************************************************************
 *
 * dqlite__message_body_get
 *
 ******************************************************************************/

/* Attempting to read a string when the read cursor is not at word boundary
 * results in an error. */
static MunitResult test_body_get_text_misaligned(const MunitParameter params[],
                                                 void *               data) {
	struct dqlite__message *message = data;

	char    buf[8] = {0, 0, 'h', 'i', 0, 0, 0, 0};
	uint8_t n;
	text_t  text;
	int     err;

	(void)params;

	message->words = 1;
	memcpy(message->body1, buf, 8);

	err = dqlite__message_body_get_uint8(message, &n);
	munit_assert_int(err, ==, 0);

	err = dqlite__message_body_get_text(message, &text);
	munit_assert_int(err, ==, DQLITE_PARSE);

	munit_assert_string_equal(message->error, "misaligned read");

	return MUNIT_OK;
}

/* If no terminating null byte is found within the message body, an error is
 * returned. */
static MunitResult test_body_get_text_not_found(const MunitParameter params[],
                                                void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	text_t                  text;
	char buf[8] = {255, 255, 255, 255, 255, 255, 255, 255};

	(void)params;

	message->words = 1;
	memcpy(message->body1, buf, 8);

	err = dqlite__message_body_get_text(message, &text);

	munit_assert_int(err, ==, DQLITE_PARSE);

	munit_assert_string_equal(message->error, "no string found");

	return MUNIT_OK;
}

/* Read one string. */
static MunitResult test_body_get_text_one_string(const MunitParameter params[],
                                                 void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	text_t                  text;
	char                    buf[8] = {'h', 'e', 'l', 'l', 'o', '!', '!', 0};

	(void)params;

	message->words = 1;
	memcpy(message->body1, buf, 8);

	err = dqlite__message_body_get_text(message, &text);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_string_equal(text, "hello!!");

	return MUNIT_OK;
}

/* Read two strings. */
static MunitResult test_body_get_text_two_strings(const MunitParameter params[],
                                                  void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	text_t                  text;
	char                    buf[16] = {
            'h', 'e', 'l', 'l', 'o', 0, 0, 0, 'w', 'o', 'r', 'l', 'd', 0, 0, 0};

	(void)params;

	message->words = 2;
	memcpy(message->body1, buf, 16);

	err = dqlite__message_body_get_text(message, &text);
	munit_assert_int(err, ==, 0);
	munit_assert_string_equal(text, "hello");

	err = dqlite__message_body_get_text(message, &text);
	munit_assert_int(err, ==, DQLITE_EOM);
	munit_assert_string_equal(text, "world");

	return MUNIT_OK;
}

/* Read a string from a message that uses the dynamic message body buffer. */
static MunitResult test_body_get_text_from_dyn_buf(
    const MunitParameter params[],
    void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	text_t                  text;
	uv_buf_t                buf;

	(void)params;

	message->words = 513;

	err = dqlite__message_body_recv_start(message, &buf);
	munit_assert_int(err, ==, 0);

	strcpy(buf.base, "hello");

	err = dqlite__message_body_get_text(message, &text);
	munit_assert_int(err, ==, 0);

	munit_assert_string_equal(text, "hello");

	return MUNIT_OK;
}

/* Read four uint8 values. */
static MunitResult test_body_get_uint8_four_values(
    const MunitParameter params[],
    void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	uint8_t                 buf;
	uint8_t                 value;

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

	err = dqlite__message_body_get_uint8(message, &value);
	munit_assert_int(err, ==, 0);

	munit_assert_int(value, ==, 12);

	err = dqlite__message_body_get_uint8(message, &value);
	munit_assert_int(err, ==, 0);

	munit_assert_int(value, ==, 77);

	err = dqlite__message_body_get_uint8(message, &value);
	munit_assert_int(err, ==, 0);

	munit_assert_int(value, ==, 128);

	err = dqlite__message_body_get_uint8(message, &value);
	munit_assert_int(err, ==, 0);

	munit_assert_int(value, ==, 255);

	return MUNIT_OK;
}

/* Trying to read a uint8 value past the end of the message body results in an
 * error. */
static MunitResult test_body_get_uint8_overflow(const MunitParameter params[],
                                                void *               data) {
	struct dqlite__message *message = data;

	int     i;
	uint8_t value;
	int     err;

	(void)params;

	message->words = 1;

	for (i = 0; i < 7; i++) {
		err = dqlite__message_body_get_uint8(message, &value);
		munit_assert_int(err, ==, 0);
	}

	err = dqlite__message_body_get_uint8(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	err = dqlite__message_body_get_uint8(message, &value);
	munit_assert_int(err, ==, DQLITE_OVERFLOW);

	return MUNIT_OK;
}

/* Read two uint32 values. */
static MunitResult test_body_get_uint32_two_values(
    const MunitParameter params[],
    void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	uint32_t                buf;
	uint32_t                value;

	(void)params;

	message->words = 1;

	buf = dqlite__flip32(12);
	memcpy(message->body1, &buf, 4);

	buf = dqlite__flip32(77);
	memcpy(message->body1 + 4, &buf, 4);

	err = dqlite__message_body_get_uint32(message, &value);
	munit_assert_int(err, ==, 0);

	munit_assert_int(value, ==, 12);

	err = dqlite__message_body_get_uint32(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_int(value, ==, 77);

	return MUNIT_OK;
}

/* Trying to read a uint32 when the read cursor is not 4-byte aligned results in
 * an error. */
static MunitResult test_body_get_uint32_misaligned(
    const MunitParameter params[],
    void *               data) {
	struct dqlite__message *message = data;

	uint8_t  value1;
	uint32_t value2;
	int      err;

	(void)params;

	message->words = 1;

	err = dqlite__message_body_get_uint8(message, &value1);
	munit_assert_int(err, ==, 0);

	err = dqlite__message_body_get_uint32(message, &value2);
	munit_assert_int(err, ==, DQLITE_PARSE);

	munit_assert_string_equal(message->error, "misaligned read");

	return MUNIT_OK;
}

/* Trying to read a uint32 value past the end of the message body results in an
 * error. */
static MunitResult test_body_get_uint32_overflow(const MunitParameter params[],
                                                 void *               data) {
	struct dqlite__message *message = data;

	uint32_t value;
	int      err;

	(void)params;

	message->words = 1;

	err = dqlite__message_body_get_uint32(message, &value);
	munit_assert_int(err, ==, 0);

	err = dqlite__message_body_get_uint32(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	err = dqlite__message_body_get_uint32(message, &value);
	munit_assert_int(err, ==, DQLITE_OVERFLOW);

	return MUNIT_OK;
}

/* Read one uint64 value. */
static MunitResult test_body_get_uint64_one_value(const MunitParameter params[],
                                                  void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	uint64_t                buf;
	uint64_t                value;

	(void)params;

	message->words = 1;

	buf = dqlite__flip64(123456789);
	memcpy(message->body1, &buf, 8);

	err = dqlite__message_body_get_uint64(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_int(value, ==, 123456789);

	return MUNIT_OK;
}

/* Read two uint64 values. */
static MunitResult test_body_get_uint64_two_values(
    const MunitParameter params[],
    void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	uint64_t                buf;
	uint64_t                value;

	(void)params;

	message->words = 2;

	buf = dqlite__flip64(12);
	memcpy(message->body1, &buf, 8);

	buf = dqlite__flip64(77);
	memcpy(message->body1 + 8, &buf, 8);

	err = dqlite__message_body_get_uint64(message, &value);
	munit_assert_int(err, ==, 0);

	munit_assert_int(value, ==, 12);

	err = dqlite__message_body_get_uint64(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_int(value, ==, 77);

	return MUNIT_OK;
}

/* Trying to read a uint64 when the read cursor is not word aligned results in
 * an error. */
static MunitResult test_body_get_uint64_misaligned(
    const MunitParameter params[],
    void *               data) {
	struct dqlite__message *message = data;

	uint8_t  value1;
	uint64_t value2;
	int      err;

	(void)params;

	message->words = 2;

	err = dqlite__message_body_get_uint8(message, &value1);
	munit_assert_int(err, ==, 0);

	err = dqlite__message_body_get_uint64(message, &value2);
	munit_assert_int(err, ==, DQLITE_PARSE);

	munit_assert_string_equal(message->error, "misaligned read");

	return MUNIT_OK;
}

/* Trying to read a uint64 value past the end of the message body results in an
 * error. */
static MunitResult test_body_get_uint64_overflow(const MunitParameter params[],
                                                 void *               data) {
	struct dqlite__message *message = data;

	uint64_t value;
	int      err;

	(void)params;

	message->words = 1;

	err = dqlite__message_body_get_uint64(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	err = dqlite__message_body_get_uint64(message, &value);
	munit_assert_int(err, ==, DQLITE_OVERFLOW);

	return MUNIT_OK;
}

/* Read one int64 value. */
static MunitResult test_body_get_int64_one_value(const MunitParameter params[],
                                                 void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	uint64_t                buf;
	int64_t                 value;

	(void)params;

	message->words = 1;

	buf = dqlite__flip64(123456789);
	memcpy(message->body1, &buf, 8);

	err = dqlite__message_body_get_int64(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_int(value, ==, 123456789);

	return MUNIT_OK;
}

/* Read two int64 values. */
static MunitResult test_body_get_int64_two_values(const MunitParameter params[],
                                                  void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	uint64_t                buf;
	int64_t                 value;

	(void)params;

	message->words = 2;

	buf = dqlite__flip64((uint64_t)(-12));
	memcpy(message->body1, &buf, 8);

	buf = dqlite__flip64(23);
	memcpy(message->body1 + 8, &buf, 8);

	err = dqlite__message_body_get_int64(message, &value);
	munit_assert_int(err, ==, 0);

	munit_assert_int(value, ==, -12);

	err = dqlite__message_body_get_int64(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_int(value, ==, 23);

	return MUNIT_OK;
}

/* Read a double value. */
static MunitResult test_body_get_double_one_value(const MunitParameter params[],
                                                  void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	uint64_t *              buf;
	double                  pi = 3.1415926535;
	double                  value;

	(void)params;

	message->words = 1;

	buf  = (uint64_t *)(&pi);
	*buf = dqlite__flip64(*buf);
	memcpy(message->body1, buf, 8);

	err = dqlite__message_body_get_double(message, &value);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_double(value, ==, 3.1415926535);

	return MUNIT_OK;
}

static MunitResult test_body_get_servers_one(const MunitParameter params[],
                                             void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	servers_t               servers;
	uv_buf_t                buf;

	(void)params;

	message->words = 3;

	err = dqlite__message_body_recv_start(message, &buf);
	munit_assert_int(err, ==, 0);

	*(uint64_t *)(buf.base) = dqlite__flip64(1);
	strcpy(buf.base + 8, "1.2.3.4:666");

	err = dqlite__message_body_get_servers(message, &servers);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_int(servers[0].id, ==, 1);
	munit_assert_string_equal(servers[0].address, "1.2.3.4:666");

	munit_assert_int(servers[1].id, ==, 0);
	munit_assert_ptr_equal(servers[1].address, NULL);

	sqlite3_free(servers);

	return MUNIT_OK;
}

static MunitResult test_body_get_servers_two(const MunitParameter params[],
                                             void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	servers_t               servers;
	uv_buf_t                buf;

	(void)params;

	message->words = 6;

	err = dqlite__message_body_recv_start(message, &buf);
	munit_assert_int(err, ==, 0);

	*(uint64_t *)(buf.base) = dqlite__flip64(1);
	strcpy(buf.base + 8, "1.2.3.4:666");

	*(uint64_t *)(buf.base + 24) = dqlite__flip64(2);
	strcpy(buf.base + 32, "5.6.7.8:666");

	err = dqlite__message_body_get_servers(message, &servers);
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

static MunitTest body_get_tests[] = {
    {"_text/misaligned",
     test_body_get_text_misaligned,
     setup,
     tear_down,
     0,
     NULL},
    {"_text/not-found",
     test_body_get_text_not_found,
     setup,
     tear_down,
     0,
     NULL},
    {"_text/one-string",
     test_body_get_text_one_string,
     setup,
     tear_down,
     0,
     NULL},
    {"_text/two-strings",
     test_body_get_text_two_strings,
     setup,
     tear_down,
     0,
     NULL},
    {"_text/dyn-buf",
     test_body_get_text_from_dyn_buf,
     setup,
     tear_down,
     0,
     NULL},
    {"_uint8/four", test_body_get_uint8_four_values, setup, tear_down, 0, NULL},
    {"_uint8/overflow",
     test_body_get_uint8_overflow,
     setup,
     tear_down,
     0,
     NULL},
    {"_uint32/two", test_body_get_uint32_two_values, setup, tear_down, 0, NULL},
    {"_uint32/misaligned",
     test_body_get_uint32_misaligned,
     setup,
     tear_down,
     0,
     NULL},
    {"_uint32/overflow",
     test_body_get_uint32_overflow,
     setup,
     tear_down,
     0,
     NULL},
    {"_uint64/one", test_body_get_uint64_one_value, setup, tear_down, 0, NULL},
    {"_uint64/two", test_body_get_uint64_two_values, setup, tear_down, 0, NULL},
    {"_uint64/misaligned",
     test_body_get_uint64_misaligned,
     setup,
     tear_down,
     0,
     NULL},
    {"_uint64/overflow",
     test_body_get_uint64_overflow,
     setup,
     tear_down,
     0,
     NULL},
    {"_int64/one-value",
     test_body_get_int64_one_value,
     setup,
     tear_down,
     0,
     NULL},
    {"_int64/two-values",
     test_body_get_int64_two_values,
     setup,
     tear_down,
     0,
     NULL},
    {"_double/one-value",
     test_body_get_double_one_value,
     setup,
     tear_down,
     0,
     NULL},
    {"_servers/one", test_body_get_servers_one, setup, tear_down, 0, NULL},
    {"_servers/two", test_body_get_servers_two, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL}};

/******************************************************************************
 *
 * dqlite__message_header_put
 *
 ******************************************************************************/

/* Set the type of a message. */
static MunitResult test_header_put_type(const MunitParameter params[],
                                        void *               data) {
	struct dqlite__message *message = data;

	(void)params;

	dqlite__message_header_put(message, 123, 0);
	munit_assert_int(message->type, ==, 123);

	return MUNIT_OK;
}

/* Set the message flags. */
static MunitResult test_header_put_flags(const MunitParameter params[],
                                         void *               data) {
	struct dqlite__message *message = data;

	(void)params;

	dqlite__message_header_put(message, 0, 255);
	munit_assert_int(message->flags, ==, 255);

	return MUNIT_OK;
}

static MunitTest header_put_tests[] = {
    {"/type", test_header_put_type, setup, tear_down, 0, NULL},
    {"/flags", test_header_put_flags, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL}};

/******************************************************************************
 *
 * dqlite__message_body_put
 *
 ******************************************************************************/

/* Trying to write a string when the write cursor is not at word boundary
 * results in an error. */
static MunitResult test_body_put_text_misaligned(const MunitParameter params[],
                                                 void *               data) {
	struct dqlite__message *message = data;
	int                     err;

	(void)params;

	err = dqlite__message_body_put_uint8(message, 123);
	munit_assert_int(err, ==, 0);

	err = dqlite__message_body_put_text(message, "hello");
	munit_assert_int(err, ==, DQLITE_PROTO);

	munit_assert_string_equal(message->error, "misaligned write");

	return MUNIT_OK;
}

static MunitResult test_body_put_text_one(const MunitParameter params[],
                                          void *               data) {
	struct dqlite__message *message = data;
	int                     err;

	(void)params;

	err = dqlite__message_body_put_text(message, "hello");

	munit_assert_int(err, ==, 0);
	munit_assert_int(message->offset1, ==, 8);

	munit_assert_string_equal(message->body1, "hello");

	/* Padding */
	munit_assert_int(message->body1[6], ==, 0);
	munit_assert_int(message->body1[7], ==, 0);

	return MUNIT_OK;
}

static MunitResult test_body_put_text_one_no_pad(const MunitParameter params[],
                                                 void *               data) {
	struct dqlite__message *message = data;
	int                     err;

	(void)params;

	err = dqlite__message_body_put_text(message, "hello!!");

	munit_assert_int(err, ==, 0);
	munit_assert_int(message->offset1, ==, 8);

	munit_assert_string_equal(message->body1, "hello!!");

	return MUNIT_OK;
}

static MunitResult test_body_put_text_two(const MunitParameter params[],
                                          void *               data) {
	struct dqlite__message *message = data;
	int                     err;

	(void)params;

	err = dqlite__message_body_put_text(message, "hello");
	munit_assert_int(err, ==, 0);

	err = dqlite__message_body_put_text(message, "world");
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
static MunitResult test_body_put_text_body2(const MunitParameter params[],
                                            void *               data) {
	struct dqlite__message *message = data;
	int                     err;

	(void)params;

	message->offset1 = 4088;

	err = dqlite__message_body_put_text(message, "hello world");
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 4088);
	munit_assert_int(message->offset2, ==, 16);

	munit_assert_string_equal(message->body2.base, "hello world");

	return MUNIT_OK;
}

static MunitResult test_body_put_uint8_four(const MunitParameter params[],
                                            void *               data) {
	struct dqlite__message *message = data;
	int                     err;

	(void)params;

	err = dqlite__message_body_put_uint8(message, 25);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 1);

	err = dqlite__message_body_put_uint8(message, 50);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 2);

	err = dqlite__message_body_put_uint8(message, 100);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 3);

	err = dqlite__message_body_put_uint8(message, 200);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 4);

	munit_assert_int(*(uint8_t *)(message->body1), ==, 25);
	munit_assert_int(*(uint8_t *)(message->body1 + 1), ==, 50);
	munit_assert_int(*(uint8_t *)(message->body1 + 2), ==, 100);
	munit_assert_int(*(uint8_t *)(message->body1 + 3), ==, 200);

	return MUNIT_OK;
}

static MunitResult test_body_put_uint32_two(const MunitParameter params[],
                                            void *               data) {
	struct dqlite__message *message = data;
	int                     err;

	(void)params;

	err = dqlite__message_body_put_uint32(message, 99);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 4);

	err = dqlite__message_body_put_uint32(message, 66);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 8);

	munit_assert_int(dqlite__flip32(*(uint32_t *)(message->body1)), ==, 99);
	munit_assert_int(
	    dqlite__flip32(*(uint32_t *)(message->body1 + 4)), ==, 66);

	return MUNIT_OK;
}

static MunitResult test_body_put_int64_one(const MunitParameter params[],
                                           void *               data) {
	struct dqlite__message *message = data;
	int                     err;

	(void)params;

	err = dqlite__message_body_put_int64(message, -12);

	munit_assert_int(err, ==, 0);
	munit_assert_int(message->offset1, ==, 8);

	munit_assert_int(
	    (int64_t)dqlite__flip64(*(uint64_t *)(message->body1)), ==, -12);

	return MUNIT_OK;
}

static MunitResult test_body_put_uint64_one(const MunitParameter params[],
                                            void *               data) {
	struct dqlite__message *message = data;
	int                     err;

	(void)params;

	err = dqlite__message_body_put_uint64(message, 99);

	munit_assert_int(err, ==, 0);
	munit_assert_int(message->offset1, ==, 8);

	munit_assert_int(dqlite__flip64(*(uint64_t *)(message->body1)), ==, 99);

	return MUNIT_OK;
}

static MunitResult test_body_put_double_one(const MunitParameter params[],
                                            void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	uint64_t                buf;
	double                  value;

	(void)params;

	err = dqlite__message_body_put_double(message, 3.1415926535);

	munit_assert_int(err, ==, 0);
	munit_assert_int(message->offset1, ==, 8);

	buf = dqlite__flip64(*(uint64_t *)(message->body1));

	memcpy(&value, &buf, sizeof(buf));

	munit_assert_double(value, ==, 3.1415926535);

	return MUNIT_OK;
}

static MunitResult test_body_put_dyn_buf(const MunitParameter params[],
                                         void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	uint64_t                i;

	(void)params;

	for (i = 0; i < 4096 / 8; i++) {
		err = dqlite__message_body_put_uint64(message, i);
		munit_assert_int(err, ==, 0);
	}

	munit_assert_int(message->offset1, ==, 4096);
	munit_assert_int(message->offset2, ==, 0);

	err = dqlite__message_body_put_uint64(message, 666);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset2, ==, 8);

	return MUNIT_OK;
}

static MunitResult test_body_put_servers_one(const MunitParameter params[],
                                             void *               data) {
	struct dqlite__message *message   = data;
	dqlite_server_info      servers[] = {
            {1, "1.2.3.4:666"},
            {0, NULL},
        };
	int         err;
	uint64_t    id;
	const char *address;

	(void)params;

	err = dqlite__message_body_put_servers(message, servers);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 24);

	id = dqlite__flip64(*(uint64_t *)(message->body1));
	munit_assert_int(id, ==, 1);

	address = (const char *)(message->body1 + 8);
	munit_assert_string_equal(address, "1.2.3.4:666");

	return MUNIT_OK;
}

static MunitTest body_put_tests[] = {
    {"_text/misaligned",
     test_body_put_text_misaligned,
     setup,
     tear_down,
     0,
     NULL},
    {"_text/one", test_body_put_text_one, setup, tear_down, 0, NULL},
    {"_text/one-no-pad",
     test_body_put_text_one_no_pad,
     setup,
     tear_down,
     0,
     NULL},
    {"_text/two", test_body_put_text_two, setup, tear_down, 0, NULL},
    {"_text/body2", test_body_put_text_body2, setup, tear_down, 0, NULL},
    {"_uint8/four", test_body_put_uint8_four, setup, tear_down, 0, NULL},
    {"_uint32/two", test_body_put_uint32_two, setup, tear_down, 0, NULL},
    {"_int64/one", test_body_put_int64_one, setup, tear_down, 0, NULL},
    {"_uint64/one", test_body_put_uint64_one, setup, tear_down, 0, NULL},
    {"_double/one", test_body_put_double_one, setup, tear_down, 0, NULL},
    {"_uint64/dyn-buf", test_body_put_dyn_buf, setup, tear_down, 0, NULL},
    {"_servers/one", test_body_put_servers_one, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL}};

/******************************************************************************
 *
 * dqlite__message_send_start
 *
 ******************************************************************************/

static MunitResult test_send_start_no_dyn_buf(const MunitParameter params[],
                                              void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	uv_buf_t                bufs[3];
	struct dqlite__message  message2;
	uv_buf_t                buf;
	uint64_t                value;
	text_t                  text;

	(void)params;

	dqlite__message_header_put(message, 9, 123);

	err = dqlite__message_body_put_uint64(message, 78);
	munit_assert_int(err, ==, 0);

	err = dqlite__message_body_put_text(message, "hello");
	munit_assert_int(err, ==, 0);

	dqlite__message_send_start(message, bufs);

	munit_assert_ptr_equal(bufs[0].base, message);
	munit_assert_int(bufs[0].len, ==, 8);

	munit_assert_ptr_equal(bufs[1].base, message->body1);
	munit_assert_int(bufs[1].len, ==, 16);

	munit_assert_ptr_equal(bufs[2].base, NULL);
	munit_assert_int(bufs[2].len, ==, 0);

	dqlite__message_init(&message2);

	dqlite__message_header_recv_start(&message2, &buf);
	memcpy(buf.base, bufs[0].base, bufs[0].len);

	err = dqlite__message_header_recv_done(&message2);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message2.type, ==, 9);
	munit_assert_int(message2.flags, ==, 123);

	err = dqlite__message_body_recv_start(&message2, &buf);
	munit_assert_int(err, ==, 0);

	memcpy(buf.base, bufs[1].base, bufs[1].len);

	err = dqlite__message_body_get_uint64(&message2, &value);
	munit_assert_int(err, ==, 0);

	munit_assert_int(value, ==, 78);

	err = dqlite__message_body_get_text(&message2, &text);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_string_equal(text, "hello");

	dqlite__message_recv_reset(&message2);
	dqlite__message_send_reset(message);

	dqlite__message_close(&message2);

	return MUNIT_OK;
}

static MunitResult test_send_start_dyn_buf(const MunitParameter params[],
                                           void *               data) {
	struct dqlite__message *message = data;
	int                     err;
	uint64_t                i;
	uv_buf_t                bufs[3];
	struct dqlite__message  message2;
	uv_buf_t                buf;
	uint64_t                value;
	text_t                  text;

	(void)params;

	dqlite__message_header_put(message, 9, 123);

	for (i = 0; i < 4088 / 8; i++) {
		err = dqlite__message_body_put_uint64(message, i);
		munit_assert_int(err, ==, 0);
	}
	munit_assert_int(message->offset1, ==, 4088);

	err = dqlite__message_body_put_text(message, "hello world");
	munit_assert_int(err, ==, 0);

	munit_assert_int(message->offset1, ==, 4088);
	munit_assert_int(message->offset2, ==, 16);

	dqlite__message_send_start(message, bufs);

	munit_assert_ptr_equal(bufs[0].base, message);
	munit_assert_int(bufs[0].len, ==, 8);

	munit_assert_ptr_equal(bufs[1].base, message->body1);
	munit_assert_int(bufs[1].len, ==, 4088);

	munit_assert_ptr_not_equal(bufs[2].base, NULL);
	munit_assert_int(bufs[2].len, ==, 16);

	dqlite__message_init(&message2);

	dqlite__message_header_recv_start(&message2, &buf);
	memcpy(buf.base, bufs[0].base, bufs[0].len);

	err = dqlite__message_header_recv_done(&message2);
	munit_assert_int(err, ==, 0);

	munit_assert_int(message2.type, ==, 9);
	munit_assert_int(message2.flags, ==, 123);

	err = dqlite__message_body_recv_start(&message2, &buf);
	munit_assert_int(err, ==, 0);

	memcpy(buf.base, bufs[1].base, bufs[1].len);
	memcpy(buf.base + bufs[1].len, bufs[2].base, bufs[2].len);

	for (i = 0; i < 4088 / 8; i++) {
		err = dqlite__message_body_get_uint64(&message2, &value);
		munit_assert_int(err, ==, 0);
		munit_assert_int(value, ==, i);
	}

	err = dqlite__message_body_get_text(&message2, &text);
	munit_assert_int(err, ==, DQLITE_EOM);

	munit_assert_string_equal(text, "hello world");

	dqlite__message_recv_reset(&message2);
	dqlite__message_send_reset(message);

	dqlite__message_close(&message2);

	return MUNIT_OK;
}

static MunitTest send_start_tests[] = {
    {"/no-dyn-buf", test_send_start_no_dyn_buf, setup, tear_down, 0, NULL},
    {"/dyn-buf", test_send_start_dyn_buf, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__message suite
 *
 ******************************************************************************/

MunitSuite dqlite__message_suites[] = {
    {"_header_recv_start", header_recv_start_tests, NULL, 1, 0},
    {"_header_recv_done", header_recv_done_tests, NULL, 1, 0},
    {"_body_recv_start", body_recv_start_tests, NULL, 1, 0},
    {"_body_get", body_get_tests, NULL, 1, 0},
    {"_header_put", header_put_tests, NULL, 1, 0},
    {"_body_put", body_put_tests, NULL, 1, 0},
    {"_send_start", send_start_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
