#ifndef DQLITE_TEST_MESSAGE_H
#define DQLITE_TEST_MESSAGE_H

#include <CUnit/CUnit.h>

void test_dqlite__message_setup();
void test_dqlite__message_teardown();

void test_dqlite__message_header_buf_buf();
void test_dqlite__message_header_buf_len();

CU_TestInfo dqlite__message_header_buf_suite[] = {
	{"buf", test_dqlite__message_header_buf_buf},
	{"len", test_dqlite__message_header_buf_len},
	CU_TEST_INFO_NULL,
};

void test_dqlite__message_body_len_0();
void test_dqlite__message_body_len_8();
void test_dqlite__message_body_len_64();
void test_dqlite__message_body_len_1K();
void test_dqlite__message_body_len_1M();

CU_TestInfo dqlite__message_body_len_suite[] = {
	{"0",  test_dqlite__message_body_len_0},
	{"64", test_dqlite__message_body_len_64},
	{"1K", test_dqlite__message_body_len_1K},
	{"1M", test_dqlite__message_body_len_1M},
	CU_TEST_INFO_NULL,
};

void test_dqlite__message_write_text_one_string();
void test_dqlite__message_write_text_two_strings();
void test_dqlite__message_write_text_overflow();

CU_TestInfo dqlite__message_write_suite[] = {
	{"text one string", test_dqlite__message_write_text_one_string},
	{"text two strings", test_dqlite__message_write_text_two_strings},
	{"text overflow",    test_dqlite__message_write_text_overflow},
	CU_TEST_INFO_NULL,
};

void test_dqlite__message_read_text_one_string();
void test_dqlite__message_read_text_two_strings();
void test_dqlite__message_read_text_parse_error();
void test_dqlite__message_read_int64_one_value();
void test_dqlite__message_read_int64_two_values();
void test_dqlite__message_read_uint64_one_value();
void test_dqlite__message_read_uint64_two_values();

CU_TestInfo dqlite__message_read_suite[] = {
	{"text one string", test_dqlite__message_read_text_one_string},
	{"text two strings", test_dqlite__message_read_text_two_strings},
	{"text parse error", test_dqlite__message_read_text_parse_error},
	{"int64 one value",  test_dqlite__message_read_int64_one_value},
	{"int64 two values", test_dqlite__message_read_int64_two_values},
	{"uint64 one value", test_dqlite__message_read_uint64_one_value},
	{"uint64 overflow",  test_dqlite__message_read_uint64_two_values},
	CU_TEST_INFO_NULL,
};

CU_SuiteInfo dqlite__message_suites[] = {
	{
		"dqlite__message_header_buf",
		NULL, NULL,
		test_dqlite__message_setup, test_dqlite__message_teardown,
		dqlite__message_header_buf_suite
	},
	{
		"dqlite__message_size",
		NULL, NULL,
		test_dqlite__message_setup, test_dqlite__message_teardown,
		dqlite__message_body_len_suite
	},
	{
		"dqlite__message_write",
		NULL, NULL,
		test_dqlite__message_setup, test_dqlite__message_teardown,
		dqlite__message_write_suite
	},
	{
		"dqlite__message_read",
		NULL, NULL,
		test_dqlite__message_setup, test_dqlite__message_teardown,
		dqlite__message_read_suite
	},
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_TEST_MESSAGE_H */
