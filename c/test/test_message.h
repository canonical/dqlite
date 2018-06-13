#ifndef DQLITE_TEST_MESSAGE_H
#define DQLITE_TEST_MESSAGE_H

#include <CUnit/CUnit.h>

void test_dqlite__message_setup();
void test_dqlite__message_teardown();

void test_dqlite__message_header_recv_start_base();
void test_dqlite__message_header_recv_start_len();

CU_TestInfo dqlite__message_header_recv_start_suite[] = {
	{"buf", test_dqlite__message_header_recv_start_base},
	{"len", test_dqlite__message_header_recv_start_len},
	CU_TEST_INFO_NULL,
};

void test_dqlite__message_header_recv_done_empty_body();

CU_TestInfo dqlite__message_header_recv_done_suite[] = {
	{"empty body", test_dqlite__message_header_recv_done_empty_body},
	CU_TEST_INFO_NULL,
};

void test_dqlite__message_body_recv_start_1();
void test_dqlite__message_body_recv_start_513();

CU_TestInfo dqlite__message_body_recv_start_suite[] = {
	{"1 word",    test_dqlite__message_body_recv_start_1},
	{"513 words", test_dqlite__message_body_recv_start_513},
	CU_TEST_INFO_NULL,
};

void test_dqlite__message_body_get_text_one_string();
void test_dqlite__message_body_get_text_two_strings();
void test_dqlite__message_body_get_text_parse_error();
void test_dqlite__message_body_get_text_from_dyn_buf();
void test_dqlite__message_body_get_text_list_one_item();
void test_dqlite__message_body_get_text_list_two_items();
void test_dqlite__message_body_get_int64_one_value();
void test_dqlite__message_body_get_int64_two_values();
void test_dqlite__message_body_get_uint64_one_value();
void test_dqlite__message_body_get_uint64_two_values();

CU_TestInfo dqlite__message_body_get_suite[] = {
	{"text one string",     test_dqlite__message_body_get_text_one_string},
	{"text two strings",    test_dqlite__message_body_get_text_two_strings},
	{"text parse error",    test_dqlite__message_body_get_text_parse_error},
	{"text from dyn buf",   test_dqlite__message_body_get_text_from_dyn_buf},
	{"text list one item",  test_dqlite__message_body_get_text_list_one_item},
	{"text list two items", test_dqlite__message_body_get_text_list_two_items},
	{"int64 one value",     test_dqlite__message_body_get_int64_one_value},
	{"int64 two values",    test_dqlite__message_body_get_int64_two_values},
	{"uint64 one value",    test_dqlite__message_body_get_uint64_one_value},
	{"uint64 two value",    test_dqlite__message_body_get_uint64_two_values},
	CU_TEST_INFO_NULL,
};

void test_dqlite__message_header_put_type();
void test_dqlite__message_header_put_flags();

CU_TestInfo dqlite__message_header_put_suite[] = {
	{"type",  test_dqlite__message_header_put_type},
	{"flags", test_dqlite__message_header_put_flags},
	CU_TEST_INFO_NULL,
};

void test_dqlite__message_body_put_text_one();
void test_dqlite__message_body_put_text_one_no_pad();
void test_dqlite__message_body_put_text_two();
void test_dqlite__message_body_put_int64_one();
void test_dqlite__message_body_put_uint64_one();
void test_dqlite__message_body_put_dyn_buf();

CU_TestInfo dqlite__message_body_put_suite[] = {
	{"text one",        test_dqlite__message_body_put_text_one},
	{"text one no pad", test_dqlite__message_body_put_text_one_no_pad},
	{"text two",        test_dqlite__message_body_put_text_two},
	{"int64 one ",      test_dqlite__message_body_put_int64_one},
	{"uint64 one ",     test_dqlite__message_body_put_uint64_one},
	{"dyn buf",         test_dqlite__message_body_put_dyn_buf},
	CU_TEST_INFO_NULL,
};

void test_dqlite__message_send_start_no_dyn_buf();
void test_dqlite__message_send_start_dyn_buf();

CU_TestInfo dqlite__message_send_start_suite[] = {
	{"send no dyn buf", test_dqlite__message_send_start_no_dyn_buf},
	{"send dyn buf",    test_dqlite__message_send_start_dyn_buf},
	CU_TEST_INFO_NULL,
};

CU_SuiteInfo dqlite__message_suites[] = {
	{
		"dqlite__message_header_recv_start",
		NULL, NULL,
		test_dqlite__message_setup, test_dqlite__message_teardown,
		dqlite__message_header_recv_start_suite
	},
	{
		"dqlite__message_header_recv_done",
		NULL, NULL,
		test_dqlite__message_setup, test_dqlite__message_teardown,
		dqlite__message_header_recv_done_suite
	},
	{
		"dqlite__message_body_recv_start",
		NULL, NULL,
		test_dqlite__message_setup, test_dqlite__message_teardown,
		dqlite__message_body_recv_start_suite
	},
	{
		"dqlite__message_body_get",
		NULL, NULL,
		test_dqlite__message_setup, test_dqlite__message_teardown,
		dqlite__message_body_get_suite
	},
	{
		"dqlite__message_header_put",
		NULL, NULL,
		test_dqlite__message_setup, test_dqlite__message_teardown,
		dqlite__message_header_put_suite
	},
	{
		"dqlite__message_body_put",
		NULL, NULL,
		test_dqlite__message_setup, test_dqlite__message_teardown,
		dqlite__message_body_put_suite
	},
	{
		"dqlite__message_send_start",
		NULL, NULL,
		test_dqlite__message_setup, test_dqlite__message_teardown,
		dqlite__message_send_start_suite
	},
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_TEST_MESSAGE_H */
