#ifndef DQLITE_TEST_CONN_H
#define DQLITE_TEST_CONN_H

#include <CUnit/CUnit.h>

void test_dqlite__conn_setup();
void test_dqlite__conn_teardown();

void test_dqlite__conn_abort_immediately();
void test_dqlite__conn_abort_during_handshake();
void test_dqlite__conn_abort_after_handshake();
void test_dqlite__conn_abort_during_header();
void test_dqlite__conn_abort_after_header();
void test_dqlite__conn_abort_during_body();
void test_dqlite__conn_abort_after_body();
void test_dqlite__conn_abort_after_heartbeat_timeout();

CU_TestInfo dqlite__conn_abort_suite[] = {
	{"immediately",             test_dqlite__conn_abort_immediately},
	{"during handshake",        test_dqlite__conn_abort_during_handshake},
	{"after handshake",         test_dqlite__conn_abort_after_handshake},
	{"during header",           test_dqlite__conn_abort_during_header},
	{"after header",            test_dqlite__conn_abort_after_header},
	{"during body",             test_dqlite__conn_abort_during_body},
	{"after body",              test_dqlite__conn_abort_after_body},
	{"after heartbeat timeout", test_dqlite__conn_abort_after_heartbeat_timeout},
	CU_TEST_INFO_NULL,
};

void test_dqlite__conn_read_cb_unknown_protocol();
void test_dqlite__conn_read_cb_empty_body();
void test_dqlite__conn_read_cb_body_too_large();

CU_TestInfo dqlite__conn_read_cb_suite[] = {
	{"unknown protocol", test_dqlite__conn_read_cb_unknown_protocol},
	{"empty body",       test_dqlite__conn_read_cb_empty_body},
	{"body too large",   test_dqlite__conn_read_cb_body_too_large},
	CU_TEST_INFO_NULL,
};

CU_TestInfo dqlite__conn_write_suite[] = {
	CU_TEST_INFO_NULL,
};

CU_SuiteInfo dqlite__conn_suites[] = {
	{
		"dqlite__conn abort",
		NULL, NULL,
		test_dqlite__conn_setup, test_dqlite__conn_teardown,
		dqlite__conn_abort_suite
	},
	{
		"dqlite__conn_read_cb",
		NULL, NULL,
		test_dqlite__conn_setup, test_dqlite__conn_teardown,
		dqlite__conn_read_cb_suite
	},
	/* { */
	/* 	"dqlite__conn_write", */
	/* 	NULL, NULL, */
	/* 	test_dqlite__conn_setup, test_dqlite__conn_teardown, */
	/* 	dqlite__conn_write_suite */
	/* }, */
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_TEST_CONN_H */
