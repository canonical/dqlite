#ifndef DQLITE_TEST_SCHEMA_H
#define DQLITE_TEST_SCHEMA_H

#include <CUnit/CUnit.h>

void test_dqlite__schema_setup();
void test_dqlite__schema_teardown();

void test_dqlite__schema_encoder_encode_two_uint64();
void test_dqlite__schema_encoder_encode_uint64_and_text();
void test_dqlite__schema_encoder_encode_unknown_type();

CU_TestInfo dqlite__schema_encoder_encode_suite[] = {
	{"two uint64 fields",      test_dqlite__schema_encoder_encode_two_uint64},
	{"uint64 and text fields", test_dqlite__schema_encoder_encode_uint64_and_text},
	{"unknown type",           test_dqlite__schema_encoder_encode_unknown_type},
	CU_TEST_INFO_NULL,
};

void test_dqlite__schema_decoder_decode_invalid_text();
void test_dqlite__schema_decoder_decode_unknown_type();
void test_dqlite__schema_decoder_decode_two_uint64();

CU_TestInfo dqlite__schema_decoder_decode_suite[] = {
	{"invalid text",      test_dqlite__schema_decoder_decode_invalid_text},
	{"unknown type",      test_dqlite__schema_decoder_decode_unknown_type},
	{"two uint64 fields", test_dqlite__schema_decoder_decode_two_uint64},
	CU_TEST_INFO_NULL,
};

CU_SuiteInfo dqlite__schema_suites[] = {
	{
		"dqlite__schema_encoder_encode",
		NULL, NULL,
		test_dqlite__schema_setup, test_dqlite__schema_teardown,
		dqlite__schema_encoder_encode_suite
	},
	{
		"dqlite__schema_decoder_decode",
		NULL, NULL,
		test_dqlite__schema_setup, test_dqlite__schema_teardown,
		dqlite__schema_decoder_decode_suite
	},
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_TEST_SCHEMA_H */
