#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "../include/dqlite.h"

#include "munit.h"

extern MunitSuite dqlite__conn_suites[];
extern MunitSuite dqlite__db_suites[];
extern MunitSuite dqlite__error_suites[];
extern MunitSuite dqlite__gateway_suites[];
extern MunitSuite dqlite__integration_suites[];
extern MunitSuite dqlite__message_suites[];
extern MunitSuite dqlite__queue_suites[];
extern MunitSuite dqlite__registry_suites[];
extern MunitSuite dqlite__request_suites[];
extern MunitSuite dqlite__response_suites[];
extern MunitSuite dqlite__schema_suites[];
extern MunitSuite dqlite__server_suites[];
extern MunitSuite dqlite__stmt_suites[];
extern MunitSuite dqlite__vfs_suites[];

static MunitSuite dqlite__test_suites[] = {
    {"dqlite__conn", NULL, dqlite__conn_suites, 1, MUNIT_SUITE_OPTION_NONE},
    {"dqlite__db", NULL, dqlite__db_suites, 1, MUNIT_SUITE_OPTION_NONE},
    {"dqlite__error", NULL, dqlite__error_suites, 1, MUNIT_SUITE_OPTION_NONE},
    {"dqlite__gateway", NULL, dqlite__gateway_suites, 1, MUNIT_SUITE_OPTION_NONE},
    {"dqlite__integration",
     NULL,
     dqlite__integration_suites,
     1,
     MUNIT_SUITE_OPTION_NONE},
    {"dqlite__message", NULL, dqlite__message_suites, 1, MUNIT_SUITE_OPTION_NONE},
    {"dqlite__queue", NULL, dqlite__queue_suites, 1, MUNIT_SUITE_OPTION_NONE},
    {"dqlite__registry", NULL, dqlite__registry_suites, 1, MUNIT_SUITE_OPTION_NONE},
    {"dqlite__request", NULL, dqlite__request_suites, 1, MUNIT_SUITE_OPTION_NONE},
    {"dqlite__response", NULL, dqlite__response_suites, 1, MUNIT_SUITE_OPTION_NONE},
    {"dqlite__schema", NULL, dqlite__schema_suites, 1, MUNIT_SUITE_OPTION_NONE},
    {"dqlite__server", NULL, dqlite__server_suites, 1, MUNIT_SUITE_OPTION_NONE},
    {"dqlite__stmt", NULL, dqlite__stmt_suites, 1, MUNIT_SUITE_OPTION_NONE},
    {"dqlite__vfs", NULL, dqlite__vfs_suites, 1, MUNIT_SUITE_OPTION_NONE},
    {NULL, NULL, NULL, 0, MUNIT_SUITE_OPTION_NONE}};

static MunitSuite dqlite__test_suite = {
    (char *)"", NULL, dqlite__test_suites, 1, MUNIT_SUITE_OPTION_NONE};

/* Test runner executable */
int main(int argc, char *argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
	return munit_suite_main(&dqlite__test_suite, (void *)"µnit", argc, argv);
}
