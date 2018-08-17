#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "../include/dqlite.h"

#include "munit.h"

extern MunitSuite dqlite__conn_suites[];
extern MunitSuite dqlite__db_suites[];
extern MunitSuite dqlite__error_suites[];
extern MunitSuite dqlite__file_suites[];
extern MunitSuite dqlite__format_suites[];
extern MunitSuite dqlite__gateway_suites[];
extern MunitSuite dqlite__integration_suites[];
extern MunitSuite dqlite__message_suites[];
extern MunitSuite dqlite__queue_suites[];
#ifdef DQLITE_EXPERIMENTAL
extern MunitSuite dqlite__replication_suites[];
#endif /* DQLITE_EXPERIMENTAL */
extern MunitSuite dqlite__registry_suites[];
extern MunitSuite dqlite__request_suites[];
extern MunitSuite dqlite__response_suites[];
extern MunitSuite dqlite__schema_suites[];
extern MunitSuite dqlite__server_suites[];
extern MunitSuite dqlite__stmt_suites[];
extern MunitSuite dqlite__uv_suites[];
extern MunitSuite dqlite__vfs_suites[];

static MunitSuite dqlite__test_suites[] = {
    {"dqlite__conn", NULL, dqlite__conn_suites, 1, 0},
    {"dqlite__db", NULL, dqlite__db_suites, 1, 0},
    {"dqlite__error", NULL, dqlite__error_suites, 1, 0},
    {"dqlite__file", NULL, dqlite__file_suites, 1, 0},
    {"dqlite__format", NULL, dqlite__format_suites, 1, 0},
    {"dqlite__gateway", NULL, dqlite__gateway_suites, 1, 0},
    {"dqlite__integration", NULL, dqlite__integration_suites, 1, 0},
    {"dqlite__message", NULL, dqlite__message_suites, 1, 0},
    {"dqlite__queue", NULL, dqlite__queue_suites, 1, 0},
    {"dqlite__registry", NULL, dqlite__registry_suites, 1, 0},
#ifdef DQLITE_EXPERIMENTAL
    {"dqlite__replication", NULL, dqlite__replication_suites, 1, 0},
#endif /* DQLITE_EXPERIMENTAL */
    {"dqlite__request", NULL, dqlite__request_suites, 1, 0},
    {"dqlite__response", NULL, dqlite__response_suites, 1, 0},
    {"dqlite__schema", NULL, dqlite__schema_suites, 1, 0},
    {"dqlite__server", NULL, dqlite__server_suites, 1, 0},
    {"dqlite__stmt", NULL, dqlite__stmt_suites, 1, 0},
    {"dqlite__uv", NULL, dqlite__uv_suites, 1, 0},
    {"dqlite__vfs", NULL, dqlite__vfs_suites, 1, 0},
    {NULL, NULL, NULL, 0, 0}};

static MunitSuite dqlite__test_suite = {(char *)"",
                                        NULL,
                                        dqlite__test_suites,
                                        1,
                                        0};

/* Test runner executable */
int main(int argc, char *argv[MUNIT_ARRAY_PARAM(argc + 1)])
{
	return munit_suite_main(
	    &dqlite__test_suite, (void *)"µnit", argc, argv);
}
