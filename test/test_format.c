#include <stdint.h>

#include <sqlite3.h>

#include "../src/format.h"

#include "leak.h"
#include "munit.h"

/* Parse the page size stored in a database file header. */
static MunitResult test_get_page_size_db(const MunitParameter params[], void *data) {
	uint8_t      buf[DQLITE__FORMAT_DB_HDR_SIZE];
	unsigned int page_size;
	int          rc;

	(void)params;
	(void)data;

	buf[16] = 16;
	buf[17] = 0;

	rc = dqlite__format_get_page_size(DQLITE__FORMAT_DB, buf, &page_size);
	munit_assert_int(rc, ==, SQLITE_OK);

	munit_assert_int(page_size, ==, 4096);

	return MUNIT_OK;
}

/* Parse the page size stored in a WAL file header. */
static MunitResult test_get_page_size_wal(const MunitParameter params[],
                                          void *               data) {
	uint8_t      buf[DQLITE__FORMAT_WAL_HDR_SIZE];
	unsigned int page_size;
	int          rc;

	(void)params;
	(void)data;

	buf[8]  = 0;
	buf[9]  = 0;
	buf[10] = 16;
	buf[11] = 0;

	rc = dqlite__format_get_page_size(DQLITE__FORMAT_WAL, buf, &page_size);
	munit_assert_int(rc, ==, SQLITE_OK);

	munit_assert_int(page_size, ==, 4096);

	return MUNIT_OK;
}

/* If the stored value is 1, the resulting page size is the maximum one. */
static MunitResult test_get_page_size_max(const MunitParameter params[],
                                          void *               data) {
	uint8_t      buf[DQLITE__FORMAT_DB_HDR_SIZE];
	unsigned int page_size;
	int          rc;

	(void)params;
	(void)data;

	buf[16] = 0;
	buf[17] = 1;

	rc = dqlite__format_get_page_size(DQLITE__FORMAT_DB, buf, &page_size);
	munit_assert_int(rc, ==, SQLITE_OK);

	munit_assert_int(page_size, ==, 65536);

	return MUNIT_OK;
}

/* If the stored value is smaller than the minimum size, an error is returned. */
static MunitResult test_get_page_size_too_small(const MunitParameter params[],
                                                void *               data) {
	uint8_t      buf[DQLITE__FORMAT_DB_HDR_SIZE];
	unsigned int page_size;
	int          rc;

	(void)params;
	(void)data;

	buf[16] = 0;
	buf[17] = 128;

	rc = dqlite__format_get_page_size(DQLITE__FORMAT_DB, buf, &page_size);
	munit_assert_int(rc, ==, SQLITE_CORRUPT);

	return MUNIT_OK;
}

/* If the stored is value larger than the maximum size, an error is returned. */
static MunitResult test_get_page_size_too_large(const MunitParameter params[],
                                                void *               data) {
	uint8_t      buf[DQLITE__FORMAT_DB_HDR_SIZE];
	unsigned int page_size;
	int          rc;

	(void)params;
	(void)data;

	buf[16] = 0xff;
	buf[17] = 0xff;

	rc = dqlite__format_get_page_size(DQLITE__FORMAT_DB, buf, &page_size);
	munit_assert_int(rc, ==, SQLITE_CORRUPT);

	return MUNIT_OK;
}

/* If the stored value is not a power of 2, an error is returned. */
static MunitResult test_get_page_size_not_power_of_2(const MunitParameter params[],
                                                     void *               data) {
	uint8_t      buf[DQLITE__FORMAT_DB_HDR_SIZE];
	unsigned int page_size;
	int          rc;

	(void)params;
	(void)data;

	buf[16] = 6;
	buf[17] = 12;

	rc = dqlite__format_get_page_size(DQLITE__FORMAT_DB, buf, &page_size);
	munit_assert_int(rc, ==, SQLITE_CORRUPT);

	return MUNIT_OK;
}

static MunitTest dqlite__format_page_size_tests[] = {
    {"/db", test_get_page_size_db, NULL, NULL, 0, NULL},
    {"/wal", test_get_page_size_wal, NULL, NULL, 0, NULL},
    {"/max", test_get_page_size_max, NULL, NULL, 0, NULL},
    {"/too-small", test_get_page_size_too_small, NULL, NULL, 0, NULL},
    {"/too-large", test_get_page_size_too_large, NULL, NULL, 0, NULL},
    {"/not-power-of-2", test_get_page_size_not_power_of_2, NULL, NULL, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

MunitSuite dqlite__format_suites[] = {
    {"_get_page_size", dqlite__format_page_size_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
