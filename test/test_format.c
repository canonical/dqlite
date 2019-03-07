#include <stdint.h>

#include <sqlite3.h>

#include "../src/format.h"

#include "./lib/runner.h"

TEST_MODULE(format);

TEST_SUITE(get_page_size);

/* Parse the page size stored in a database file header. */
TEST_CASE(get_page_size, db, NULL)
{
	uint8_t      buf[FORMAT__DB_HDR_SIZE];
	unsigned int page_size;
	int          rc;

	(void)params;
	(void)data;

	buf[16] = 16;
	buf[17] = 0;

	rc = format__get_page_size(FORMAT__DB, buf, &page_size);
	munit_assert_int(rc, ==, SQLITE_OK);

	munit_assert_int(page_size, ==, 4096);

	return MUNIT_OK;
}

/* Parse the page size stored in a WAL file header. */
TEST_CASE(get_page_size, wal, NULL)
{
	uint8_t      buf[FORMAT__WAL_HDR_SIZE];
	unsigned int page_size;
	int          rc;

	(void)params;
	(void)data;

	buf[8]  = 0;
	buf[9]  = 0;
	buf[10] = 16;
	buf[11] = 0;

	rc = format__get_page_size(FORMAT__WAL, buf, &page_size);
	munit_assert_int(rc, ==, SQLITE_OK);

	munit_assert_int(page_size, ==, 4096);

	return MUNIT_OK;
}

/* If the stored value is 1, the resulting page size is the maximum one. */
TEST_CASE(get_page_size, max, NULL)
{
	uint8_t      buf[FORMAT__DB_HDR_SIZE];
	unsigned int page_size;
	int          rc;

	(void)params;
	(void)data;

	buf[16] = 0;
	buf[17] = 1;

	rc = format__get_page_size(FORMAT__DB, buf, &page_size);
	munit_assert_int(rc, ==, SQLITE_OK);

	munit_assert_int(page_size, ==, 65536);

	return MUNIT_OK;
}

/* If the stored value is smaller than the minimum size, an error is returned. */
TEST_CASE(get_page_size, too_small, NULL)
{
	uint8_t      buf[FORMAT__DB_HDR_SIZE];
	unsigned int page_size;
	int          rc;

	(void)params;
	(void)data;

	buf[16] = 0;
	buf[17] = 128;

	rc = format__get_page_size(FORMAT__DB, buf, &page_size);
	munit_assert_int(rc, ==, SQLITE_CORRUPT);

	return MUNIT_OK;
}

/* If the stored is value larger than the maximum size, an error is returned. */
TEST_CASE(get_page_size, too_large, NULL)
{
	uint8_t      buf[FORMAT__DB_HDR_SIZE];
	unsigned int page_size;
	int          rc;

	(void)params;
	(void)data;

	buf[16] = 0xff;
	buf[17] = 0xff;

	rc = format__get_page_size(FORMAT__DB, buf, &page_size);
	munit_assert_int(rc, ==, SQLITE_CORRUPT);

	return MUNIT_OK;
}

/* If the stored value is not a power of 2, an error is returned. */
TEST_CASE(get_page_size, not_power_of_2, NULL)
{
	uint8_t      buf[FORMAT__DB_HDR_SIZE];
	unsigned int page_size;
	int          rc;

	(void)params;
	(void)data;

	buf[16] = 6;
	buf[17] = 12;

	rc = format__get_page_size(FORMAT__DB, buf, &page_size);
	munit_assert_int(rc, ==, SQLITE_CORRUPT);

	return MUNIT_OK;
}
