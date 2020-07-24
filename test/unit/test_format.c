#include <stdint.h>

#include <sqlite3.h>

#include "../lib/runner.h"

#include "../../src/format.h"

/******************************************************************************
 *
 * formatWalGetPageSize
 *
 ******************************************************************************/

SUITE(formatWalGetPageSize);

TEST(formatWalGetPageSize, valid, NULL, NULL, 0, NULL)
{
	uint8_t header[FORMAT__WAL_HDR_SIZE];
	unsigned page_size;

	header[8] = 0;
	header[9] = 0;
	header[10] = 16;
	header[11] = 0;

	formatWalGetPageSize(header, &page_size);
	munit_assert_int(page_size, ==, 4096);

	return MUNIT_OK;
}

/* If the stored value is 1, the resulting page size is the maximum one. */
TEST(formatWalGetPageSize, max, NULL, NULL, 0, NULL)
{
	uint8_t header[FORMAT__WAL_HDR_SIZE];
	unsigned page_size;

	header[8] = 0;
	header[9] = 0;
	header[10] = 0;
	header[11] = 1;

	formatWalGetPageSize(header, &page_size);
	munit_assert_int(page_size, ==, 65536);

	return MUNIT_OK;
}

/* The stored value is smaller than the minimum size. */
TEST(formatWalGetPageSize, tooSmall, NULL, NULL, 0, NULL)
{
	uint8_t header[FORMAT__WAL_HDR_SIZE];
	unsigned page_size;

	header[8] = 0;
	header[9] = 0;
	header[10] = 0;
	header[11] = 128;

	formatWalGetPageSize(header, &page_size);
	munit_assert_int(page_size, ==, 0);

	return MUNIT_OK;
}

/* The stored value is larger than the maximum size. */
TEST(formatWalGetPageSize, tooLarge, NULL, NULL, 0, NULL)
{
	uint8_t header[FORMAT__WAL_HDR_SIZE];
	unsigned page_size;

	header[8] = 0;
	header[9] = 0;
	header[10] = 0xff;
	header[11] = 0xff;

	formatWalGetPageSize(header, &page_size);
	munit_assert_int(page_size, ==, 0);

	return MUNIT_OK;
}

/* The stored value is not a power of 2. */
TEST(formatWalGetPageSize, notPowerOf2, NULL, NULL, 0, NULL)
{
	uint8_t header[FORMAT__WAL_HDR_SIZE];
	unsigned page_size;

	header[8] = 0;
	header[9] = 0;
	header[10] = 6;
	header[11] = 12;

	formatWalGetPageSize(header, &page_size);
	munit_assert_int(page_size, ==, 0);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * formatDatabaseGetPageSize
 *
 ******************************************************************************/

SUITE(formatDatabaseGetPageSize);

/* Parse the page size stored in a database file header. */
TEST(formatDatabaseGetPageSize, valid, NULL, NULL, 0, NULL)
{
	uint8_t header[FORMAT__DB_HDR_SIZE];
	unsigned page_size;

	header[16] = 16;
	header[17] = 0;

	formatDatabaseGetPageSize(header, &page_size);
	munit_assert_int(page_size, ==, 4096);

	return MUNIT_OK;
}

/* If the stored value is 1, the resulting page size is the maximum one. */
TEST(formatDatabaseGetPageSize, max, NULL, NULL, 0, NULL)
{
	uint8_t header[FORMAT__DB_HDR_SIZE];
	unsigned page_size;

	header[16] = 0;
	header[17] = 1;

	formatDatabaseGetPageSize(header, &page_size);
	munit_assert_int(page_size, ==, 65536);

	return MUNIT_OK;
}

/* The stored value is smaller than the minimum size. */
TEST(formatDatabaseGetPageSize, tooSmall, NULL, NULL, 0, NULL)
{
	uint8_t header[FORMAT__DB_HDR_SIZE];
	unsigned page_size;

	header[16] = 0;
	header[17] = 128;

	formatDatabaseGetPageSize(header, &page_size);
	munit_assert_int(page_size, ==, 0);

	return MUNIT_OK;
}

/* The stored value is larger than the maximum size. */
TEST(formatDatabaseGetPageSize, tooLarge, NULL, NULL, 0, NULL)
{
	uint8_t header[FORMAT__DB_HDR_SIZE];
	unsigned page_size;

	header[16] = 0xff;
	header[17] = 0xff;

	formatDatabaseGetPageSize(header, &page_size);
	munit_assert_int(page_size, ==, 0);

	return MUNIT_OK;
}

/* The stored value is not a power of 2. */
TEST(formatDatabaseGetPageSize, notPowerOf2, NULL, NULL, 0, NULL)
{
	uint8_t header[FORMAT__DB_HDR_SIZE];
	unsigned page_size;

	header[16] = 6;
	header[17] = 12;

	formatDatabaseGetPageSize(header, &page_size);
	munit_assert_int(page_size, ==, 0);

	return MUNIT_OK;
}
