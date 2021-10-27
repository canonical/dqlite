#include "../../../src/lib/buffer.h"

#include "../../lib/runner.h"

TEST_MODULE(lib_buffer);

/******************************************************************************
 *
 * Fixture
 *
 ******************************************************************************/

struct fixture
{
	struct buffer buffer;
};

static void *setup(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	int rc;
	(void)params;
	(void)user_data;
	rc = buffer__init(&f->buffer);
	munit_assert_int(rc, ==, 0);
	return f;
}

static void tear_down(void *data)
{
	struct fixture *f = data;
	buffer__close(&f->buffer);
	free(f);
}

/******************************************************************************
 *
 * Helper macros.
 *
 ******************************************************************************/

#define ADVANCE(SIZE)                                       \
	{                                                   \
		cursor = buffer__advance(&f->buffer, SIZE); \
		munit_assert_ptr_not_null(cursor);          \
	}

/******************************************************************************
 *
 * Assertions.
 *
 ******************************************************************************/

#define ASSERT_N_PAGES(N) munit_assert_int(f->buffer.n_pages, ==, N)

/******************************************************************************
 *
 * buffer__init
 *
 ******************************************************************************/

TEST_SUITE(init);
TEST_SETUP(init, setup);
TEST_TEAR_DOWN(init, tear_down);

/* If n is 0, then the prefix is used to dermine the number of elements of the
 * tuple. */
TEST_CASE(init, n_pages, NULL)
{
	struct fixture *f = data;
	(void)params;
	ASSERT_N_PAGES(1);
	munit_assert_long(f->buffer.page_size, ==, sysconf(_SC_PAGESIZE));
	return MUNIT_OK;
}

/******************************************************************************
 *
 * buffer__advance
 *
 ******************************************************************************/

TEST_SUITE(advance);
TEST_SETUP(advance, setup);
TEST_TEAR_DOWN(advance, tear_down);

/* The buffer already has enough capacity. */
TEST_CASE(advance, enough, NULL)
{
	struct fixture *f = data;
	void *cursor;
	(void)params;
	ADVANCE(16);
	ASSERT_N_PAGES(1);
	return MUNIT_OK;
}

/* The buffer needs to double its size once. */
TEST_CASE(advance, double, NULL)
{
	struct fixture *f = data;
	void *cursor;
	(void)params;
	ADVANCE(16 + f->buffer.page_size);
	ASSERT_N_PAGES(2);
	return MUNIT_OK;
}

/* The buffer needs to double its sice twice. */
TEST_CASE(advance, double_twice, NULL)
{
	struct fixture *f = data;
	void *cursor;
	(void)params;
	ADVANCE(16 + 3 * f->buffer.page_size);
	ASSERT_N_PAGES(4);
	return MUNIT_OK;
}
