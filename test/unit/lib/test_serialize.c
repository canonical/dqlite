#include "../../../src/lib/serialize.h"

#include "../../lib/runner.h"

TEST_MODULE(lib_serialize);

/******************************************************************************
 *
 * Simple schema with stock fields.
 *
 ******************************************************************************/

#define PERSON(X, ...)               \
	X(text, name, ##__VA_ARGS__) \
	X(uint64, age, ##__VA_ARGS__)

SERIALIZE__DEFINE(person, PERSON);
SERIALIZE__IMPLEMENT(person, PERSON);

/******************************************************************************
 *
 * Complex schema with a custom field.
 *
 ******************************************************************************/

struct pages {
	uint16_t n;    /* Number of pages */
	uint16_t size; /* Size of each page */
	uint32_t __unused__;
	void **bufs; /* Array of page buffers */
};

static void create_pages(unsigned n, unsigned size, struct pages *pages)
{
	unsigned i;
	pages->n = n;
	pages->size = size;
	pages->bufs = munit_malloc(n * sizeof *pages->bufs);
	for (i = 0; i < pages->n; i++) {
		pages->bufs[i] = munit_malloc(size);
	}
}

static void destroy_pages(struct pages *pages)
{
	unsigned i;
	for (i = 0; i < pages->n; i++) {
		free(pages->bufs[i]);
	}
	free(pages->bufs);
}

/* Opaque pointer to a struct pages object. */
typedef struct pages pages_t;
typedef struct person person_t;

static size_t pages__sizeof(const pages_t *pages)
{
	return uint16__sizeof(&pages->n) + uint16__sizeof(&pages->size) +
	       uint32__sizeof(&pages->__unused__) +
	       pages->size * pages->n /* bufs */;
}

static void pages__encode(const pages_t *pages, char **cursor)
{
	unsigned i;
	uint16__encode(&pages->n, cursor);
	uint16__encode(&pages->size, cursor);
	uint32__encode(&pages->__unused__, cursor);
	for (i = 0; i < pages->n; i++) {
		memcpy(*cursor, pages->bufs[i], pages->size);
		*cursor += pages->size;
	}
}

static int pages__decode(struct cursor *cursor, pages_t *pages)
{
	unsigned i;
	uint16__decode(cursor, &pages->n);
	uint16__decode(cursor, &pages->size);
	uint32__decode(cursor, &pages->__unused__);
	pages->bufs = munit_malloc(pages->n * sizeof *pages->bufs);
	for (i = 0; i < pages->n; i++) {
		pages->bufs[i] = (void *)cursor->p;
		cursor->p += pages->size;
		cursor->cap -= pages->size;
	}
	return 0;
}

#define BOOK(X, ...)                     \
	X(text, title, ##__VA_ARGS__)    \
	X(person, author, ##__VA_ARGS__) \
	X(pages, pages, ##__VA_ARGS__)

SERIALIZE__DEFINE(book, BOOK);
SERIALIZE__IMPLEMENT(book, BOOK);

/******************************************************************************
 *
 * Fixture
 *
 ******************************************************************************/

struct fixture {
	struct person person;
	struct book book;
};

static void *setup(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	(void)params;
	(void)user_data;
	return f;
}

static void tear_down(void *data)
{
	free(data);
}

/******************************************************************************
 *
 * Fields definition.
 *
 ******************************************************************************/

TEST_SUITE(fields);
TEST_SETUP(fields, setup);
TEST_TEAR_DOWN(fields, tear_down);

/* The expected fields are defined on the struct. */
TEST_CASE(fields, define, NULL)
{
	struct fixture *f = data;
	(void)params;
	f->person.name = "John Doh";
	f->person.age = 40;
	return MUNIT_OK;
}

/******************************************************************************
 *
 * Sizeof method.
 *
 ******************************************************************************/

TEST_SUITE(sizeof);
TEST_SETUP(sizeof, setup);
TEST_TEAR_DOWN(sizeof, tear_down);

/* Padding is added if needed. */
TEST_CASE(sizeof, padding, NULL)
{
	struct fixture *f = data;
	size_t size;
	(void)params;
	f->person.name = "John Doh";
	f->person.age = 40;
	size = person__sizeof(&f->person);
	munit_assert_int(size, ==, 16 /* name */ + 8 /* age */);
	return MUNIT_OK;
}

/* Padding is not added if a string ends exactly at word boundary. */
TEST_CASE(sizeof, no_padding, NULL)
{
	struct fixture *f = data;
	size_t size;
	(void)params;
	f->person.name = "Joe Doh";
	f->person.age = 40;
	size = person__sizeof(&f->person);
	munit_assert_int(size, ==, 8 /* name */ + 8 /* age */);
	return MUNIT_OK;
}

/******************************************************************************
 *
 * Encode method.
 *
 ******************************************************************************/

TEST_SUITE(encode);
TEST_SETUP(encode, setup);
TEST_TEAR_DOWN(encode, tear_down);

/* Padding is added if needed. */
TEST_CASE(encode, padding, NULL)
{
	struct fixture *f = data;
	size_t size;
	void *buf;
	char *cursor;
	(void)params;
	f->person.name = "John Doh";
	f->person.age = 40;
	size = person__sizeof(&f->person);
	buf = munit_malloc(size);
	cursor = buf;
	person__encode(&f->person, &cursor);
	munit_assert_string_equal(buf, "John Doh");
	munit_assert_int(ByteFlipLe64(*(uint64_t *)(buf + 16)), ==, 40);
	free(buf);
	return MUNIT_OK;
}

/* Padding is not added if a string ends exactly at word boundary. */
TEST_CASE(encode, no_padding, NULL)
{
	struct fixture *f = data;
	size_t size;
	void *buf;
	char *cursor;
	(void)params;
	f->person.name = "Joe Doh";
	f->person.age = 40;
	size = person__sizeof(&f->person);
	buf = munit_malloc(size);
	cursor = buf;
	person__encode(&f->person, &cursor);
	munit_assert_string_equal(buf, "Joe Doh");
	munit_assert_int(ByteFlipLe64(*(uint64_t *)(buf + 8)), ==, 40);
	free(buf);
	return MUNIT_OK;
}

/* Encode a custom complex field. */
TEST_CASE(encode, custom, NULL)
{
	struct fixture *f = data;
	size_t size;
	void *buf;
	char *cursor;
	(void)params;
	f->book.title = "Les miserables";
	f->book.author.name = "Victor Hugo";
	f->book.author.age = 40;
	create_pages(2, 8, &f->book.pages);
	strcpy(f->book.pages.bufs[0], "Fantine");
	strcpy(f->book.pages.bufs[1], "Cosette");

	size = book__sizeof(&f->book);
	munit_assert_int(size, ==,
			 16 +     /* title                                   */
			     16 + /* author name                             */
			     8 +  /* author age                              */
			     2 +  /* n pages                                 */
			     2 +  /* page size                               */
			     4 +  /* unused                                  */
			     8 * 2 /* page buffers */);

	buf = munit_malloc(size);
	cursor = buf;
	book__encode(&f->book, &cursor);

	cursor = buf;

	munit_assert_string_equal(cursor, "Les miserables");
	cursor += 16;

	munit_assert_string_equal(cursor, "Victor Hugo");
	cursor += 16;

	uint64_t x;
	memcpy(&x, cursor, sizeof(x));
	munit_assert_uint64(ByteFlipLe64(x), ==, 40);
	cursor += 8;

	uint16_t y;
	memcpy(&y, cursor, sizeof(y));
	munit_assert_uint16(ByteFlipLe16(y), ==, 2);
	cursor += 2;

	uint16_t z;
	memcpy(&z, cursor, sizeof(z));
	munit_assert_uint16(ByteFlipLe16(z), ==, 8);
	cursor += 2;

	cursor += 4; /* Unused */

	munit_assert_string_equal(cursor, "Fantine");
	cursor += 8;

	munit_assert_string_equal(cursor, "Cosette");

	free(buf);
	destroy_pages(&f->book.pages);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * Decode method.
 *
 ******************************************************************************/

TEST_SUITE(decode);
TEST_SETUP(decode, setup);
TEST_TEAR_DOWN(decode, tear_down);

/* Padding is added if needed. */
TEST_CASE(decode, padding, NULL)
{
	struct fixture *f = data;
	void *buf = munit_malloc(16 + 8);
	struct cursor cursor = {buf, 16 + 8};
	(void)params;
	strcpy(buf, "John Doh");
	*(uint64_t *)(buf + 16) = ByteFlipLe64(40);
	person__decode(&cursor, &f->person);
	munit_assert_string_equal(f->person.name, "John Doh");
	munit_assert_int(f->person.age, ==, 40);
	free(buf);
	return MUNIT_OK;
}

/* Padding is not added if a string ends exactly at word boundary. */
TEST_CASE(decode, no_padding, NULL)
{
	struct fixture *f = data;
	void *buf = munit_malloc(16 + 8);
	struct cursor cursor = {buf, 16 + 8};
	(void)params;
	strcpy(buf, "Joe Doh");
	*(uint64_t *)(buf + 8) = ByteFlipLe64(40);
	person__decode(&cursor, &f->person);
	munit_assert_string_equal(f->person.name, "Joe Doh");
	munit_assert_int(f->person.age, ==, 40);
	free(buf);
	return MUNIT_OK;
}

/* The given buffer has not enough data. */
TEST_CASE(decode, short, NULL)
{
	struct fixture *f = data;
	void *buf = munit_malloc(16);
	struct cursor cursor = {buf, 16};
	int rc;
	(void)params;
	strcpy(buf, "John Doh");
	rc = person__decode(&cursor, &f->person);
	munit_assert_int(rc, ==, DQLITE_PARSE);
	free(buf);
	return MUNIT_OK;
}

/* Decode a custom complex field. */
TEST_CASE(decode, custom, NULL)
{
	struct fixture *f = data;
	size_t len = 16 + /* title */
		     16 + /* author name */
		     8 +  /* author age */
		     2 +  /* n pages  */
		     2 +  /* page size  */
		     4 +  /* unused  */
		     8 * 2 /* page buffers */;
	void *buf = munit_malloc(len);
	void *p = buf;
	struct cursor cursor = {buf, len};
	(void)params;

	strcpy(p, "Les miserables");
	p += 16;

	strcpy(p, "Victor Hugo");
	p += 16;

	*(uint64_t *)p = ByteFlipLe64(40);
	p += 8;

	*(uint16_t *)p = ByteFlipLe16(2);
	p += 2;

	*(uint16_t *)p = ByteFlipLe16(8);
	p += 2;

	p += 4; /* Unused */

	strcpy(p, "Fantine");
	p += 8;

	strcpy(p, "Cosette");

	book__decode(&cursor, &f->book);

	munit_assert_string_equal(f->book.title, "Les miserables");
	munit_assert_string_equal(f->book.author.name, "Victor Hugo");
	munit_assert_int(f->book.author.age, ==, 40);
	munit_assert_int(f->book.pages.n, ==, 2);
	munit_assert_int(f->book.pages.size, ==, 8);
	munit_assert_string_equal(f->book.pages.bufs[0], "Fantine");
	munit_assert_string_equal(f->book.pages.bufs[1], "Cosette");

	free(f->book.pages.bufs);

	free(buf);
	return MUNIT_OK;
}
