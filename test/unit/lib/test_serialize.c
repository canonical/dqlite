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

struct pages
{
	unsigned n;    /* Number of pages */
	unsigned size; /* Size of each page */
	void **bufs;   /* Array of page buffers */
};

static struct pages *create_pages(unsigned n, unsigned size)
{
	struct pages *pages = munit_malloc(sizeof *pages);
	unsigned i;
	pages->n = n;
	pages->size = size;
	pages->bufs = munit_malloc(n * sizeof *pages->bufs);
	for (i = 0; i < pages->n; i++) {
		pages->bufs[i] = munit_malloc(size);
	}
	return pages;
}

static void destroy_pages(struct pages *pages)
{
	unsigned i;
	for (i = 0; i < pages->n; i++) {
		free(pages->bufs[i]);
	}
	free(pages->bufs);
	free(pages);
}

/* Opaque pointer to a struct pages object. */
typedef struct pages *pages_t;

static size_t byte__sizeof_pages(pages_t pages)
{
	size_t s = byte__sizeof_uint16(0) /* n */ +
		   byte__sizeof_uint16(0) /* size */ +
		   byte__sizeof_uint32(0) /* unused */ +
		   pages->size * pages->n /* buf */;
	return s;
}

static void byte__encode_pages(pages_t value, void **cursor)
{
	struct pages *pages = value;
	unsigned i;
	byte__encode_uint16(pages->n, cursor);
	byte__encode_uint16(pages->size, cursor);
	byte__encode_uint32(0, cursor);
	for (i = 0; i < pages->n; i++) {
		memcpy(*cursor, pages->bufs[i], pages->size);
		*cursor += pages->size;
	}
}

static pages_t byte__decode_pages(const void **cursor)
{
	struct pages *pages;
	unsigned i;
	pages = munit_malloc(sizeof *pages);
	pages->n = byte__decode_uint16(cursor);
	pages->size = byte__decode_uint16(cursor);
	byte__decode_uint32(cursor); /* Unused */
	pages->bufs = munit_malloc(pages->n * sizeof *pages->bufs);
	for (i = 0; i < pages->n; i++) {
		pages->bufs[i] = (void *)*cursor;
		*cursor += pages->size;
	}
	return pages;
}

#define BOOK(X, ...)                  \
	X(text, title, ##__VA_ARGS__) \
	X(pages, pages, ##__VA_ARGS__)

SERIALIZE__DEFINE(book, BOOK);
SERIALIZE__IMPLEMENT(book, BOOK);

/******************************************************************************
 *
 * Fixture
 *
 ******************************************************************************/

struct fixture
{
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
	(void)params;
	f->person.name = "John Doh";
	f->person.age = 40;
	size = person__sizeof(&f->person);
	buf = munit_malloc(size);
	person__encode(&f->person, buf);
	munit_assert_string_equal(buf, "John Doh");
	munit_assert_int(byte__flip64(*(uint64_t *)(buf + 16)), ==, 40);
	free(buf);
	return MUNIT_OK;
}

/* Padding is not added if a string ends exactly at word boundary. */
TEST_CASE(encode, no_padding, NULL)
{
	struct fixture *f = data;
	size_t size;
	void *buf;
	(void)params;
	f->person.name = "Joe Doh";
	f->person.age = 40;
	size = person__sizeof(&f->person);
	buf = munit_malloc(size);
	person__encode(&f->person, buf);
	munit_assert_string_equal(buf, "Joe Doh");
	munit_assert_int(byte__flip64(*(uint64_t *)(buf + 8)), ==, 40);
	free(buf);
	return MUNIT_OK;
}

/* Encode a custom complex field. */
TEST_CASE(encode, custom, NULL)
{
	struct fixture *f = data;
	size_t size;
	void *buf;
	(void)params;
	f->book.title = "Les miserables";
	f->book.pages = create_pages(2, 8);
	strcpy(f->book.pages->bufs[0], "Fantine");
	strcpy(f->book.pages->bufs[1], "Cosette");

	size = book__sizeof(&f->book);
	munit_assert_int(size, ==,
			 16 +    /* title                                   */
			     2 + /* n pages                                 */
			     2 + /* page size                               */
			     4 + /* unused                                  */
			     8 * 2 /* page buffers */);

	buf = munit_malloc(size);
	book__encode(&f->book, buf);

	munit_assert_string_equal(buf, "Les miserables");
	munit_assert_int(byte__flip16(*(uint16_t *)(buf + 16)), ==, 2);
	munit_assert_int(byte__flip16(*(uint16_t *)(buf + 18)), ==, 8);
	munit_assert_string_equal(buf + 24, "Fantine");
	munit_assert_string_equal(buf + 32, "Cosette");

	free(buf);
	destroy_pages(f->book.pages);

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
	(void)params;
	strcpy(buf, "John Doh");
	*(uint64_t *)(buf + 16) = byte__flip64(40);
	person__decode(buf, &f->person);
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
	(void)params;
	strcpy(buf, "Joe Doh");
	*(uint64_t *)(buf + 8) = byte__flip64(40);
	person__decode(buf, &f->person);
	munit_assert_string_equal(f->person.name, "Joe Doh");
	munit_assert_int(f->person.age, ==, 40);
	free(buf);
	return MUNIT_OK;
}

/* Decode a custom complex field. */
TEST_CASE(decode, custom, NULL)
{
	struct fixture *f = data;
	void *buf = munit_malloc(16 + /* title */
				 2 +  /* n pages  */
				 2 +  /* page size  */
				 4 +  /* unused  */
				 8 * 2 /* page buffers */);
	(void)params;
	strcpy(buf, "Les miserables");
	*(uint16_t *)(buf + 16) = byte__flip16(2);
	*(uint16_t *)(buf + 18) = byte__flip16(8);
	strcpy(buf + 24, "Fantine");
	strcpy(buf + 32, "Cosette");
	book__decode(buf, &f->book);

	munit_assert_string_equal(f->book.title, "Les miserables");
	munit_assert_int(f->book.pages->n, ==, 2);
	munit_assert_int(f->book.pages->size, ==, 8);
	munit_assert_string_equal(f->book.pages->bufs[0], "Fantine");
	munit_assert_string_equal(f->book.pages->bufs[1], "Cosette");

	free(f->book.pages->bufs);
	free(f->book.pages);

	free(buf);
	return MUNIT_OK;
}
