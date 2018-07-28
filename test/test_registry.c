#include <assert.h>
#include <stdlib.h>

#include "../src/registry.h"

#include "leak.h"
#include "munit.h"

struct test_item {
	size_t id;
	int *  ptr;
};

static void test_item_init(struct test_item *i) {
	assert(i != NULL);

	i->ptr  = (int *)sqlite3_malloc(sizeof(*(i->ptr)));
	*i->ptr = 123;
}

static void test_item_close(struct test_item *i) {
	assert(i != NULL);
	assert(i->ptr != NULL);

	sqlite3_free(i->ptr);
}

static const char *test_item_hash(struct test_item *i) {
	assert(i != NULL);

	return "x";
}

DQLITE__REGISTRY(test_registry, test_item);
DQLITE__REGISTRY_METHODS(test_registry, test_item);

static void *setup(const MunitParameter params[], void *user_data) {
	struct test_registry *registry;

	(void)params;
	(void)user_data;

	registry = (struct test_registry *)munit_malloc(sizeof(*registry));

	test_registry_init(registry);

	return registry;
}

static void tear_down(void *data) {
	struct test_registry *registry = data;

	test_registry_close(registry);

	test_assert_no_leaks();
}

/* Add N items. */
static MunitResult test_add(const MunitParameter params[], void *data) {
	struct test_registry *registry = data;
	int                   err;
	struct test_item *    item;
	int                   n;
	int                   i;

	n = atoi(munit_parameters_get(params, "n"));
	munit_assert_int(n, >, 0);

	for (i = 0; i < n; i++) {
		err = test_registry_add(registry, &item);
		munit_assert_int(err, ==, 0);

		munit_assert_ptr_not_equal(item, NULL);
		munit_assert_ptr_not_equal(item->ptr, NULL);
		munit_assert_int(123, ==, *item->ptr);
	}

	return MUNIT_OK;
}

/* Add three items, delete the second, and then add another one. The original ID
 * of the deleted item gets reused. */
static MunitResult test_add_del_add(const MunitParameter params[], void *data) {
	struct test_registry *registry = data;
	int                   err;
	struct test_item *    item1;
	struct test_item *    item2;
	struct test_item *    item3;
	struct test_item *    item4;

	(void)params;

	err = test_registry_add(registry, &item1);
	munit_assert_int(err, ==, 0);

	err = test_registry_add(registry, &item2);
	munit_assert_int(err, ==, 0);

	err = test_registry_add(registry, &item3);
	munit_assert_int(err, ==, 0);

	err = test_registry_del(registry, item2);
	munit_assert_int(err, ==, 0);

	err = test_registry_add(registry, &item4);
	munit_assert_int(err, ==, 0);

	munit_assert_int(item4->id, ==, item2->id);

	return MUNIT_OK;
}

/* Add N items and then delete them all. */
static MunitResult test_add_and_del_n(const MunitParameter params[], void *data) {
	struct test_registry *registry = data;
	int                   err;
	struct test_item **   items;
	int                   n;
	int                   i;

	n = atoi(munit_parameters_get(params, "n"));
	munit_assert_int(n, >, 0);

	items = munit_malloc(n * sizeof(*items));

	for (i = 0; i < n; i++) {
		err = test_registry_add(registry, &items[i]);
		munit_assert_int(err, ==, 0);
	}

	for (i = 0; i < n; i++) {
		err = test_registry_del(registry, items[i]);
		munit_assert_int(err, ==, 0);
	}

	return MUNIT_OK;
}

/* Retrieve a previously added item. */
static MunitResult test_get(const MunitParameter params[], void *data) {
	struct test_registry *registry = data;
	int                   err;
	struct test_item *    item;

	(void)params;

	err = test_registry_add(registry, &item);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_equal(test_registry_get(registry, item->id), item);

	return MUNIT_OK;
}

/* An item gets added and then deleted. Trying to fetch the item using its
 * former ID results in a NULL pointer. */
static MunitResult test_get_deleted(const MunitParameter params[], void *data) {
	struct test_registry *registry = data;
	int                   err;
	struct test_item *    item;
	size_t                id;

	(void)params;

	err = test_registry_add(registry, &item);
	munit_assert_int(err, ==, 0);

	id = item->id;

	err = test_registry_del(registry, item);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_equal(test_registry_get(registry, id), NULL);

	return MUNIT_OK;
}

/* Retrieve an item with an ID bigger than the current registry's length. */
static MunitResult test_get_out_of_bound(const MunitParameter params[], void *data) {
	struct test_registry *registry = data;
	struct test_item *    item     = test_registry_get(registry, 123);

	(void)params;

	munit_assert_ptr_equal(item, NULL);

	return MUNIT_OK;
}

/* Find the index of a matching item. */
static MunitResult test_idx_found(const MunitParameter params[], void *data) {
	struct test_registry *registry = data;
	struct test_item *    item;
	size_t                i;
	int                   err;

	(void)params;

	err = test_registry_add(registry, &item);
	munit_assert_int(err, ==, 0);

	err = test_registry_idx(registry, "x", &i);
	munit_assert_int(err, ==, 0);

	munit_assert_int(i, ==, item->id);

	return MUNIT_OK;
}

/* No matching item. */
static MunitResult test_idx_not_found(const MunitParameter params[], void *data) {
	struct test_registry *registry = data;
	struct test_item *    item1;
	struct test_item *    item2;
	size_t                i;
	int                   err;

	(void)params;

	err = test_registry_add(registry, &item1);
	munit_assert_int(err, ==, 0);

	err = test_registry_add(registry, &item2);
	munit_assert_int(err, ==, 0);

	err = test_registry_del(registry, item1);
	munit_assert_int(err, ==, 0);

	err = test_registry_idx(registry, "y", &i);
	munit_assert_int(err, ==, DQLITE_NOTFOUND);

	return MUNIT_OK;
}

/* Delete an item from the registry. */
static MunitResult test_del(const MunitParameter params[], void *data) {
	struct test_registry *registry = data;
	int                   err;
	struct test_item *    item;

	(void)params;

	err = test_registry_add(registry, &item);
	munit_assert_int(err, ==, 0);

	err = test_registry_del(registry, item);
	munit_assert_int(err, ==, 0);

	return MUNIT_OK;
}

/* Deleting an item twice results in an error. */
static MunitResult test_del_twice(const MunitParameter params[], void *data) {
	struct test_registry *registry = data;
	int                   err;
	struct test_item *    item;

	(void)params;

	err = test_registry_add(registry, &item);
	munit_assert_int(err, ==, 0);

	err = test_registry_del(registry, item);
	munit_assert_int(err, ==, 0);

	err = test_registry_del(registry, item);
	munit_assert_int(err, ==, DQLITE_NOTFOUND);

	return MUNIT_OK;
}

/* Deleting an item twice results in an error, also if the item being the
 * deleted again had an ID lower than the highest one. */
static MunitResult test_del_twice_middle(const MunitParameter params[], void *data) {
	struct test_registry *registry = data;
	int                   err;
	struct test_item *    item1;
	struct test_item *    item2;

	(void)params;

	err = test_registry_add(registry, &item1);
	munit_assert_int(err, ==, 0);

	err = test_registry_add(registry, &item2);
	munit_assert_int(err, ==, 0);

	err = test_registry_del(registry, item1);
	munit_assert_int(err, ==, 0);

	err = test_registry_del(registry, item1);
	munit_assert_int(err, ==, DQLITE_NOTFOUND);

	return MUNIT_OK;
}

/* Deleting an item with an unknown ID results in an error. */
static MunitResult test_del_out_of_bound(const MunitParameter params[], void *data) {
	struct test_registry *registry = data;
	struct test_item      item;
	int                   err;

	(void)params;

	item.id = 123;

	err = test_registry_del(registry, &item);

	munit_assert_int(err, ==, DQLITE_NOTFOUND);

	return MUNIT_OK;
}

/* Add several items and then delete them. */
static MunitResult test_del_many(const MunitParameter params[], void *data) {
	struct test_registry *registry = data;
	int                   err;
	struct test_item *    item1;
	struct test_item *    item2;
	struct test_item *    item3;

	(void)params;

	err = test_registry_add(registry, &item1);
	munit_assert_int(err, ==, 0);

	munit_assert_int(item1->id, ==, 0);

	err = test_registry_add(registry, &item2);
	munit_assert_int(err, ==, 0);

	munit_assert_int(item2->id, ==, 1);

	err = test_registry_add(registry, &item3);
	munit_assert_int(err, ==, 0);

	munit_assert_int(item3->id, ==, 2);

	munit_assert_int(3, ==, registry->len);
	munit_assert_int(4, ==, registry->cap);

	err = test_registry_del(registry, item3);
	munit_assert_int(err, ==, 0);

	munit_assert_int(2, ==, registry->len);
	munit_assert_int(4, ==, registry->cap);

	err = test_registry_del(registry, item2);
	munit_assert_int(err, ==, 0);

	munit_assert_int(1, ==, registry->len);
	munit_assert_int(2, ==, registry->cap);

	return MUNIT_OK;
}

static char *test_add_n[] = {"1", "2", "3", "5", "6", "7", "8", "9", "10", NULL};

static MunitParameterEnum test_add_params[] = {
    {"n", test_add_n},
    {NULL, NULL},
};

MunitTest dqlite__registry_tests[] = {
    {"_add", test_add, setup, tear_down, 0, test_add_params},
    {"_add/then-del-and-add-again", test_add_del_add, setup, tear_down, 0, NULL},
    {"_add/add-and-del-many",
     test_add_and_del_n,
     setup,
     tear_down,
     0,
     test_add_params},
    {"_get", test_get, setup, tear_down, 0, NULL},
    {"_get/deleted", test_get_deleted, setup, tear_down, 0, NULL},
    {"_get/out-of-bound", test_get_out_of_bound, setup, tear_down, 0, NULL},
    {"_idx/found", test_idx_found, setup, tear_down, 0, NULL},
    {"_idx/not-found", test_idx_not_found, setup, tear_down, 0, NULL},
    {"_del", test_del, setup, tear_down, 0, NULL},
    {"_del/twice", test_del_twice, setup, tear_down, 0, NULL},
    {"_del/twice-middle", test_del_twice_middle, setup, tear_down, 0, NULL},
    {"_del/out-of-bound", test_del_out_of_bound, setup, tear_down, 0, NULL},
    {"_del/many", test_del_many, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

MunitSuite dqlite__registry_suites[] = {
    {"", dqlite__registry_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE},
    {NULL, NULL, NULL, 0, MUNIT_SUITE_OPTION_NONE},
};
