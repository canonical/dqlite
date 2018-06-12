#include <assert.h>

#include <CUnit/CUnit.h>

#include "../src/registry.h"

struct test_item {
	int *ptr;
};

void test_item_init(struct test_item *i)
{
	assert(i != NULL);

	i->ptr = (int*)sqlite3_malloc(sizeof(*(i->ptr)));
	*i->ptr = 123;
}

void test_item_close(struct test_item *i)
{
	assert(i != NULL);
	assert(i->ptr != NULL);

	sqlite3_free(i->ptr);
}

DQLITE__REGISTRY(test_registry, test_item);
DQLITE__REGISTRY_METHODS(test_registry, test_item);

static struct test_registry registry;

void test_dqlite__registry_setup()
{
	test_registry_init(&registry);
}

void test_dqlite__registry_teardown()
{
	test_registry_close(&registry);
}

void test_dqlite__registry_add()
{
	int err;
	size_t i;
	struct test_item *item;

	err = test_registry_add(&registry, &item, &i);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(i, 0);
	CU_ASSERT_PTR_NOT_NULL(item);
	CU_ASSERT_PTR_NOT_NULL(item->ptr);
	CU_ASSERT_EQUAL(123, *item->ptr);
}

void test_dqlite__registry_get()
{
	int err;
	size_t i;
	struct test_item *item;

	err = test_registry_add(&registry, &item, &i);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_EQUAL(test_registry_get(&registry, i), item);
}

void test_dqlite__registry_get_deleted()
{
	int err;
	size_t i;
	struct test_item *item;

	err = test_registry_add(&registry, &item, &i);
	CU_ASSERT_EQUAL(err, 0);

	err = test_registry_del(&registry, i);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NULL(test_registry_get(&registry, i));
}

void test_dqlite__registry_get_out_of_bound()
{
	struct test_item *item = test_registry_get(&registry, 123);

	CU_ASSERT_PTR_NULL(item);
}

void test_dqlite__registry_del()
{
	int err;
	size_t i;
	struct test_item *item;

	err = test_registry_add(&registry, &item, &i);
	CU_ASSERT_EQUAL(err, 0);

	err = test_registry_del(&registry, i);
	CU_ASSERT_EQUAL(err, 0);
}

void test_dqlite__registry_del_twice()
{
	int err;
	size_t i;
	struct test_item *item;

	err = test_registry_add(&registry, &item, &i);
	CU_ASSERT_EQUAL(err, 0);

	err = test_registry_del(&registry, i);
	CU_ASSERT_EQUAL(err, 0);

	err = test_registry_del(&registry, i);
	CU_ASSERT_EQUAL(err, DQLITE_NOTFOUND);
}

void test_dqlite__registry_del_out_of_bound()
{
	int err = test_registry_del(&registry, 123);

	CU_ASSERT_EQUAL(err, DQLITE_NOTFOUND);
}

void test_dqlite__registry_del_many()
{
	int err;
	size_t i;
	struct test_item *item;

	err = test_registry_add(&registry, &item, &i);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(i, 0);

	err = test_registry_add(&registry, &item, &i);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(i, 1);

	err = test_registry_add(&registry, &item, &i);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(i, 2);

	CU_ASSERT_EQUAL(3, registry.len);
	CU_ASSERT_EQUAL(4, registry.cap);

	err = test_registry_del(&registry, 2);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(2, registry.len);
	CU_ASSERT_EQUAL(4, registry.cap);

	err = test_registry_del(&registry, 1);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(1, registry.len);
	CU_ASSERT_EQUAL(2, registry.cap);
}
