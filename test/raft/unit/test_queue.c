#include "../../../src/lib/queue.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture with a single queue and a few test items that can be added to it.
 *
 *****************************************************************************/

struct item
{
    int value;
    queue queue;
};

struct fixture
{
    queue queue;
    struct item items[3];
};

static void *setUp(MUNIT_UNUSED const MunitParameter params[],
                   MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    queue_init(&f->queue);
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    free(f);
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/* Initialize and push the given number of fixture items to the fixture's
 * queue. Each item will have a value equal to its index plus one. */
#define PUSH(N)                                   \
    {                                             \
        int i_;                                   \
        for (i_ = 0; i_ < N; i_++) {              \
            struct item *item_ = &f->items[i_];   \
            item_->value = i_ + 1;                \
            queue_insert_tail(&f->queue, &item_->queue); \
        }                                         \
    }

/* Remove the i'th fixture item from the fixture queue. */
#define REMOVE(I) queue_remove(&f->items[I].queue)

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Assert that the item at the head of the fixture's queue has the given
 * value. */
#define ASSERT_HEAD(VALUE)                             \
    {                                                  \
        queue *head_ = queue_head(&f->queue);          \
        struct item *item_;                            \
        item_ = QUEUE_DATA(head_, struct item, queue); \
        munit_assert_int(item_->value, ==, VALUE);     \
    }

/* Assert that the item at the tail of the queue has the given value. */
#define ASSERT_TAIL(VALUE)                             \
    {                                                  \
        queue *tail_ = queue_tail(&f->queue);          \
        struct item *item_;                            \
        item_ = QUEUE_DATA(tail_, struct item, queue); \
        munit_assert_int(item_->value, ==, VALUE);     \
    }

/* Assert that the fixture's queue is empty. */
#define ASSERT_EMPTY munit_assert_true(queue_empty(&f->queue))

/* Assert that the fixture's queue is not empty. */
#define ASSERT_NOT_EMPTY munit_assert_false(queue_empty(&f->queue))

/******************************************************************************
 *
 * queue_empty
 *
 *****************************************************************************/

SUITE(queue_empty)

TEST(queue_empty, yes, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ASSERT_EMPTY;
    return MUNIT_OK;
}

TEST(queue_empty, no, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PUSH(1);
    ASSERT_NOT_EMPTY;
    return MUNIT_OK;
}

/******************************************************************************
 *
 * queue_insert_tail
 *
 *****************************************************************************/

SUITE(queue_insert_tail)

TEST(queue_insert_tail, one, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PUSH(1);
    ASSERT_HEAD(1);
    return MUNIT_OK;
}

TEST(queue_insert_tail, two, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    int i;
    PUSH(2);
    for (i = 0; i < 2; i++) {
        ASSERT_HEAD(i + 1);
        REMOVE(i);
    }
    ASSERT_EMPTY;
    return MUNIT_OK;
}

/******************************************************************************
 *
 * queue_remove
 *
 *****************************************************************************/

SUITE(queue_remove)

TEST(queue_remove, first, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PUSH(3);
    REMOVE(0);
    ASSERT_HEAD(2);
    return MUNIT_OK;
}

TEST(queue_remove, second, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PUSH(3);
    REMOVE(1);
    ASSERT_HEAD(1);
    return MUNIT_OK;
}

TEST(queue_remove, success, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PUSH(3);
    REMOVE(2);
    ASSERT_HEAD(1);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * queue_tail
 *
 *****************************************************************************/

SUITE(queue_tail)

TEST(queue_tail, one, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PUSH(1);
    ASSERT_TAIL(1);
    return MUNIT_OK;
}

TEST(queue_tail, two, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PUSH(2);
    ASSERT_TAIL(2);
    return MUNIT_OK;
}

TEST(queue_tail, three, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PUSH(3);
    ASSERT_TAIL(3);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * QUEUE_FOREACH
 *
 *****************************************************************************/

SUITE(QUEUE_FOREACH)

/* Loop through a queue of zero items. */
TEST(QUEUE_FOREACH, zero, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    queue *head;
    int count = 0;
    QUEUE_FOREACH (head, &f->queue) {
        count++;
    }
    munit_assert_int(count, ==, 0);
    return MUNIT_OK;
}

/* Loop through a queue of one item. */
TEST(QUEUE_FOREACH, one, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    queue *head;
    int count = 0;
    PUSH(1);
    QUEUE_FOREACH (head, &f->queue) {
        count++;
    }
    munit_assert_int(count, ==, 1);
    return MUNIT_OK;
}

/* Loop through a queue of two items. The order of the loop is from the head to
 * the tail. */
TEST(QUEUE_FOREACH, two, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    queue *head;
    int values[2] = {0, 0};
    int i = 0;
    PUSH(2);
    QUEUE_FOREACH (head, &f->queue) {
        struct item *item;
        item = QUEUE_DATA(head, struct item, queue);
        values[i] = item->value;
        i++;
    }
    munit_assert_int(values[0], ==, 1);
    munit_assert_int(values[1], ==, 2);
    return MUNIT_OK;
}
