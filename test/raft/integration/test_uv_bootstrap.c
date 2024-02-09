#include "../lib/runner.h"
#include "../lib/uv.h"

/******************************************************************************
 *
 * Fixture with a libuv-based raft_io instance and an empty configuration.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV_DEPS;
    FIXTURE_UV;
    struct raft_configuration conf;
};

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/* Add a server to the fixture's configuration. */
#define CONFIGURATION_ADD(ID, ADDRESS)                                   \
    {                                                                    \
        int rv_;                                                         \
        rv_ = raft_configuration_add(&f->conf, ID, ADDRESS, RAFT_VOTER); \
        munit_assert_int(rv_, ==, 0);                                    \
    }

/* Invoke f->io->bootstrap() and assert that no error occurs. */
#define BOOTSTRAP                                \
    {                                            \
        int rv_;                                 \
        rv_ = f->io.bootstrap(&f->io, &f->conf); \
        munit_assert_int(rv_, ==, 0);            \
    }

/******************************************************************************
 *
 * Set up and tear down.
 *
 *****************************************************************************/

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV_DEPS;
    SETUP_UV;
    raft_configuration_init(&f->conf);
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    raft_configuration_close(&f->conf);
    TEAR_DOWN_UV;
    TEAR_DOWN_UV_DEPS;
    free(f);
}

/******************************************************************************
 *
 * raft_io->bootstrap()
 *
 *****************************************************************************/

SUITE(bootstrap)

/* Invoke f->io->bootstrap() and assert that it returns the given error code and
 * message. */
#define BOOTSTRAP_ERROR(RV, ERRMSG)                      \
    {                                                    \
        int rv_;                                         \
        rv_ = f->io.bootstrap(&f->io, &f->conf);         \
        munit_assert_int(rv_, ==, RV);                   \
        munit_assert_string_equal(f->io.errmsg, ERRMSG); \
    }

/* Bootstrap a pristine server. */
TEST(bootstrap, pristine, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CONFIGURATION_ADD(1, "1");
    BOOTSTRAP;
    return MUNIT_OK;
}

/* The data directory already has metadata files with a non-zero term. */
TEST(bootstrap, termIsNonZero, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CONFIGURATION_ADD(1, "1");
    BOOTSTRAP;
    BOOTSTRAP_ERROR(RAFT_CANTBOOTSTRAP, "metadata contains term 1");
    return MUNIT_OK;
}
