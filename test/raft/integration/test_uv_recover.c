#include "../lib/runner.h"
#include "../lib/uv.h"

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV_DEPS;
    FIXTURE_UV;
};

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV_DEPS;
    SETUP_UV;
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_UV;
    TEAR_DOWN_UV_DEPS;
    free(f);
}

/******************************************************************************
 *
 * raft_io->recover()
 *
 *****************************************************************************/

SUITE(recover)

/* Invoke recover and assert that it fails with the given error. */
#define RECOVER_ERROR(RV, CONF)            \
    {                                      \
        int rv_;                           \
        rv_ = f->io.recover(&f->io, CONF); \
        munit_assert_int(rv_, ==, RV);     \
    }

/* Invoke recover and assert that it succeeds */
#define RECOVER(CONF) RECOVER_ERROR(0, CONF)

/* If the instance has been already initialized, an error is returned. */
/* A new configuration is saved as last entry on disk. */
TEST(recover, newConfiguration, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration1;
    struct raft_configuration configuration2;
    int rv;

    /* Bootstrap using an initial configuration */
    raft_configuration_init(&configuration1);
    rv = raft_configuration_add(&configuration1, 1, "1", RAFT_VOTER);
    munit_assert_int(rv, ==, 0);
    rv = raft_configuration_add(&configuration1, 2, "2", RAFT_VOTER);
    munit_assert_int(rv, ==, 0);
    rv = f->io.bootstrap(&f->io, &configuration1);
    munit_assert_int(rv, ==, 0);

    /* Bootstrap using a different configuration */
    raft_configuration_init(&configuration2);
    rv = raft_configuration_add(&configuration2, 1, "1", RAFT_VOTER);
    munit_assert_int(rv, ==, 0);

    RECOVER(&configuration2);

    raft_configuration_close(&configuration1);
    raft_configuration_close(&configuration2);

    return 0;
}
