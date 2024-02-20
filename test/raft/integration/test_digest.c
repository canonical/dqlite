#include "../../../src/raft.h"
#include "../lib/runner.h"

SUITE(raft_digest)

/* Generation of the ID of the bootstrap dqlite node. */
TEST(raft_digest, bootstrapServerId, NULL, NULL, 0, NULL)
{
    const char *address = "127.0.0.1:65536";
    unsigned long long id;
    id = raft_digest(address, 0);
    munit_assert_int(id, ==, 138882483);
    return MUNIT_OK;
}
