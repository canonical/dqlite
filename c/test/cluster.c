#include <assert.h>

#include <sqlite3.h>

#include "dqlite.h"

const char *test__cluster_leader(void *ctx)
{
  return "127.0.0.1:666";
}

static dqlite_cluster test__cluster = {
  0,
  test__cluster_leader,
  0,
  0,
};

dqlite_cluster* test_cluster()
{
	return &test__cluster;
}
