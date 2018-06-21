#include <assert.h>

#include <sqlite3.h>

#include "dqlite.h"

const char *test__cluster_leader(void *ctx)
{
  return "127.0.0.1:666";
}

const int test__cluster_servers(void *ctx, const char ***out)
{
	static const char *addresses[] = {
		"1.2.3.4:666",
		"5.6.7.8:666",
		NULL,
	};

	*out = addresses;

	return 0;
}

static dqlite_cluster test__cluster = {
  0,
  test__cluster_leader,
  test__cluster_servers,
  0,
};

dqlite_cluster* test_cluster()
{
	return &test__cluster;
}
