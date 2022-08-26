#include <sys/socket.h>
#include <sys/un.h>

#include "../../../src/lib/addr.h"

#include "../../lib/runner.h"

TEST_MODULE(lib_addr);

struct fixture
{
	struct sockaddr_un addr_un;
};

static void *setup(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof(*f));
	(void)params;
	(void)user_data;
	return f;
}

static void tear_down(void *data)
{
	struct fixture *f = data;
	free(f);
}

#define ASSERT_PARSE(ADDR, STATUS, FAMILY)                              \
	socklen_t addr_len = sizeof(f->addr_un);                        \
	int rv;                                                         \
	rv = AddrParse(ADDR, (struct sockaddr *)&f->addr_un, &addr_len, \
		       "8080", DQLITE_ADDR_PARSE_UNIX);                 \
	munit_assert_int(rv, ==, STATUS);                               \
	munit_assert_int(f->addr_un.sun_family, ==, FAMILY)

TEST_SUITE(parse);
TEST_SETUP(parse, setup);
TEST_TEAR_DOWN(parse, tear_down);

TEST_CASE(parse, ipv4_no_port, NULL)
{
	struct fixture *f = data;
	(void)params;
	ASSERT_PARSE("1.2.3.4", 0, AF_INET);
	return MUNIT_OK;
}

TEST_CASE(parse, ipv4_with_port, NULL)
{
	struct fixture *f = data;
	(void)params;
	ASSERT_PARSE("127.0.0.1:9001", 0, AF_INET);
	return MUNIT_OK;
}

TEST_CASE(parse, ipv6_no_port, NULL)
{
	struct fixture *f = data;
	(void)params;
	ASSERT_PARSE("::1", 0, AF_INET6);
	return MUNIT_OK;
}

TEST_CASE(parse, ipv6_with_port, NULL)
{
	struct fixture *f = data;
	(void)params;
	ASSERT_PARSE("[2001:4860:4860::8888]:9001", 0, AF_INET6);
	return MUNIT_OK;
}

TEST_CASE(parse, unix, NULL)
{
	struct fixture *f = data;
	(void)params;
	ASSERT_PARSE("@xyz", 0, AF_UNIX);
	return MUNIT_OK;
}

TEST_CASE(parse, unix_auto, NULL)
{
	struct fixture *f = data;
	(void)params;
	ASSERT_PARSE("@", 0, AF_UNIX);
	return MUNIT_OK;
}
