#include <libco.h>

#include "../../lib/runner.h"

TEST_MODULE(ext_co);

/* Execution context of a test coroutine, passed using the global ctx
 * variable. */
struct ctx
{
	cothread_t main; /* Reference to the main coroutine */
	int v1;
	int v2;
};

static struct ctx *ctx; /* Argument for test coroutines */

/* Test coroutine entry point */
static void coro()
{
	struct ctx *c = ctx;
	c->v1 = 1;
	co_switch(c->main);
	c->v2 = 2;
	co_switch(c->main);
}

struct fixture
{
	cothread_t main;  /* Main coroutine */
	cothread_t coro1; /* First coroutine */
	cothread_t coro2; /* Second coroutine */
	struct ctx ctx1;  /* Context for first coroutine */
	struct ctx ctx2;  /* Context for second coroutine */
};

TEST_SUITE(switch);

TEST_SETUP(switch)
{
	struct fixture *f = munit_calloc(1, sizeof *f);
	(void)params;
	(void)user_data;
	f->main = co_active();
	f->coro1 = co_create(1024 * 1024, coro);
	f->coro2 = co_create(1024 * 1024, coro);
	f->ctx1.main = f->main;
	f->ctx2.main = f->main;
	return f;
}

TEST_TEAR_DOWN(switch)
{
	struct fixture *f = data;
	co_delete(f->coro1);
	co_delete(f->coro2);
	free(f);
}

/* Assert the v1 and v2 fields of a ctx object. */
#define assert_ctx(CTX, V1, V2)              \
	munit_assert_int((CTX)->v1, ==, V1); \
	munit_assert_int((CTX)->v2, ==, V2);

#define assert_ctx1(V1, V2) assert_ctx(&f->ctx1, V1, V2);
#define assert_ctx2(V1, V2) assert_ctx(&f->ctx2, V1, V2);

/* Switch execution from main to a coroutine, then back from the coroutine to
 * main, then resume the coroutine and finally back to main again. */
TEST_CASE(switch, resume, NULL)
{
	struct fixture *f = data;
	(void)params;

	/* Start executing coro1 */
	ctx = &f->ctx1;
	co_switch(f->coro1);

	/* The v1 field of the context has been initialized, but v2 has not. */
	assert_ctx1(1, 0);

	/* Resume execution of coro1 */
	co_switch(f->coro1);

	/* The v2 field has been initialized too. */
	assert_ctx1(1, 2);

	return MUNIT_OK;
};

/* Switch execution from main to a coroutine, then back from that coroutine to
 * main, then switch execution to a second coroutine, then back to main, then
 * back to the second coroutine, then back to main, then back to the first
 * coroutine and finally back to main again.. */
TEST_CASE(switch, concurrent, NULL)
{
	struct fixture *f = data;
	(void)params;

	/* Start executing coro1 */
	ctx = &f->ctx1;
	co_switch(f->coro1);

	/* The v1 field of the context has been initialized, but v2 has not. */
	assert_ctx1(1, 0);

	/* Start executing coro2 */
	ctx = &f->ctx2;
	co_switch(f->coro2);

	/* The v1 field of the second context has been initialized, but v2 has
	 * not. */
	assert_ctx2(1, 0);

	/* The fields of the first context are still the same. */
	assert_ctx1(1, 0);

	/* Resume execution of coro2 */
	co_switch(f->coro2);

	/* The v2 field of the second context has been initialized too, but the
	 * one of the first context still hasn't. */
	assert_ctx2(1, 2);
	assert_ctx1(1, 0);

	/* Resume execution of coro1 */
	co_switch(f->coro1);

	/* The v2 field of the first context has been initialized too now. */
	assert_ctx1(1, 2);

	return MUNIT_OK;
};
