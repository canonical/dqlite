/**
 * Define the assert() macro, either as the standard one or the test one.
 */

#ifndef DQLITE_ASSERT_H
#define DQLITE_ASSERT_H

#if defined(DQLITE_TEST)
  #include "../test/lib/munit.h"
  #define assert(expr) munit_assert(expr)
#else
  #include <assert.h>
#endif

#endif /* DQLITE_ASSERT_H */
