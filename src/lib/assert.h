/**
 * Define the assert() macro, either as the standard one or the test one.
 */

#ifndef LIB_ASSERT_H_
#define LIB_ASSERT_H_

#if defined(DQLITE_TEST)
  #include "../../test/lib/munit.h"
  #define assert(expr) munit_assert(expr)
#else
  #include <assert.h>
#endif

#endif /* LIB_ASSERT_H_ */
