#ifndef TEST_THREAD_H
#define TEST_THREAD_H

#include <sys/un.h>

#define FIXTURE_THREAD pthread_t thread;

#define THREAD_START(T, FUNC, DATA)                       \
	{                                                 \
		int rv2;                                  \
		rv2 = pthread_create(&T, 0, &FUNC, DATA); \
		munit_assert_int(rv2, ==, 0);             \
	}

#define THREAD_JOIN(T)                          \
	{                                       \
		void *retval;                   \
		int rv2;                        \
		rv2 = pthread_join(T, &retval); \
		munit_assert_int(rv2, ==, 0);   \
		munit_assert_ptr_null(retval);  \
	}

#endif /* TEST_THREAD_H */
