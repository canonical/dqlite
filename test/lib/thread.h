#ifndef TEST_THREAD_H
#define TEST_THREAD_H

#include <sys/un.h>

#define FIXTURE_THREAD pthread_t thread;

#define THREAD_START(FUNC, DATA)                                  \
	{                                                         \
		int rv2;                                          \
		rv2 = pthread_create(&f->thread, 0, &FUNC, DATA); \
		munit_assert_int(rv2, ==, 0);                     \
	}

#define THREAD_JOIN                                     \
	{                                               \
		void *retval;                           \
		int rv2;                                \
		rv2 = pthread_join(f->thread, &retval); \
		munit_assert_int(rv2, ==, 0);           \
		munit_assert_ptr_null(retval);          \
	}

#endif /* TEST_THREAD_H */
