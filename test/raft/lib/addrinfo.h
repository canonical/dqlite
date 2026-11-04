/* Support for getaddrinfo mocking for test purposes.
 *
 * The addrinfo.c test fixture includes definitions of getaddrinfo and
 * freeaddinfo that override the libc definitions, adding usage checks and the
 * ability to inject responses. These additional features are activated by
 * adding SET_UP_ADDRINFO/TEAR_DOWN_ADDRINFO to the fixture constructor and
 * destructor.
 *
 * The overriding definitions of getaddrinfo and freeaddrinfo affect all code
 * that's linked with addrinfo.c, and we rely on being able to retrieve the
 * original libc definitions using dlsym. When libc is statically linked, or
 * when libuv is a Darwin dylib whose internal getaddrinfo calls are not
 * interposed by this test binary, this is not possible. In those cases, skip
 * tests that rely on getaddrinfo result injection.
 */

#ifndef TEST_ADDRINFO_H
#define TEST_ADDRINFO_H

#include "test/lib/munit.h"

#if defined(DQLITE_STATIC_LIBC) || defined(_WIN32) || \
    (defined(__APPLE__) && defined(__MACH__))

/* Trickery to cause tests that use getaddrinfo result injection to be skipped
 * when building with WITH_STATIC_DEPS or on Windows. */
#define ADDRINFO_TEST(S, C, SETUP, TEAR_DOWN, OPTIONS, PARAMS) \
    TEST(S, C, SETUP, TEAR_DOWN, OPTIONS, PARAMS) \
    { \
        return MUNIT_SKIP; \
    } \
    static MUNIT_UNUSED MunitResult test_unused_##S##_##C( \
        MUNIT_UNUSED const MunitParameter params[], MUNIT_UNUSED void *data)

#else /* ifndef DQLITE_STATIC_LIBC && !_WIN32 && !Darwin */

#define ADDRINFO_TEST(S, C, SETUP, TEAR_DOWN, OPTIONS, PARAMS) \
    TEST(S, C, SETUP, TEAR_DOWN, OPTIONS, PARAMS)

#endif /* ifdef DQLITE_STATIC_LIBC || _WIN32 || Darwin ... else */

#define SET_UP_ADDRINFO AddrinfoInjectSetUp(params)
#define TEAR_DOWN_ADDRINFO AddrinfoInjectTearDown()

typedef struct AddrinfoResult
{
    const char *ip;
    const int port;
} AddrinfoResult_t;

void AddrinfoInjectSetResponse(int rv,
                               int num_results,
                               const struct AddrinfoResult *results);

void AddrinfoInjectSetUp(const MunitParameter params[]);
void AddrinfoInjectTearDown(void);

#endif  // #ifndef TEST_ADDRINFO_H
