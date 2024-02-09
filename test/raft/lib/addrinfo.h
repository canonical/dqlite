/* Support for getaddrinfo injection for test purpose
 *
 * Provide a local bound version to capture teh getaddrinfo/freeaddrinfo
 * incovation The helper may operate in three different modes: a) Transparent
 * forward calls to system getaddrinfo/freeaddrinfo function, if the
 * SET_UP_ADDRINFO/TEAR_DOWN_ADDRINFO is not added to the test test case setup
 * teardown. b) Check, if all results requested by getaddrinfo are freed using
 * freeaddrinfo. Activated by adding the SET_UP_ADDRINFO/SET_UP_ADDRINFO macros
 * to the test fixture. c) Inject artifical responses into the the getaddrinfo
 * requests for test purpose additionally to b) by using
 * AddrinfoInjectSetResponse before triggering the getaddrinfo calls.
 */

#ifndef TEST_ADDRINFO_H
#define TEST_ADDRINFO_H

#include "munit.h"

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
