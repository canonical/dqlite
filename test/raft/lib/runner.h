/* Convenience macros to reduce munit boiler plate. */

#ifndef TEST_RUNNER_H_
#define TEST_RUNNER_H_

#include "munit.h"

/* Top-level suites array declaration.
 *
 * These top-level suites hold all module-level child suites and must be defined
 * and then set as child suites of a root suite created at runtime by the test
 * runner's main(). This can be done using the TEST_RUNNER macro. */
extern MunitSuite _main_suites[];
extern int _main_suites_n;

/* Maximum number of test cases for each suite */
#define SUITE__CAP 128

/* Define the top-level suites array and the main() function of the test. */
#define RUNNER(NAME)                                               \
    MunitSuite _main_suites[SUITE__CAP];                           \
    int _main_suites_n = 0;                                        \
                                                                   \
    int main(int argc, char *argv[MUNIT_ARRAY_PARAM(argc)])        \
    {                                                              \
        MunitSuite suite = {(char *)"", NULL, _main_suites, 1, 0}; \
        return munit_suite_main(&suite, (void *)NAME, argc, argv); \
    }

/* Declare and register a new test suite #S belonging to the file's test module.
 *
 * A test suite is a pair of static variables:
 *
 * static MunitTest _##S##_suites[SUITE__CAP]
 * static MunitTest _##S##_tests[SUITE__CAP]
 *
 * The tests and suites attributes of the next available MunitSuite slot in the
 * _module_suites array will be set to the suite's tests and suites arrays, and
 * the prefix attribute of the slot will be set to /S. */
#define SUITE(S)      \
    SUITE__DECLARE(S) \
    SUITE__ADD_CHILD(main, #S, S)

/* Declare and register a new test. */
#define TEST(S, C, SETUP, TEAR_DOWN, OPTIONS, PARAMS)                \
    static MunitResult test_##S##_##C(const MunitParameter params[], \
                                      void *data);                   \
    TEST__ADD_TO_SUITE(S, C, SETUP, TEAR_DOWN, OPTIONS, PARAMS)      \
    static MunitResult test_##S##_##C(                               \
        MUNIT_UNUSED const MunitParameter params[], MUNIT_UNUSED void *data)

#define SKIP_IF_NO_FIXTURE \
    if (f == NULL) {       \
        return MUNIT_SKIP; \
    }

/* Declare the MunitSuite[] and the MunitTest[] arrays that compose the test
 * suite identified by S. */
#define SUITE__DECLARE(S)                                           \
    static MunitSuite _##S##_suites[SUITE__CAP];                    \
    static MunitTest _##S##_tests[SUITE__CAP];                      \
    static MunitTestSetup _##S##_setup = NULL;                      \
    static MunitTestTearDown _##S##_tear_down = NULL;               \
    static int _##S##_suites_n = 0;                                 \
    static int _##S##_tests_n = 0;                                  \
    __attribute__((constructor(101))) static void _##S##_init(void) \
    {                                                               \
        memset(_##S##_suites, 0, sizeof(_##S##_suites));            \
        memset(_##S##_tests, 0, sizeof(_##S##_tests));              \
        (void)_##S##_suites_n;                                      \
        (void)_##S##_tests_n;                                       \
        (void)_##S##_setup;                                         \
        (void)_##S##_tear_down;                                     \
    }

/* Set the tests and suites attributes of the next available slot of the
 * MunitSuite[] array of S1 to the MunitTest[] and MunitSuite[] arrays of S2,
 * using the given PREFIX. */
#define SUITE__ADD_CHILD(S1, PREFIX, S2)                                    \
    __attribute__((constructor(102))) static void _##S1##_##S2##_init(void) \
    {                                                                       \
        int n = _##S1##_suites_n;                                           \
        _##S1##_suites[n].prefix = PREFIX;                                  \
        _##S1##_suites[n].tests = _##S2##_tests;                            \
        _##S1##_suites[n].suites = _##S2##_suites;                          \
        _##S1##_suites[n].iterations = 0;                                   \
        _##S1##_suites[n].options = 0;                                      \
        _##S1##_suites_n = n + 1;                                           \
    }

/* Add a test case to the MunitTest[] array of suite S. */
#define TEST__ADD_TO_SUITE(S, C, SETUP, TEAR_DOWN, OPTIONS, PARAMS)            \
    __attribute__((constructor(103))) static void _##S##_tests_##C##_init(     \
        void)                                                                  \
    {                                                                          \
        MunitTest *tests = _##S##_tests;                                       \
        int n = _##S##_tests_n;                                                \
        TEST__SET_IN_ARRAY(tests, n, "/" #C, test_##S##_##C, SETUP, TEAR_DOWN, \
                           OPTIONS, PARAMS);                                   \
        _##S##_tests_n = n + 1;                                                \
    }

/* Set the values of the I'th test case slot in the given test array */
#define TEST__SET_IN_ARRAY(TESTS, I, NAME, FUNC, SETUP, TEAR_DOWN, OPTIONS, \
                           PARAMS)                                          \
    TESTS[I].name = NAME;                                                   \
    TESTS[I].test = FUNC;                                                   \
    TESTS[I].setup = SETUP;                                                 \
    TESTS[I].tear_down = TEAR_DOWN;                                         \
    TESTS[I].options = OPTIONS;                                             \
    TESTS[I].parameters = PARAMS

#endif /* TEST_RUNNER_H_ */
