/* Convenience macros to reduce munit boiler plate. */

#ifndef TEST_RUNNER_H
#define TEST_RUNNER_H

#include <signal.h>

#include "munit.h"

#include "../../src/tracing.h"

/* Top-level suites array declaration.
 *
 * These top-level suites hold all module-level child suites and must be defined
 * and then set as child suites of a root suite created at runtime by the test
 * runner's main(). This can be done using the RUNNER macro. */
extern MunitSuite _main_suites[];
extern int _main_suites_n;

/* Maximum number of test cases for each suite */
#define SUITE__CAP 128
#define TEST__CAP SUITE__CAP

/* Define the top-level suites array and the main() function of the test. */
#define RUNNER(NAME)                                                       \
	MunitSuite _main_suites[SUITE__CAP];                               \
	int _main_suites_n = 0;                                            \
                                                                           \
	int main(int argc, char *argv[MUNIT_ARRAY_PARAM(argc)])            \
	{                                                                  \
		signal(SIGPIPE, SIG_IGN);                                  \
		dqliteTracingMaybeEnable(true);                            \
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
#define SUITE(S)          \
	SUITE__DECLARE(S) \
	SUITE__ADD_CHILD(main, #S, S)

/* Declare and register a new test. */
#define TEST(S, C, SETUP, TEAR_DOWN, OPTIONS, PARAMS)                    \
	static MunitResult test_##S##_##C(const MunitParameter params[], \
					  void *data);                   \
	TEST__ADD_TO_SUITE(S, C, SETUP, TEAR_DOWN, OPTIONS, PARAMS)      \
	static MunitResult test_##S##_##C(                               \
	    MUNIT_UNUSED const MunitParameter params[],                  \
	    MUNIT_UNUSED void *data)

#define SKIP_IF_NO_FIXTURE         \
	if (f == NULL) {           \
		return MUNIT_SKIP; \
	}

/* Declare the MunitSuite[] and the MunitTest[] arrays that compose the test
 * suite identified by S. */
#define SUITE__DECLARE(S)                                               \
	static MunitSuite _##S##_suites[SUITE__CAP];                    \
	static MunitTest _##S##_tests[SUITE__CAP];                      \
	static MunitTestSetup _##S##_setup = NULL;                      \
	static MunitTestTearDown _##S##_tear_down = NULL;               \
	static int _##S##_suites_n = 0;                                 \
	static int _##S##_tests_n = 0;                                  \
	__attribute__((constructor(101))) static void _##S##_init(void) \
	{                                                               \
		memset(_##S##_suites, 0, sizeof(_##S##_suites));        \
		memset(_##S##_tests, 0, sizeof(_##S##_tests));          \
		(void)_##S##_suites_n;                                  \
		(void)_##S##_tests_n;                                   \
		(void)_##S##_setup;                                     \
		(void)_##S##_tear_down;                                 \
	}

/* Set the tests and suites attributes of the next available slot of the
 * MunitSuite[] array of S1 to the MunitTest[] and MunitSuite[] arrays of S2,
 * using the given PREXIX. */
#define SUITE__ADD_CHILD(S1, PREFIX, S2)                                        \
	__attribute__((constructor(102))) static void _##S1##_##S2##_init(void) \
	{                                                                       \
		int n = _##S1##_suites_n;                                       \
		_##S1##_suites[n].prefix = PREFIX;                              \
		_##S1##_suites[n].tests = _##S2##_tests;                        \
		_##S1##_suites[n].suites = _##S2##_suites;                      \
		_##S1##_suites[n].iterations = 0;                               \
		_##S1##_suites[n].options = 0;                                  \
		_##S1##_suites_n = n + 1;                                       \
	}

/* Add a test case to the MunitTest[] array of suite S. */
#define TEST__ADD_TO_SUITE(S, C, SETUP, TEAR_DOWN, OPTIONS, PARAMS)                 \
	__attribute__((constructor(103))) static void _##S##_tests_##C##_init(void) \
	{                                                                           \
		MunitTest *tests = _##S##_tests;                                    \
		int n = _##S##_tests_n;                                             \
		TEST__SET_IN_ARRAY(tests, n, "/" #C, test_##S##_##C, SETUP,         \
				   TEAR_DOWN, OPTIONS, PARAMS);                     \
		_##S##_tests_n = n + 1;                                             \
	}

/* Set the values of the I'th test case slot in the given test array */
#define TEST__SET_IN_ARRAY(TESTS, I, NAME, FUNC, SETUP, TEAR_DOWN, OPTIONS, \
			   PARAMS)                                          \
	TESTS[I].name = NAME;                                               \
	TESTS[I].test = FUNC;                                               \
	TESTS[I].setup = SETUP;                                             \
	TESTS[I].tear_down = TEAR_DOWN;                                     \
	TESTS[I].options = OPTIONS;                                         \
	TESTS[I].parameters = PARAMS

/**
 * Declare and register a new test module #M.
 *
 * A test module is a test suite (i.e. a pair of MunitTest[] and MunitSuite[]
 * arrays), directly or indirectly containing all test cases in a test file.
 *
 * This macro uses hard-coded names to declare the module's tests and suites
 * arrays static, so they can be easly referenced in other static declarations
 * generated by the macros below:
 *
 * static MunitTest _module_tests[TEST__CAP];
 * static MunitSuite _module_suites[TEST__CAP];
 *
 * The tests and suites attributes of the next available MunitSuite slot in the
 * top-level suites array will be set to the module's tests and suites arrays,
 * and the prefix attribute of the slot will be set to #M.
 *
 * Each test file should declare one and only one test module.
 */
#define TEST_MODULE(M)               \
	TEST_SUITE__DECLARE(module); \
	TEST_SUITE__ADD_CHILD(main, #M, module);

/**
 * Declare and register a new test suite #S belonging to the file's test module.
 *
 * A test suite is a pair of static variables:
 *
 * static MunitTest _##S##_suites[TEST__CAP]
 * static MunitTest _##S##_tests[TEST__CAP]
 *
 * The tests and suites attributes of the next available MunitSuite slot in the
 * #_module_suites array will be set to the suite's tests and suites arrays, and
 * the prefix attribute of the slot will be set to /S.
 *
 * All tests in the suite will use the same setup and tear down functions.
 */
#define TEST_SUITE(S)           \
	TEST_SUITE__DECLARE(S); \
	TEST_SUITE__ADD_CHILD(module, "/" #S, S);

/**
 * Declare a setup function.
 *
 * Possible signatures are:
 *
 * - TEST_SETUP(S): Declare the setup function for suite S inline.
 * - TEST_SETUP(S, F): Set the setup function for suite S to F.
 */
#define TEST_SETUP(...) TEST_SETUP__MACRO_CHOOSER(__VA_ARGS__)(__VA_ARGS__)
#define TEST_SETUP_(S)                                          \
	static void *S##_setup(const MunitParameter[], void *); \
	_##S##_setup = S##_setup;                               \
	static void *S##_setup(const MunitParameter params[], void *user_data)

/**
 * Declare a tear down function.
 *
 * Possible signatures are:
 *
 * - TEST_SETUP(S): Declare the tear down function for suite S inline.
 * - TEST_SETUP(S, F): Set the tear down function for suite S to F.
 */
#define TEST_TEAR_DOWN(...) \
	TEST_TEAR_DOWN__MACRO_CHOOSER(__VA_ARGS__)(__VA_ARGS__)

/**
 * Declare and register a new group of tests #G, belonging to suite #S in the
 * file's test module.
 */
#define TEST_GROUP(C, T)                                \
	static MunitTest _##C##_##T##_tests[TEST__CAP]; \
	static int _##C##_##T##_tests_n = 0;            \
	TEST_SUITE__ADD_GROUP(C, T);

/**
 * Declare and register a new test case.
 *
 * Possible signatures are:
 *
 * - TEST_CASE(C): C gets added to the tests array of the file module.
 * - TEST_CASE(S, C): C gets added to the tests array of suite S.
 * - TEST_CASE(S, G, C): C gets added to the tests array of group G in suite S.
 *
 * The test body declaration must follow the macro.
 */
#define TEST_CASE(...) TEST_CASE__MACRO_CHOOSER(__VA_ARGS__)(__VA_ARGS__)

/* Declare the MunitSuite[] and the MunitTest[] arrays that compose the test
 * suite identified by S. */
#define TEST_SUITE__DECLARE(S)                                     \
	static MunitSuite _##S##_suites[TEST__CAP];                \
	static MunitTest _##S##_tests[TEST__CAP];                  \
	static MunitTestSetup _##S##_setup = NULL;                 \
	static MunitTestTearDown _##S##_tear_down = NULL;          \
	static int _##S##_suites_n = 0;                            \
	static int _##S##_tests_n = 0;                             \
	__attribute__((constructor)) static void _##S##_init(void) \
	{                                                          \
		memset(_##S##_suites, 0, sizeof(_##S##_suites));   \
		memset(_##S##_tests, 0, sizeof(_##S##_tests));     \
		(void)_##S##_suites_n;                             \
		(void)_##S##_tests_n;                              \
		(void)_##S##_setup;                                \
		(void)_##S##_tear_down;                            \
	}

/* Set the tests and suites attributes of the next available slot of the
 * MunitSuite[] array of S1 to the MunitTest[] and MunitSuite[] arrays of S2,
 * using the given PREXIX. */
#define TEST_SUITE__ADD_CHILD(S1, PREFIX, S2)                              \
	__attribute__((constructor)) static void _##S1##_##S2##_init(void) \
	{                                                                  \
		int n = _##S1##_suites_n;                                  \
		_##S1##_suites[n].prefix = PREFIX;                         \
		_##S1##_suites[n].tests = _##S2##_tests;                   \
		_##S1##_suites[n].suites = _##S2##_suites;                 \
		_##S1##_suites[n].iterations = 0;                          \
		_##S1##_suites[n].options = 0;                             \
		_##S1##_suites_n = n + 1;                                  \
	}

/* Set the tests attribute of the next available slot of the MunitSuite[] array
 * of S to the MunitTest[] array of G, using /G as prefix. */
#define TEST_SUITE__ADD_GROUP(S, G)                                      \
	__attribute__((constructor)) static void _##S##_##G##_init(void) \
	{                                                                \
		int n = _##S##_suites_n;                                 \
		_##S##_suites[n].prefix = "/" #G;                        \
		_##S##_suites[n].tests = _##S##_##G##_tests;             \
		_##S##_suites[n].suites = NULL;                          \
		_##S##_suites[n].iterations = 0;                         \
		_##S##_suites[n].options = 0;                            \
		_##S##_suites_n = n + 1;                                 \
	}

/* Choose the appropriate TEST_SETUP__N_ARGS() macro depending on the number of
 * arguments passed to TEST_SETUP(). */
#define TEST_SETUP__MACRO_CHOOSER(...) \
	TEST__GET_3RD_ARG(__VA_ARGS__, TEST_SETUP__2_ARGS, TEST_SETUP__1_ARGS)

#define TEST_SETUP__1_ARGS(S)                                            \
	static void *S##__setup(const MunitParameter[], void *);         \
	__attribute__((constructor)) static void _##S##_setup_init(void) \
	{                                                                \
		_##S##_setup = S##__setup;                               \
	}                                                                \
	static void *S##__setup(const MunitParameter params[], void *user_data)

#define TEST_SETUP__2_ARGS(S, F)                                         \
	__attribute__((constructor)) static void _##S##_setup_init(void) \
	{                                                                \
		_##S##_setup = F;                                        \
	}

/* Choose the appropriate TEST_TEAR_DOWN__N_ARGS() macro depending on the number
 * of arguments passed to TEST_TEAR_DOWN(). */
#define TEST_TEAR_DOWN__MACRO_CHOOSER(...)                     \
	TEST__GET_3RD_ARG(__VA_ARGS__, TEST_TEAR_DOWN__2_ARGS, \
			  TEST_TEAR_DOWN__1_ARGS)

#define TEST_TEAR_DOWN__1_ARGS(S)                                             \
	static void S##__tear_down(void *data);                               \
	__attribute__((constructor)) static void _##S##__tear_down_init(void) \
	{                                                                     \
		_##S##_tear_down = S##__tear_down;                            \
	}                                                                     \
	static void S##__tear_down(void *data)

#define TEST_TEAR_DOWN__2_ARGS(S, F)                                         \
	__attribute__((constructor)) static void _##S##_tear_down_init(void) \
	{                                                                    \
		_##S##_tear_down = F;                                        \
	}

/* Choose the appropriate TEST_CASE__N_ARGS() macro depending on the number of
 * arguments passed to TEST_CASE(). */
#define TEST_CASE__MACRO_CHOOSER(...)                                        \
	TEST__GET_5TH_ARG(__VA_ARGS__, TEST_CASE__4_ARGS, TEST_CASE__3_ARGS, \
			  TEST_CASE__2_ARGS)

/* Add the test case to the module's MunitTest[] array. */
#define TEST_CASE__2_ARGS(C, PARAMS)                                 \
	static MunitResult test_##C(const MunitParameter[], void *); \
	TEST_CASE__ADD_TO_MODULE(C, PARAMS);                         \
	static MunitResult test_##C(const MunitParameter params[], void *data)

/* Add test case C to the MunitTest[] array of suite S. */
#define TEST_CASE__3_ARGS(S, C, PARAMS)                                    \
	static MunitResult test_##S##_##C(const MunitParameter[], void *); \
	TEST_CASE__ADD_TO_SUITE(S, C, PARAMS);                             \
	static MunitResult test_##S##_##C(const MunitParameter params[],   \
					  void *data)

/* Add test case C to the MunitTest[] array of group G of suite S. */
#define TEST_CASE__4_ARGS(S, G, C, PARAMS)                                     \
	static MunitResult test_##S##_##G##_##C(const MunitParameter[],        \
						void *);                       \
	TEST_CASE__ADD_TO_GROUP(S, G, C, PARAMS);                              \
	static MunitResult test_##S##_##G##_##C(const MunitParameter params[], \
						void *data)

/* Add a test case to the MunitTest[] array of the file module. */
#define TEST_CASE__ADD_TO_MODULE(C, PARAMS)                                \
	__attribute__((constructor)) static void _module_tests_##C##_init( \
	    void)                                                          \
	{                                                                  \
		MunitTest *tests = _module_tests;                          \
		int n = _module_tests_n;                                   \
		TEST_CASE__SET_IN_ARRAY(tests, n, "/" #C, test_##C, NULL,  \
					NULL, PARAMS);                     \
		_module_tests_n = n + 1;                                   \
	}

/* Add a test case to the MunitTest[] array of suite S. */
#define TEST_CASE__ADD_TO_SUITE(S, C, PARAMS)                                  \
	__attribute__((constructor)) static void _##S##_tests_##C##_init(void) \
	{                                                                      \
		MunitTest *tests = _##S##_tests;                               \
		int n = _##S##_tests_n;                                        \
		TEST_CASE__SET_IN_ARRAY(tests, n, "/" #C, test_##S##_##C,      \
					_##S##_setup, _##S##_tear_down,        \
					PARAMS);                               \
		_##S##_tests_n = n + 1;                                        \
	}

/* Add a test case to MunitTest[] array of group G in suite S. */
#define TEST_CASE__ADD_TO_GROUP(S, G, C, PARAMS)                            \
	__attribute__(                                                      \
	    (constructor)) static void _##S##_##G##_tests_##C##_init(void)  \
	{                                                                   \
		MunitTest *tests = _##S##_##G##_tests;                      \
		int n = _##S##_##G##_tests_n;                               \
		TEST_CASE__SET_IN_ARRAY(tests, n, "/" #C,                   \
					test_##S##_##G##_##C, _##S##_setup, \
					_##S##_tear_down, PARAMS);          \
		_##S##_##G##_tests_n = n + 1;                               \
	}

/* Set the values of the I'th test case slot in the given test array */
#define TEST_CASE__SET_IN_ARRAY(TESTS, I, NAME, FUNC, SETUP, TEAR_DOWN, \
				PARAMS)                                 \
	TESTS[I].name = NAME;                                           \
	TESTS[I].test = FUNC;                                           \
	TESTS[I].setup = SETUP;                                         \
	TESTS[I].tear_down = TEAR_DOWN;                                 \
	TESTS[I].options = 0;                                           \
	TESTS[I].parameters = PARAMS

#define TEST__GET_3RD_ARG(arg1, arg2, arg3, ...) arg3
#define TEST__GET_5TH_ARG(arg1, arg2, arg3, arg4, arg5, ...) arg5

#endif /* TEST_RUNNER_H */
