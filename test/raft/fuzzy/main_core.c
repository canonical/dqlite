#include "../../lib/runner.h"

__attribute__((noreturn)) void dqlite_fail(const char *__assertion,
                                           const char *__file,
                                           unsigned int __line,
                                           const char *__function)
{
    /* The fuzzy runner does not use RUNNER(), so provide the assert hook. */
    (void)__assertion;
    (void)__file;
    (void)__line;
    (void)__function;
    abort();
}

MunitSuite _main_suites[64];
int _main_suites_n = 0;

/* Test runner executable */
int main(int argc, char *argv[MUNIT_ARRAY_PARAM(argc)])
{
    MunitSuite suite = {(char *)"", NULL, _main_suites, 1, 0};
    return munit_suite_main(&suite, (void *)"unit", argc, argv);
}
