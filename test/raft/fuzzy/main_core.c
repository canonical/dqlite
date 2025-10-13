#include "../../lib/runner.h"

MunitSuite _main_suites[64];
int _main_suites_n = 0;

/* Test runner executable */
int main(int argc, char *argv[MUNIT_ARRAY_PARAM(argc)])
{
    MunitSuite suite = {(char *)"", NULL, _main_suites, 1, 0};
    return munit_suite_main(&suite, (void *)"unit", argc, argv);
}
