/**
 * Options object for tests.
 */

#ifndef TEST_OPTIONS_H
#define TEST_OPTIONS_H

#include "../../src/options.h"

#define OPTIONS_FIXTURE struct options options;
#define OPTIONS_SETUP options__init(&f->options);
#define OPTIONS_TEAR_DOWN options__close(&f->options);

#endif /* TEST_OPTIONS_H */
