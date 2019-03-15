#ifndef TEST_REGISTRY_H
#define TEST_REGISTRY_H

#include "../../src/registry.h"

#include "options.h"

#define FIXTURE_REGISTRY struct registry registry
#define SETUP_REGISTRY SETUP_REGISTRY_X(f)
#define TEAR_DOWN_REGISTRY TEAR_DOWN_REGISTRY_X(f)

#define SETUP_REGISTRY_X(F) registry__init(&F->registry, &F->options)
#define TEAR_DOWN_REGISTRY_X(F) registry__close(&F->registry);

#endif /* TEST_REGISTRY_H */
