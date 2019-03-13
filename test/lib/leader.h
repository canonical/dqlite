/**
 * Setup a test struct leader object.
 */

#ifndef TEST_LEADER_H
#define TEST_LEADER_H

#include "../../src/leader.h"

#define FIXTURE_LEADER struct leader leader

#define SETUP_LEADER leader__init(&f->leader, &f->db)
#define TEAR_DOWN_LEADER leader__close(&f->leader)

#endif /* TEST_DB_H */
