/**
 * Setup a test struct db object.
 */

#ifndef TEST_DB_H
#define TEST_DB_H

#include "../../src/db.h"

#define FIXTURE_DB struct db db

#define SETUP_DB db__init(&f->db, &f->options, "test.db")
#define TEAR_DOWN_DB db__close(&f->db)

#endif /* TEST_DB_H */
