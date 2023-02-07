/**
 * Step through a query progressively encoding a the row tuples.
 */

#ifndef QUERY_H_
#define QUERY_H_

#include <sqlite3.h>

#include "lib/serialize.h"
#include "lib/buffer.h"

/**
 * Step through the given query statement progressively encoding the yielded row
 * tuples, either until #SQLITE_DONE is returned or a full page of the given
 * buffer is filled.
 *
 * @stmt should have been stepped at most once before calling this function.
 * @prev_status should be 0 if this is not the first time calling query__batch on
 * this statement, or if this is the first time calling query__batch and the
 * given statement has not been stepped before. If this is the first time calling
 * query__batch and the statement has been successfully stepped once before,
 * it should be either #SQLITE_DONE or #SQLITE_ROW, reflecting the return code
 * of sqlite3_step.
 */
int query__batch(sqlite3_stmt *stmt, struct buffer *buffer, int prev_status);

#endif /* QUERY_H_*/
