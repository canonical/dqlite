/**
 * Bind statement parameters decoding them from a client request payload.
 */

#ifndef BIND_H_
#define BIND_H_

#include <sqlite3.h>

#include "lib/serialize.h"

struct value;

int parseParams(struct cursor *cursor, int format, struct value **out);

int bindParams(sqlite3_stmt *stmt, const struct value *params);

void freeParams(struct value *params);

#endif /* BIND_H_*/
