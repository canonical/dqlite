/**
 * Bind statement parameters decoding them from a client request payload.
 */

#ifndef BIND_H_
#define BIND_H_

#include "./lib/serialize.h"

/* Bind the parameters of the underlying statement by decoding the given
 * message. */
int bind__params(sqlite3_stmt *stmt, struct cursor *cursor);

#endif /* BIND_H_*/
