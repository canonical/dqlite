/**
 * Bind statement parameters decoding them from a client request payload.
 */

#ifndef BIND_H_
#define BIND_H_

#include <sqlite3.h>

#include "lib/serialize.h"
#include "tuple.h"

/**
 * Bind the parameters of the given statement by decoding the given payload.
 * 
 * If the number of parameters in the statement is higher that the number of
 * parameters in the payload, the extra parameters are not bound and the
 * function returns success.
 */
int bind__params(sqlite3_stmt *stmt, struct tuple_decoder *decoder);

#endif /* BIND_H_*/
