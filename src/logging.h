#ifndef LOGGING_H_
#define LOGGING_H_

#include "../../include/dqlite.h"

/**
 * Emit a log message with a certain level.
 */
#define debugf(L, FORMAT, ...) \
	L->emit(L->data, DQLITE_DEBUG, FORMAT, ##__VA_ARGS__);

#endif /* LOGGING_H_ */
