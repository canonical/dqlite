#ifndef DQLITE_LOGGER_H_
#define DQLITE_LOGGER_H_

#include "../../include/dqlite.h"

/* Emit a message with level DQLITE_LOG_DEBUG */
void debugf(struct dqlite_logger *logger, const char *format, ...);

/* Emit a message with level DQLITE_LOG_INFO  */
void infof(struct dqlite_logger *logger, const char *format, ...);

/* Emit a message with level DQLITE_LOG_WARN  */
void warnf(struct dqlite_logger *logger, const char *format, ...);

/* Emit a message with level #DQLITE_ERROR */
void errorf(struct dqlite_logger *logger, const char *format, ...);

#endif /* DQLITE_LOGGER_H_ */
